from datetime import datetime, timedelta, date, timezone
from typing import List
from sqlalchemy import Integer
from sqlalchemy.orm import (
    joinedload,
    sessionmaker,
    contains_eager,
    aliased,
    scoped_session,
)
from sqlalchemy.sql import column, func, select, text, values
from uuid import uuid4

from model import (
    Work,
    Edition,
    Link,
    Item,
    Record,
    Collection,
    User,
    AutomaticCollection,
)
from managers.db import DBManager
from utils.utils import read_env

from .utils import APIUtils


# Initialize ORM Session
# global session object is scoped/closed at end of request via handler in api/app.py
# based on best practice described here:
# - https://docs.sqlalchemy.org/en/20/orm/contextual.html#using-thread-local-scope-with-web-applications
# - https://flask.palletsprojects.com/en/stable/patterns/sqlalchemy/
# - https://docs.sqlalchemy.org/en/20/orm/session_basics.html#using-a-sessionmaker
_engine = None
_Session = None


def get_session():
    """Retrieve singleton scoped session factory.

    Defers database connection until first access, allowing this module to be
    imported as a library without requiring environment variables to be set.

    Returns:
        scoped_session: A thread-local session factory that can be called to get a session.
    """
    global _engine, _Session
    if _Session is None:
        _engine = DBManager(host=read_env("POSTGRES_READ_HOST")).generate_engine()
        _Session = scoped_session(sessionmaker(bind=_engine))
        # Consider autocommit=False, autoflush=False see: https://docs.sqlalchemy.org/en/20/orm/session_basics.html#flushing
    return _Session


def get_frbr_data_by_edition(edition_ids: List):
    """
    Return list of (Work, Edition) Rows for each of the passed edition_ids
    present in the DB.
    Edition includes the following eager loaded relationships: Edition.items,
    Edition.items.links, Edition.items.rights, Edition.links.
    """
    # ALT: also sort the results by edition_ids order (using CTE as in fetchEditions() below)

    edition_link_alias = aliased(Link)
    item_link_alias = aliased(Link)

    Session = get_session()
    with Session() as session:
        rows = (
            session.query(Work, Edition)
            .join(Work.editions)  # Work → Edition
            # Edition.links (draws from aliased Link via edition_links crosswalk)
            .outerjoin(Edition.links.of_type(edition_link_alias))
            .outerjoin(Edition.items)  # Edition → Item
            # Item.links (draws from aliased Link via item_links crosswalk)
            .outerjoin(Item.links.of_type(item_link_alias))
            .outerjoin(Item.rights)
            .filter(Edition.id.in_(edition_ids))
            .options(
                # Eager-load Edition.items.links (Item.links)
                contains_eager(Edition.items).contains_eager(
                    Item.links.of_type(item_link_alias)
                ),
                # Eager-load Edition.items.rights (Item.rights)
                contains_eager(Edition.items).contains_eager(Item.rights),
                # Eager-load Edition.links (Edition.links)
                contains_eager(Edition.links.of_type(edition_link_alias)),
            )
            .all()
        )
    return rows


class DBClient:
    def __init__(self, engine):
        self.engine = engine

    def createSession(self):
        self.session = sessionmaker(bind=self.engine)()

    def closeSession(self):
        self.session.close()

    def __enter__(self):
        self.createSession()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.closeSession()

    def fetchSearchedWorks(self, ids):
        uuids = [i[0] for i in ids]
        editionIds = list(set(APIUtils.flatten([i[1] for i in ids])))

        return (
            self.session.query(Work)
            .join(Edition)
            .options(
                joinedload(Work.editions, Edition.links),
                joinedload(Work.editions, Edition.items),
                joinedload(Work.editions, Edition.items, Item.links, innerjoin=True),
                joinedload(Work.editions, Edition.items, Item.rights),
            )
            .filter(Work.uuid.in_(uuids), Edition.id.in_(editionIds))
            .all()
        )

    def fetchSingleWork(self, uuid):
        return (
            self.session.query(Work)
            .options(
                joinedload(Work.editions),
                joinedload(Work.editions, Edition.rights),
                joinedload(Work.editions, Edition.items),
                joinedload(Work.editions, Edition.items, Item.links, innerjoin=True),
            )
            .filter(Work.uuid == uuid)
            .first()
        )

    def fetchSingleEdition(self, editionID, showAll=False):
        return (
            self.session.query(Edition)
            .options(
                joinedload(Edition.links),
                joinedload(Edition.items),
                joinedload(Edition.items, Item.links),
                joinedload(Edition.items, Item.rights),
            )
            .filter(Edition.id == editionID)
            .first()
        )

    def fetchAllPreferredEditions(
        self,
        sortField: str,
        sortDirection: str,
        page: int,
        perPage: int,
    ):
        """Fetch up to `limit` editions, sorting by the given sort fields and
        respecting the given page fields (using basic limit / offset paging

        Note, we only accept sorting by title, date or uuid for now. We may want
        to consider opening that up.
        """

        # For perf reasons, filter to the last 100 days.  We might be able to tune the
        # query to improve this...
        startDate = datetime.now(timezone.utc).replace(tzinfo=None).date() - timedelta(
            days=100
        )
        # Sort all the `Work`s and rank their editions by oldest
        workQuery = (
            self.session.query(
                Work,
                Edition.id.label("edition_id"),
                func.rank()
                .over(
                    order_by=(Edition.date_created.asc(), Edition.id.asc()),
                    partition_by=Work.id,
                )
                .label("rnk"),
            )
            .join(Edition)
            .where((Edition.date_created > startDate) & (Work.date_created > startDate))
        ).subquery()

        offset = (page - 1) * perPage

        sortClause = {
            "title": workQuery.c.title,
            "date": workQuery.c.date_created,
            "uuid": workQuery.c.uuid,
        }.get(sortField)

        if sortClause is None:
            raise ValueError(f"Invalid sort param {sortField}")

        if sortDirection == "DESC":
            sortClause = sortClause.desc()

        editionsQuery = (
            self.session.query(Edition)
            # Get the editions from the sorted works
            .join(workQuery, Edition.id == workQuery.c.edition_id)
            # And filter to only the oldest edition per work to get
            # the 'preferred' edition
            .where(workQuery.c.rnk == 1)
            .order_by(sortClause)
        )

        return (
            editionsQuery.count(),
            editionsQuery.offset(offset).limit(perPage).all(),
        )

    def fetchEditions(self, editionIDs):
        """Fetch the editions in the given list, respecting the order
        of the passed in list
        """

        # First build a CTE of the passed in ids and their index in the
        # given list
        editionIdCTE = select(
            values(
                column("idx", Integer),
                column("edition_id", Integer),
                name="subquery",
            ).data(list(enumerate(editionIDs)))
        ).cte("edition_ids")
        return (
            self.session.query(Edition)
            .options(
                joinedload(Edition.links),
                joinedload(Edition.items),
                joinedload(Edition.items, Item.links),
                joinedload(Edition.items, Item.rights),
            )
            # join on the defined CTE and order below by the index
            .join(
                editionIdCTE,
                Edition.id == editionIdCTE.c.edition_id,
            )
            .order_by(editionIdCTE.c.idx)
            .all()
        )

    def fetchSingleLink(self, linkID):
        return self.session.query(Link).filter(Link.id == linkID).first()

    def fetchRecordsByUUID(self, uuids):
        return self.session.query(Record).filter(Record.uuid.in_(uuids)).all()

    def fetchRowCounts(self):
        countQuery = text("""SELECT relname AS table, reltuples AS row_count
            FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace)
            WHERE nspname NOT IN ('pg_catalog', 'information_schema')
            AND relkind = 'r'
            AND relname IN ('records', 'works', 'editions', 'items', 'links')
        """)

        return self.session.execute(countQuery)

    def fetchNewWorks(self, page=0, size=50):
        offset = page * size

        createdSince = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            days=1
        )

        baseQuery = self.session.query(Work).filter(Work.date_created >= createdSince)

        return (baseQuery.count(), baseQuery.offset(offset).limit(size).all())

    def fetchSingleCollection(self, uuid):
        return (
            self.session.query(Collection)
            .options(
                joinedload(Collection.editions),
                joinedload(Collection.editions, Edition.links),
                joinedload(Collection.editions, Edition.items),
                joinedload(Collection.editions, Edition.items, Item.links),
                joinedload(Collection.editions, Edition.items, Item.rights),
            )
            .filter(Collection.uuid == uuid)
            .one()
        )

    def fetchCollections(self, sort=None, page=1, perPage=10):
        offset = (page - 1) * perPage

        if sort:
            # The `sort` param is either `title` or `creator`, optionally with a sort direction
            # of `asc` or `desc` appended with a colon.  However, we can't just pass a plain
            # string to sqlalchemy, as the `title` field is ambiguous across different entities.
            # So instead, turn the sort field into a proper sort-clause on the Collection table.
            sort_field, *suffix = sort.split(":")
            sort_clause = getattr(Collection, sort_field)
            if suffix and suffix[0] == "desc":
                sort_clause = sort_clause.desc()
        else:
            sort_clause = Collection.title

        return (
            self.session.query(Collection)
            .order_by(text(sort))
            .offset(offset)
            .limit(perPage)
            .all()
        )

    def fetchAutomaticCollection(self, collection_id: int):
        return (
            self.session.query(AutomaticCollection)
            .filter(AutomaticCollection.collection_id == collection_id)
            .one()
        )

    def createStaticCollection(
        self,
        title,
        creator,
        description,
        owner,
        workUUIDs=[],
        editionIDs=[],
    ):
        newCollection = Collection(
            uuid=uuid4(),
            title=title,
            creator=creator,
            description=description,
            owner=owner,
            type="static",
        )

        collectionEditions = []
        if len(workUUIDs) > 0:
            collectionWorks = (
                self.session.query(Work)
                .join(Work.editions)
                .filter(Work.uuid.in_(workUUIDs))
                .all()
            )

            for work in collectionWorks:
                editions = list(
                    sorted(
                        [ed for ed in work.editions],
                        key=lambda x: x.publication_date
                        if x.publication_date
                        else date.today(),
                    )
                )

                for edition in editions:
                    if len(edition.items) > 0:
                        collectionEditions.append(edition)
                        break

        if len(editionIDs) > 0:
            collectionEditions.extend(
                self.session.query(Edition).filter(Edition.id.in_(editionIDs)).all()
            )

        newCollection.editions = collectionEditions

        self.session.add(newCollection)

        return newCollection

    def createAutomaticCollection(
        self,
        title,
        creator,
        description,
        owner,
        *,
        sortField,
        sortDirection,
        limit=None,
        keywordQuery=None,
        authorQuery=None,
        titleQuery=None,
        subjectQuery=None,
    ):
        newCollection = Collection(
            uuid=uuid4(),
            title=title,
            creator=creator,
            description=description,
            owner=owner,
            type="automatic",
        )
        nonNullKwargs = {
            k: v
            for k, v in {
                "sort_field": sortField,
                "sort_direction": sortDirection,
                "limit": limit,
            }.items()
            if v
        }
        automaticCollection = AutomaticCollection(
            keyword_query=keywordQuery,
            author_query=authorQuery,
            title_query=titleQuery,
            subject_query=subjectQuery,
            **nonNullKwargs,
        )
        newCollection.auto.append(automaticCollection)
        self.session.add(automaticCollection)
        return newCollection

    def deleteCollection(self, uuid):
        return self.session.query(Collection).filter(Collection.uuid == uuid).delete()

    def fetchUser(self, user):
        return self.session.query(User).filter(User.user == user).one_or_none()

import os
from elasticsearch7.exceptions import NotFoundError

from model import Work, Item, Edition, ESWork
from managers import DBManager, ElasticsearchManager


def main():
    """Updating NYPL catalog records with new link to replace text/html link"""

    dbManager = DBManager(
        user=os.environ.get("POSTGRES_USER", None),
        pswd=os.environ.get("POSTGRES_PSWD", None),
        host=os.environ.get("POSTGRES_HOST", None),
        port=os.environ.get("POSTGRES_PORT", None),
        db=os.environ.get("POSTGRES_NAME", None),
    )

    esManager = ElasticsearchManager()
    esManager.create_elastic_connection()

    dbManager.create_session()

    catalogLinkQuery = (
        dbManager.session.query(Work)
        .join(Edition)
        .join(Item)
        .filter(Item.source == "nypl")
    )

    for work in catalogLinkQuery.all():
        workEdFormats = {}
        print(work.uuid)
        for edition in work.editions:
            workEdFormats[edition.id] = getEditionFormats(edition.items)

        try:
            workRec = ESWork.get(work.uuid, index=esManager.index)
        except NotFoundError:
            print("Work not indexed, skipping")
            continue

        saveWork = False

        for edition in workRec.editions:
            if edition.edition_id not in workEdFormats.keys():
                continue

            if edition.formats != workEdFormats[edition.edition_id]:
                print("REPLACE")
                edition.formats = workEdFormats[edition.edition_id]
                saveWork = True
            else:
                print("SKIP")

        if saveWork:
            workRec.save()

    dbManager.close_connection()


def getEditionFormats(items):
    """Accepts an array that represent all the Items associated with an Edition
    from the database. It builds a unique array of all the media_types of all
    the links associated with all of the items.

    In this process it replaces the media_type for any link that represents a
    catalog URL with the value of application/html+catalog. This more specific
    media_type allows consuming application to distinguish catalog links from
    other links that resolve to web pages.

    Parametes:
    items -- an array of Item ORM records, drawn from a single Edition record

    Response:
    Unique array of media_type strings or None
    """
    formats = set()

    for item in items:
        links = [(l.media_type, l.flags) for l in item.links]
        htmlReadable = linkCriteriaChecker(item.links, "catalog", False)
        htmlCatalog = linkCriteriaChecker(item.links, "catalog", True)

        if htmlReadable and htmlCatalog:
            formats.add("text/html")
            formats.add("application/html+catalog")
        elif htmlReadable:
            formats.add("text/html")
        elif htmlCatalog:
            formats.add("application/html+catalog")

        for link in links:
            if link[0] not in ["text/html", "application/html+catalog"]:
                formats.add(link[0])

    return list(formats) if len(formats) > 0 else None


def linkCriteriaChecker(links, flag, value):
    """Takes the provided Link object and, if it has a media_type of text/html,
    evaluates it for the presence/value of a specified flag. This allows us to
    evaluate links that have ambiguous media_types and assert whether or not it
    meets certain criteria based on the more specific flag object.

    Parameters:
    link -- a ORM Link object
    flag -- a key in the flag JSON object of the Link
    value -- a boolean value that validates the expected value of the flag

    Response:
    Boolean
    """
    return (
        len(
            list(
                filter(
                    lambda x: x.flags[flag] == value and x.media_type == "text/html",
                    links,
                )
            )
        )
        > 0
    )


if __name__ == "__main__":
    main()

from base64 import b64decode
from flask import Blueprint, request, current_app, jsonify
from functools import wraps
import os
import re
from sqlalchemy.orm.exc import NoResultFound

from ..automaticCollectionUtils import fetchAutomaticCollectionEditions
from ..db import DBClient
from ..elastic import ElasticClient
from ..opdsUtils import OPDSUtils
from ..utils import APIUtils
from ..validation_utils import is_valid_uuid
from ..opds2 import Feed, Publication
from logger import create_log
from model import Work, Edition
from model.postgres.collection import COLLECTION_EDITIONS
from ..decorators import deprecated

logger = create_log(__name__)

collection = Blueprint('collection', __name__, url_prefix='/collection')
collections = Blueprint('collections', __name__, url_prefix='/collections')


def validateToken(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        logger.debug(request.headers)

        headers = {k.lower(): v for k, v in request.headers.items()}

        try:
            _, loginPair = headers['authorization'].split(' ')
            loginBytes = loginPair.encode('utf-8')
            user, password = b64decode(loginBytes).decode('utf-8').split(':')
        except KeyError:
            return APIUtils.formatResponseObject(
                403, 'authResponse', {'message': 'user/password not provided'}
            )

        dbClient = DBClient(current_app.config['DB_CLIENT'])
        dbClient.createSession()

        user = dbClient.fetchUser(user)

        if not user or APIUtils.validatePassword(password, user.password, user.salt) is False:
            return APIUtils.formatResponseObject(
                401, 'authResponse', {'message': 'invalid user/password'}
            )

        dbClient.closeSession()

        kwargs['user'] = user.user

        return func(*args, **kwargs)

    return decorator


@collection.route('', methods=['POST'])
@deprecated('This endpoint is deprecated please use /collections instead.')
@collections.route('', methods=['POST'])
@validateToken
def collectionCreate(user=None):
    logger.info('Creating new collection')

    collectionData = request.json
    errMsg = _validateCollectionCreate(collectionData)
    if errMsg:
        return APIUtils.formatResponseObject(400, 'createCollection', {'message': errMsg})

    # TODO: Connect to write client: https://newyorkpubliclibrary.atlassian.net/browse/SFR-2668
    dbClient = DBClient(current_app.config['DB_CLIENT'])
    dbClient.createSession()

    if "workUUIDs" in collectionData or "editionIDs" in collectionData:
        newCollection = dbClient.createStaticCollection(
            collectionData['title'],
            collectionData['creator'],
            collectionData['description'],
            user,
            workUUIDs=collectionData.get('workUUIDs', []),
            editionIDs=collectionData.get('editionIDs', []),
        )

    elif "autoDef" in collectionData:
        autoDef = collectionData["autoDef"]
        queryFields = ["keywordQuery", "authorQuery", "titleQuery", "subjectQuery"]
        errMsg = _validateAutoCollectionDef(autoDef)
        if errMsg:
            return APIUtils.formatResponseObject(400, "createCollection", {'message': errMsg})
        newCollection = dbClient.createAutomaticCollection(
            collectionData['title'],
            collectionData['creator'],
            collectionData['description'],
            owner=user,
            sortField=autoDef.get("sortField"),
            sortDirection=autoDef.get("sortDirection"),
            limit=autoDef.get("limit"),
            **{field: autoDef.get(field) for field in queryFields},
        )


    dbClient.session.commit()

    logger.info('Created collection {}'.format(newCollection))

    opdsFeed = constructOPDSFeed(newCollection, dbClient)

    return APIUtils.formatOPDS2Object(201, opdsFeed)


def _validateCollectionCreate(data: dict) -> str:
    """Return an error message if the collection create data is invalid"""

    if len(set(data.keys()) & set(['title', 'creator', 'description'])) < 3:
        return 'title, creator and description fields are required'

    isAuto = 'autoDef' in data
    isStatic = 'workUUIDs' in data or 'editionIDs' in data
    if not isAuto and not isStatic:
        return (
            'Need either workUUIDs, editionIDs or an automatic collection definition'
            'to create a collection'
        )

    if isAuto and isStatic:
        return (
            'Cannot create a collection with both an automatic collection definition '
            'and editionIDs or workUUIDs'
        )


def _validateAutoCollectionDef(autoDef: dict) -> str:
    """Return an error message if the definition is not valid"""
    sortField = autoDef.get("sortField", "uuid")
    if sortField not in ["uuid", "title", "author", "date"]:
        return f"Invalid sort field {sortField}"

    sortDirection = autoDef.get("sortDirection", "ASC")
    if sortDirection not in ["ASC", "DESC"]:
        return f"Invalid sort direction {sortDirection}"


@collection.route('/replace/<uuid>', methods=['POST'])
@deprecated('This endpoint is deprecated please use /collections/replace/<uuid> instead.')
@collections.route('/replace/<uuid>', methods=['POST'])
@validateToken
def collectionReplace(uuid, user=None):
    logger.info('Handling collection replacement request')

    collectionData = request.json
    dataKeys = collectionData.keys()

    if {'title', 'creator', 'description'}.issubset(set(dataKeys)) == False\
            or len(set(dataKeys) & set(['workUUIDs', 'editionIDs'])) == 0:
        errMsg = {
            'message':
                'title, creator and description fields are required'
                ', with one of workUUIDs or editionIDs to create a collection'
        }

        return APIUtils.formatResponseObject(400, 'createCollection', errMsg)

    # TODO: Connect to write client: https://newyorkpubliclibrary.atlassian.net/browse/SFR-2668
    dbClient = DBClient(current_app.config['DB_CLIENT'])
    dbClient.createSession()

    #Getting the collection the user wants to replace
    try:
        collection = dbClient.fetchSingleCollection(uuid)
    except NoResultFound:
        errMsg = {'message': 'Unable to locate collection {}'.format(uuid)}
        return APIUtils.formatResponseObject(404, 'fetchSingleCollection', errMsg)

    workUUIDs=collectionData.get('workUUIDs', [])
    editionIDs=collectionData.get('editionIDs', [])

    collection.title = collectionData['title']
    collection.creator = collectionData['creator']
    collection.description = collectionData['description']

    removeAllEditionsFromCollection(dbClient, collection)

    if editionIDs:
        addEditionsToCollection(dbClient, collection, editionIDs)

    if workUUIDs:
        addWorksToCollection(dbClient, collection, workUUIDs)

    dbClient.session.commit()

    logger.info('Replaced collection {}'.format(collection.uuid))

    opdsFeed = constructOPDSFeed(collection, dbClient)

    return APIUtils.formatOPDS2Object(201, opdsFeed)

@collection.route('/update/<uuid>', methods=['POST'])
@deprecated('This endpoint is deprecated please use /collections/update/<uuid> instead.')
@collections.route('/update/<uuid>', methods=['POST'])
@validateToken
def collectionUpdate(uuid, user=None):
    logger.info('Handling collection update request')

    # TODO: Connect to write client: https://newyorkpubliclibrary.atlassian.net/browse/SFR-2668
    dbClient = DBClient(current_app.config['DB_CLIENT'])
    dbClient.createSession()

    title = request.args.get('title', None)
    creator = request.args.get('creator', None)
    description = request.args.get('description', None)
    editionIDs = request.args.get('editionIDs', None)
    workUUIDs = request.args.get('workUUIDs', None)

    if len(set(request.args) & set(['title', 'creator', 'description', 'editionIDs', 'workUUIDs'])) == 0:
        errMsg = {
            'message':
                'At least one of these fields(title, creator, description, etc.) are required'
        }

        return APIUtils.formatResponseObject(400, 'updateCollection', errMsg)

    #Getting the collection the user wants to update
    try:
        collection = dbClient.fetchSingleCollection(uuid)
    except NoResultFound:
        errMsg = {'message': 'Unable to locate collection {}'.format(uuid)}
        return APIUtils.formatResponseObject(404, 'fetchSingleCollection', errMsg)

    if title:
        collection.title = title
    if creator:
        collection.creator = creator
    if description:
        collection.description = description

    if editionIDs:
        editionIDsList = editionIDs.split(',')
        if len(editionIDs) > 10:
            errMsg = {'message': 'Size of editionIDsList must not exceed 10 IDs'}
            return APIUtils.formatResponseObject(400, 'collectionUpdate', errMsg)
        #Check if all the editionIDs are actually edition ids in the database
        for eid in editionIDsList:
            if dbClient.fetchSingleEdition(eid) == None:
                errMsg = {'message': 'Unable to locate edition with id {}'.format(eid)}
                return APIUtils.formatResponseObject(404, 'fetchSingleEdition', errMsg)

        addEditionsToCollection(dbClient, collection, editionIDsList)

    if workUUIDs:
        workUUIDsList = workUUIDs.split(',')
        if len(workUUIDsList) > 10:
            errMsg = {'message': 'Size of workUUIDsList must not exceed 10 UUIDs'}
            return APIUtils.formatResponseObject(400, 'collectionUpdate', errMsg)
        #Check if all the workUUIDs are actually work uuids in the database
        for workUUID in workUUIDsList:
            if dbClient.fetchSingleWork(workUUID) == None:
                errMsg = {'message': 'Unable to locate work with uuid {}'.format(workUUID)}
                return APIUtils.formatResponseObject(404, 'fetchSingleWork', errMsg)

        addWorksToCollection(dbClient, collection, workUUIDsList)

    dbClient.session.commit()

    opdsFeed = constructOPDSFeed(collection, dbClient)

    return APIUtils.formatOPDS2Object(200, opdsFeed)


@collection.route('/<uuid>', methods=['GET'])
@deprecated('This endpoint is deprecated please use /collections/<uuid> instead.')
@collections.route('/<uuid>', methods=['GET'])
def get_collection(uuid):
    logger.info(f'Getting collection with id {uuid}')
    response_type = 'fetchCollection'

    if not is_valid_uuid(uuid):
        return APIUtils.formatResponseObject(400, response_type, { 'message': f'Collection id {uuid} is invalid' })

    try:
        db_client = DBClient(current_app.config['DB_CLIENT'])
        db_client.createSession()

        sort = request.args.get('sort', None)
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('perPage', 10))

        collection = db_client.fetchSingleCollection(uuid)

        if not collection:
            return APIUtils.formatResponseObject(404, response_type, { 'message:' f'No collection found with id {uuid}' })

        opds_feed = constructOPDSFeed(collection, db_client, sort=sort, page=page, perPage=per_page)

        db_client.closeSession()

        return APIUtils.formatOPDS2Object(200, opds_feed)
    except NoResultFound:
        return APIUtils.formatResponseObject(404, response_type, { 'message': f'No collection found with id {uuid}' })
    except Exception:
        logger.exception(f'Unable to get collection with id {uuid}')
        return APIUtils.formatResponseObject(500, response_type, { 'message': f'Unable to get collection with id {uuid}' })

@collection.route('/<uuid>', methods=['DELETE'])
@deprecated('This endpoint is deprecated please use /collections/<uuid> instead.')
@collections.route('/<uuid>', methods=['DELETE'])
@validateToken
def collectionDelete(uuid, user=None):
    logger.info('Deleting collection {}'.format(uuid))

    # TODO: Connect to write client: https://newyorkpubliclibrary.atlassian.net/browse/SFR-2668
    dbClient = DBClient(current_app.config['DB_CLIENT'])
    dbClient.createSession()

    deleteCount = dbClient.deleteCollection(uuid)

    if deleteCount is None or deleteCount < 1:
        errMsg = {'message': 'No collection with UUID {} exists'.format(uuid)}
        return APIUtils.formatResponseObject(404, 'deleteCollection', errMsg)

    dbClient.session.commit()

    logger.info('Successfully Deleted Collection')

    return (jsonify({'message': 'Deleted {}'.format(uuid)}), 200)

@collection.route('/delete/<uuid>', methods=['DELETE'])
@deprecated('This endpoint is deprecated please use /collections/delete/<uuid> instead.')
@collections.route('/delete/<uuid>', methods=['DELETE'])
@validateToken
def collectionDeleteWorkEdition(uuid, user=None):
    logger.info('Handling collection work/edition deletion request')

    editionIDs = request.args.get('editionIDs', None)
    workUUIDs = request.args.get('workUUIDs', None)

    if len(set(request.args) & set(['editionIDs', 'workUUIDs'])) == 0:
        errMsg = {
            'message':
                'At least one of these fields(editionIDs & workUUIDs) are required'
        }

        return APIUtils.formatResponseObject(400, 'deleteCollectionWorkEdition', errMsg)

    # TODO: Connect to write client: https://newyorkpubliclibrary.atlassian.net/browse/SFR-2668
    dbClient = DBClient(current_app.config['DB_CLIENT'])
    dbClient.createSession()

    #Getting the collection the user wants to replace
    try:
        collection = dbClient.fetchSingleCollection(uuid)
    except NoResultFound:
        errMsg = {'message': 'Unable to locate collection {}'.format(uuid)}
        return APIUtils.formatResponseObject(404, 'fetchSingleCollection', errMsg)

    if editionIDs:
        editionIDsList = editionIDs.split(',')
    else:
        editionIDsList = None
    if workUUIDs:
        workUUIDsList = workUUIDs.split(',')
    else:
        workUUIDsList = None

    removeWorkEditionsFromCollection(dbClient, editionIDsList, workUUIDsList)

    dbClient.session.commit()

    opdsFeed = constructOPDSFeed(collection, dbClient)

    return APIUtils.formatOPDS2Object(200, opdsFeed)


@collection.route('/list', methods=['GET'])
@deprecated('This endpoint is deprecated please use /collections instead.')
@collections.route('', methods=['GET'])
def get_collections():
    logger.info('Getting all collections')
    response_type = 'collectionList'

    sort = request.args.get('sort', 'title')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('perPage', 10))

    if not re.match(r'(?:title|creator)(?::(?:asc|desc))*', sort):
        return APIUtils.formatResponseObject(
            400, 
            response_type,
            { 'message': 'Sort fields are invalid' }
        )

    try:
        db_client = DBClient(current_app.config['DB_CLIENT'])
        db_client.createSession()

        collections = db_client.fetchCollections(sort=sort, page=page, perPage=per_page)

        opds_feed = Feed()

        opds_feed.addMetadata({ 'title': 'Digital Research Books Collections' })
        opds_feed.addLink({ 'rel': 'self', 'href': request.path, 'type': 'application/opds+json' })

        OPDSUtils.addPagingOptions(
            opds_feed, 
            request.full_path, 
            len(collections),
            page=page, 
            perPage=per_page
        )

        for collection in collections:
            uuid = collection.uuid
            path = '/collection/{}'.format(uuid)

            group = constructOPDSFeed(collection, db_client, perPage=5, path=path, build_publications=False)

            opds_feed.addGroup(group)

        return APIUtils.formatOPDS2Object(200, opds_feed)
    except Exception:
        logger.exception('Unable to get collections')
        return APIUtils.formatResponseObject(500, response_type, { 'message': 'Unable to get collections' })
    finally:
        db_client.closeSession()


def constructSortMethod(sort):
    sortSettings = sort.split(':')

    def sortEds(ed):
        sortValue = getattr(ed.metadata, sortSettings[0])

        if isinstance(sortValue, str):
            sortValue = sortValue.lower()

        return sortValue

    if len(sortSettings) == 2:
        reversed = True if sortSettings[1].lower() == 'desc' else False
    else:
        reversed = False

    return (sortEds, reversed)


def constructOPDSFeed(
    collection, dbClient, sort=None, page=1, perPage=10, path=None, build_publications: bool=True
):
    uuid = collection.uuid

    opdsFeed = Feed()

    opdsFeed.addMetadata({
        'title': collection.title,
        'creator': collection.creator,
        'description': collection.description
    })

    path = request.full_path\
        if str(uuid) in request.path\
        else '/collection/{}'.format(uuid)

    opdsFeed.addLink({
        'rel': 'self', 'href': path, 'type': 'application/opds+json'
    })

    if build_publications:
        if collection.type == "static":
            _addStaticPubsToFeed(opdsFeed, collection, path, page, perPage, sort)
        elif collection.type == "automatic":
            esClient = ElasticClient(current_app.config["REDIS_CLIENT"])
            _addAutomaticPubsToFeed(opdsFeed, dbClient, esClient, collection.id, path, page, perPage)
        else:
            raise ValueError(f"Encountered collection with unhandleable type {collection.type}")
    else:
        opdsFeed.metadata.addField('numberOfItems', len(collection.editions))

    return opdsFeed


def _addStaticPubsToFeed(opdsFeed, collection, path, page, perPage, sort):
    start = (page - 1) * perPage
    end = start + perPage

    opdsPubs = _buildPublications(collection.editions[start:end])
    
    if sort:
        sorter, reversed_ = constructSortMethod(sort)
        opdsPubs.sort(key=sorter, reverse=reversed_)

    opdsFeed.addPublications(opdsPubs)

    OPDSUtils.addPagingOptions(
        opdsFeed, path, len(collection.editions), page=page, perPage=perPage
    )


def _addAutomaticPubsToFeed(opdsFeed, dbClient, esClient, collectionId, path, page, perPage):
    totalCount, editions = fetchAutomaticCollectionEditions(
        dbClient,
        esClient,
        collectionId,
        page=page,
        perPage=perPage,
    )
    opdsPubs = _buildPublications(editions)
    opdsFeed.addPublications(opdsPubs)
    OPDSUtils.addPagingOptions(
        opdsFeed, path, totalCount, page=page, perPage=perPage
    )


def _buildPublications(editions):
    host = 'digital-research-books-beta'\
        if os.environ['ENVIRONMENT'] == 'production' else 'drb-qa'

    opdsPubs = []
    for ed in editions:
        pub = Publication()

        pub.parseEditionToPublication(ed)
        pub.addLink({
            'rel': 'alternate',
            'href': 'https://{}.nypl.org/edition/{}'.format(host, ed.id),
            'type': 'text/html',
            'identifier': 'readable'
        })

        opdsPubs.append(pub)

    return opdsPubs

def removeWorkEditionsFromCollection(dbClient, editionIDs=None, workUUIDs=None):

    '''Deleting the rows of collection_editions that were in the original collection'''

    #Delete the rows that match the editionIDs
    if editionIDs != None:
        dbClient.session.execute(COLLECTION_EDITIONS.delete()\
            .where(COLLECTION_EDITIONS.c.edition_id.in_(editionIDs)))
    #Delete the rows that match the workUUIDs
    if workUUIDs != None:
        collectionWorks = dbClient.session.query(Work)\
            .join(Work.editions)\
            .filter(Work.uuid.in_(workUUIDs))\
            .all()

        for work in collectionWorks:
            collectionEdition = dbClient.session.query(Edition)\
                .filter(Edition.work_id == work.id)\
                .order_by(Edition.date_created.asc())\
                .limit(1)\
                .scalar()

            dbClient.session.execute(COLLECTION_EDITIONS.delete()\
                .where(COLLECTION_EDITIONS.c.edition_id == collectionEdition.id))

def removeAllEditionsFromCollection(dbClient, collection):

    '''Deleting the rows of collection_editions that were in the original collection'''
    dbClient.session.execute(COLLECTION_EDITIONS.delete()\
        .where(COLLECTION_EDITIONS.c.collection_id == collection.id))

def addEditionsToCollection(dbClient, collection, editionIDs):

    '''Inserting rows of collection_editions based on editionIDs array'''

    #This for loop format helps to avoid inserting duplicate editionIDs
    for eid in editionIDs:
        if dbClient.session.query(COLLECTION_EDITIONS.c.collection_id)\
            .filter(COLLECTION_EDITIONS.c.edition_id == eid).first() == None:

            dbClient.session.execute(COLLECTION_EDITIONS.insert()\
                .values({"collection_id": collection.id, "edition_id": eid}))

def addWorksToCollection(dbClient, collection, workUUIDs):

    '''Inserting rows of collection_editions based on workUUIDs array'''

    collectionWorks = dbClient.session.query(Work)\
            .join(Work.editions)\
            .filter(Work.uuid.in_(workUUIDs))\
            .all()

    for work in collectionWorks:
        collectionEdition = dbClient.session.query(Edition)\
            .filter(Edition.work_id == work.id)\
            .order_by(Edition.date_created.asc())\
            .limit(1)\
            .scalar()

        #This for loop format helps to avoid inserting duplicate editionIDs
        if dbClient.session.query(COLLECTION_EDITIONS.c.collection_id)\
            .filter(COLLECTION_EDITIONS.c.edition_id == collectionEdition.id)\
            .first() == None:

            dbClient.session.execute(COLLECTION_EDITIONS.insert()\
                .values({"collection_id": collection.id, "edition_id": collectionEdition.id}))

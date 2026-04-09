from collections import OrderedDict
from datetime import datetime, timezone
from hashlib import scrypt
from flask import jsonify
from itertools import repeat
from math import ceil
from json import JSONEncoder
from uuid import UUID
import re
import os

from model import Collection, Edition
from model.postgres.collection import COLLECTION_EDITIONS
from utils.common import require_env
from logger import create_log

from botocore.exceptions import ClientError
from urllib.parse import urlparse
from sqlalchemy import inspect
from sqlalchemy.orm.attributes import NO_VALUE


logger = create_log(__name__)


def json_dump_uuid(obj):
    if isinstance(obj, UUID):
        return str(obj)
    return JSONEncoder().default(obj)


def shorten(s):
    return s[:200] + "..."
    # TODO: cut out center not end


def remove_markdown_comments(markdown_text):
    # The regex pattern matches <!-- followed by any characters (including newlines)
    # in a non-greedy way (.*?) until --> is found.
    # re.DOTALL ensures that '.' matches newline characters as well.
    cleaned_text = re.sub(r"<!--.*?-->", "", markdown_text, flags=re.DOTALL)
    return cleaned_text


# see: https://chatgpt.com/c/694e008d-e04c-8326-8c94-223abfd642ba
# TODO: add column name mapping (if needed)
def orm_to_dict(obj, exclude=None, visited=None, column_formatters=None):
    """
    Convert a single SQLAlchemy ORM object to a dict, following relationships
    recursively to created a nested structure.
    Skips relationships that are not eagerly loaded.
    Avoids following infinite loop circular relationships.

    Params:
        obj: sqlalchemy declarative entity instance
        exclude: list of column specs to exclude from returned dict, at any
            level of nesting. Column specs are tuples of (<declarative table>, 'column name').
            Does not validate the exclude column specifications. Does nothing if
            none of the exclude columns are encountered while traversing the
            ORM object being converted.
        visited: set() of ORM obj identifiers that have been visited
            (to avoid infinite loop circular relationships).
    """
    if visited is None:
        visited = set()
    if exclude is None:
        exclude = []

    insp = inspect(obj)

    # Handle recursive circular references (using obj identity/PK values)
    # NOTE: None when ORM objs created locally and PKs haven't been set (NOTE: python id could collide with ORM PK but .identity is always a tuple so no worries)
    id_key = id(obj) if insp.identity is None else insp.identity
    identity = (insp.mapper.class_, id_key)
    if identity in visited:
        # Break circular reference cycle
        return {
            "_ref": {
                "class": insp.mapper.class_.__name__,
                "identity": f"primary key unset (python obj id={id(obj)})"
                if insp.identity is None
                else insp.identity,
                # TODO: {"py_id": id(obj)} if insp.identity is None else {"pk": insp.identity} (but label the identity keys as a dict)
            }
        }
    visited.add(identity)

    data = {}

    # Field values
    for column in insp.mapper.column_attrs:
        if (insp.mapper.class_, column.key) not in exclude:
            # ALT FUTURE: for effciency check check col key first. To handle Mixin classes do issubclass(insp.mapper.class_, exclude_cls)
            value = getattr(obj, column.key)
            if column_formatters and column.key in column_formatters:
                try:
                    data[column.key] = column_formatters[column.key](value)
                except Exception:
                    # If formatter fails, fall back to raw value
                    data[column.key] = value
            else:
                data[column.key] = value

    # Related entities (recursive, nested)
    for relationship in insp.mapper.relationships:
        rel_loaded = insp.attrs[relationship.key].loaded_value
        # Skip if relationship is not already eagerly loaded
        if rel_loaded is NO_VALUE:
            continue

        # Handle various loaded value types
        if relationship.uselist:  # collection of related entities
            # copy visited so each branch of the JSON tree tracks its own visited depth-wise (could this cause memory overflow problems)
            relations = [
                orm_to_dict(rel, exclude=exclude, visited=visited.copy())
                for rel in rel_loaded
            ]
        else:
            # NOTE: only a scalar relation can be None, if the FK column is NULL, the related row was deleted, etc..
            if rel_loaded is None:
                relations = []  # ALT: [{}] (or handle orm_to_dict(None) at top of func and return {}) # this loses the distinction between “relationship loaded but NULL” and “collection loaded but empty”.
            else:
                relations = [orm_to_dict(rel_loaded, exclude=exclude, visited=visited)]
        data[relationship.key] = relations

    return data


class APIUtils:
    QUERY_TERMS = [
        "keyword",
        "title",
        "author",
        "subject",
        "identifier",
        "authority: identifier",
        "viaf",
        "lcnaf",
        "date",
        "startYear",
        "endYear",
        "language",
        "format",
        "govDoc",
        "showAll",
        "key",
    ]

    FORMAT_CROSSWALK = {
        "epub_zip": ["application/epub+zip", "application/epub+xml"],
        "epub_xml": ["application/epub+zip", "application/epub+xml"],
        "html": ["text/html"],
        "html_edd": ["application/html+edd", "application/x.html+edd"],
        "html_catalog": ["application/html+catalog"],
        "pdf": ["application/pdf"],
        "webpub_json": ["application/webpub+json"],
        "readable": ["application/epub+xml", "text/html", "application/webpub+json"],
        "downloadable": ["application/pdf", "application/epub+zip"],
        "requestable": ["application/html+edd", "application/x.html+edd"],
    }

    SOURCE_PRIORITY = {
        "SCH Collection/Hathi files": 1,
        "gutenberg": 2,
        "doab": 3,
        "loc": 4,
        "muse": 5,
        "met": 6,
        "isac": 7,
        "UofMichigan Backlist": 8,
        "UofMichigan": 8,
        "UofM": 8,
        "UofSC": 9,
        "hathitrust": 10,
        "oclc": 11,
        "nypl": 12,
        "grin": 13,
    }

    DEFAULT_PRIORITY = 100

    @staticmethod
    def normalizeQueryParams(params):
        paramDict = params.to_dict(flat=False)
        return {k: v for k, v in paramDict.items()}

    @classmethod
    def extractParamPairs(cls, param, pairs):
        outPairs = []

        for pairStr in pairs.get(param, []):
            if len(re.findall(r'"', pairStr)) % 2 != 0:
                pairStr = "".join(pairStr.rsplit('"', 1))

            # Regex to split query string on field terms
            queryTermRegex = (
                ",*({}):".format("|".join(cls.QUERY_TERMS))
                .encode("unicode-escape")
                .decode()
            )

            pairs = list(filter(lambda x: x != "", re.split(queryTermRegex, pairStr)))

            i = 0
            while True:
                pairElements = pairs[i : i + 2]

                if len(pairElements) == 1 or pairElements[0] not in cls.QUERY_TERMS:
                    pairSet = (param, pairs[i])
                    i += 1
                else:
                    pairSet = (pairElements[0], ":".join(pairElements[1:]))
                    i += 2

                outPairs.append(pairSet)

                if i >= len(pairs):
                    break

        return outPairs

    @classmethod
    def formatAggregationResult(cls, aggregations, parentKey=None):
        aggs = {}

        for key, value in aggregations.items():
            if key == "buckets":
                aggs[parentKey] = [
                    {"value": b["key"], "count": b["editions_per"]["doc_count"]}
                    if "editions_per" in b.keys()
                    else {"value": b["key_as_string"], "count": b["doc_count"]}
                    for b in value
                ]
                return aggs

            if isinstance(value, dict):
                aggs = {**aggs, **cls.formatAggregationResult(value, parentKey=key)}

        return aggs

    @staticmethod
    def formatFilters(terms):
        formats = [
            mediaType
            for f in list(filter(lambda x: x[0] == "format", terms["filter"]))
            for mediaType in APIUtils.FORMAT_CROSSWALK[f[1]]
        ]
        return formats

    @staticmethod
    def formatPagingOptions(page, pageSize, totalHits):
        if totalHits == 0:
            return {}

        lastPage = ceil(totalHits / pageSize)

        return {
            "recordsPerPage": pageSize,
            "firstPage": 1,
            "previousPage": page - 1 if page > 1 else None,
            "currentPage": page,
            "nextPage": page + 1 if page < lastPage else None,
            "lastPage": lastPage,
            "totalRecords": totalHits,
        }

    @classmethod
    def formatWorkOutput(
        cls,
        works,
        identifiers,
        dbClient,
        request,
        showAll=True,
        formats=None,
        reader=None,
    ):
        # Multiple formatted works with formats specified
        if isinstance(works, list):
            outWorks = []
            workDict = {str(work.uuid): work for work in works}

            for workUUID, editionIds, highlights in identifiers:
                work = workDict.get(workUUID, None)

                if work is None:
                    continue

                outWork = cls.formatWork(
                    work,
                    editionIds,
                    showAll,
                    dbClient,
                    formats=formats,
                    reader=reader,
                    request=request,
                )

                cls.addWorkMeta(outWork, highlights=highlights)

                outWorks.append(outWork)

            return outWorks
        # Formatted work with a specific format given
        elif formats != None and identifiers == None:
            formattedWork = cls.formatWork(
                works,
                None,
                showAll,
                dbClient,
                formats=formats,
                reader=reader,
                request=request,
            )

            formattedWork["editions"].sort(
                key=lambda x: x["publication_date"] if x["publication_date"] else 9999
            )
            return formattedWork
        # Formatted work with no format specified
        else:
            formattedWork = cls.formatWork(
                works, None, showAll, dbClient, reader=reader, request=request
            )

            formattedWork["editions"].sort(
                key=lambda x: x["publication_date"] if x["publication_date"] else 9999
            )
            return formattedWork

    @classmethod
    def formatWork(
        cls,
        work,
        editionIds,
        showAll,
        dbClient=None,
        formats=None,
        reader=None,
        request=None,
    ):
        workDict = dict(work)
        workDict["edition_count"] = len(work.editions)
        workDict["inCollections"] = cls.checkEditionInCollection(
            work, None, dbClient=dbClient
        )
        workDict["date_created"] = work.date_created.strftime("%Y-%m-%dT%H:%M:%S")
        workDict["date_modified"] = work.date_modified.strftime("%Y-%m-%dT%H:%M:%S")

        orderedEds = OrderedDict.fromkeys(editionIds) if editionIds else OrderedDict()

        for edition in work.editions:
            if editionIds and edition.id not in editionIds:
                continue

            editInCollection = cls.checkEditionInCollection(None, edition, dbClient)

            editionDict = cls.formatEdition(
                edition,
                editionInCollection=editInCollection,
                formats=formats,
                reader=reader,
            )

            if showAll is True or (showAll is False and len(editionDict["items"]) > 0):
                orderedEds[edition.id] = editionDict

        workDict["editions"] = list(filter(None, [e for _, e in orderedEds.items()]))

        for edition in workDict["editions"]:
            for item in edition["items"]:
                if item.get("links"):
                    # Map over item links and patch with pre-signed URL where necessary
                    item["links"] = list(
                        map(
                            APIUtils.replacePrivateLinkUrl,
                            item["links"],
                            repeat(request),
                        )
                    )
        return workDict

    @classmethod
    def addWorkMeta(cls, work, **kwargs):
        work["_meta"] = {
            metaField: metaValue for metaField, metaValue in kwargs.items()
        }

    @classmethod
    def formatEditionOutput(
        cls,
        edition,
        request,
        records=None,
        dbClient=None,
        showAll=False,
        formats=None,
        reader=None,
    ):
        editionWorkTitle = edition.work.title
        editionWorkAuthors = edition.work.authors
        editionInCollection = cls.checkEditionInCollection(None, edition, dbClient)

        formattedEdition = cls.formatEdition(
            edition,
            editionWorkTitle,
            editionWorkAuthors,
            editionInCollection,
            records,
            formats,
            showAll=showAll,
            reader=reader,
        )

        if formattedEdition.get("instances"):
            for instance in formattedEdition["instances"]:
                for item in instance["items"]:
                    if item.get("links"):
                        # Map over item links and patch with pre-signed URL where necessary
                        item["links"] = list(
                            map(
                                APIUtils.replacePrivateLinkUrl,
                                item["links"],
                                repeat(request),
                            )
                        )

        return formattedEdition

    @classmethod
    def checkEditionInCollection(cls, work, edition, dbClient):
        collectionMetadata = []

        if work != None:
            for edit in dbClient.session.query(Edition).filter(
                Edition.work_id == work.id
            ):
                for collection in (
                    dbClient.session.query(Collection)
                    .join(COLLECTION_EDITIONS)
                    .filter(COLLECTION_EDITIONS.c.edition_id == edit.id)
                ):
                    metadataOBJ = {
                        "uuid": collection.uuid,
                        "title": collection.title,
                        "creator": collection.creator,
                        "description": collection.description,
                        "numberOfItems": len(collection.editions),
                    }
                    collectionMetadata.append(metadataOBJ)

        else:
            for collection in (
                dbClient.session.query(Collection)
                .join(COLLECTION_EDITIONS)
                .filter(COLLECTION_EDITIONS.c.edition_id == edition.id)
            ):
                metadataOBJ = {
                    "uuid": collection.uuid,
                    "title": collection.title,
                    "creator": collection.creator,
                    "description": collection.description,
                    "numberOfItems": len(collection.editions),
                }
                collectionMetadata.append(metadataOBJ)

        return collectionMetadata

    @classmethod
    def formatEdition(
        cls,
        edition,
        editionWorkTitle=None,
        editionWorkAuthors=None,
        editionInCollection=None,
        records=None,
        formats=None,
        showAll=False,
        reader=None,
    ):
        editionDict = dict(edition)
        editionDict["edition_id"] = edition.id
        editionDict["work_uuid"] = edition.work.uuid
        editionDict["inCollections"] = editionInCollection
        editionDict["publication_date"] = (
            edition.publication_date.year if edition.publication_date else None
        )

        editionDict["work_title"] = editionWorkTitle
        editionDict["work_authors"] = editionWorkAuthors

        editionDict["links"] = [
            {"link_id": link.id, "mediaType": link.media_type, "url": link.url}
            for link in edition.links
        ]

        editionDict["items"] = []
        for item in edition.items:
            itemDict = dict(item)
            itemDict["item_id"] = item.id
            itemDict["location"] = (
                item.physical_location["name"] if item.physical_location else None
            )

            itemDict["links"] = []

            if formats:
                formats.append("application/webpub+json")
                validLinks = list(filter(lambda x: x.media_type in formats, item.links))
            else:
                validLinks = item.links

            for link in validLinks:
                flags = link.flags

                if (reader == "v2" and link.media_type == "application/epub+xml") or (
                    reader != "v2" and link.media_type == "application/webpub+json"
                ):
                    flags["reader"] = False

                if link.media_type == "application/ocr":
                    page_id = link.url.split("/")[-1]
                    link.url = (
                        require_env("DRB_API_URL") + f"/items/{item.id}/read/{page_id}"
                    )

                itemDict["links"].append(
                    {
                        "link_id": link.id,
                        "mediaType": link.media_type,
                        "url": link.url,
                        "flags": flags,
                    }
                )

            itemDict["links"].sort(key=cls.sortByMediaType)

            itemDict["rights"] = [
                {
                    "source": rights.source,
                    "license": rights.license,
                    "rightsStatement": rights.rights_statement,
                }
                for rights in item.rights
            ]

            editionDict["items"].append(itemDict)

        emptyListFlag = False

        # This for loop is meant to check if a empty links array exists in a item dictionary
        # If one exists, then sorting by source priority and empty/non-empty priority occurs
        # Otherwise, sorting by source and link media type occurs
        for itemDict in editionDict["items"]:
            if itemDict["links"] == []:
                editionDict["items"].sort(
                    key=lambda x: (
                        cls.SOURCE_PRIORITY.get(x["source"], cls.DEFAULT_PRIORITY),
                        x["links"] == [],
                    )
                )
                emptyListFlag = True
                break

        if emptyListFlag == False:
            editionDict["items"].sort(
                key=lambda x: (
                    cls.SOURCE_PRIORITY.get(x["source"], cls.DEFAULT_PRIORITY),
                    cls.sortByMediaType(x["links"][0]),
                )
            )

        if records is not None:
            itemsByLink = {}
            for item in editionDict["items"]:
                for link in item["links"]:
                    itemsByLink[link["url"]] = item

            editionDict["instances"] = []
            for rec in records:
                formattedRec = cls.formatRecord(rec, itemsByLink)

                if showAll is False and len(formattedRec["items"]) < 1:
                    continue

                editionDict["instances"].append(formattedRec)

            del editionDict["items"]

        return editionDict

    @staticmethod
    def sortByMediaType(link):
        scores = {
            "application/webpub+json": 1,
            "application/pdf": 2,
            "application/epub+xml": 3,
            "application/epub+zip": 3,
            "text/html": 4,
            "application/html+edd": 5,
            "application/x.html+edd": 5,
            "application/html+catalog": 6,
        }

        return scores.get(link["mediaType"], 7)

    @classmethod
    def formatRecord(cls, record, itemsByLink):
        outRecord = {
            "instance_id": record.id,
            "title": record.title,
            "publication_place": record.spatial,
            "extent": record.extent,
            "summary": record.abstract,
            "table_of_contents": record.table_of_contents,
        }

        outRecord["authors"] = cls.formatPipeDelimitedData(
            record.authors, ["name", "viaf", "lcnaf", "primary"]
        )
        outRecord["contributors"] = cls.formatPipeDelimitedData(
            record.contributors, ["name", "viaf", "lcnaf", "rolse"]
        )
        outRecord["publishers"] = cls.formatPipeDelimitedData(
            record.publisher, ["name", "viaf", "lcnaf"]
        )
        outRecord["dates"] = cls.formatPipeDelimitedData(record.dates, ["date", "type"])
        outRecord["languages"] = cls.formatPipeDelimitedData(
            record.languages, ["language", "iso_2", "iso_3"]
        )
        outRecord["identifiers"] = cls.formatPipeDelimitedData(
            record.identifiers, ["identifier", "authority"]
        )

        recordItems = {}
        for hasPart in record.has_part:
            _, url, *_ = hasPart.split("|")
            urlItem = itemsByLink.get(re.sub(r"https?:\/\/", "", url), None)

            if urlItem:
                recordItems[urlItem["item_id"]] = urlItem

        outRecord["items"] = [item for _, item in recordItems.items()]

        return outRecord

    @classmethod
    def formatLinkOutput(cls, link, request):
        linkItem = dict(link.items[0])
        linkItem["item_id"] = link.items[0].id

        linkEdition = dict(link.items[0].edition)
        linkEdition["edition_id"] = link.items[0].edition.id
        linkEdition["work_uuid"] = link.items[0].edition.work.uuid
        linkEdition["publication_date"] = (
            link.items[0].edition.publication_date.year
            if link.items[0].edition.publication_date
            else None
        )

        linkDict = dict(link)
        linkDict["link_id"] = link.id
        linkDict["work"] = dict(link.items[0].edition.work)
        linkDict["work"]["editions"] = [linkEdition]
        linkDict["work"]["editions"][0]["items"] = [linkItem]

        # Amend link to include /fulfill link if appropriate
        linkDict = APIUtils.replacePrivateLinkUrl(linkDict, request)

        return linkDict

    @classmethod
    def formatLanguages(cls, aggregations, counts=False):
        if counts:
            return sorted(
                [
                    {"language": lang.key, "work_total": lang.work_totals.doc_count}
                    for lang in aggregations.languages.languages.buckets
                ],
                key=lambda x: x["work_total"],
                reverse=True,
            )
        else:
            return sorted(
                [
                    {"language": lang.key}
                    for lang in aggregations.languages.languages.buckets
                ],
                key=lambda x: x["language"],
            )

    @classmethod
    def formatTotals(cls, response):
        return {r[0]: r[1] for r in response}

    @classmethod
    def flatten(cls, nested):
        try:
            iter(nested)
        except TypeError:
            yield nested
        else:
            for elem in nested:
                yield from cls.flatten(elem)

    @staticmethod
    def formatResponseObject(status, responseType, datablock, headers={}):
        response = jsonify(
            {
                "status": status,
                "timestamp": datetime.now(timezone.utc).replace(tzinfo=None),
                "responseType": responseType,
                "data": datablock,
            }
        )
        response.headers.extend(headers)
        return (response, status)

    @staticmethod
    def formatOPDS2Object(status, feedObject):
        return (jsonify(dict(feedObject)), status)

    @staticmethod
    def formatPipeDelimitedData(data, fields):
        if data is None:
            return None
        elif isinstance(data, list):
            dataList = list(filter(None, data))
            return [dict(zip(fields, d.split("|"))) for d in dataList]
        else:
            return dict(zip(fields, data.split("|")))

    @staticmethod
    def validatePassword(password, hash, salt):
        # encrypt password (with the same salt/key with which it was encrypted)
        password = password.encode("utf-8")
        hashedPassword = scrypt(password, salt=salt, n=2**14, r=8, p=1)

        return hashedPassword == hash

    @staticmethod
    def generate_presigned_url(s3_client, client_method, method_parameters, expires_in):
        """
        Generate a presigned Amazon S3 URL that can be used to perform an action.

        :param s3_client: A Boto3 Amazon S3 client.
        :param client_method: The name of the client method that the URL performs.
        :param method_parameters: The parameters of the specified client method.
        :param expires_in: The number of seconds the presigned URL is valid for.
        :return: The presigned URL.
        """
        try:
            url = s3_client.generate_presigned_url(
                ClientMethod=client_method,
                Params=method_parameters,
                ExpiresIn=expires_in,
            )
            logger.info("Got presigned URL: %s", url)
        except ClientError:
            logger.exception(
                "Couldn't get a presigned URL for client method '%s'.", client_method
            )
            raise
        return url

    @staticmethod
    def getPresignedUrlFromObjectUrl(client, url):
        """
        Given the URL of an S3 resource, generate a presigned Amazon S3 URL
        that can be used to access that resource. This function assumes S3
        URLs in the "virtual hosted bucket" style, not deprecated path style.

        :param s3_client: A Boto3 Amazon S3 client
        :param url: The URL of the desired resource
        """

        if "//" not in url:
            url = "//" + url

        parsedUrl = urlparse(url)

        if "s3" not in parsedUrl.hostname:
            raise ValueError("s3 helper function given a non-s3 or malformed URL")

        bucketName = parsedUrl.hostname.split(".")[0]
        objectKey = parsedUrl.path[1:]
        timeValid = 1000 * 30

        return APIUtils.generate_presigned_url(
            client, "get_object", {"Bucket": bucketName, "Key": objectKey}, timeValid
        )

    @staticmethod
    def replacePrivateLinkUrl(link, request):
        """
        Given a link object, return a link object with the url replaced if
        the link has flags indicating it should be fulfilled via /fulfill
        """
        if link["flags"].get("edd") or not link["flags"].get("nypl_login"):
            return link
        else:
            link["url"] = request.host + "/fulfill/" + str(link["link_id"])
        return link

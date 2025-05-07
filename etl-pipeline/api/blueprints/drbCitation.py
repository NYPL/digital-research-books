from flask import Blueprint, request, current_app
from ..db import DBClient
from ..utils import APIUtils
from ..validation_utils import is_valid_uuid
from logger import create_log
from datetime import date

logger = create_log(__name__)

citation = Blueprint("citation", __name__, url_prefix="/citation")

citation_set = {"mla", "apa", "chicago"}


@citation.route("/<uuid>", methods=["GET"])
def get_citation(uuid):
    logger.info(f"Getting citation for work id {uuid}")
    response_type = "citation"

    if not is_valid_uuid(uuid):
        return APIUtils.formatResponseObject(
            400, response_type, {"message": f"Work id {uuid} is invalid"}
        )

    try:
        citation_formats = request.args.get("format", default="")
        formats = set(citation_formats.split(","))

        if not formats.issubset(citation_set):
            return APIUtils.formatResponseObject(
                400, response_type, {"message": "Citation formats are invalid"}
            )

        with DBClient(current_app.config["DB_CLIENT"]) as db_client:
            work_to_cite = db_client.fetchSingleWork(uuid)

            if not work_to_cite:
                return APIUtils.formatResponseObject(
                    404, response_type, {"message": f"No work found with id {uuid}"}
                )

            output_citations = {}

            for format in formats:
                if format == "mla":
                    mla_citation = mla_generator(work_to_cite)
                    output_citations[format] = mla_citation

            return APIUtils.formatResponseObject(200, response_type, output_citations)
    except Exception:
        logger.exception(f"Unable to get citation for work with id {uuid}")
        return APIUtils.formatResponseObject(
            500,
            response_type,
            {"message": f"Unable to get citation for work with id {uuid}"},
        )


# TODO: refactor, apply snake_case, and move into a seperate citation utils file
def mla_generator(citationWork):
    if citationWork:
        # Made title lowercase for consistency when finding this info in the database
        if citationWork.title.lower() == "the bible":
            return mlaBibleGenerator(citationWork)

        if citationWork.editions[0].measurements:
            govDocOnly = filter(
                lambda x: (x["type"] == "government_document"),
                citationWork.editions[0].measurements,
            )
            if govDocOnly:
                listGovDocOnly = list(govDocOnly)
                if listGovDocOnly[0]["value"] == "1":
                    return mlaGovGenerator(citationWork)

        if len(citationWork.authors) == 0:
            return mlaNoAuthorGenerator(citationWork)

        elif len(citationWork.authors) == 1:
            return mlaOneAuthorGenerator(citationWork)

        elif len(citationWork.authors) == 2:
            return mlaTwoAuthorsGenerator(citationWork)

        elif len(citationWork.authors) >= 3:
            return mlaThreeAuthorsGenerator(citationWork)

        else:
            return "MLA Citation not avaliable"

    else:
        return "MLA Citation not avaliable"


# String that encompasses the publication info of a work
# * Why is 1900 the cutoff date for MLA citations?
# * Pre-1900: Use publication location in citation
# * Post-1900: Use publisher name in citation
def generatePubString(edition):
    pubString = ""
    if edition.publication_date < date(1900, 1, 1):
        pubString = f"{edition.publication_place}, \
                    {edition.publication_date.year}"
    else:
        pubString = f"{edition.publishers[0]['name']}, {edition.publication_date.year}"

    return pubString


def mlaBibleGenerator(citationWork):
    pubString = generatePubString(citationWork.editions[0])

    # Specific version info of Bible currently not displayed in database
    citationResponse = f"{citationWork.title}. Unknown version, {pubString}."

    return " ".join(citationResponse.split())


def mlaGovGenerator(citationWork):
    govName = citationWork.authors[0]["name"]
    pubString = generatePubString(citationWork.editions[0])

    # Gov Pubs have government name and agency/division as the author
    # * There's a period separating the Gov name and agency/division
    # * Must replace with comma instead to match MLA rule
    govNameAndAgency = govName.replace(".", ",", 1)

    citationResponse = f"{govNameAndAgency}. {citationWork.title}. {pubString}."

    return " ".join(citationResponse.split())


def nameSplit(name):
    listName = name.split(", ")
    lastName = listName[0]
    firstName = listName[1]
    return (firstName, lastName)


def pubTransEditString(citationWork):
    if citationWork.contributors:
        i = 0
        transName = None
        editorName = None

        while i < len(citationWork.contributors):
            if citationWork.contributors[i]["roles"] == "translator":
                transName = citationWork.contributors[i]["name"]
            if citationWork.contributors[i]["roles"] == "editor":
                editorName = citationWork.contributors[i]["name"]
            i += 1

        if transName and editorName:
            transFirstName, transLastName = nameSplit(transName)
            editorFirstName, editorLastName = nameSplit(editorName)
            return f"Translated by {transFirstName} {transLastName}, \
                    edited by {editorFirstName} {editorLastName}"

        elif transName:
            transFirstName, transLastName = nameSplit(transName)
            return f"Translated by {transFirstName} {transLastName}"

        elif editorName:
            editorFirstName, editorLastName = nameSplit(editorName)
            return f"edited by {editorFirstName} {editorLastName}"

    return None


def editionStatement(editStatement):
    return f"{editStatement}" if editStatement else None


def mlaNoAuthorGenerator(citationWork):
    transEditString = pubTransEditString(citationWork)
    pubString = generatePubString(citationWork.editions[0])
    editStatement = editionStatement(citationWork.editions[0].edition_statement)

    # return translated and/or editor work
    if transEditString:
        if transEditString.split(" ")[0] == "edited":
            citationResponse = f"{citationWork.title}, {transEditString}, {pubString}."
        else:
            citationResponse = f"{citationWork.title}. {transEditString}, {pubString}."

    # return multiple edition work
    elif editStatement:
        citationResponse = f"{citationWork.title}. {editStatement}, {pubString}."

    # return single edition work
    else:
        citationResponse = f"{citationWork.title}. {pubString}."

    return " ".join(citationResponse.split())


def mlaOneAuthorGenerator(citationWork):
    authName = citationWork.authors[0]["name"]
    transEditString = pubTransEditString(citationWork)
    pubString = generatePubString(citationWork.editions[0])
    editStatement = editionStatement(citationWork.editions[0].edition_statement)

    # return translated and/or editor work
    if transEditString:
        # Editor work has comma after title instead of period
        if transEditString.split(" ")[0] == "edited":
            citationResponse = (
                f"{authName}. {citationWork.title}, {transEditString}, {pubString}."
            )
        else:
            citationResponse = (
                f"{authName}. {citationWork.title}. {transEditString}, {pubString}."
            )

    # return multiple edition work
    elif editStatement:
        # Book by corporate author or organization
        if authName == citationWork.editions[0].publishers[0]["name"]:
            citationResponse = f"{citationWork.title}. {editStatement}, {pubString}."

        else:
            citationResponse = (
                f"{authName}. {citationWork.title}. {editStatement}, {pubString}."
            )

    # return single edition work
    else:
        # Book by corporate author or organization
        if authName == citationWork.editions[0].publishers[0]["name"]:
            citationResponse = f"{citationWork.title}. {pubString}."

        else:
            citationResponse = f"{authName}. {citationWork.title}. {pubString}."

    return " ".join(citationResponse.split())


def mlaTwoAuthorsGenerator(citationWork):
    authName1 = citationWork.authors[0]["name"]
    authName2 = citationWork.authors[1]["name"]
    listAuthName2 = authName2.split(", ")
    auth2LastName = listAuthName2[0]
    auth2FirstName = listAuthName2[1]

    transEditString = pubTransEditString(citationWork)
    pubString = generatePubString(citationWork.editions[0])
    editStatement = editionStatement(citationWork.editions[0].edition_statement)

    # return translated and/or editor work
    if transEditString:
        # Editor work has comma after title instead of period
        if transEditString.split(" ")[0] == "edited":
            citationResponse = f"{authName1}, and {auth2FirstName} {auth2LastName}. \
                                 {citationWork.title}, {transEditString}, {pubString}."

        else:
            citationResponse = f"{authName1}, and {auth2FirstName} {auth2LastName}. \
                                 {citationWork.title}. {transEditString}, {pubString}."
    # return multiple edition work
    elif editStatement:
        citationResponse = f"{authName1}, and {auth2FirstName} {auth2LastName}. \
                             {citationWork.title}. {editStatement}, {pubString}."

    # return single edition work
    else:
        citationResponse = f"{authName1}, and {auth2FirstName} {auth2LastName}. \
                             {citationWork.title}. {pubString}."

    return " ".join(citationResponse.split())


def mlaThreeAuthorsGenerator(citationWork):
    authName = citationWork.authors[0]["name"]
    transEditString = pubTransEditString(citationWork)
    pubString = generatePubString(citationWork.editions[0])
    editStatement = editionStatement(citationWork.editions[0].edition_statement)

    # return translated and/or editor work
    if transEditString:
        # Editor work has comma after title instead of period
        if transEditString.split(" ")[0] == "edited":
            citationResponse = f"{authName}, et al. {citationWork.title}, {transEditString}, {pubString}."
        else:
            citationResponse = f"{authName}, et al. {citationWork.title}. {transEditString}, {pubString}."

    # return multiple edition work
    elif editStatement:
        citationResponse = (
            f"{authName}, et al. {citationWork.title}. {editStatement}, {pubString}."
        )

    # return single edition work
    else:
        citationResponse = f"{authName}, et al. {citationWork.title}. {pubString}."

    return " ".join(citationResponse.split())

from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from model import Record, RecordState, Source, FileFlags, Part


# TOOO: map entire oclc bib record
def map_oclc_record(oclc_bib) -> Optional[Record]:
    identifiers = oclc_bib.get("identifier", {})
    oclc_number = identifiers.get("oclcNumber")
    parts = _get_parts(oclc_bib)

    if not oclc_number or not parts:
        return None

    creators = _get_creators(oclc_bib)
    authors = _get_authors(creators)
    contributors = _get_contributors(creators)

    return Record(
        uuid=uuid4(),
        frbr_status="complete",
        cluster_status=False,
        state=RecordState.EMBELLISHED.value,
        source=Source.OCLC_CATALOG.value,
        source_id=f"{oclc_number}|oclc",
        title=oclc_bib.get("title", {}).get("mainTitles", [{}])[0].get("text"),
        subjects=_map_subjects(oclc_bib),
        authors=_map_authors(authors),
        contributors=_map_contributors(contributors),
        publisher=_get_publishers(oclc_bib),
        dates=_get_dates(oclc_bib),
        languages=_get_languages(oclc_bib),
        identifiers=_get_identifiers(oclc_bib, oclc_number),
        has_part=parts,
        date_created=datetime.now(timezone.utc).replace(tzinfo=None),
        date_modified=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def _get_identifiers(oclc_bib, oclc_number):
    owi_number = oclc_bib.get("work", {}).get("id")
    edition_cluster = oclc_bib.get("editionCluster", {}).get("id")

    return [
        f"{id}|{id_type}"
        for id, id_type in [
            (oclc_number, "oclc"),
            (owi_number, "owi"),
            (edition_cluster, "oec"),
        ]
        if id is not None
    ]


def _get_publishers(oclc_bib):
    publishers = oclc_bib.get("publishers")

    if not publishers:
        return None

    return [
        f"{publisher_name}||"
        for publisher in publishers
        if (publisher_name := publisher.get("publisherName", {}).get("text"))
    ]


def _get_dates(oclc_bib):
    dates = oclc_bib.get("date")

    if not dates:
        return None

    publication_date = dates.get("publicationDate")

    if not publication_date:
        return None

    return [f"{publication_date}|publication_date"]


def _get_languages(oclc_bib):
    languages = oclc_bib.get("language")

    if not languages:
        return None

    item_language = languages.get("itemLanguage")

    if not item_language:
        return None

    return [f"||{item_language}"]


def _get_parts(oclc_bib):
    digital_access_locations = oclc_bib.get("digitalAccessAndLocations")

    if not digital_access_locations:
        return None

    return [
        str(
            Part(
                index=1,
                url=uri,
                source="oclc",
                file_type="text/html",
                flags=str(FileFlags(embed=True)),
            )
        )
        for digital_access_location in digital_access_locations
        if (uri := digital_access_location.get("uri"))
    ]


def _get_creators(oclc_bib):
    if not oclc_bib.get("contributor"):
        return None

    return list(
        filter(
            lambda creator: creator.get("secondName") or creator.get("firstName"),
            oclc_bib.get("contributor", {}).get("creators", []),
        )
    )


def _get_authors(creators):
    if not creators:
        return None

    return list(
        filter(
            lambda creator: creator.get("isPrimary", False) or _is_author(creator),
            creators,
        )
    )


def _get_contributors(creators):
    if not creators:
        return None

    return list(
        filter(
            lambda creator: not creator.get("isPrimary", False)
            and not _is_author(creator),
            creators,
        )
    )


def _is_author(creator):
    for role in set(
        map(
            lambda relator: relator.get("term", "").lower(), creator.get("relators", [])
        )
    ):
        if "author" in role.lower() or "writer" in role.lower():
            return True

    return False


def _map_subjects(oclc_bib) -> list[str]:
    return [
        f"{subject_name}||{subject.get('vocabulary', '')}"
        for subject in oclc_bib.get("subjects", [])
        if (subject_name := subject.get("subjectName", {}).get("text"))
    ]


def _map_authors(authors) -> Optional[list[str]]:
    if not authors:
        return None

    return [
        f"{author_name}|||true"
        for author in authors
        if (author_name := _get_contributor_name(author))
    ]


def _map_contributors(contributors) -> Optional[list[str]]:
    if not contributors:
        return None

    return [
        f"{contributor_name}|||{', '.join(list(map(lambda relator: relator.get('term', ''), contributor.get('relators', []))))}"
        for contributor in contributors
        if (contributor_name := _get_contributor_name(contributor))
    ]


def _get_contributor_name(contributor) -> Optional[str]:
    first_name = _get_name(contributor.get("firstName"))
    second_name = _get_name(contributor.get("secondName"))

    if not first_name and not second_name:
        return None

    if first_name and second_name:
        return f"{second_name}, {first_name}"

    return f"{first_name or second_name}"


def _get_name(name_data) -> Optional[str]:
    if not name_data:
        return None

    return name_data.get("text") or name_data.get("romanizedText")

import json

from models.data.interaction_event import UsageType


def format_to_interaction_event(data, referrer_url):
    publication_year = _pull_publication_year(data)
    usage_type = _determine_usage(data)
    isbns = [
        identifier.split("|")[0]
        for identifier in data.identifiers
        if "isbn" in identifier
    ]
    oclc_numbers = [
        identifier.split("|")[0]
        for identifier in data.identifiers
        if "oclc" in identifier
    ]

    authors = [author.split("|")[0] for author in data.authors]
    disciplines = [subject.split("|")[0] for subject in data.subjects or []]
    search_col = _parse_has_part(data)

    return {
        "title": data.title,
        "publication_year": publication_year,
        "book_id": f"{referrer_url}edition/{data.id}",
        "usage_type": usage_type.value,
        "isbns": ", ".join(isbns),
        "oclc_numbers": ", ".join(oclc_numbers),
        "authors": "; ".join(authors),
        "disciplines": ", ".join(disciplines),
        "search_col": search_col,
        "accessed": data.accessed,
    }


def _parse_has_part(data):
    parsed = []
    for part in data.has_part:
        split_part = part.split("|")
        parsed.append(split_part[1])

    return ",".join(parsed)


def _pull_publication_year(data):
    for date in data.dates:
        if "publication" in date["type"]:
            return date["date"]
    return None


def _determine_usage(match_data):
    if match_data.has_part:
        flags = [_load_flags(tuple(link.split("|"))[4]) for link in match_data.has_part]

        if any(flag.get("nypl_login", False) for flag in flags):
            return UsageType.LIMITED_ACCESS

        has_read_flag = any(
            flag.get("embed", False) or flag.get("reader", False) for flag in flags
        )
        has_download_flag = any(flag.get("download", False) for flag in flags)

        if has_read_flag and has_download_flag:
            return UsageType.FULL_ACCESS

    return UsageType.VIEW_ACCESS


def _load_flags(flag_string):
    try:
        flags = json.loads(flag_string)
        return flags if isinstance(flags, dict) else {}
    except json.decoder.JSONDecodeError as e:
        raise S3LogParsingError(e.msg)


class S3LogParsingError(Exception):
    def __init__(self, message=None):
        self.message = message

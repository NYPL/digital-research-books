from dataclasses import dataclass
from enum import Enum


class InteractionType(Enum):
    DOWNLOAD = "Download"
    VIEW = "View"


class UsageType(Enum):
    FULL_ACCESS = "Full Access"
    LIMITED_ACCESS = "Limited Access"
    VIEW_ACCESS = "View Access"


@dataclass(init=True, repr=True)
class InteractionEvent:
    country: str | None
    title: str
    book_id: str
    authors: str
    isbns: str
    oclc_numbers: str | None
    publication_year: str | None
    disciplines: str | None
    usage_type: str
    timestamp: str | None

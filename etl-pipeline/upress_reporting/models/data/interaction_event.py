from dataclasses import dataclass
from enum import Enum
from typing import Optional


class InteractionType(Enum):
    DOWNLOAD = "Download"
    VIEW = "View"


class UsageType(Enum):
    FULL_ACCESS = "Full Access"
    LIMITED_ACCESS = "Limited Access"
    VIEW_ACCESS = "View Access"


@dataclass(init=True, repr=True)
class InteractionEvent:
    country: Optional[str]
    title: str
    book_id: str
    authors: str
    isbns: str
    oclc_numbers: Optional[str]
    publication_year: Optional[str]
    disciplines: Optional[str]
    usage_type: str
    timestamp: Optional[str]

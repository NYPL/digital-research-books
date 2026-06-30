"""Shared types and value objects"""

from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Literal, Optional, Union, List, Iterator, Callable, Tuple
from typing_extensions import TypedDict


@dataclass
class Snippet:
    """Relevant snippet data as needed by frontend"""

    text: str
    item_id: Optional[int]  # item id used to link to a physical copy of a digital book
    chunk_score: Optional[float]
    start_page: Optional[int] = None
    end_page: Optional[int] = None


@dataclass(kw_only=True)
class BaseEditionResult:
    """Common base for all edition search result value objects."""

    edition_id: int
    chunk_hits: list
    snippets: List[Snippet] = field(default_factory=list)


@dataclass(kw_only=True)
class ContentSearchResult(BaseEditionResult):
    """In-book (single-edition) content search result."""

    frbr_fields: Dict


@dataclass(kw_only=True)
class CatalogSearchResult(BaseEditionResult):
    """Catalog (multi-edition) search result value object."""

    orm_work: Any
    orm_edition: Any
    agg_score: float
    barcode: Optional[str] = None

"""Pydantic model for the TurboPuffer filter syntax used in catalog/content search."""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import Field, RootModel

FilterField = Literal[
    "text",
    "subject",
    "title",
    "author",
    "language",
    "publication_date",
]

FilterOperator = Literal[
    # Equality
    "Eq",
    "Contains",
    "ContainsAny",
    # Comparison
    "Lt",
    "Lte",
    "Gt",
    "Gte",
    # Word-token
    "ContainsAllTokens",
    "ContainsTokenSequence",
    "ContainsAnyToken",
]

# A leaf condition: [field, operator, value]
# value is str | None | list[str] covering all documented operator value types.
Condition = tuple[FilterField, FilterOperator, Union[str, None, list[str]]]


class Filter(RootModel):
    """Recursive model for a TurboPuffer filter expression.

    Valid shapes:
      - Condition : [field, operator, value]
      - And       : ["And", [Filter, ...]]
      - Or        : ["Or",  [Filter, ...]]
      - Not       : ["Not", Filter]
    """

    root: Annotated[
        Union[
            tuple[Literal["And"], Annotated[list[Filter], Field(min_length=2)]],
            tuple[Literal["Or"], Annotated[list[Filter], Field(min_length=2)]],
            tuple[Literal["Not"], Filter],
            Condition,
        ],
        Field(
            union_mode="left_to_right"
        ),  # Checks for match in order of union definition: And > Or > ....
    ]


Filter.model_rebuild()  # allows recursive model definition by resolving forward reference.

"""
Unit tests validating the XML-like output of format_search_results() against
a Pydantic schema.

format_search_results() / display_book() (api/assistant/agent.py) build their
output via raw string concatenation with NO XML-escaping of book titles,
author names, or chunk text. Real book text may contain '&', '<', or '>',
which breaks strict XML parsing (result_reason.py already tolerates this,
see its `except ET.XMLSyntaxError` guard).

We parse with lxml's lenient HTML parser (recover=True) instead of a strict
XML parser here, matching how a permissive consumer would need to read this
output. Verified empirically (see the last two tests):
  - bare '&' and bare '>' survive recovery intact.
  - a bare '<' does NOT just corrupt the tag-like substring after it -- it
    silently truncates ALL text in that element from the '<' onward. This
    is a real, likely-common failure mode for OCR'd or excerpted book text
    (stray angle brackets, "<3", inequality signs, etc.), not just text that
    happens to collide with one of this schema's own tag names.
"""

from types import SimpleNamespace
from typing import List, Optional

from lxml import etree
from pydantic import BaseModel
import pytest

from api.assistant.agent import format_search_results
from api.assistant.types import CatalogSearchResult, ContentSearchResult


class ChunkXML(BaseModel):
    item_id: int
    page: str
    text: str


class EditionXML(BaseModel):
    edition_id: int
    barcode: Optional[str] = None
    title: str
    authors: str
    publisher: str
    date: str
    subjects: str
    language: str
    chunks: List[ChunkXML]


class SearchResultsXML(BaseModel):
    query: Optional[str] = None
    search_tool_call_id: Optional[str] = None
    editions: List[EditionXML]


def parse_search_results(xml_str: str) -> SearchResultsXML:
    """
    Parse format_search_results() output with a lenient HTML parser
    (recover=True) -- see module docstring for why a strict XML parser isn't
    used -- then validate the parsed structure against SearchResultsXML.
    """
    root = etree.fromstring(xml_str, parser=etree.HTMLParser(recover=True))
    search_results_el = root.find(".//search_results")
    assert search_results_el is not None, f"No <search_results> found in:\n{xml_str}"

    editions = []
    for edition_el in search_results_el.findall("edition"):
        chunks = []
        chunks_el = edition_el.find("chunks")
        if chunks_el is not None:
            for chunk_el in chunks_el.findall("chunk"):
                chunks.append(
                    {
                        "item_id": chunk_el.findtext("item_id"),
                        "page": chunk_el.findtext("page"),
                        "text": chunk_el.findtext("text"),
                    }
                )
        editions.append(
            {
                "edition_id": edition_el.findtext("edition_id"),
                "barcode": edition_el.findtext("barcode"),
                "title": edition_el.findtext("title"),
                "authors": edition_el.findtext("authors"),
                "publisher": edition_el.findtext("publisher"),
                "date": edition_el.findtext("date"),
                "subjects": edition_el.findtext("subjects"),
                "language": edition_el.findtext("language"),
                "chunks": chunks,
            }
        )

    return SearchResultsXML.model_validate(
        {
            "query": search_results_el.findtext("query"),
            "search_tool_call_id": search_results_el.findtext("search_tool_call_id"),
            "editions": editions,
        }
    )


def make_catalog_result(
    edition_id=1,
    barcode="00000000000001",
    title="A Tale of Two Cities",
    authors=("Charles Dickens",),
    subjects=("Fiction",),
    languages=("English",),
    publishers=("Chapman and Hall",),
    pub_date="1859",
    chunk_texts=("It was the best of times.",),
):
    orm_work = SimpleNamespace(
        title=title,
        authors=[{"name": a} for a in authors],
        subjects=[{"heading": s} for s in subjects],
    )
    orm_edition = SimpleNamespace(
        publication_date=pub_date,
        languages=[{"language": lang} for lang in languages],
        publishers=[{"name": p} for p in publishers],
    )
    chunk_hits = [
        {"text": t, "item_id": i + 1, "start_page": i + 1, "end_page": i + 1}
        for i, t in enumerate(chunk_texts)
    ]
    return CatalogSearchResult(
        edition_id=edition_id,
        barcode=barcode,
        orm_work=orm_work,
        orm_edition=orm_edition,
        agg_score=0.9,
        chunk_hits=chunk_hits,
    )


class TestFormatSearchResultsSchema:
    def test_catalog_result_matches_schema(self):
        result = make_catalog_result()
        xml_str = format_search_results([result], as_str=True)

        parsed = parse_search_results(xml_str)

        assert len(parsed.editions) == 1
        edition = parsed.editions[0]
        assert edition.edition_id == 1
        assert edition.barcode == "00000000000001"
        assert edition.title == "A Tale of Two Cities"
        assert edition.authors == "Charles Dickens"
        assert len(edition.chunks) == 1
        assert edition.chunks[0].text.strip() == "It was the best of times."

    def test_multiple_editions_and_chunks_matches_schema(self):
        result_1 = make_catalog_result(
            edition_id=1, barcode="1" * 14, chunk_texts=("chunk one", "chunk two")
        )
        result_2 = make_catalog_result(
            edition_id=2, barcode="2" * 14, title="Other Book"
        )
        xml_str = format_search_results([result_1, result_2], as_str=True)

        parsed = parse_search_results(xml_str)

        assert [e.edition_id for e in parsed.editions] == [1, 2]
        assert len(parsed.editions[0].chunks) == 2
        assert parsed.editions[1].title == "Other Book"

    def test_content_search_result_matches_schema(self):
        frbr_fields = {
            "title": "Moby Dick",
            "author_names": "Herman Melville",
            "subject_list": "Whaling",
            "pub_date": "1851",
            "publisher_names": "Harper and Brothers",
            "language_list": "English",
        }
        result = ContentSearchResult(
            edition_id=5,
            frbr_fields=frbr_fields,
            chunk_hits=[
                {
                    "text": "Call me Ishmael.",
                    "item_id": 1,
                    "start_page": 1,
                    "end_page": 1,
                }
            ],
        )
        xml_str = format_search_results([result], as_str=True)

        parsed = parse_search_results(xml_str)

        assert len(parsed.editions) == 1
        assert parsed.editions[0].barcode is None
        assert parsed.editions[0].title == "Moby Dick"

    def test_ampersand_and_greater_than_are_recovered(self):
        """
        Bare '&' and '>' characters (unescaped by display_book()) are
        preserved as literal text by the lenient HTML parser -- unlike a
        bare '<' (see test_less_than_truncates_remaining_text below).
        """
        result = make_catalog_result(
            title="Smith & Sons",
            chunk_texts=("Profits were up (5 > 3) for Smith & Sons.",),
        )
        xml_str = format_search_results([result], as_str=True)

        parsed = parse_search_results(xml_str)

        assert parsed.editions[0].title == "Smith & Sons"
        assert (
            parsed.editions[0].chunks[0].text.strip()
            == "Profits were up (5 > 3) for Smith & Sons."
        )

    @pytest.mark.xfail(
        reason=(
            "display_book() does not XML-escape chunk text. A bare '<' "
            "character is not just mis-nested -- the lenient HTML parser "
            "silently drops ALL remaining text in that element from the "
            "'<' onward, even when what follows doesn't match any real "
            "schema tag name (e.g. plain inequality text)."
        )
    )
    def test_less_than_truncates_remaining_text(self):
        result = make_catalog_result(
            chunk_texts=("Value is 5 < 10 and that matters.",),
        )
        xml_str = format_search_results([result], as_str=True)

        parsed = parse_search_results(xml_str)

        assert (
            parsed.editions[0].chunks[0].text.strip()
            == "Value is 5 < 10 and that matters."
        )

    @pytest.mark.xfail(
        reason=(
            "display_book() does not XML-escape chunk text. When the text "
            "contains a substring that looks like one of the schema's own "
            "tags (e.g. '<edition>'), the lenient HTML parser opens a real "
            "nested element instead of treating it as literal text, and "
            "everything after it in that text node is dropped."
        )
    )
    def test_text_containing_schema_tag_name_is_not_dropped(self):
        result = make_catalog_result(
            chunk_texts=("The chapter titled <edition> discusses early printings.",),
        )
        xml_str = format_search_results([result], as_str=True)

        parsed = parse_search_results(xml_str)

        assert (
            "The chapter titled <edition> discusses early printings."
            in parsed.editions[0].chunks[0].text
        )

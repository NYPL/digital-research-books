from lxml import etree
import pytest

from api.assistant.agent import SEARCH_RESULTS_SCHEMA, format_search_results
from api.assistant.types import ContentSearchResult

from .conftest import make_catalog_search_result


def parse_and_validate(xml_str: str):
    """
    Parse format_search_results() output  then validate the
    <search_results> subtree against SEARCH_RESULTS_SCHEMA. (with a lenient HTML parser
    (recover=True) to allow XML special characters within element text)

      Returns the
    validated <search_results> element for further inspection.
    """
    root = etree.fromstring(xml_str, parser=etree.HTMLParser(recover=True))
    search_results_el = root.find(".//search_results")
    assert search_results_el is not None, f"No <search_results> found in:\n{xml_str}"
    SEARCH_RESULTS_SCHEMA.assertValid(search_results_el)
    return search_results_el


class TestFormatSearchResultsSchema:
    """
    Test that format_search_results outputs the expected schema.

    NOTE: We can avoid testing XML special characters in element text edge cases
    because text escaping and parsing is handled via regex for
    format_search_result() output, so text loss due to xml parsing in recovery
    mode is out of scope.
    """

    def test_catalog_result_matches_schema(self):
        result = make_catalog_search_result()
        xml_str = format_search_results([result])

        search_results_el = parse_and_validate(xml_str)

        editions = search_results_el.findall("edition")
        assert len(editions) == 1
        edition = editions[0]
        assert edition.findtext("edition_id") == "1"
        assert edition.findtext("barcode") == "00000000000001"
        assert edition.findtext("title") == "A Tale of Two Cities"
        assert edition.findtext("authors") == "Charles Dickens"
        chunks = edition.find("chunks").findall("chunk")
        assert len(chunks) == 1
        assert chunks[0].findtext("text").strip() == "It was the best of times."

    def test_multiple_editions_and_chunks_matches_schema(self):
        result_1 = make_catalog_search_result(
            edition_id=1, barcode="1" * 14, chunk_texts=("chunk one", "chunk two")
        )
        result_2 = make_catalog_search_result(
            edition_id=2, barcode="2" * 14, title="Other Book"
        )
        xml_str = format_search_results([result_1, result_2])

        search_results_el = parse_and_validate(xml_str)

        editions = search_results_el.findall("edition")
        assert [e.findtext("edition_id") for e in editions] == ["1", "2"]
        assert len(editions[0].find("chunks").findall("chunk")) == 2
        assert editions[1].findtext("title") == "Other Book"

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
        xml_str = format_search_results([result])

        search_results_el = parse_and_validate(xml_str)

        editions = search_results_el.findall("edition")
        assert len(editions) == 1
        assert editions[0].findtext("barcode") is None
        assert editions[0].findtext("title") == "Moby Dick"


class TestFormatSearchResult:
    def test_known_element_in_text_is_escaped_in_raw_output(self):
        """
        Test that chunk text containing a literal known-element tag is XML-escaped
        in place, rather than left as-is.
        """
        result = make_catalog_search_result(
            chunk_texts=("A chapter ends with </chunk> right here.",),
        )
        xml_str = format_search_results([result])

        assert "</chunk> right here." not in xml_str
        assert "&lt;/chunk&gt; right here." in xml_str
        assert "NOTE:" in xml_str

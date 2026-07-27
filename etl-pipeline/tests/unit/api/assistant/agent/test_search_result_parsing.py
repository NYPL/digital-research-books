"""
Unit tests for functions that parse the output of format_search_results()
(e.g. get_result_count() and find_result_by_barcode())
"""

from api.assistant.agent import (
    find_result_by_barcode,
    format_search_results,
    get_result_count,
)

from .conftest import make_catalog_search_result


class TestGetResultCount:
    def test_zero_editions(self):
        assert get_result_count(format_search_results([])) == 0

    def test_counts_multiple_editions(self):
        results = [
            make_catalog_search_result(edition_id=i, barcode=str(i) * 14)
            for i in range(1, 4)
        ]
        xml_str = format_search_results(results)

        assert get_result_count(xml_str) == 3

    def test_does_not_count_escaped_edition_tag_in_text(self):
        """
        A chunk whose text literally contains "<edition>" gets escaped by
        format_search_results() (see its NOTE), so it must not inflate the
        real structural edition count.
        """
        results = [
            make_catalog_search_result(edition_id=1, barcode="1" * 14),
            make_catalog_search_result(
                edition_id=2,
                barcode="2" * 14,
                chunk_texts=("See also the <edition> discussed on page 12.",),
            ),
        ]
        xml_str = format_search_results(results)
        assert "NOTE:" in xml_str

        assert get_result_count(xml_str) == 2


class TestFindResultByBarcode:
    def test_finds_correct_edition_among_multiple(self):
        """
        Regression test for the bug described in the module docstring:
        searching for the second edition's barcode must return only that
        edition's block, not both editions concatenated.
        """
        result_1 = make_catalog_search_result(
            edition_id=1, barcode="1" * 14, title="Book One"
        )
        result_2 = make_catalog_search_result(
            edition_id=2, barcode="2" * 14, title="Book Two"
        )
        xml_str = format_search_results([result_1, result_2])

        block = find_result_by_barcode(xml_str, "2" * 14)

        assert block is not None
        assert get_result_count(block) == 1
        assert "<edition_id>2</edition_id>" in block
        assert "Book Two" in block
        assert "<edition_id>1</edition_id>" not in block
        assert "Book One" not in block

    def test_finds_correct_edition_when_target_is_first(self):
        result_1 = make_catalog_search_result(
            edition_id=1, barcode="1" * 14, title="Book One"
        )
        result_2 = make_catalog_search_result(
            edition_id=2, barcode="2" * 14, title="Book Two"
        )
        xml_str = format_search_results([result_1, result_2])

        block = find_result_by_barcode(xml_str, "1" * 14)

        assert block is not None
        assert get_result_count(block) == 1
        assert "<edition_id>1</edition_id>" in block
        assert "<edition_id>2</edition_id>" not in block

    def test_returns_none_when_barcode_not_found(self):
        result = make_catalog_search_result(barcode="1" * 14)
        xml_str = format_search_results([result])

        assert find_result_by_barcode(xml_str, "9" * 14) is None

    def test_returns_none_for_empty_results(self):
        xml_str = format_search_results([])

        assert find_result_by_barcode(xml_str, "1" * 14) is None

    def test_does_not_match_escaped_edition_text_as_a_real_block(self):
        """
        A chunk containing text that looks like a fake nested edition/barcode
        pair is escaped by format_search_results(), so searching for that
        fake barcode must not match it.
        """
        result = make_catalog_search_result(
            edition_id=1,
            barcode="1" * 14,
            chunk_texts=(
                "The catalog record read <edition><barcode>99999999999999"
                "</barcode></edition> in an earlier printing.",
            ),
        )
        xml_str = format_search_results([result])

        assert find_result_by_barcode(xml_str, "9" * 14) is None
        assert find_result_by_barcode(xml_str, "1" * 14) is not None

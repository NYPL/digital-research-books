"""Tests for rank fusion strategies."""

import pytest
from api.assistant.search.rankfusers import ReciprocalRankFuser, ScoredHit
from vector_indexing.core.types import ChunkDocument, BookMetadata


def make_chunk(doc_id: str, book_id: str = "1") -> ChunkDocument:
    """Create a minimal ChunkDocument for testing."""
    return ChunkDocument(
        doc_id=doc_id,
        text=f"Text for {doc_id}",
        barcode="test_barcode",
        book_id=book_id,
        chunk_index=0,
        start_page=1,
        end_page=1,
        book_metadata=BookMetadata(
            edition_id=1,
            title="Test Book",
            author=["Test Author"],
            subject=["Test Subject"],
            publication_date="2024-01-01",
            language=["en"],
        ),
    )


def make_hit(doc_id: str, score: float = 0.5, book_id: str = "1") -> ScoredHit:
    """Create a ScoredHit tuple for testing."""
    return (make_chunk(doc_id, book_id), score)


class TestReciprocalRankFuser:
    """Tests for ReciprocalRankFuser."""

    def test_empty_input(self):
        """Empty input returns empty output."""
        fuser = ReciprocalRankFuser(k=60)
        result = fuser.fuse([])
        assert result == []

    def test_single_list(self):
        """Single list returns items with RRF scores."""
        fuser = ReciprocalRankFuser(k=60)
        hits = [make_hit("doc_1"), make_hit("doc_2"), make_hit("doc_3")]

        result = fuser.fuse([hits])

        # Should preserve order (rank 1, 2, 3)
        assert len(result) == 3
        assert result[0][0].doc_id == "doc_1"
        assert result[1][0].doc_id == "doc_2"
        assert result[2][0].doc_id == "doc_3"

        # Check RRF scores: 1/(k+rank)
        assert result[0][1] == pytest.approx(1.0 / (60 + 1))  # rank 1
        assert result[1][1] == pytest.approx(1.0 / (60 + 2))  # rank 2
        assert result[2][1] == pytest.approx(1.0 / (60 + 3))  # rank 3

    def test_two_lists_same_order(self):
        """Two lists with same order - scores accumulate."""
        fuser = ReciprocalRankFuser(k=60)
        list1 = [make_hit("doc_1"), make_hit("doc_2")]
        list2 = [make_hit("doc_1"), make_hit("doc_2")]

        result = fuser.fuse([list1, list2])

        assert len(result) == 2
        assert result[0][0].doc_id == "doc_1"
        assert result[1][0].doc_id == "doc_2"

        # Scores should be summed: 2 * 1/(k+rank)
        assert result[0][1] == pytest.approx(2.0 / (60 + 1))
        assert result[1][1] == pytest.approx(2.0 / (60 + 2))

    def test_two_lists_reversed_order(self):
        """Two lists with reversed order - middle ground wins."""
        fuser = ReciprocalRankFuser(k=60)
        list1 = [make_hit("doc_A"), make_hit("doc_B")]
        list2 = [make_hit("doc_B"), make_hit("doc_A")]

        result = fuser.fuse([list1, list2])

        # Both have same total score: 1/(k+1) + 1/(k+2)
        assert len(result) == 2
        expected_score = 1.0 / (60 + 1) + 1.0 / (60 + 2)
        assert result[0][1] == pytest.approx(expected_score)
        assert result[1][1] == pytest.approx(expected_score)

    def test_disjoint_lists(self):
        """Two lists with no overlap - all items appear."""
        fuser = ReciprocalRankFuser(k=60)
        list1 = [make_hit("doc_A"), make_hit("doc_B")]
        list2 = [make_hit("doc_C"), make_hit("doc_D")]

        result = fuser.fuse([list1, list2])

        assert len(result) == 4
        # All rank-1 items tie, all rank-2 items tie
        assert result[0][1] == pytest.approx(1.0 / (60 + 1))
        assert result[1][1] == pytest.approx(1.0 / (60 + 1))
        assert result[2][1] == pytest.approx(1.0 / (60 + 2))
        assert result[3][1] == pytest.approx(1.0 / (60 + 2))

    def test_partial_overlap(self):
        """Some documents appear in both lists, some in one only."""
        fuser = ReciprocalRankFuser(k=60)
        # doc_shared appears in both at rank 2 and rank 1
        list1 = [make_hit("doc_A"), make_hit("doc_shared")]
        list2 = [make_hit("doc_shared"), make_hit("doc_B")]

        result = fuser.fuse([list1, list2])

        assert len(result) == 3

        # doc_shared has highest score: rank 2 in list1 + rank 1 in list2
        shared_score = 1.0 / (60 + 2) + 1.0 / (60 + 1)
        assert result[0][0].doc_id == "doc_shared"
        assert result[0][1] == pytest.approx(shared_score)

        # doc_A and doc_B each have single-list scores
        single_scores = {
            result[1][0].doc_id: result[1][1],
            result[2][0].doc_id: result[2][1],
        }
        assert single_scores["doc_A"] == pytest.approx(1.0 / (60 + 1))
        assert single_scores["doc_B"] == pytest.approx(1.0 / (60 + 2))

    def test_k_parameter_affects_scores(self):
        """Different k values produce different score distributions."""
        hits = [make_hit("doc_1"), make_hit("doc_2")]

        fuser_low_k = ReciprocalRankFuser(k=1)
        fuser_high_k = ReciprocalRankFuser(k=100)

        result_low = fuser_low_k.fuse([hits])
        result_high = fuser_high_k.fuse([hits])

        # Lower k = higher scores and bigger gaps between ranks
        assert result_low[0][1] > result_high[0][1]

        # Gap between rank 1 and 2 is larger with lower k
        gap_low = result_low[0][1] - result_low[1][1]
        gap_high = result_high[0][1] - result_high[1][1]
        assert gap_low > gap_high

    def test_original_score_discarded(self):
        """Original scores are discarded in favor of RRF scores."""
        fuser = ReciprocalRankFuser(k=60)
        # High original score
        hit_high = make_hit("doc_high", score=0.99)
        # Low original score but same rank
        hit_low = make_hit("doc_low", score=0.01)

        result = fuser.fuse([[hit_high], [hit_low]])

        # Both get same RRF score since both are rank 1 in their list
        assert result[0][1] == result[1][1]
        assert result[0][1] == pytest.approx(1.0 / (60 + 1))

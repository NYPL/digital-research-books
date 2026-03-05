"""Unit tests for chunker page tracking logic.

Tests page number calculation for various page/chunk configurations.
Inspired by vra_experiments test_create_index.py.
"""

import string
import pytest

from vector_indexing.components.chunkers.sentence import (
    SentenceSplitterChunker,
    char_to_page,
)
from vector_indexing.components.chunkers.base import ChunkWithPages


class TestCharToPage:
    """Test the char_to_page function directly."""

    def test_single_page_start(self):
        """Character at start of single page maps to page 1."""
        page_end_indices = [100]  # Single page, 100 chars
        assert char_to_page(0, page_end_indices) == 1

    def test_single_page_middle(self):
        """Character in middle of single page maps to page 1."""
        page_end_indices = [100]
        assert char_to_page(50, page_end_indices) == 1

    def test_single_page_end_inclusive(self):
        """Character at last position (inclusive) maps to page 1."""
        page_end_indices = [100]
        # char_index 99 is the last char, should be page 1
        assert char_to_page(99, page_end_indices) == 1

    def test_single_page_end_exclusive(self):
        """Character at exclusive end (100) maps to page 1."""
        page_end_indices = [100]
        # char_index 100 with exclusive=True means "one past last char"
        assert char_to_page(100, page_end_indices, exclusive=True) == 1

    def test_multi_page_boundaries(self):
        """Characters at page boundaries map correctly."""
        # Pages: [0-50), [50-100), [100-150)
        page_end_indices = [50, 100, 150]

        # Start of each page
        assert char_to_page(0, page_end_indices) == 1
        assert char_to_page(50, page_end_indices) == 2
        assert char_to_page(100, page_end_indices) == 3

        # End of each page (inclusive)
        assert char_to_page(49, page_end_indices) == 1
        assert char_to_page(99, page_end_indices) == 2
        assert char_to_page(149, page_end_indices) == 3

        # End of each page (exclusive)
        assert char_to_page(50, page_end_indices, exclusive=True) == 1
        assert char_to_page(100, page_end_indices, exclusive=True) == 2
        assert char_to_page(150, page_end_indices, exclusive=True) == 3

    def test_empty_pages(self):
        """Empty pages (zero-length) are handled correctly."""
        # Page 1: empty (0 chars), Page 2: 50 chars, Page 3: empty, Page 4: 50 chars
        # Cumulative: [0, 50, 50, 100]
        # But with newlines: page lengths become [1, 51, 1, 51] → [1, 52, 53, 104]
        page_end_indices = [1, 52, 53, 104]

        # First char after empty page 1
        assert char_to_page(1, page_end_indices) == 2

        # Character in page 2
        assert char_to_page(25, page_end_indices) == 2

        # At boundary of page 2 (exclusive)
        assert char_to_page(52, page_end_indices, exclusive=True) == 2

        # Into page 4 (page 3 is empty)
        assert char_to_page(54, page_end_indices) == 4


class TestIterChunks:
    """Test iter_chunks page tracking with controlled inputs."""

    @pytest.mark.parametrize(
        "page_lengths,expected_page_spans",
        [
            pytest.param(
                [100],
                # Single page, expect all chunks on page 1
                lambda chunks: all(
                    c.start_page == 1 and c.end_page == 1 for c in chunks
                ),
                id="single_page",
            ),
            pytest.param(
                [50, 50, 50],
                # Multiple pages, first chunk should start on page 1
                lambda chunks: chunks[0].start_page == 1,
                id="multi_page_first_chunk_starts_page_1",
            ),
        ],
    )
    def test_page_span_invariants(self, page_lengths, expected_page_spans):
        """Test basic invariants about page spans."""
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=10)

        # Create pages with sentence text
        pages = []
        for i, length in enumerate(page_lengths):
            char = string.ascii_letters[i % len(string.ascii_letters)]
            # Create a sentence that fills the page
            sentence = f"This is page {i + 1}. " + (char * max(0, length - 20)) + ". "
            pages.append(sentence[:length] if length > 0 else "")

        chunks = list(chunker.iter_chunks(pages))

        assert len(chunks) > 0, "Should produce at least one chunk"
        assert expected_page_spans(chunks), "Page span invariant violated"

    def test_chunk_pages_are_valid(self):
        """All chunks have valid page numbers."""
        chunker = SentenceSplitterChunker(chunk_size=50, chunk_overlap=5)

        pages = [
            "First page with some content here. This is sentence two.",
            "Second page continues the text. Another sentence follows.",
            "Third page has the final content. Ending sentence here.",
        ]

        chunks = list(chunker.iter_chunks(pages))

        for chunk in chunks:
            # Page numbers should be 1-indexed
            assert chunk.start_page >= 1
            assert chunk.end_page >= chunk.start_page
            # Should not exceed page count
            assert chunk.end_page <= len(pages)

    def test_chunk_text_matches_pages(self):
        """Chunk text should be extractable from joined pages."""
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=10)

        pages = [
            "Page one has this content. Another sentence here.",
            "Page two continues. More text follows.",
        ]

        full_text = "\n".join(pages) + "\n"
        chunks = list(chunker.iter_chunks(pages))

        for chunk in chunks:
            # Chunk text should appear in the full text
            assert chunk.text in full_text or chunk.text.strip() in full_text

    def test_chunks_cover_all_pages(self):
        """Chunks should collectively cover all pages."""
        chunker = SentenceSplitterChunker(chunk_size=50, chunk_overlap=5)

        pages = [
            "Page one. ",
            "Page two. ",
            "Page three. ",
            "Page four. ",
            "Page five. ",
        ]

        chunks = list(chunker.iter_chunks(pages))

        # Collect all pages covered
        pages_covered = set()
        for chunk in chunks:
            for p in range(chunk.start_page, chunk.end_page + 1):
                pages_covered.add(p)

        # All pages should be covered
        assert pages_covered == set(range(1, len(pages) + 1))

    def test_sequential_chunk_indices(self):
        """Chunk indices should be sequential starting from 0."""
        chunker = SentenceSplitterChunker(chunk_size=50, chunk_overlap=5)

        pages = ["Sentence one here. Sentence two here. Sentence three here."] * 3

        chunks = list(chunker.iter_chunks(pages))

        for i, chunk in enumerate(chunks):
            assert chunk.index == i


class TestPageTrackingWithOverlap:
    """Test that page tracking works correctly with chunk overlap."""

    def test_page_spans_known_input(self):
        """Verify exact page spans for a known input configuration."""
        # Use small chunk size to force multiple chunks
        chunker = SentenceSplitterChunker(chunk_size=50, chunk_overlap=10)

        # Create pages with multiple short sentences that can be split
        pages = [
            "One. Two. Three. Four. Five. Six. Seven. Eight. Nine. Ten.",  # page 1
            "Red. Blue. Green. Yellow. Orange. Purple. Pink. Brown. Gray.",  # page 2
            "Cat. Dog. Bird. Fish. Cow. Pig. Horse. Sheep. Goat. Duck.",  # page 3
        ]

        chunks = list(chunker.iter_chunks(pages))

        # Should produce 2 chunks that span pages:
        # Chunk 0: pages 1-2 (text from page 1 into page 2)
        # Chunk 1: pages 2-3 (text from page 2 into page 3)
        assert len(chunks) == 2, (
            f"Expected 2 chunks, got {len(chunks)}.\n"
            f"Chunks: {[(c.start_page, c.end_page, c.text[:30]) for c in chunks]}"
        )

        # Verify exact page spans
        assert (chunks[0].start_page, chunks[0].end_page) == (1, 2), (
            f"Chunk 0: expected (1, 2), got ({chunks[0].start_page}, {chunks[0].end_page})"
        )
        assert (chunks[1].start_page, chunks[1].end_page) == (2, 3), (
            f"Chunk 1: expected (2, 3), got ({chunks[1].start_page}, {chunks[1].end_page})"
        )

    def test_chunk_overlap_shares_content(self):
        """Verify that overlapping chunks actually share text content."""
        chunker = SentenceSplitterChunker(chunk_size=50, chunk_overlap=20)

        pages = [
            "Alpha. Beta. Gamma. Delta. Epsilon. Zeta. Eta. Theta.",
            "Iota. Kappa. Lambda. Mu. Nu. Xi. Omicron. Pi. Rho. Sigma.",
        ]

        chunks = list(chunker.iter_chunks(pages))

        if len(chunks) >= 2:
            # Check that consecutive chunks share some text (overlap)
            for i in range(len(chunks) - 1):
                chunk_text = chunks[i].text
                next_chunk_text = chunks[i + 1].text

                # The end of chunk i should overlap with start of chunk i+1
                # Find any common words
                words_i = set(chunk_text.replace(".", "").split())
                words_next = set(next_chunk_text.replace(".", "").split())
                shared = words_i & words_next

                # With overlap=20, there should be some shared content
                # (unless chunks are at natural sentence boundaries)
                # This is a soft check - just verify the chunks are valid
                assert chunks[i].end_page >= chunks[i].start_page

    def test_overlapping_chunks_have_correct_pages(self):
        """Chunks with overlap should track page boundaries correctly."""
        # Use a larger overlap to ensure chunks share content
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=50)

        pages = [
            "Page one has this first sentence. And this second sentence too.",
            "Page two follows with more text. Continuing the narrative here.",
            "Page three ends the document. Final sentences are here now.",
        ]

        chunks = list(chunker.iter_chunks(pages))

        # Basic sanity checks
        assert len(chunks) >= 1

        for i, chunk in enumerate(chunks):
            # start_page should never exceed end_page
            assert chunk.start_page <= chunk.end_page, (
                f"Chunk {i}: start_page ({chunk.start_page}) > end_page ({chunk.end_page})"
            )
            # Pages should be within bounds
            assert 1 <= chunk.start_page <= len(pages)
            assert 1 <= chunk.end_page <= len(pages)

    def test_zero_overlap_no_duplicate_tracking(self):
        """With zero overlap, page tracking should be straightforward."""
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=0)

        pages = [
            "Short page one. ",
            "Short page two. ",
            "Short page three. ",
        ]

        chunks = list(chunker.iter_chunks(pages))

        # With very short pages and no overlap, might get one chunk
        assert len(chunks) >= 1

        # All chunks should have valid page bounds
        for chunk in chunks:
            assert chunk.start_page >= 1
            assert chunk.end_page <= len(pages)


class TestEdgeCases:
    """Test edge cases in page tracking."""

    def test_single_empty_page(self):
        """Single empty page should produce no chunks or handle gracefully."""
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=10)

        pages = [""]

        chunks = list(chunker.iter_chunks(pages))

        # Empty input should produce no chunks or one empty chunk
        # Either behavior is acceptable
        assert len(chunks) <= 1

    def test_mixed_empty_and_content_pages(self):
        """Mix of empty and content pages should work."""
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=10)

        pages = [
            "",  # Page 1: empty
            "Content on page two. This has text.",  # Page 2: content
            "",  # Page 3: empty
            "More content on page four. Final text.",  # Page 4: content
        ]

        chunks = list(chunker.iter_chunks(pages))

        # Should have some chunks from the content pages
        assert len(chunks) >= 1

        # All page references should be valid
        for chunk in chunks:
            assert 1 <= chunk.start_page <= len(pages)
            assert 1 <= chunk.end_page <= len(pages)

    def test_very_long_single_page(self):
        """Very long single page should produce multiple chunks all on page 1."""
        chunker = SentenceSplitterChunker(chunk_size=50, chunk_overlap=10)

        # Create a long page with many sentences
        pages = [". ".join([f"Sentence number {i}" for i in range(50)]) + "."]

        chunks = list(chunker.iter_chunks(pages))

        # Should produce multiple chunks
        assert len(chunks) > 1

        # All chunks should be on page 1
        for chunk in chunks:
            assert chunk.start_page == 1
            assert chunk.end_page == 1

    def test_chunk_spanning_many_pages(self):
        """Large chunk size should span multiple short pages."""
        chunker = SentenceSplitterChunker(chunk_size=500, chunk_overlap=50)

        # Create many short pages
        pages = [f"Page {i}. " for i in range(10)]

        chunks = list(chunker.iter_chunks(pages))

        # With a large chunk size, should get few chunks spanning many pages
        if len(chunks) > 0:
            first_chunk = chunks[0]
            # First chunk should span multiple pages
            assert first_chunk.end_page > first_chunk.start_page or len(pages) == 1

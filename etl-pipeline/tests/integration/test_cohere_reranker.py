"""Integration test for Cohere reranker.

This is a simple smoke test to verify the Cohere API integration works.
Requires COHERE_API_KEY environment variable to be set.
"""

import os

import pytest

from vector_indexing.components.rerankers.cohere import CohereReranker
from vector_indexing.core.types import BookMetadata, ChunkDocument


@pytest.fixture
def test_documents() -> list[ChunkDocument]:
    metadata = BookMetadata(
        edition_id=1,
        title="",
        author=[""],
        subject=[""],
        publication_date="2026",
        language=[""],
    )

    doc1 = ChunkDocument.create(
        barcode="test1",
        book_id="book1",
        chunk_index=0,
        text="Here is the house. It is green and white. It has a red door. It is very pretty.",
        start_page=1,
        end_page=1,
        book_metadata=metadata,
    )

    doc2 = ChunkDocument.create(
        barcode="test2",
        book_id="book2",
        chunk_index=0,
        text="Python is a popular programming language widely used for web development, data analysis, artificial intelligence, and scientific computing.",
        start_page=1,
        end_page=1,
        book_metadata=metadata,
    )

    doc3 = ChunkDocument.create(
        barcode="test3",
        book_id="book3",
        chunk_index=0,
        text="It was a bright cold day in April, and the clocks were striking thirteen.",
        start_page=1,
        end_page=1,
        book_metadata=metadata,
    )

    return [doc1, doc2, doc3]


def test_cohere_reranker_smoke(setup_env, test_documents):
    """Verify Cohere reranker returns ranked results."""
    if not os.environ.get("COHERE_API_KEY"):
        pytest.skip("COHERE_API_KEY not set")

    reranker = CohereReranker()

    results = reranker.rerank(
        query="programming language",
        documents=test_documents,
    )

    assert len(results) == len(test_documents)
    assert all(r.relevance_score >= 0 for r in results)
    print(results)
    assert results[0].document.book_id == "book2"

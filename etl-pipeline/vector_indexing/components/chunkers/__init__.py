"""Text chunking components."""

from vector_indexing.components.chunkers.base import TextChunker
from vector_indexing.components.chunkers.sentence import SentenceSplitterChunker

__all__ = [
    "TextChunker",
    "SentenceSplitterChunker",
]

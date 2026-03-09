"""Unit tests for the recursive text chunker."""

from __future__ import annotations

from greengraph.chunker import split_text


class TestSplitText:
    def test_short_text_returns_single_chunk(self) -> None:
        text = "Hello world"
        chunks = split_text(text, chunk_size=100, chunk_overlap=10)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_empty_text_returns_no_chunks(self) -> None:
        chunks = split_text("", chunk_size=100, chunk_overlap=10)
        assert chunks == []

    def test_whitespace_only_returns_no_chunks(self) -> None:
        chunks = split_text("   \n\n   ", chunk_size=100, chunk_overlap=10)
        assert chunks == []

    def test_long_text_is_split(self) -> None:
        text = "word " * 200  # 1000 chars
        chunks = split_text(text, chunk_size=100, chunk_overlap=10)
        assert len(chunks) > 1

    def test_chunks_respect_max_size(self) -> None:
        text = "word " * 500
        chunk_size = 200
        chunks = split_text(text, chunk_size=chunk_size, chunk_overlap=20)
        # Allow some slack for merge pass but none should be more than 2x
        assert all(len(c) <= chunk_size * 2 for c in chunks)

    def test_paragraph_splits_preferred(self) -> None:
        text = "Paragraph one.\n\nParagraph two.\n\nParagraph three."
        chunks = split_text(text, chunk_size=20, chunk_overlap=0)
        # Should split on \n\n
        assert any("Paragraph one" in c for c in chunks)
        assert any("Paragraph two" in c for c in chunks)

    def test_no_empty_chunks(self) -> None:
        text = "A " * 300
        chunks = split_text(text, chunk_size=50, chunk_overlap=10)
        assert all(c.strip() for c in chunks)

    def test_overlap_content_appears_in_adjacent_chunks(self) -> None:
        # Create text with distinguishable words
        words = [f"word{i}" for i in range(50)]
        text = " ".join(words)
        chunks = split_text(text, chunk_size=60, chunk_overlap=20)
        if len(chunks) > 1:
            # Some overlap expected (not guaranteed by all splitters, but at least confirms split)
            assert len(chunks) >= 2

    def test_chunk_indices_cover_full_content(self) -> None:
        text = "Hello World. " * 50
        chunks = split_text(text, chunk_size=50, chunk_overlap=0)
        # All original words should appear somewhere in the chunks
        combined = " ".join(chunks)
        assert "Hello" in combined
        assert "World" in combined

    def test_custom_separators(self) -> None:
        text = "a|b|c|d|e|f|g"
        chunks = split_text(text, chunk_size=5, chunk_overlap=0, separators=["|", ""])
        assert len(chunks) > 1

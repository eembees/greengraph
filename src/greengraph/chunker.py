"""Recursive character-based text chunker.

Splits text into overlapping chunks targeting `chunk_size` characters with
`chunk_overlap` characters of overlap between consecutive chunks. Prefers
splitting on paragraph boundaries, then sentence boundaries, then spaces.
"""

from __future__ import annotations

SPLIT_SEPARATORS = ["\n\n", "\n", ". ", "! ", "? ", " ", ""]


def split_text(
    text: str,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    separators: list[str] | None = None,
) -> list[str]:
    """Recursively split text into chunks of approximately `chunk_size` characters.

    Args:
        text: The input text to split.
        chunk_size: Target character length per chunk.
        chunk_overlap: Number of characters to overlap between chunks.
        separators: Priority-ordered list of separator strings to try.

    Returns:
        List of non-empty text chunks.
    """
    if separators is None:
        separators = SPLIT_SEPARATORS

    chunks = _split_recursive(text, chunk_size, chunk_overlap, separators)
    # Merge small chunks
    return _merge_chunks(chunks, chunk_size, chunk_overlap)


def _split_recursive(
    text: str,
    chunk_size: int,
    chunk_overlap: int,
    separators: list[str],
) -> list[str]:
    """Split text using the best available separator."""
    if len(text) <= chunk_size:
        return [text] if text.strip() else []

    # Find the highest-priority separator present in the text
    separator = ""
    remaining_separators: list[str] = []
    for i, sep in enumerate(separators):
        if sep == "" or sep in text:
            separator = sep
            remaining_separators = separators[i + 1 :]
            break

    if separator == "":
        # No separator found — hard split
        return _hard_split(text, chunk_size, chunk_overlap)

    splits = text.split(separator)
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for split in splits:
        split_with_sep = split + separator if separator else split
        split_len = len(split_with_sep)

        if current_len + split_len > chunk_size and current:
            # Flush current accumulation
            chunk_text = separator.join(current).strip()
            if chunk_text:
                chunks.append(chunk_text)
            # Keep overlap
            overlap_tokens: list[str] = []
            overlap_len = 0
            for token in reversed(current):
                if overlap_len + len(token) + len(separator) <= chunk_overlap:
                    overlap_tokens.insert(0, token)
                    overlap_len += len(token) + len(separator)
                else:
                    break
            current = overlap_tokens
            current_len = overlap_len

        current.append(split)
        current_len += split_len

    if current:
        chunk_text = separator.join(current).strip()
        if chunk_text:
            chunks.append(chunk_text)

    # Recursively split chunks that are still too large
    final: list[str] = []
    for chunk in chunks:
        if len(chunk) > chunk_size and remaining_separators:
            final.extend(_split_recursive(chunk, chunk_size, chunk_overlap, remaining_separators))
        else:
            final.append(chunk)
    return final


def _hard_split(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    """Fall back to hard character-level splitting when no separator is found."""
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        start += chunk_size - chunk_overlap
    return chunks


def _merge_chunks(chunks: list[str], chunk_size: int, chunk_overlap: int) -> list[str]:
    """Merge very small chunks into their neighbors to avoid tiny tail chunks."""
    if len(chunks) <= 1:
        return chunks

    merged: list[str] = []
    buffer = chunks[0]

    for chunk in chunks[1:]:
        if len(buffer) + len(chunk) + 1 <= chunk_size:
            buffer = buffer + " " + chunk
        else:
            merged.append(buffer.strip())
            buffer = chunk

    if buffer.strip():
        merged.append(buffer.strip())

    return merged

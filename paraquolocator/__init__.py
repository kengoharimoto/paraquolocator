"""paraquolocator — fuzzy parallel-passage and quotation detection for texts.

Public API
----------
>>> from paraquolocator import TextMatcher, load_buffer
>>> matcher = TextMatcher(score=70)
>>> source = load_buffer("base_text.txt")
>>> target = load_buffer("commentary.txt", mode="fixed", chunk_size=500)
>>> for hit in matcher.find_quotes(source, target):
...     print(hit)
"""

from .core import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_IGNORE_PATTERNS,
    DEFAULT_MIN_LENGTH,
    DEFAULT_SCORE,
    TextMatcher,
    load_buffer,
    load_ignore_patterns,
)

__all__ = [
    "TextMatcher",
    "load_buffer",
    "load_ignore_patterns",
    "DEFAULT_SCORE",
    "DEFAULT_MIN_LENGTH",
    "DEFAULT_CHUNK_SIZE",
    "DEFAULT_IGNORE_PATTERNS",
]

"""Core fuzzy-matching logic for parallel passage and quote detection.

This module provides the :class:`TextMatcher` class and buffer-loading helpers
used by the command-line tools.  It is also importable as a library for use
inside Jupyter notebooks or custom pipelines.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Generator, Iterator

from rapidfuzz import fuzz, process, utils

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

#: Default similarity threshold (0–100).  100 = identical, 0 = nothing shared.
DEFAULT_SCORE: int = 70

#: Lines shorter than this (in characters) are silently skipped.
DEFAULT_MIN_LENGTH: int = 30

#: Default chunk size used when loading a file in ``'fixed'`` mode.
DEFAULT_CHUNK_SIZE: int = 500

#: Built-in ignore patterns (IAST-transliterated Sanskrit colophon phrases
#: typical of Advaita Vedānta manuscripts).  Override via an ignore-patterns
#: file when using the CLI, or pass a custom list to :class:`TextMatcher`.
DEFAULT_IGNORE_PATTERNS: list[str] = [
    r"śrīmatparamahaṃsaparivrājak",
    r"śiṣyasya śrīmacchaṃkarabhagavat",
    r"-{6,}",
    r"_{4,}",
    r"={5,}",
    r"^\%",
    r"paramahaṃsaparivrājakācāry",
    r"pūjyapādaśiṣya",
    r"pratham.*dhyāya",
    r"pratham.*pāda",
    r"dvitīy.*dhyāy",
    r"(?:\. ){6,}",
    r"^\<[^>]*\>$",
    r"^ *\<rdg.*\<\/rdg\>$",
]


# ---------------------------------------------------------------------------
# Buffer loading
# ---------------------------------------------------------------------------

def load_buffer(
    path: str | Path,
    mode: str = "line",
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> list[str]:
    """Load a text file into a list of strings.

    Parameters
    ----------
    path:
        Path to the text file (UTF-8 encoding assumed).
    mode:
        How to split the file into segments:

        * ``'line'``  — one entry per newline-delimited line (default).
        * ``'danda'`` — split each line further at ``|`` characters
          (useful for Sanskrit verse divided by daṇḍas).
        * ``'fixed'`` — read the whole file as one string, replacing newlines
          with ``¶``, then cut into fixed-length chunks of *chunk_size*
          characters.  Useful when the target is a prose commentary where
          paragraph structure is not meaningful for matching.
    chunk_size:
        Character length of each chunk in ``'fixed'`` mode (ignored otherwise).

    Returns
    -------
    list[str]
        The segments ready for matching.

    Raises
    ------
    ValueError
        If *mode* is not one of the recognised values.
    FileNotFoundError
        If *path* does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    if mode == "line":
        with path.open(encoding="utf-8") as fh:
            return [line.replace("\t", "    ") for line in fh]

    if mode == "danda":
        segments: list[str] = []
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.replace("\t", "    ")
                segments.extend(line.split("|"))
        return segments

    if mode == "fixed":
        text = path.read_text(encoding="utf-8")
        text = text.replace("\n", "¶").replace("\t", "    ")
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]

    raise ValueError(
        f"Unknown mode {mode!r}. Choose 'line', 'danda', or 'fixed'."
    )


def load_ignore_patterns(path: str | Path) -> list[str]:
    """Load ignore patterns from a plain-text file.

    Each non-empty line that does not start with ``#`` is treated as a
    Python regular expression.

    Parameters
    ----------
    path:
        Path to the patterns file.

    Returns
    -------
    list[str]
        The patterns, ready to be passed to :class:`TextMatcher`.
    """
    patterns: list[str] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            patterns.append(line)
    return patterns


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class TextMatcher:
    """Fuzzy text matcher for detecting parallel passages and quotations.

    Parameters
    ----------
    score:
        Similarity cut-off (0–100).  Matches below this value are discarded.
    min_length:
        Segments shorter than this (characters) are skipped without comparison.
    ignore_patterns:
        List of regex strings.  Segments matching any of these are skipped.
        Defaults to :data:`DEFAULT_IGNORE_PATTERNS` when *None*.

    Examples
    --------
    >>> from paraquolocator.core import TextMatcher, load_buffer
    >>> matcher = TextMatcher(score=70)
    >>> source = load_buffer("base_text.txt")
    >>> target = load_buffer("commentary.txt", mode="fixed", chunk_size=500)
    >>> for hit in matcher.find_quotes(source, target):
    ...     print(hit)
    """

    def __init__(
        self,
        score: int = DEFAULT_SCORE,
        min_length: int = DEFAULT_MIN_LENGTH,
        ignore_patterns: list[str] | None = None,
    ) -> None:
        self.score = score
        self.min_length = min_length
        patterns = ignore_patterns if ignore_patterns is not None else DEFAULT_IGNORE_PATTERNS
        self._compiled: list[re.Pattern[str]] = [re.compile(p) for p in patterns]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_skip(self, segment: str) -> bool:
        """Return ``True`` if *segment* should be excluded from matching."""
        if len(segment) < self.min_length:
            return True
        return any(pat.search(segment) for pat in self._compiled)

    # ------------------------------------------------------------------
    # Public matching methods
    # ------------------------------------------------------------------

    def compare_lines(
        self,
        source: list[str],
        target: list[str],
    ) -> Iterator[dict]:
        """Find the single best match in *target* for each line in *source*.

        Uses :func:`rapidfuzz.fuzz.ratio` (full-string similarity), which
        works best when both texts are divided into comparable units
        (e.g. verse lines vs. verse lines).

        Parameters
        ----------
        source:
            Segments to search *from* (typically the shorter / query text).
        target:
            Segments to search *in*.

        Yields
        ------
        dict
            Keys: ``source_line``, ``source_text``, ``target_line``,
            ``target_text``, ``score``.
        """
        for i, segment in enumerate(source, start=1):
            if self._should_skip(segment):
                continue
            result = process.extractOne(
                segment,
                target,
                scorer=fuzz.ratio,
                processor=utils.default_process,
                score_cutoff=self.score,
            )
            if result is not None:
                match_text, match_score, match_idx = result
                yield {
                    "source_line": i,
                    "source_text": segment.rstrip(),
                    "target_line": match_idx + 1,
                    "target_text": match_text.rstrip(),
                    "score": match_score,
                }

    def find_quotes(
        self,
        source: list[str],
        target: list[str],
        progress: bool = True,
    ) -> Iterator[dict]:
        """Find where segments of *source* appear embedded inside *target*.

        Uses :func:`rapidfuzz.fuzz.partial_ratio_alignment`, which detects
        the best *sub-string* alignment.  Ideal for locating verse lines
        that are quoted inside prose commentaries.

        Parameters
        ----------
        source:
            Segments to search *from* (e.g. verse lines of a base text).
        target:
            Segments to search *in* (e.g. fixed-length chunks of a commentary).
        progress:
            If ``True`` (default), write a percentage progress indicator to
            *stderr*.

        Yields
        ------
        dict
            Keys: ``source_line``, ``source_text``, ``target_chunk``,
            ``matched_excerpt``, ``target_text``, ``score``.
        """
        total = len(source)
        for i, segment in enumerate(source, start=1):
            if progress:
                pct = i / total * 100
                end = "\n" if i % 20 == 0 else " "
                sys.stderr.write(f"{pct:.2f}{end}")
                sys.stderr.flush()

            if self._should_skip(segment):
                continue

            for j, chunk in enumerate(target, start=1):
                if self._should_skip(chunk):
                    continue
                result = fuzz.partial_ratio_alignment(
                    segment,
                    chunk,
                    processor=utils.default_process,
                    score_cutoff=self.score,
                )
                if result is not None and result.score > 0:
                    yield {
                        "source_line": i,
                        "source_text": segment.rstrip(),
                        "target_chunk": j,
                        "matched_excerpt": chunk[result.dest_start : result.dest_end],
                        "target_text": chunk.rstrip(),
                        "score": result.score,
                    }

        if progress:
            sys.stderr.write("\n")
            sys.stderr.flush()

#!/usr/bin/env python3
"""Detect parallel passages and quotations between texts using fuzzy string matching.

Usage:
    python paraquolocator.py parallel SOURCE TARGET [options]
    python paraquolocator.py quotes   SOURCE TARGET [options]

Output is tab-separated (TSV) on stdout; progress messages go to stderr.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator, Sequence

from rapidfuzz import fuzz, process, utils

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_SCORE: int = 70
DEFAULT_MIN_LENGTH: int = 30
DEFAULT_CHUNK_SIZE: int = 500
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

def load_buffer(path: str | Path, mode: str = "line", chunk_size: int = DEFAULT_CHUNK_SIZE) -> list[str]:
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
                segments.extend(line.replace("\t", "    ").split("|"))
        return segments
    if mode == "fixed":
        text = path.read_text(encoding="utf-8")
        text = text.replace("\n", "¶").replace("\t", "    ")
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
    raise ValueError(f"Unknown mode {mode!r}. Choose 'line', 'danda', or 'fixed'.")


def load_ignore_patterns(path: str | Path) -> list[str]:
    patterns: list[str] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            patterns.append(line)
    return patterns

# ---------------------------------------------------------------------------
# N-gram candidate index (cheap pre-filter so we skip most fuzzy comparisons)
# ---------------------------------------------------------------------------

_NGRAM_SIZE: int = 3
_NGRAM_INDEX_THRESHOLD: int = 500   # only build an index when target > this
_CANDIDATE_LIMIT: int = 500         # top-k candidates to keep per query


def _build_ngram_index(processed_strings: list[str], n: int = _NGRAM_SIZE) -> dict[str, list[int]]:
    """Inverted index: character n-gram → list of string indices containing it."""
    index: dict[str, list[int]] = defaultdict(list)
    for i, s in enumerate(processed_strings):
        seen: set[str] = set()
        for j in range(max(0, len(s) - n + 1)):
            ng = s[j : j + n]
            if ng not in seen:
                seen.add(ng)
                index[ng].append(i)
    return dict(index)


def _top_candidates(
    query: str, index: dict[str, list[int]],
    n: int = _NGRAM_SIZE, limit: int = _CANDIDATE_LIMIT,
) -> list[int]:
    """Return the *limit* target indices that share the most n-grams with *query*."""
    counts: Counter[int] = Counter()
    for j in range(max(0, len(query) - n + 1)):
        ng = query[j : j + n]
        if ng in index:
            for idx in index[ng]:
                counts[idx] += 1
    return [idx for idx, _ in counts.most_common(limit)]


# ---------------------------------------------------------------------------
# Multiprocessing worker (must be module-level so it can be pickled)
# ---------------------------------------------------------------------------

_mp_target: list[str] = []
_mp_processed_target: list[str] = []
_mp_ngram_index: dict[str, list[int]] = {}
_mp_use_index: bool = False
_mp_score: int = 0
_mp_min_length: int = 0
_mp_compiled: list[re.Pattern[str]] = []


def _mp_init(target: list[str], score: int, min_length: int, patterns: list[str]) -> None:
    """Initializer for each worker process — builds its own pre-processed
    target list and n-gram index so no large structures cross the pickle boundary."""
    global _mp_target, _mp_processed_target, _mp_ngram_index, _mp_use_index
    global _mp_score, _mp_min_length, _mp_compiled
    _mp_target = target
    _mp_processed_target = [utils.default_process(t) for t in target]
    _mp_use_index = len(target) > _NGRAM_INDEX_THRESHOLD
    _mp_ngram_index = _build_ngram_index(_mp_processed_target) if _mp_use_index else {}
    _mp_score = score
    _mp_min_length = min_length
    _mp_compiled = [re.compile(p) for p in patterns]


def _mp_match_one(args: tuple[int, str]) -> dict | None:
    i, segment = args
    if len(segment) < _mp_min_length or any(p.search(segment) for p in _mp_compiled):
        return None
    query = utils.default_process(segment)
    if _mp_use_index:
        candidate_indices = _top_candidates(query, _mp_ngram_index)
        choices = [_mp_processed_target[c] for c in candidate_indices]
    else:
        candidate_indices = None
        choices = _mp_processed_target
    result = process.extractOne(
        query, choices,
        scorer=fuzz.ratio,
        processor=None,
        score_cutoff=_mp_score,
    )
    if result is not None:
        _, match_score, local_idx = result
        orig_idx = candidate_indices[local_idx] if candidate_indices is not None else local_idx
        return {
            "source_line": i,
            "source_text": segment.rstrip(),
            "target_line": orig_idx + 1,
            "target_text": _mp_target[orig_idx].rstrip(),
            "score": match_score,
        }
    return None


# ---------------------------------------------------------------------------
# Progress bar
# ---------------------------------------------------------------------------

def _progress_bar(done: int, total: int, label: str = "Progress", width: int = 30) -> None:
    frac = done / total if total else 1.0
    filled = int(width * frac)
    bar = "\u2588" * filled + "\u2591" * (width - filled)
    sys.stderr.write(f"\r{label}: [{bar}] {frac * 100:.1f}% ({done}/{total})")
    sys.stderr.flush()


def _progress_done() -> None:
    sys.stderr.write("\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------

class TextMatcher:
    def __init__(self, score: int = DEFAULT_SCORE, min_length: int = DEFAULT_MIN_LENGTH,
                 ignore_patterns: list[str] | None = None) -> None:
        self.score = score
        self.min_length = min_length
        patterns = ignore_patterns if ignore_patterns is not None else DEFAULT_IGNORE_PATTERNS
        self._compiled: list[re.Pattern[str]] = [re.compile(p) for p in patterns]

    def _should_skip(self, segment: str) -> bool:
        if len(segment) < self.min_length:
            return True
        return any(pat.search(segment) for pat in self._compiled)

    def compare_lines(self, source: list[str], target: list[str],
                      progress: bool = True, workers: int = 1) -> Iterator[dict]:
        total = len(source)
        if workers == 1:
            # Pre-process target once; build n-gram index for large targets.
            if progress:
                sys.stderr.write("\rIndexing target…")
                sys.stderr.flush()
            processed_target = [utils.default_process(t) for t in target]
            use_index = len(target) > _NGRAM_INDEX_THRESHOLD
            ngram_index = _build_ngram_index(processed_target) if use_index else {}
            if progress:
                sys.stderr.write("\r" + " " * 40 + "\r")
                sys.stderr.flush()

            def _match_one(i: int, segment: str) -> dict | None:
                if self._should_skip(segment):
                    return None
                query = utils.default_process(segment)
                if use_index:
                    candidate_indices = _top_candidates(query, ngram_index)
                    choices = [processed_target[c] for c in candidate_indices]
                else:
                    candidate_indices = None
                    choices = processed_target
                result = process.extractOne(
                    query, choices,
                    scorer=fuzz.ratio,
                    processor=None,
                    score_cutoff=self.score,
                )
                if result is not None:
                    _, match_score, local_idx = result
                    orig_idx = candidate_indices[local_idx] if candidate_indices is not None else local_idx
                    return {
                        "source_line": i,
                        "source_text": segment.rstrip(),
                        "target_line": orig_idx + 1,
                        "target_text": target[orig_idx].rstrip(),
                        "score": match_score,
                    }
                return None

            for i, segment in enumerate(source, start=1):
                if progress:
                    _progress_bar(i, total, label="Comparing")
                hit = _match_one(i, segment)
                if hit is not None:
                    yield hit
            if progress:
                _progress_done()
        else:
            # Each worker process builds its own pre-processed target + index.
            max_workers = os.cpu_count() if workers == -1 else workers
            pattern_strings = [p.pattern for p in self._compiled]
            tasks = list(enumerate(source, start=1))
            if progress:
                sys.stderr.write("\rIndexing target (per worker)…")
                sys.stderr.flush()
            done = 0
            with ProcessPoolExecutor(
                max_workers=max_workers,
                initializer=_mp_init,
                initargs=(target, self.score, self.min_length, pattern_strings),
            ) as pool:
                for hit in pool.map(_mp_match_one, tasks, chunksize=64):
                    done += 1
                    if progress:
                        _progress_bar(done, total, label="Comparing")
                    if hit is not None:
                        yield hit
            if progress:
                _progress_done()

    def find_quotes(self, source: list[str], target: list[str],
                    progress: bool = True, workers: int = 1) -> Iterator[dict]:
        # Pre-filter target once so _search_line doesn't repeat the check per call.
        filtered_target: list[tuple[int, str]] = [
            (j, chunk) for j, chunk in enumerate(target, start=1)
            if not self._should_skip(chunk)
        ]
        # Pre-process once to avoid redundant processor calls in the inner loop.
        processed_texts: list[str] = [utils.default_process(chunk) for _, chunk in filtered_target]

        def _search_line(i: int, segment: str) -> list[dict]:
            if self._should_skip(segment):
                return []
            hits: list[dict] = []
            query = utils.default_process(segment)
            # Stage 1: fast bulk filter with partial_ratio (C-level batch).
            candidates = process.extract(
                query,
                processed_texts,
                scorer=fuzz.partial_ratio,
                processor=None,
                score_cutoff=self.score,
                limit=None,
            )
            # Stage 2: alignment info only for the hits that passed.
            for _, _, local_idx in candidates:
                j, chunk = filtered_target[local_idx]
                result = fuzz.partial_ratio_alignment(
                    query, processed_texts[local_idx],
                    processor=None,
                    score_cutoff=self.score,
                )
                if result is not None and result.score > 0:
                    hits.append({
                        "source_line": i,
                        "source_text": segment.rstrip(),
                        "target_chunk": j,
                        "matched_excerpt": processed_texts[local_idx][result.dest_start : result.dest_end],
                        "target_text": chunk.rstrip(),
                        "score": result.score,
                    })
            return hits

        total = len(source)

        if workers == 1:
            for i, segment in enumerate(source, start=1):
                if progress:
                    _progress_bar(i, total, label="Searching")
                yield from _search_line(i, segment)
            if progress:
                _progress_done()
        else:
            max_workers = os.cpu_count() if workers == -1 else workers
            all_hits: dict[int, list[dict]] = {}
            completed = 0
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(_search_line, i, seg): i for i, seg in enumerate(source, start=1)}
                for future in as_completed(futures):
                    all_hits[futures[future]] = future.result()
                    completed += 1
                    if progress:
                        _progress_bar(completed, total, label="Searching")
            if progress:
                _progress_done()
            for i in sorted(all_hits):
                yield from all_hits[i]

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_PARALLEL_COLUMNS = ["source_line", "source_text", "target_line", "target_text", "score"]
_QUOTES_COLUMNS = ["source_line", "source_text", "target_chunk", "matched_excerpt", "target_text", "score"]


def _write_output(
    results: list[dict],
    columns: list[str],
    fmt: str,
    header: bool,
    source_path: str = "",
    target_path: str = "",
    source_lines: list[str] | None = None,
    target_lines: list[str] | None = None,
) -> None:
    if fmt == "tsv":
        if source_path:
            print(f"# source_file: {source_path}")
        if target_path:
            print(f"# target_file: {target_path}")
        if header:
            print("\t".join(columns))
        for row in results:
            print("\t".join(str(row[col]) for col in columns))
    elif fmt == "csv":
        writer = csv.writer(sys.stdout)
        if source_path:
            writer.writerow([f"# source_file: {source_path}"])
        if target_path:
            writer.writerow([f"# target_file: {target_path}"])
        if header:
            writer.writerow(columns)
        for row in results:
            writer.writerow([row[col] for col in columns])
    elif fmt == "json":
        output: dict = {
            "source_file": source_path,
            "target_file": target_path,
            "source_lines": source_lines or [],
            "target_lines": target_lines or [],
            "results": results,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
    elif fmt == "markdown":
        if source_path:
            print(f"<!-- source_file: {source_path} -->")
        if target_path:
            print(f"<!-- target_file: {target_path} -->")
        print("| " + " | ".join(columns) + " |")
        print("| " + " | ".join("---" for _ in columns) + " |")
        for row in results:
            cells = [str(row[col]).replace("|", "\\|") for col in columns]
            print("| " + " | ".join(cells) + " |")


def _build_ignore_patterns(args: argparse.Namespace) -> list[str] | None:
    """Merge --ignore-file and --ignore patterns.

    Returns None (use built-in defaults) only when neither option is given.
    Returns a list (possibly empty) whenever at least one option is given.
    """
    if not args.ignore_file and not args.ignore:
        return None
    patterns: list[str] = []
    if args.ignore_file:
        patterns.extend(load_ignore_patterns(args.ignore_file))
    patterns.extend(args.ignore)
    return patterns


def cmd_parallel(args: argparse.Namespace) -> None:
    ignore = _build_ignore_patterns(args)
    matcher = TextMatcher(score=args.score, min_length=args.min_length, ignore_patterns=ignore)
    source = load_buffer(args.source, mode=args.source_mode, chunk_size=args.chunk_size)
    target = load_buffer(args.target, mode=args.target_mode, chunk_size=args.chunk_size)
    results = list(matcher.compare_lines(source, target, progress=not args.no_progress, workers=args.workers))
    _write_output(
        results, _PARALLEL_COLUMNS, args.format, args.header,
        source_path=str(Path(args.source).resolve()),
        target_path=str(Path(args.target).resolve()),
        source_lines=source,
        target_lines=target,
    )


def cmd_quotes(args: argparse.Namespace) -> None:
    ignore = _build_ignore_patterns(args)
    matcher = TextMatcher(score=args.score, min_length=args.min_length, ignore_patterns=ignore)
    source = load_buffer(args.source, mode=args.source_mode)
    target = load_buffer(args.target, mode=args.target_mode, chunk_size=args.chunk_size)
    results = list(matcher.find_quotes(source, target, progress=not args.no_progress, workers=args.workers))
    _write_output(
        results, _QUOTES_COLUMNS, args.format, args.header,
        source_path=str(Path(args.source).resolve()),
        target_path=str(Path(args.target).resolve()),
        source_lines=source,
        target_lines=target,
    )


def _common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("source", help="Source text file (the query text)")
    parser.add_argument("target", help="Target text file (the corpus to search)")
    parser.add_argument("--score", type=int, default=None, metavar="N",
                        help="Similarity cut-off 0–100 (default: 70 for 'parallel', 60 for 'quotes')")
    parser.add_argument("--min-length", type=int, default=DEFAULT_MIN_LENGTH, dest="min_length", metavar="N",
                        help=f"Skip segments shorter than N characters (default: {DEFAULT_MIN_LENGTH})")
    parser.add_argument("--ignore-file", metavar="FILE", dest="ignore_file", default=None,
                        help="File of regex patterns to skip (one per line, # = comment). "
                             "Replaces the built-in Sanskrit colophon patterns when supplied.")
    parser.add_argument("--ignore", metavar="PATTERN", dest="ignore", action="append", default=[],
                        help="Regex pattern to skip (repeatable). Combined with --ignore-file if both are given.")
    parser.add_argument("--format", choices=["tsv", "csv", "json", "markdown"], default="tsv",
                        help="Output format (default: tsv). Header is always included for json and markdown.")
    parser.add_argument("--header", action="store_true", help="Print a header row (tsv and csv only)")
    parser.add_argument("--no-progress", action="store_true", dest="no_progress",
                        help="Suppress the stderr progress indicator")
    parser.add_argument("--workers", type=int, default=1, metavar="N",
                        help="Number of parallel threads (default: 1, -1 = all CPU cores)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="paraquolocator",
        description=(
            "Detect parallel passages and quotations between texts using "
            "fuzzy string matching.\n\n"
            "Output is tab-separated (TSV) and goes to stdout; progress "
            "messages go to stderr."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_parallel = sub.add_parser("parallel",
        help="Find the best parallel match for each source line (verse-to-verse)",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    _common_args(p_parallel)
    p_parallel.add_argument("--source-mode", choices=["line", "danda", "fixed"], default="line", dest="source_mode")
    p_parallel.add_argument("--target-mode", choices=["line", "danda", "fixed"], default="line", dest="target_mode")
    p_parallel.add_argument("--chunk-size", type=int, default=30, dest="chunk_size", metavar="N",
                            help="Chunk size in characters for --*-mode fixed (default: 30)")
    p_parallel.set_defaults(func=cmd_parallel, score=70)

    p_quotes = sub.add_parser("quotes",
        help="Find where source lines are embedded inside target text (verse-in-prose)",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    _common_args(p_quotes)
    p_quotes.add_argument("--source-mode", choices=["line", "danda", "fixed"], default="line", dest="source_mode")
    p_quotes.add_argument("--target-mode", choices=["line", "danda", "fixed"], default="fixed", dest="target_mode")
    p_quotes.add_argument("--chunk-size", type=int, default=500, dest="chunk_size", metavar="N",
                          help="Chunk size in characters for --target-mode fixed (default: 500)")
    p_quotes.set_defaults(func=cmd_quotes, score=60)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except FileNotFoundError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted.\n")
        sys.exit(130)


if __name__ == "__main__":
    main()

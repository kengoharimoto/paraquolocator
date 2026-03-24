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
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def compare_lines(self, source: list[str], target: list[str], workers: int = 1) -> Iterator[dict]:
        def _match_one(i: int, segment: str) -> dict | None:
            if self._should_skip(segment):
                return None
            result = process.extractOne(
                segment, target,
                scorer=fuzz.ratio,
                processor=utils.default_process,
                score_cutoff=self.score,
            )
            if result is not None:
                match_text, match_score, match_idx = result
                return {
                    "source_line": i,
                    "source_text": segment.rstrip(),
                    "target_line": match_idx + 1,
                    "target_text": match_text.rstrip(),
                    "score": match_score,
                }
            return None

        if workers == 1:
            for i, segment in enumerate(source, start=1):
                hit = _match_one(i, segment)
                if hit is not None:
                    yield hit
        else:
            max_workers = os.cpu_count() if workers == -1 else workers
            hits: dict[int, dict] = {}
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {pool.submit(_match_one, i, seg): i for i, seg in enumerate(source, start=1)}
                for future in as_completed(futures):
                    hit = future.result()
                    if hit is not None:
                        hits[hit["source_line"]] = hit
            for i in sorted(hits):
                yield hits[i]

    def find_quotes(self, source: list[str], target: list[str],
                    progress: bool = True, workers: int = 1) -> Iterator[dict]:
        def _search_line(i: int, segment: str) -> list[dict]:
            if self._should_skip(segment):
                return []
            hits: list[dict] = []
            for j, chunk in enumerate(target, start=1):
                if self._should_skip(chunk):
                    continue
                result = fuzz.partial_ratio_alignment(
                    segment, chunk,
                    processor=utils.default_process,
                    score_cutoff=self.score,
                )
                if result is not None and result.score > 0:
                    hits.append({
                        "source_line": i,
                        "source_text": segment.rstrip(),
                        "target_chunk": j,
                        "matched_excerpt": chunk[result.dest_start : result.dest_end],
                        "target_text": chunk.rstrip(),
                        "score": result.score,
                    })
            return hits

        total = len(source)

        if workers == 1:
            for i, segment in enumerate(source, start=1):
                if progress:
                    end = "\n" if i % 20 == 0 else " "
                    sys.stderr.write(f"{i / total * 100:.2f}{end}")
                    sys.stderr.flush()
                yield from _search_line(i, segment)
            if progress:
                sys.stderr.write("\n")
                sys.stderr.flush()
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
                        end = "\n" if completed % 20 == 0 else " "
                        sys.stderr.write(f"{completed / total * 100:.2f}{end}")
                        sys.stderr.flush()
            if progress:
                sys.stderr.write("\n")
                sys.stderr.flush()
            for i in sorted(all_hits):
                yield from all_hits[i]

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

_PARALLEL_COLUMNS = ["source_line", "source_text", "target_line", "target_text", "score"]
_QUOTES_COLUMNS = ["source_line", "source_text", "target_chunk", "matched_excerpt", "target_text", "score"]


def _write_output(results: list[dict], columns: list[str], fmt: str, header: bool) -> None:
    if fmt == "tsv":
        if header:
            print("\t".join(columns))
        for row in results:
            print("\t".join(str(row[col]) for col in columns))
    elif fmt == "csv":
        writer = csv.writer(sys.stdout)
        if header:
            writer.writerow(columns)
        for row in results:
            writer.writerow([row[col] for col in columns])
    elif fmt == "json":
        print(json.dumps(results, ensure_ascii=False, indent=2))
    elif fmt == "markdown":
        print("| " + " | ".join(columns) + " |")
        print("| " + " | ".join("---" for _ in columns) + " |")
        for row in results:
            cells = [str(row[col]).replace("|", "\\|") for col in columns]
            print("| " + " | ".join(cells) + " |")


def cmd_parallel(args: argparse.Namespace) -> None:
    score = args.score if args.score is not None else 70
    ignore = load_ignore_patterns(args.ignore_file) if args.ignore_file else None
    matcher = TextMatcher(score=score, min_length=args.min_length, ignore_patterns=ignore)
    source = load_buffer(args.source, mode=args.source_mode, chunk_size=args.chunk_size)
    target = load_buffer(args.target, mode=args.target_mode, chunk_size=args.chunk_size)
    results = list(matcher.compare_lines(source, target, workers=args.workers))
    _write_output(results, _PARALLEL_COLUMNS, args.format, args.header)


def cmd_quotes(args: argparse.Namespace) -> None:
    score = args.score if args.score is not None else 60
    ignore = load_ignore_patterns(args.ignore_file) if args.ignore_file else None
    matcher = TextMatcher(score=score, min_length=args.min_length, ignore_patterns=ignore)
    source = load_buffer(args.source, mode=args.source_mode)
    target = load_buffer(args.target, mode=args.target_mode, chunk_size=args.chunk_size)
    results = list(matcher.find_quotes(source, target, progress=not args.no_progress, workers=args.workers))
    _write_output(results, _QUOTES_COLUMNS, args.format, args.header)


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
    p_parallel.set_defaults(func=cmd_parallel)

    p_quotes = sub.add_parser("quotes",
        help="Find where source lines are embedded inside target text (verse-in-prose)",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    _common_args(p_quotes)
    p_quotes.add_argument("--source-mode", choices=["line", "danda", "fixed"], default="line", dest="source_mode")
    p_quotes.add_argument("--target-mode", choices=["line", "danda", "fixed"], default="fixed", dest="target_mode")
    p_quotes.add_argument("--chunk-size", type=int, default=500, dest="chunk_size", metavar="N",
                          help="Chunk size in characters for --target-mode fixed (default: 500)")
    p_quotes.set_defaults(func=cmd_quotes)

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

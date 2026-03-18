"""Command-line interface for paraquolocator.

Two sub-commands are provided:

* ``parallel`` — find the single best parallel match for each source line
  (suited for verse-to-verse comparison).
* ``quotes``   — find where source lines appear embedded inside target text
  (suited for locating verse quotations inside prose).

Run ``paraquolocator --help`` or ``paraquolocator <command> --help`` for
full usage information.
"""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from .core import (
    DEFAULT_MIN_LENGTH,
    DEFAULT_SCORE,
    TextMatcher,
    load_buffer,
    load_ignore_patterns,
)

# ---------------------------------------------------------------------------
# TSV output helpers
# ---------------------------------------------------------------------------

_PARALLEL_COLUMNS = ["source_line", "source_text", "target_line", "target_text", "score"]
_QUOTES_COLUMNS = [
    "source_line",
    "source_text",
    "target_chunk",
    "matched_excerpt",
    "target_text",
    "score",
]


def _print_tsv_row(row: dict, columns: list[str]) -> None:
    print("\t".join(str(row[col]) for col in columns))


def _print_tsv_header(columns: list[str]) -> None:
    print("\t".join(columns))


# ---------------------------------------------------------------------------
# Sub-command implementations
# ---------------------------------------------------------------------------

def cmd_parallel(args: argparse.Namespace) -> None:
    """Run parallel passage detection (full-string ratio)."""
    score = args.score if args.score is not None else 70

    ignore = load_ignore_patterns(args.ignore_file) if args.ignore_file else None
    matcher = TextMatcher(score=score, min_length=args.min_length, ignore_patterns=ignore)

    source = load_buffer(args.source, mode=args.source_mode, chunk_size=args.chunk_size)
    target = load_buffer(args.target, mode=args.target_mode, chunk_size=args.chunk_size)

    if args.header:
        _print_tsv_header(_PARALLEL_COLUMNS)

    for hit in matcher.compare_lines(source, target):
        _print_tsv_row(hit, _PARALLEL_COLUMNS)


def cmd_quotes(args: argparse.Namespace) -> None:
    """Run quote / embedded-passage detection (partial ratio alignment)."""
    score = args.score if args.score is not None else 60

    ignore = load_ignore_patterns(args.ignore_file) if args.ignore_file else None
    matcher = TextMatcher(score=score, min_length=args.min_length, ignore_patterns=ignore)

    source = load_buffer(args.source, mode=args.source_mode)
    target = load_buffer(
        args.target, mode=args.target_mode, chunk_size=args.chunk_size
    )

    if args.header:
        _print_tsv_header(_QUOTES_COLUMNS)

    for hit in matcher.find_quotes(source, target, progress=not args.no_progress):
        _print_tsv_row(hit, _QUOTES_COLUMNS)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _common_args(parser: argparse.ArgumentParser) -> None:
    """Add arguments shared by both sub-commands."""
    parser.add_argument("source", help="Source text file (the query text)")
    parser.add_argument("target", help="Target text file (the corpus to search)")
    parser.add_argument(
        "--score",
        type=int,
        default=None,
        metavar="N",
        help="Similarity cut-off 0–100 (default: 70 for 'parallel', 60 for 'quotes')",
    )
    parser.add_argument(
        "--min-length",
        type=int,
        default=DEFAULT_MIN_LENGTH,
        dest="min_length",
        metavar="N",
        help=f"Skip segments shorter than N characters (default: {DEFAULT_MIN_LENGTH})",
    )
    parser.add_argument(
        "--ignore-file",
        metavar="FILE",
        dest="ignore_file",
        default=None,
        help="File of regex patterns to skip (one per line, # = comment). "
             "Replaces the built-in Sanskrit colophon patterns when supplied.",
    )
    parser.add_argument(
        "--header",
        action="store_true",
        help="Print a TSV header row before results",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        dest="no_progress",
        help="Suppress the stderr progress indicator",
    )


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

    # -- parallel ------------------------------------------------------------
    p_parallel = sub.add_parser(
        "parallel",
        help="Find the best parallel match for each source line (verse-to-verse)",
        description=(
            "For each line in SOURCE that passes the length and ignore filters, "
            "find the single best-matching line in TARGET using full-string "
            "fuzzy ratio.  Best suited for comparing two verse texts."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _common_args(p_parallel)
    p_parallel.add_argument(
        "--source-mode",
        choices=["line", "danda", "fixed"],
        default="line",
        dest="source_mode",
        help="How to split the source file into segments (default: line)",
    )
    p_parallel.add_argument(
        "--target-mode",
        choices=["line", "danda", "fixed"],
        default="line",
        dest="target_mode",
        help="How to split the target file into segments (default: line)",
    )
    p_parallel.add_argument(
        "--chunk-size",
        type=int,
        default=30,
        dest="chunk_size",
        metavar="N",
        help="Chunk size in characters when using --*-mode fixed (default: 30)",
    )
    p_parallel.set_defaults(func=cmd_parallel)

    # -- quotes --------------------------------------------------------------
    p_quotes = sub.add_parser(
        "quotes",
        help="Find where source lines are embedded inside target text (verse-in-prose)",
        description=(
            "For each line in SOURCE, search every chunk of TARGET for a "
            "sub-string match using partial_ratio_alignment.  Best suited for "
            "locating verse lines that are quoted inside prose commentaries."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _common_args(p_quotes)
    p_quotes.add_argument(
        "--source-mode",
        choices=["line", "danda", "fixed"],
        default="line",
        dest="source_mode",
        help="How to split the source file (default: line)",
    )
    p_quotes.add_argument(
        "--target-mode",
        choices=["line", "danda", "fixed"],
        default="fixed",
        dest="target_mode",
        help="How to split the target file (default: fixed)",
    )
    p_quotes.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        dest="chunk_size",
        metavar="N",
        help="Chunk size in characters when using --target-mode fixed (default: 500)",
    )
    p_quotes.set_defaults(func=cmd_quotes)

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

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

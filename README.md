# paraquolocator

**paraquolocator** detects parallel passages and quotations between two text files using fuzzy string matching.  It was designed for Sanskrit philological work — finding where one text quotes or parallels another — but works on any UTF-8 plain-text corpus.

Matching is powered by [rapidfuzz](https://github.com/maxbachmann/RapidFuzz).

---

## Features

- Two matching modes:
  - **`parallel`** — finds the single best match in a target text for each line in the source (full-string ratio; best for verse-to-verse comparison)
  - **`quotes`** — finds every place a source line appears *embedded* inside a target chunk (partial ratio alignment; best for locating verse lines quoted inside prose)
- Flexible text segmentation: by newline, by daṇḍa (`|`), or by fixed-length character chunks
- Configurable similarity threshold, minimum segment length, and chunk size
- Customisable ignore-pattern file (regex-per-line) to skip colophons, separators, and other boilerplate
- TSV output to stdout (easy to pipe into `sort`, `awk`, or a spreadsheet)
- Progress indicator on stderr

---

## Installation

Install the one dependency, then run the script directly:

```bash
pip install rapidfuzz
python3 paraquolocator.py --help
```

Requires **Python 3.10+**.

---

## Quick start

### Verse-to-verse parallel detection

```bash
python3 paraquolocator.py parallel base_text.txt parallel_text.txt
```

Output columns (TSV):

| source\_line | source\_text | target\_line | target\_text | score |
|---|---|---|---|---|
| 42 | deho 'yam… | 17 | deho 'yam… | 94 |

### Locating verse quotations inside prose

```bash
python3 paraquolocator.py quotes verse_text.txt commentary.txt --header
```

Output columns (TSV):

| source\_line | source\_text | target\_chunk | matched\_excerpt | target\_text | score |
|---|---|---|---|---|---|
| 5 | tat tvam asi | 23 | tat tvam asi | …atra tat tvam asi iti śrutiḥ… | 100 |

---

## Options

All options are shared between `parallel` and `quotes` unless noted.

```
positional arguments:
  source              Source text file (the query text)
  target              Target text file (the corpus to search)

options:
  --score N           Similarity cut-off 0–100
                        (default: 70 for 'parallel', 60 for 'quotes')
  --min-length N      Skip segments shorter than N characters (default: 30)
  --ignore-file FILE  File of regex patterns to skip (one per line, # = comment).
                      Replaces the built-in Sanskrit colophon patterns when supplied.
  --header            Print a TSV header row before results
  --no-progress       Suppress the stderr progress indicator
  --workers N         Parallel threads: 1 = single-threaded (default),
                      -1 = all CPU cores. rapidfuzz releases the GIL so
                      threads provide genuine speedup on multi-core machines.

parallel only:
  --source-mode {line,danda,fixed}   default: line
  --target-mode {line,danda,fixed}   default: line
  --chunk-size N                     Character chunk size for 'fixed' mode (default: 30)

quotes only:
  --source-mode {line,danda,fixed}   default: line
  --target-mode {line,danda,fixed}   default: fixed
  --chunk-size N                     Character chunk size for 'fixed' mode (default: 500)
```

---

## Text segmentation modes

| Mode | Description | Typical use |
|---|---|---|
| `line` | One entry per newline | Verse texts, line-aligned files |
| `danda` | Split further at `\|` (daṇḍa) | Sanskrit verse with half-verse markers |
| `fixed` | Fixed-length character chunks | Prose commentaries, unstructured text |

---

## Ignore patterns

The built-in patterns (see [`ignore_patterns.txt`](ignore_patterns.txt)) skip common Advaita Vedānta colophon phrases and separator lines.  To use a custom set, pass `--ignore-file your_patterns.txt`.  Format:

```
# This is a comment
regex_pattern_one
regex_pattern_two
```

---

## Output format

Results go to **stdout** as tab-separated values.  Redirect to a file:

```bash
python3 paraquolocator.py quotes verse.txt commentary.txt --header > results.tsv
```

Progress messages (percentages) go to **stderr** and can be suppressed with `--no-progress`.

---

## License

MIT

# md-stats

A zero-dependency TypeScript CLI that prints statistics for Markdown files:
word count, headings by level, link count, and code-block count. Give it a
file or a directory (directories are walked recursively) — `--json` prints
the same data as JSON.

## Usage

```
md-stats <file-or-dir> [--json]
```

From this repository, without a global install:

```console
$ npm --prefix tools/md-stats run build
$ node tools/md-stats/dist/cli.js README.md
File       Words  H1  H2  H3  H4  H5  H6  Links  Code blocks
------------------------------------------------------------
README.md    120   1   1   0   0   0   0      1            0
```

The output above was produced by running the tool on this repository's
`README.md` — one word-counted heading hierarchy (`#`/`##`) and one bare URL
(`http://localhost:8000`), which GitHub renders as a link.

With `--json` (one object per file, `headings` always carries all six levels):

```console
$ node tools/md-stats/dist/cli.js --json README.md
[
  {
    "path": "README.md",
    "words": 120,
    "links": 1,
    "codeBlocks": 0,
    "headings": {
      "h1": 1,
      "h2": 1,
      "h3": 0,
      "h4": 0,
      "h5": 0,
      "h6": 0
    }
  }
]
```

On a directory, every `.md` file (case-insensitive) is walked recursively in
sorted order and a `TOTAL` row is added:

```console
$ md-stats docs
File                 Words  H1  H2  H3  H4  H5  H6  Links  Code blocks
----------------------------------------------------------------------
docs/guide/index.md      4   1   0   0   0   0   0      1            0
docs/note.md             1   0   0   0   0   0   0      0            0
TOTAL                    5   1   0   0   0   0   0      1            0
```

A directory with no Markdown files — this repository's `.github`, for
instance — prints `No Markdown files found.` and exits 0:

```console
$ md-stats .github
No Markdown files found.
```

A missing path or a missing argument prints an error to stderr and exits 1.

## What counts

- **Words** — whitespace-separated tokens on prose lines. Heading text, list
  items (including the `-` bullet) and inline code count; YAML frontmatter
  and fenced code blocks do not.
- **Headings** — ATX (`#`..`######`, optional closing hash run) and setext
  (`===`/`---` underline after a paragraph) headings, also inside
  blockquotes. `#######` (seven hashes) and `#NoSpace` are prose, `---` after
  a blank line is a thematic break, and a link-reference definition is not a
  paragraph, so `===` right after one is prose too.
- **Links** — what GitHub would render as a hyperlink: inline
  `[text](url)`, full/collapsed/shortcut references (only when a matching
  `[label]: url` definition exists), `<http://…>` autolinks, and bare
  `http(s)://`/`ftp://`/`www.` URLs. Links are counted over whole
  paragraphs, so a code span that opens and closes on different lines still
  shields its contents (and an unclosed code span is literal text); a run of
  N backticks closes only on a run of exactly N. Unresolved bracket groups
  stay literal, so an autolink inside them (`[<http://…>]`) still counts.
  Images (`![alt](src)`), link definitions, and anything inside code spans
  or fences are not links.
- **Code blocks** — fenced blocks only (``` or ~~~), including fences the
  file never closes. A fence closes only on a run of the same character at
  least as long as its opener, and only inside the container that opened it:
  a fence opened inside a blockquote ends when the blockquote does.
  Indented (4-space) code blocks are not detected, because list content is
  indistinguishable from them without a full CommonMark parser.

The walker skips `.git` and `node_modules`, never follows symlinks, and
returns files in bytewise sorted order, so the same tree always prints the
same table.

## Development

```console
$ npm --prefix tools/md-stats install
$ npm --prefix tools/md-stats run build   # tsc -> dist/
$ npm --prefix tools/md-stats test        # vitest, 33 tests
$ npm --prefix tools/md-stats run lint    # eslint
```

Layout: `src/parse.ts` (pure parser), `src/walk.ts` (directory walker),
`src/stats.ts` (file collection), `src/render.ts` (table/JSON formatting),
`src/cli.ts` (argument parsing and entry point). The parser and walker have
no side effects; only `stats.ts` and `cli.ts` touch the filesystem.

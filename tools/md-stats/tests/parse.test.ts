import { describe, expect, it } from "vitest";
import { parseMarkdown, type HeadingCounts } from "../src/parse.js";

const zeros = (): HeadingCounts => ({ h1: 0, h2: 0, h3: 0, h4: 0, h5: 0, h6: 0 });

describe("parseMarkdown", () => {
  it("counts ATX headings by level and rejects malformed ones", () => {
    // Requirement: only `#`..`######` followed by a space or end-of-line are
    // headings; 7 hashes, a missing space, or a 4-space indent are prose.
    const md = [
      "# One",
      "## Two",
      "### Three",
      "#### Four",
      "##### Five",
      "###### Six",
      "####### Seven is not a heading",
      "#NoSpace is not a heading",
      "    # Indented is not a heading",
    ].join("\n");
    expect(parseMarkdown(md).headings).toEqual({
      h1: 1,
      h2: 1,
      h3: 1,
      h4: 1,
      h5: 1,
      h6: 1,
    });
  });

  it("counts setext headings and does not mistake thematic breaks for them", () => {
    // Requirement: `===`/`---` underline a preceding paragraph line as h1/h2;
    // the same `---` after a blank line is a thematic break, not a heading.
    const s = parseMarkdown("Title\n===\n\nSubtitle\n---\n\n---\n");
    expect(s.headings.h1).toBe(1);
    expect(s.headings.h2).toBe(1);
    expect(s.words).toBe(2);
  });

  it("counts words in prose, headings and lists, excluding frontmatter and code fences", () => {
    // Requirement: words are whitespace-separated tokens on prose lines —
    // YAML frontmatter and fenced code blocks are not prose.
    const md = [
      "---",
      "title: Frontmatter words do not count",
      "---",
      "",
      "# Heading words count",
      "",
      "Prose words count. So do `inline code words`.",
      "",
      "- list item words count",
      "",
      "```",
      "code words do not count",
      "```",
    ].join("\n");
    // Words are whitespace-separated tokens: the "-" bullet marker and the
    // tokens inside `inline code` count too.
    const s = parseMarkdown(md);
    expect(s.words).toBe(3 + 8 + 5);
    expect(s.headings.h1).toBe(1);
    expect(s.codeBlocks).toBe(1);
  });

  it("counts fenced code blocks including tilde fences and unterminated fences", () => {
    // Requirement: every fence that opens a code block counts as one block,
    // even if the source ends before it is closed.
    const md = ["```js", "code", "```", "", "~~~", "code", "~~~", "", "```", "never closed"].join("\n");
    const s = parseMarkdown(md);
    expect(s.codeBlocks).toBe(3);
    expect(s.words).toBe(0);
  });

  it("ignores headings, links and words inside fenced code blocks", () => {
    const s = parseMarkdown("```\n# Not a heading\n[not a link](http://example.com)\n```");
    expect(s).toEqual({ words: 0, headings: zeros(), links: 0, codeBlocks: 1 });
  });

  it("closes a fence only with a run at least as long as the opener", () => {
    // Requirement: CommonMark fence rules — ```` is not closed by ```, and a
    // longer closing run does close a shorter opener.
    const md = ["````", "```", "# inside", "````", "# outside"].join("\n");
    const s = parseMarkdown(md);
    expect(s.codeBlocks).toBe(1);
    expect(s.headings.h1).toBe(1);
    expect(s.words).toBe(1);
    expect(parseMarkdown("```\ncode\n````").codeBlocks).toBe(1);
  });

  it("counts inline links but not images", () => {
    const md = "See [one](http://a.example) and [two](b) but not ![img](http://c.example).";
    expect(parseMarkdown(md).links).toBe(2);
  });

  it("counts full, collapsed and shortcut reference links only when defined", () => {
    // Requirement: reference links render only when a matching definition
    // exists; `[text][undefined]` falls back to the shortcut `[text]`.
    const md = [
      "[foo], [foo][bar], [foo][nope], [bar][], [undefined],",
      "",
      "[foo]: /foo",
      "[bar]: /bar",
    ].join("\n");
    expect(parseMarkdown(md).links).toBe(4);
  });

  it("does not count link definitions or links inside inline code", () => {
    const md = "[foo]: /foo\n\nSee `[foo]` and [foo].";
    expect(parseMarkdown(md).links).toBe(1);
  });

  it("counts autolinks and bare URLs, excluding those inside code spans", () => {
    // Requirement: GitHub renders <http://x> and bare http(s)/www URLs as
    // links, but never inside inline code.
    const md = "Go to <http://a.example>, http://b.example, www.c.example but not `http://d.example`.";
    expect(parseMarkdown(md).links).toBe(3);
  });

  it("counts headings inside blockquotes", () => {
    const s = parseMarkdown("> # Quoted heading\n> prose line");
    expect(s.headings.h1).toBe(1);
    expect(s.words).toBe(4);
  });

  it("treats a trailing hash run as a closing sequence, not words", () => {
    expect(parseMarkdown("## Closed ##").headings.h2).toBe(1);
    expect(parseMarkdown("## Closed ##").words).toBe(1);
    expect(parseMarkdown("## ##").words).toBe(0);
    expect(parseMarkdown("***\n").words).toBe(0);
  });

  it("handles CRLF line endings", () => {
    const s = parseMarkdown("# Title\r\n\r\nSome words here\r\n");
    expect(s.headings.h1).toBe(1);
    expect(s.words).toBe(4); // "Title" + "Some words here"
  });

  it("returns all zeros for empty or whitespace-only input", () => {
    for (const empty of ["", "\n", "   \n\t\n"]) {
      expect(parseMarkdown(empty)).toEqual({ words: 0, headings: zeros(), links: 0, codeBlocks: 0 });
    }
  });

  it("ends a fence opened inside a blockquote when the quote ends", () => {
    // Regression: the quote-stripping erased the container, so an unclosed
    // fence inside a blockquote swallowed every line after the quote.
    const s = parseMarkdown("> ```\n> code\n\n# Outside\n[text](/url)");
    expect(s).toEqual({
      words: 2,
      headings: { ...zeros(), h1: 1 },
      links: 1,
      codeBlocks: 1,
    });
    const closed = parseMarkdown("> ```\n> c\n> ```\n\n# Out");
    expect(closed.headings.h1).toBe(1);
    expect(closed.codeBlocks).toBe(1);
  });

  it("counts links over whole paragraphs, so code spans shield URLs across line breaks", () => {
    // Regression: code spans were stripped per line, letting a URL inside a
    // span that opens and closes on different lines leak into the count.
    expect(parseMarkdown("`code\nhttp://a.example\nend`").links).toBe(0);
    // An unclosed span is literal text, so its URL is a bare link.
    expect(parseMarkdown("see `code http://x").links).toBe(1);
  });

  it("does not let a link definition turn a following === into a heading", () => {
    // Regression: definition lines set paragraph state, so a setext
    // underline after one counted as a heading. CommonMark renders the
    // underline as a plain paragraph.
    const s = parseMarkdown("[foo]: /url\n===");
    expect(s.headings).toEqual(zeros());
    expect(s.words).toBe(3); // "[foo]:" + "/url" + "==="
  });

  it("counts an autolink inside brackets when no reference definition matches", () => {
    // Regression: unresolved bracket groups were removed before autolink
    // detection, so [<http://a.example>] lost its link.
    expect(parseMarkdown("[<http://a.example>]").links).toBe(1);
  });

  it("does not close a short code span with a longer backtick run", () => {
    // Regression: the span regex let a 1-backtick opener match the first
    // backtick of a 2-backtick run, wrongly swallowing the URL.
    expect(parseMarkdown("`http://a.example``").links).toBe(1);
    expect(parseMarkdown("`` ` `` http://x").links).toBe(1);
  });
});

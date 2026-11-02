/**
 * Pure Markdown statistics parser.
 *
 * `parseMarkdown` turns Markdown source into {@link MarkdownStats}: word
 * count, heading counts by level, link count, and fenced code-block count.
 * It is a pure function — no filesystem, no globals, deterministic output.
 *
 * Counting rules (rationale in the project README):
 * - Words: whitespace-separated tokens on prose lines. Heading text, list
 *   items and inline code count; fenced code blocks and YAML frontmatter
 *   do not.
 * - Headings: ATX (`#`..`######`, with optional closing hash run) and setext
 *   (`===` / `---` underline after a paragraph) headings, also inside
 *   blockquotes. Link-reference definitions are not paragraphs, so an
 *   underline after one is prose.
 * - Links: inline, full/collapsed/shortcut references (only when a matching
 *   definition exists), `<...>` autolinks, and GFM bare URLs. Links are
 *   counted over whole paragraphs, so a code span that opens and closes on
 *   different lines still shields its contents, and an unclosed code span
 *   is literal text. Unresolved bracket groups stay in the text, keeping
 *   autolinks inside them (e.g. `[<http://…>]`) countable. Images, link
 *   definitions, and anything inside code spans or fences are not links.
 * - Code blocks: fenced blocks only (``` or ~~~), including fences the file
 *   never closes. A fence closes only on a run of the same character at
 *   least as long as the opener — and only inside the container (quoted or
 *   not) that opened it; a fence opened inside a blockquote ends when the
 *   blockquote does.
 */

export interface HeadingCounts {
  h1: number;
  h2: number;
  h3: number;
  h4: number;
  h5: number;
  h6: number;
}

export interface MarkdownStats {
  words: number;
  headings: HeadingCounts;
  links: number;
  codeBlocks: number;
}

type Level = 1 | 2 | 3 | 4 | 5 | 6;

interface Fence {
  char: string;
  length: number;
}

/** Fence bookkeeping shared by both passes; `quoted` records the blockquote
 *  container the active fence was opened in. */
interface FenceState {
  fence: Fence | null;
  quoted: boolean;
}

const QUOTE_MARK = /^ {0,3}>/;
const SPAN_RUN = /(?<!`)(`+)(?!`)/g;
const IMAGE =
  /!\[((?:[^[\]]|\[[^[\]]*\])*)\](?:\((?:[^()]|\([^()]*\))*\)|\[((?:[^[\]]|\[[^[\]]*\])*)\])?/g;
const INLINE_LINK = /\[((?:[^[\]]|\[[^[\]]*\])*)\]\((?:[^()]|\([^()]*\))*\)/g;
const REFERENCE = /\[((?:[^[\]]|\[[^[\]]*\])*)\](?:\[((?:[^[\]]|\[[^[\]]*\])*)\])?/g;
const AUTOLINK =
  /<[a-zA-Z][a-zA-Z0-9+.-]{1,31}:[^<>\s]*>|<[^<>\s@]+@[^<>\s@]+\.[^<>\s@]+>/g;
const BARE_URL = /\b(?:https?:\/\/|ftp:\/\/|www\.)[^\s<>"']+/g;
const DEFINITION = /^ {0,3}\[((?:[^[\]]|\[[^[\]]*\])*)\]:[ \t]*\S/;
const ATX = /^ {0,3}(#{1,6})(?!\S)(.*)$/;
const SETEXT = /^ {0,3}(=+|-+)[ \t]*$/;
const THEMATIC =
  /^ {0,3}(?:(?:\*[ \t]*){3,}|(?:-[ \t]*){3,}|(?:_[ \t]*){3,})$/;

/** Link-reference labels are matched case-insensitively with folded whitespace. */
const norm = (label: string): string => label.trim().replace(/\s+/g, " ").toLowerCase();

const stripQuote = (line: string): string => line.replace(/^(?: {0,3}> ?)+/, "");

function openFence(line: string): Fence | null {
  const m = /^ {0,3}(`{3,})[^`]*$/.exec(line) ?? /^ {0,3}(~{3,})/.exec(line);
  const marker = m?.[1] ?? "";
  return marker ? { char: marker.charAt(0), length: marker.length } : null;
}

function closeFence(line: string, fence: Fence): boolean {
  const marker = /^ {0,3}(`{3,}|~{3,})[ \t]*$/.exec(line)?.[1] ?? "";
  return marker.charAt(0) === fence.char && marker.length >= fence.length;
}

/**
 * One line of fence bookkeeping, shared by both passes. Classifies a line
 * as "opened" (it starts a fence), "inside" (it belongs to an open fence,
 * including the line that closes it), or "text". A fence closes only on a
 * closing line in the same container that opened it; when a fence opened
 * inside a blockquote and the quote ends, the fence ends too and the
 * boundary line is regular text.
 */
function classify(raw: string, line: string, state: FenceState): "inside" | "opened" | "text" {
  const quoted = QUOTE_MARK.test(raw);
  if (state.fence) {
    if (quoted === state.quoted && closeFence(line, state.fence)) {
      state.fence = null;
      return "inside";
    }
    if (state.quoted && !quoted) state.fence = null;
    else return "inside";
  }
  const open = openFence(line);
  if (open) {
    state.fence = open;
    state.quoted = quoted;
    return "opened";
  }
  return "text";
}

/** Line index where YAML frontmatter ends, or 0 if there is none. */
function frontmatterEnd(lines: string[]): number {
  if ((lines[0] ?? "").trimEnd() !== "---") return 0;
  for (let i = 1; i < lines.length; i++) {
    if ((lines[i] ?? "").trimEnd() === "---") return i + 1;
  }
  return 0;
}

function collectDefinitions(lines: string[], body: number): Set<string> {
  const defs = new Set<string>();
  const state: FenceState = { fence: null, quoted: false };
  for (let i = body; i < lines.length; i++) {
    const raw = lines[i] ?? "";
    const line = stripQuote(raw);
    if (classify(raw, line, state) !== "text") continue;
    const label = norm(DEFINITION.exec(line)?.[1] ?? "");
    if (label) defs.add(label);
  }
  return defs;
}

function countWords(text: string): number {
  const trimmed = text.trim();
  return trimmed ? trimmed.split(/\s+/).length : 0;
}

function countMatches(text: string, re: RegExp): number {
  return [...text.matchAll(re)].length;
}

/**
 * Replace closed inline code spans with a space. An opening run of N
 * backticks closes only on a maximal run of exactly N — a longer run does
 * not close a shorter opener — and an opener without any closer is literal
 * text, so calls are safe per block with no state carried between them.
 * Linear time: one backward pass precomputes each run's next same-length
 * run, so pairing an opener with its closer is a single lookup.
 */
function stripCodeSpans(text: string): string {
  const runs = [...text.matchAll(SPAN_RUN)].map((m) => ({
    at: m.index ?? 0,
    ticks: m[1]?.length ?? 0,
  }));
  // nextSame[i]: index of the first run after i with the same tick count.
  const nextSame: number[] = [];
  const lastSeen = new Map<number, number>();
  for (let j = runs.length - 1; j >= 0; j--) {
    const ticks = runs[j]?.ticks ?? -1;
    nextSame[j] = lastSeen.get(ticks) ?? -1;
    lastSeen.set(ticks, j);
  }
  let out = "";
  let cursor = 0;
  let i = 0;
  while (i < runs.length) {
    const opener = runs[i];
    if (!opener) break;
    const close = nextSame[i] ?? -1;
    if (close === -1) {
      i++;
      continue;
    }
    out += text.slice(cursor, opener.at) + " ";
    cursor = (runs[close]?.at ?? 0) + opener.ticks;
    i = close + 1;
  }
  return out + text.slice(cursor);
}

function countLinks(text: string, defs: Set<string>): number {
  let t = stripCodeSpans(text).replace(IMAGE, " ");
  let count = countMatches(t, INLINE_LINK);
  t = t.replaceAll(INLINE_LINK, " ");
  let refs = 0;
  t = t.replace(REFERENCE, (match, label: string, ref: string | undefined) => {
    const full = ref !== undefined && defs.has(norm(ref));
    const shortcut = defs.has(norm(label));
    if (!full && !shortcut) return match; // literal: autolinks inside remain countable
    refs++;
    // A fallback shortcut consumes only its first group; the unresolved
    // second group stays literal so autolinks inside it still count.
    return ref !== undefined && !full ? ` [${ref}]` : " ";
  });
  count += refs + countMatches(t, AUTOLINK);
  return count + countMatches(t.replaceAll(AUTOLINK, " "), BARE_URL);
}

/** ATX text with the closing hash run removed ("Closed ##" -> "Closed"). */
function headingText(raw: string): string {
  const text = raw.trim();
  return /^#+$/.test(text) ? "" : text.replace(/[ \t]+#+[ \t]*$/, "").trim();
}

export function parseMarkdown(source: string): MarkdownStats {
  const lines = source.split(/\r?\n/);
  const body = frontmatterEnd(lines);
  const defs = collectDefinitions(lines, body);
  const stats: MarkdownStats = {
    words: 0,
    links: 0,
    codeBlocks: 0,
    headings: { h1: 0, h2: 0, h3: 0, h4: 0, h5: 0, h6: 0 },
  };
  const state: FenceState = { fence: null, quoted: false };
  // Prose lines accumulate until the paragraph ends, then yield their links
  // as one text: inline constructs may span the paragraph's soft breaks.
  let buffer: string[] = [];
  const flush = (): void => {
    if (buffer.length > 0) {
      stats.links += countLinks(buffer.join("\n"), defs);
      buffer = [];
    }
  };
  for (let i = body; i < lines.length; i++) {
    const raw = lines[i] ?? "";
    const line = stripQuote(raw);
    const kind = classify(raw, line, state);
    if (kind !== "text") {
      flush();
      if (kind === "opened") stats.codeBlocks++;
      continue;
    }
    if (!line.trim()) {
      flush();
      continue;
    }
    const atx = ATX.exec(line);
    if (atx) {
      flush();
      const text = headingText(atx[2] ?? "");
      const level = (atx[1] ?? "").length as Level;
      stats.words += countWords(text);
      stats.links += countLinks(text, defs);
      stats.headings[`h${level}`]++;
      continue;
    }
    if (buffer.length > 0 && SETEXT.test(line)) {
      flush();
      stats.headings[line.trim().startsWith("=") ? "h1" : "h2"]++;
      continue;
    }
    if (THEMATIC.test(line)) {
      flush();
      continue;
    }
    stats.words += countWords(line);
    if (DEFINITION.test(line)) flush(); // a definition is its own block, not a paragraph
    else buffer.push(line);
  }
  flush();
  return stats;
}

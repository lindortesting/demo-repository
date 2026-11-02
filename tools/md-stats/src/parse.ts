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
 *   (`===` / `---` underline after a paragraph), also inside blockquotes.
 * - Links: inline, full/collapsed/shortcut references (only when a matching
 *   definition exists), `<...>` autolinks, and GFM bare URLs. Images, link
 *   definitions, and anything inside code spans or fences are not links.
 * - Code blocks: fenced blocks only (``` or ~~~), including unterminated
 *   fences. A fence closes only on a run of the same character at least as
 *   long as the opener.
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

const CODE_SPAN = /(`+)(.*?)\1/g;
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
  let fence: Fence | null = null;
  for (let i = body; i < lines.length; i++) {
    const line = stripQuote(lines[i] ?? "");
    if (fence) {
      if (closeFence(line, fence)) fence = null;
      continue;
    }
    const open = openFence(line);
    if (open) {
      fence = open;
      continue;
    }
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

function countLinks(text: string, defs: Set<string>): number {
  let t = text.replace(CODE_SPAN, " ").replace(IMAGE, " ");
  let count = countMatches(t, INLINE_LINK);
  t = t.replaceAll(INLINE_LINK, " ");
  for (const m of t.matchAll(REFERENCE)) {
    const ref = norm(m[1] ?? "");
    const label = m[2] === undefined ? ref : norm(m[2]);
    if (defs.has(label) || (m[2] !== undefined && defs.has(ref))) count++;
  }
  t = t.replaceAll(REFERENCE, " ");
  count += countMatches(t, AUTOLINK);
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
  let fence: Fence | null = null;
  let paragraph = false;
  for (let i = body; i < lines.length; i++) {
    const line = stripQuote(lines[i] ?? "");
    if (fence) {
      if (closeFence(line, fence)) fence = null;
      continue;
    }
    const open = openFence(line);
    if (open) {
      fence = open;
      stats.codeBlocks++;
      paragraph = false;
      continue;
    }
    if (!line.trim()) {
      paragraph = false;
      continue;
    }
    const atx = ATX.exec(line);
    if (atx) {
      const text = headingText(atx[2] ?? "");
      const level = (atx[1] ?? "").length as Level;
      stats.words += countWords(text);
      stats.links += countLinks(text, defs);
      stats.headings[`h${level}`]++;
      paragraph = false;
      continue;
    }
    if (paragraph && SETEXT.test(line)) {
      stats.headings[line.trim().startsWith("=") ? "h1" : "h2"]++;
      paragraph = false;
      continue;
    }
    if (THEMATIC.test(line)) {
      paragraph = false;
      continue;
    }
    stats.words += countWords(line);
    if (!DEFINITION.test(line)) stats.links += countLinks(line, defs);
    paragraph = true;
  }
  return stats;
}

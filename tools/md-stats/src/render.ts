/**
 * Output formatting: an aligned text table, or pretty-printed JSON.
 */
import type { FileStats } from "./stats.js";

const LEVELS = ["h1", "h2", "h3", "h4", "h5", "h6"] as const;

export function renderText(files: FileStats[]): string {
  if (files.length === 0) return "No Markdown files found.";
  const header = ["File", "Words", ...LEVELS.map((l) => l.toUpperCase()), "Links", "Code blocks"];
  const rows = files.map((f) => [
    f.path,
    String(f.words),
    ...LEVELS.map((l) => String(f.headings[l])),
    String(f.links),
    String(f.codeBlocks),
  ]);
  if (files.length > 1) {
    const sum = (pick: (f: FileStats) => number) =>
      String(files.reduce((total, f) => total + pick(f), 0));
    rows.push([
      "TOTAL",
      sum((f) => f.words),
      ...LEVELS.map((l) => sum((f) => f.headings[l])),
      sum((f) => f.links),
      sum((f) => f.codeBlocks),
    ]);
  }
  const widths = header.map((h, i) => Math.max(h.length, ...rows.map((r) => (r[i] ?? "").length)));
  const row = (cells: string[]) =>
    cells.map((cell, i) => (i === 0 ? cell.padEnd(widths[0] ?? 0) : cell.padStart(widths[i] ?? 0))).join("  ");
  return [row(header), "-".repeat(row(header).length), ...rows.map(row)].join("\n");
}

export function renderJson(files: FileStats[]): string {
  return JSON.stringify(files, null, 2);
}

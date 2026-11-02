import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { main } from "../src/cli.js";
import { renderText } from "../src/render.js";
import { collectStats, type FileStats } from "../src/stats.js";

const zeroHeadings = (): FileStats["headings"] => ({
  h1: 0,
  h2: 0,
  h3: 0,
  h4: 0,
  h5: 0,
  h6: 0,
});

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "md-stats-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

describe("collectStats", () => {
  it("collects stats for a single file target", async () => {
    // Requirement: `md-stats <file>` reports exactly that file, whatever its
    // extension — an explicit target is never filtered.
    const file = join(root, "doc.md");
    await writeFile(file, "# Hello\n\nWorld\n");
    expect(await collectStats(file)).toEqual([
      {
        path: file,
        words: 2,
        headings: { ...zeroHeadings(), h1: 1 },
        links: 0,
        codeBlocks: 0,
      },
    ]);
  });

  it("collects stats for every Markdown file in a directory target", async () => {
    await writeFile(join(root, "one.md"), "# A\n");
    await writeFile(join(root, "two.md"), "text\n");
    const stats = await collectStats(root);
    expect(stats.map((f) => f.path)).toEqual([
      join(root, "one.md"),
      join(root, "two.md"),
    ]);
    expect(stats[0]?.headings.h1).toBe(1);
    expect(stats[1]?.words).toBe(1);
  });
});

describe("renderText", () => {
  it("renders an aligned table with a total row for multiple files", () => {
    const files: FileStats[] = [
      { path: "a.md", words: 10, headings: { ...zeroHeadings(), h1: 1 }, links: 2, codeBlocks: 1 },
      { path: "sub/b.md", words: 5, headings: { ...zeroHeadings(), h2: 1 }, links: 0, codeBlocks: 0 },
    ];
    expect(renderText(files).split("\n")).toEqual([
      "File      Words  H1  H2  H3  H4  H5  H6  Links  Code blocks",
      "-----------------------------------------------------------",
      "a.md         10   1   0   0   0   0   0      2            1",
      "sub/b.md      5   0   1   0   0   0   0      0            0",
      "TOTAL        15   1   1   0   0   0   0      2            1",
    ]);
  });

  it("omits the total row for a single file and prints a note for none", () => {
    const single: FileStats[] = [
      { path: "only.md", words: 1, headings: zeroHeadings(), links: 0, codeBlocks: 0 },
    ];
    expect(renderText(single).split("\n")).toHaveLength(3);
    expect(renderText(single)).not.toContain("TOTAL");
    expect(renderText([])).toBe("No Markdown files found.");
  });
});

describe("main", () => {
  it("renders a text table by default", async () => {
    await writeFile(join(root, "x.md"), "# T\n");
    const out = await main([root]);
    expect(out.split("\n")[0]).toContain("Words");
    expect(out).toContain(join(root, "x.md"));
  });

  it("renders JSON with --json", async () => {
    await writeFile(join(root, "x.md"), "# T\n\n[a](/b)\n");
    const parsed = JSON.parse(await main(["--json", root])) as FileStats[];
    expect(parsed).toHaveLength(1);
    expect(parsed[0]?.words).toBe(2);
    expect(parsed[0]?.links).toBe(1);
    expect(parsed[0]?.headings).toEqual({ ...zeroHeadings(), h1: 1 });
  });

  it("rejects a missing target argument", async () => {
    await expect(main([])).rejects.toThrow(/missing/);
  });

  it("rejects a nonexistent path", async () => {
    await expect(main([join(root, "gone")])).rejects.toThrow();
  });
});

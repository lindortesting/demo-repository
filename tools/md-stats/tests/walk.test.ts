import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { findMarkdownFiles } from "../src/walk.js";

let root: string;

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "md-stats-"));
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

describe("findMarkdownFiles", () => {
  it("finds .md files recursively in deterministic sorted order", async () => {
    // Requirement: a directory is walked recursively and the output order is
    // stable across runs, so identical trees always print identical tables.
    await mkdir(join(root, "sub", "inner"), { recursive: true });
    await writeFile(join(root, "z.md"), "");
    await writeFile(join(root, "a.md"), "");
    await writeFile(join(root, "sub", "b.md"), "");
    await writeFile(join(root, "sub", "inner", "c.md"), "");
    expect(await findMarkdownFiles(root)).toEqual([
      join(root, "a.md"),
      join(root, "sub", "b.md"),
      join(root, "sub", "inner", "c.md"),
      join(root, "z.md"),
    ]);
  });

  it("matches .md case-insensitively and skips other extensions", async () => {
    await writeFile(join(root, "A.MD"), "");
    await writeFile(join(root, "notes.txt"), "");
    await writeFile(join(root, "doc.markdown"), "");
    expect(await findMarkdownFiles(root)).toEqual([join(root, "A.MD")]);
  });

  it("returns an empty list for a directory without Markdown files", async () => {
    await mkdir(join(root, "empty"));
    await writeFile(join(root, "data.json"), "{}");
    expect(await findMarkdownFiles(root)).toEqual([]);
  });

  it("skips .git and node_modules directories", async () => {
    // Requirement: walking a repository must not drown in vendored files.
    await mkdir(join(root, "node_modules", "pkg"), { recursive: true });
    await mkdir(join(root, ".git"), { recursive: true });
    await writeFile(join(root, "node_modules", "pkg", "README.md"), "");
    await writeFile(join(root, ".git", "COMMIT_EDITMSG.md"), "");
    await writeFile(join(root, "real.md"), "");
    expect(await findMarkdownFiles(root)).toEqual([join(root, "real.md")]);
  });

  it("does not follow symlinks", async () => {
    // Requirement: a symlinked directory or file is not the walker's own
    // content; following it risks cycles and duplicate counts.
    await mkdir(join(root, "target"));
    await writeFile(join(root, "target", "t.md"), "");
    await symlink(join(root, "target"), join(root, "link-dir"));
    await symlink(join(root, "target", "t.md"), join(root, "link.md"));
    expect(await findMarkdownFiles(root)).toEqual([join(root, "target", "t.md")]);
  });

  it("rejects when the root does not exist", async () => {
    await expect(findMarkdownFiles(join(root, "nope"))).rejects.toThrow();
  });
});

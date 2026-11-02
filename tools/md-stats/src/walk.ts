/**
 * Recursive Markdown file discovery.
 *
 * `findMarkdownFiles(root)` walks a directory and returns the paths of every
 * file whose name ends in `.md` (case-insensitive). `.git` and
 * `node_modules` are skipped — a repo stats tool should not report on
 * internal or vendored files. Output order is deterministic: entries are
 * compared bytewise, so the same tree always yields the same list.
 */
import { readdir } from "node:fs/promises";
import { join } from "node:path";

const SKIPPED_DIRS = new Set([".git", "node_modules"]);
const isMarkdown = (name: string): boolean => name.toLowerCase().endsWith(".md");

export async function findMarkdownFiles(root: string): Promise<string[]> {
  const files: string[] = [];
  const entries = [...(await readdir(root, { withFileTypes: true }))].sort((a, b) =>
    a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
  );
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      if (!SKIPPED_DIRS.has(entry.name)) files.push(...(await findMarkdownFiles(path)));
    } else if (entry.isFile() && isMarkdown(entry.name)) {
      files.push(path);
    }
  }
  return files;
}

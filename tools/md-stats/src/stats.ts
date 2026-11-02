/**
 * Filesystem layer: turn a file-or-directory target into per-file statistics.
 */
import { readFile, stat } from "node:fs/promises";
import { parseMarkdown, type MarkdownStats } from "./parse.js";
import { findMarkdownFiles } from "./walk.js";

export interface FileStats extends MarkdownStats {
  path: string;
}

export async function collectStats(target: string): Promise<FileStats[]> {
  const files = (await stat(target)).isFile()
    ? [target]
    : await findMarkdownFiles(target);
  return Promise.all(
    files.map(async (file) => ({
      path: file,
      ...parseMarkdown(await readFile(file, "utf8")),
    })),
  );
}

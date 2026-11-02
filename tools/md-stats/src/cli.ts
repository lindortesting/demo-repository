#!/usr/bin/env node
/**
 * md-stats CLI: `md-stats <file-or-dir> [--json]`.
 *
 * Thin shell around the pure pieces: parse args, collect stats, render.
 */
import { realpathSync } from "node:fs";
import { parseArgs } from "node:util";
import { fileURLToPath } from "node:url";
import { renderJson, renderText } from "./render.js";
import { collectStats } from "./stats.js";

const USAGE = `usage: md-stats <file-or-dir> [--json]

Print per-file Markdown statistics: word count, headings by level,
links and code blocks. Directories are walked recursively.

Options:
  --json  print the same data as JSON`;

export async function main(argv: string[]): Promise<string> {
  const { positionals, values } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: { json: { type: "boolean" }, help: { type: "boolean" } },
  });
  if (values.help) return USAGE;
  const target = positionals[0];
  if (!target) throw new Error(`missing <file-or-dir> argument\n\n${USAGE}`);
  const files = await collectStats(target);
  return values.json ? renderJson(files) : renderText(files);
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).then(
    (out) => console.log(out),
    (error) => {
      console.error(`md-stats: ${error instanceof Error ? error.message : String(error)}`);
      process.exitCode = 1;
    },
  );
}

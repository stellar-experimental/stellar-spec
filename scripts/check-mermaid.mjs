#!/usr/bin/env node
// Validate every ```mermaid block in the repo's Markdown files by running it
// through the Mermaid CLI (mermaid-cli, the same Mermaid engine GitHub uses to
// render diagrams). GitHub renders Mermaid client-side and silently shows
// "Unable to render rich display" on a parse error, so there is no native CI
// signal for it -- this script provides one.
//
// Usage:
//   node scripts/check-mermaid.mjs [files...]
// With no arguments it scans every tracked *.md file in the repo.
//
// Requires the `mmdc` binary on PATH (npm i -g @mermaid-js/mermaid-cli).
// Override the command via the MMDC env var (e.g. when running through npx).

import { readFileSync, writeFileSync, mkdtempSync, rmSync, readdirSync, statSync } from "node:fs";
import { execSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";

const MMDC = process.env.MMDC || "mmdc";

function findMarkdown(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    if (entry === ".git" || entry === "node_modules") continue;
    const full = join(dir, entry);
    const st = statSync(full);
    if (st.isDirectory()) out.push(...findMarkdown(full));
    else if (entry.endsWith(".md")) out.push(full);
  }
  return out;
}

// Extract fenced ```mermaid blocks, tracking the 1-based line of the opening fence.
function extractBlocks(file) {
  const lines = readFileSync(file, "utf8").split("\n");
  const blocks = [];
  let inBlock = false, buf = [], start = 0;
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!inBlock && /^\s*```mermaid\s*$/.test(line)) {
      inBlock = true; buf = []; start = i + 1; continue;
    }
    if (inBlock && /^\s*```\s*$/.test(line)) {
      inBlock = false;
      blocks.push({ file, line: start, code: buf.join("\n") });
      continue;
    }
    if (inBlock) buf.push(line);
  }
  return blocks;
}

const files = process.argv.slice(2).length
  ? process.argv.slice(2)
  : findMarkdown(process.cwd());

const blocks = files.flatMap(extractBlocks);

if (blocks.length === 0) {
  console.log("No mermaid blocks found.");
  process.exit(0);
}

// Puppeteer needs --no-sandbox on CI runners.
const work = mkdtempSync(join(tmpdir(), "mmcheck-"));
const puppeteerCfg = join(work, "puppeteer.json");
writeFileSync(puppeteerCfg, JSON.stringify({ args: ["--no-sandbox", "--disable-setuid-sandbox"] }));

let failures = 0;
for (const b of blocks) {
  const inFile = join(work, "diagram.mmd");
  const outFile = join(work, "diagram.svg");
  writeFileSync(inFile, b.code + "\n");
  try {
    execSync(`${MMDC} -i "${inFile}" -o "${outFile}" -p "${puppeteerCfg}" -q`, {
      stdio: ["ignore", "ignore", "pipe"],
    });
  } catch (err) {
    failures++;
    const msg = (err.stderr?.toString() || err.message || "").trim();
    const parseLine = msg.split("\n").find((l) => /parse error|error:/i.test(l)) || msg.split("\n")[0];
    console.error(`\n✖ ${b.file}:${b.line} (mermaid block)`);
    console.error(`  ${parseLine}`);
  }
}

rmSync(work, { recursive: true, force: true });

if (failures) {
  console.error(`\n${failures} of ${blocks.length} mermaid diagram(s) failed to render.`);
  process.exit(1);
}
console.log(`✓ All ${blocks.length} mermaid diagram(s) render.`);

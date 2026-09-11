// DDD architecture fitness functions for Shepherd.
//
// Enforces, in CI, the boundaries that keep the domain pure:
//   1. Dependency boundary  — shepherd-domain's dependency closure contains no infrastructure
//      crate, and the layering (infra -> app -> domain) is never reversed.
//   2. Purity scan          — shepherd-domain/src imports no runtime/OS/unsafe facilities.
//   3. Ubiquitous language  — every public type in shepherd-domain appears in docs/GLOSSARY.md.
//
// Run: `node --experimental-strip-types check.ts` (Node >= 22) from anywhere.

import { execSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "..", "..");

const failures: string[] = [];
const fail = (msg: string) => failures.push(msg);

// Crates that must never appear in the domain's dependency closure.
const INFRA_CRATES = [
  "tokio",
  "nix",
  "libc",
  "mio",
  "async-trait",
  "cgroups-rs",
  "process-wrap",
  "windows-sys",
  "windows",
  "shepherd-app",
  "shepherd-infra",
  "shepherd",
];

type MetaNode = { id: string; deps: { pkg: string }[] };
type MetaPkg = { id: string; name: string };
type Meta = {
  packages: MetaPkg[];
  resolve: { nodes: MetaNode[] };
};

function cargoMetadata(): Meta {
  const raw = execSync("cargo metadata --format-version=1", {
    cwd: repoRoot,
    maxBuffer: 64 * 1024 * 1024,
  }).toString();
  return JSON.parse(raw) as Meta;
}

function transitiveDeps(meta: Meta, rootName: string): Set<string> {
  const idToName = new Map(meta.packages.map((p) => [p.id, p.name]));
  const nodeById = new Map(meta.resolve.nodes.map((n) => [n.id, n]));
  const rootId = meta.packages.find((p) => p.name === rootName)?.id;
  if (!rootId) {
    fail(`could not find crate '${rootName}' in cargo metadata`);
    return new Set();
  }
  const seen = new Set<string>();
  const stack = [rootId];
  while (stack.length > 0) {
    const id = stack.pop()!;
    const node = nodeById.get(id);
    if (!node) continue;
    for (const dep of node.deps) {
      const name = idToName.get(dep.pkg);
      if (name && !seen.has(name)) {
        seen.add(name);
        stack.push(dep.pkg);
      }
    }
  }
  return seen;
}

function checkDependencyBoundary(meta: Meta): void {
  const domainDeps = transitiveDeps(meta, "shepherd-domain");
  for (const forbidden of INFRA_CRATES) {
    if (domainDeps.has(forbidden)) {
      fail(`shepherd-domain must not depend on '${forbidden}' (found in dependency closure)`);
    }
  }

  // Layering: app may not depend on infra, the facade, or any OS-containment crate. (libc is
  // permitted transitively via tokio; the rule targets infrastructure adapters, not the
  // async runtime.)
  const appDeps = transitiveDeps(meta, "shepherd-app");
  for (const forbidden of [
    "shepherd-infra",
    "shepherd",
    "process-wrap",
    "cgroups-rs",
    "nix",
  ]) {
    if (appDeps.has(forbidden)) {
      fail(`shepherd-app must not depend on '${forbidden}'`);
    }
  }
}

function rustFiles(dir: string): string[] {
  return readdirSync(dir, { recursive: true, encoding: "utf8" })
    .filter((f) => f.endsWith(".rs"))
    .map((f) => join(dir, f));
}

const FORBIDDEN_PATTERNS: { re: RegExp; label: string }[] = [
  { re: /\btokio\b/, label: "tokio" },
  { re: /\bnix\b/, label: "nix" },
  { re: /\blibc\b/, label: "libc" },
  { re: /std::process/, label: "std::process" },
  { re: /std::fs/, label: "std::fs" },
  { re: /std::net/, label: "std::net" },
  { re: /\bunsafe\b/, label: "unsafe" },
  { re: /process_wrap/, label: "process-wrap" },
  { re: /windows_sys/, label: "windows-sys" },
];

function checkPurity(): void {
  const srcDir = join(repoRoot, "crates", "shepherd-domain", "src");
  for (const file of rustFiles(srcDir)) {
    const text = readFileSync(file, "utf8");
    for (const line of text.split("\n")) {
      if (line.trimStart().startsWith("//")) continue; // ignore comments
      for (const { re, label } of FORBIDDEN_PATTERNS) {
        if (re.test(line)) {
          fail(`purity violation in ${file}: forbidden reference to '${label}': ${line.trim()}`);
        }
      }
    }
  }
}

function checkGlossaryCoverage(): void {
  const srcDir = join(repoRoot, "crates", "shepherd-domain", "src");
  const glossary = readFileSync(join(repoRoot, "docs", "GLOSSARY.md"), "utf8");
  const typeRe = /^\s*pub\s+(?:struct|enum|trait|type)\s+([A-Z][A-Za-z0-9]*)/;
  const seen = new Set<string>();
  for (const file of rustFiles(srcDir)) {
    for (const line of readFileSync(file, "utf8").split("\n")) {
      const m = line.match(typeRe);
      if (m) seen.add(m[1]);
    }
  }
  for (const name of [...seen].sort()) {
    if (!glossary.includes(name)) {
      fail(`ubiquitous-language drift: public domain type '${name}' is not documented in docs/GLOSSARY.md`);
    }
  }
}

const meta = cargoMetadata();
checkDependencyBoundary(meta);
checkPurity();
checkGlossaryCoverage();

if (failures.length > 0) {
  console.error("DDD architecture check FAILED:\n");
  for (const f of failures) console.error(`  - ${f}`);
  process.exit(1);
}
console.log("DDD architecture check passed: domain is pure, boundaries hold, glossary is in sync.");

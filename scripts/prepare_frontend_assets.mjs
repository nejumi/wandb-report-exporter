import {mkdir, copyFile, access} from "node:fs/promises";
import path from "node:path";
import {fileURLToPath} from "node:url";

const rootDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const vendorDir = path.join(rootDir, "app", "src", "vendor", "duckdb");

const filesToCopy = [
  "duckdb-mvp.wasm",
  "duckdb-eh.wasm",
  "duckdb-browser-mvp.worker.js",
  "duckdb-browser-eh.worker.js"
];

async function ensureInstalled(filePath) {
  try {
    await access(filePath);
  } catch {
    throw new Error(
      "Frontend dependencies are missing. Run `npm install` before building or previewing."
    );
  }
}

await mkdir(vendorDir, {recursive: true});

for (const name of filesToCopy) {
  const source = path.join(rootDir, "node_modules", "@duckdb", "duckdb-wasm", "dist", name);
  const destination = path.join(vendorDir, name);
  await ensureInstalled(source);
  await copyFile(source, destination);
}

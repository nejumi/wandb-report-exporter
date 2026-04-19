import {cp, mkdir, access, readdir, readFile, writeFile} from "node:fs/promises";
import path from "node:path";
import {fileURLToPath} from "node:url";

const rootDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const distDir = path.join(rootDir, "dist");
const sourceDir = path.join(rootDir, "app", "src");
const folders = ["data", "media", "vendor", "lib"];

await access(distDir);

for (const folder of folders) {
  const from = path.join(sourceDir, folder);
  const to = path.join(distDir, folder);
  await mkdir(to, {recursive: true});
  await cp(from, to, {recursive: true, force: true});
}

const distEntries = await readdir(distDir);
for (const entry of distEntries) {
  if (!entry.endsWith(".html") || entry === "index.html") {
    continue;
  }
  const stem = entry.slice(0, -".html".length);
  const targetDir = path.join(distDir, stem);
  await mkdir(targetDir, {recursive: true});
  const sourceHtml = await readFile(path.join(distDir, entry), "utf-8");
  const rewritten = sourceHtml.includes("<base ")
    ? sourceHtml
    : sourceHtml.replace("<head>", '<head>\n<base href="../">');
  await writeFile(path.join(targetDir, "index.html"), rewritten, "utf-8");
}

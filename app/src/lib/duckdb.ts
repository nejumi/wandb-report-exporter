import * as duckdb from "@duckdb/duckdb-wasm";
import {dataFile, loadManifest} from "./data.ts";

type DuckResultRow = Record<string, unknown>;

const bundleMap: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: new URL("../vendor/duckdb/duckdb-mvp.wasm", import.meta.url).toString(),
    mainWorker: new URL("../vendor/duckdb/duckdb-browser-mvp.worker.js", import.meta.url).toString(),
    pthreadWorker: null
  },
  eh: {
    mainModule: new URL("../vendor/duckdb/duckdb-eh.wasm", import.meta.url).toString(),
    mainWorker: new URL("../vendor/duckdb/duckdb-browser-eh.worker.js", import.meta.url).toString(),
    pthreadWorker: null
  }
};

let dbPromise: Promise<duckdb.AsyncDuckDB> | null = null;
let connectionPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null;
const registeredFiles = new Set<string>();
const createdViews = new Set<string>();

async function getDatabase(): Promise<duckdb.AsyncDuckDB> {
  if (!dbPromise) {
    dbPromise = (async () => {
      const bundle = await duckdb.selectBundle(bundleMap);
      const worker = new Worker(bundle.mainWorker!);
      const logger = new duckdb.ConsoleLogger();
      const db = new duckdb.AsyncDuckDB(logger, worker);
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      return db;
    })();
  }
  return dbPromise;
}

async function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  if (!connectionPromise) {
    connectionPromise = (async () => {
      const db = await getDatabase();
      return db.connect();
    })();
  }
  return connectionPromise;
}

async function ensureParquetRegistered(fileName: string): Promise<void> {
  if (registeredFiles.has(fileName)) {
    return;
  }
  const response = await fetch(dataFile(fileName));
  if (!response.ok) {
    throw new Error(`Unable to load ${fileName}. Run \`make export\` first.`);
  }
  const buffer = new Uint8Array(await response.arrayBuffer());
  const db = await getDatabase();
  await db.registerFileBuffer(fileName, buffer);
  registeredFiles.add(fileName);
}

function viewNameFor(fileName: string): string {
  return fileName.replace(/\.parquet$/u, "");
}

async function ensureView(fileName: string): Promise<void> {
  if (createdViews.has(fileName)) {
    return;
  }
  await ensureParquetRegistered(fileName);
  const connection = await getConnection();
  const viewName = viewNameFor(fileName);
  await connection.query(`CREATE OR REPLACE VIEW ${viewName} AS SELECT * FROM read_parquet('${fileName}')`);
  createdViews.add(fileName);
}

function normalizeResultRows(result: any): DuckResultRow[] {
  if (!result || typeof result.toArray !== "function") {
    return [];
  }
  return result.toArray().map((row: any) => {
    if (row && typeof row.toJSON === "function") {
      return row.toJSON();
    }
    if (row && typeof row === "object") {
      return {...row};
    }
    return {value: row};
  });
}

export async function queryRows(sql: string, files: string[]): Promise<DuckResultRow[]> {
  const manifest = await loadManifest();
  const available = new Set(Object.values(manifest.datasets));
  for (const fileName of files) {
    if (!available.has(fileName)) {
      throw new Error(`Dataset ${fileName} was not declared in report_manifest.json.`);
    }
    await ensureView(fileName);
  }
  const connection = await getConnection();
  const result = await connection.query(sql);
  return normalizeResultRows(result);
}

import type {ReportManifest} from "./types.ts";

let manifestPromise: Promise<ReportManifest> | null = null;

function dataUrl(fileName: string): string {
  return new URL(`../data/${fileName}`, import.meta.url).toString();
}

export function mediaUrl(relativePath: string | null | undefined): string | null {
  if (!relativePath) {
    return null;
  }
  return new URL(`../${relativePath}`, import.meta.url).toString();
}

export async function loadManifest(): Promise<ReportManifest> {
  if (!manifestPromise) {
    manifestPromise = fetch(dataUrl("report_manifest.json")).then(async (response) => {
      if (!response.ok) {
        throw new Error("report_manifest.json is missing. Run `make export` first.");
      }
      return response.json() as Promise<ReportManifest>;
    });
  }
  return manifestPromise;
}

export function dataFile(name: string): string {
  return dataUrl(name);
}

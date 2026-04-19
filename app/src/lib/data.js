let manifestPromise = null;

function dataUrl(fileName) {
  return new URL(`../../data/${fileName}`, import.meta.url).toString();
}

export function mediaUrl(relativePath) {
  if (!relativePath) {
    return null;
  }
  return new URL(`../../${relativePath}`, import.meta.url).toString();
}

export async function loadManifest() {
  if (!manifestPromise) {
    manifestPromise = fetch(dataUrl("report_manifest.json")).then(async (response) => {
      if (!response.ok) {
        throw new Error("report_manifest.json is missing. Run `make export` first.");
      }
      return response.json();
    });
  }
  return manifestPromise;
}

export function dataFile(name) {
  return dataUrl(name);
}

export async function loadReport() {
  const manifest = await loadManifest();
  return manifest.report || {blocks: []};
}

export async function loadPanelTable(relativePath) {
  const response = await fetch(dataUrl(relativePath));
  if (!response.ok) {
    throw new Error(`Unable to load panel table at ${relativePath}.`);
  }
  return response.json();
}

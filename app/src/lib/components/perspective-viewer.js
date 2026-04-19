import perspective from "@finos/perspective";
import "@finos/perspective-viewer";
import "@finos/perspective-viewer-datagrid";

let workerPromise = null;

async function getWorker() {
  if (!workerPromise) {
    workerPromise = perspective.worker();
  }
  return workerPromise;
}

export async function renderPerspective(container, rows) {
  container.className = "surface surface--padded perspective-shell";
  container.innerHTML = "";
  const title = document.createElement("div");
  title.innerHTML = `
    <h2 class="section-title">Pivot Explorer</h2>
    <p class="section-copy">Group by model, dataset, split, or correctness without leaving the static site.</p>
  `;
  const viewer = document.createElement("perspective-viewer");
  container.append(title, viewer);

  const worker = await getWorker();
  const table = await worker.table(rows);
  await viewer.load(table);
  await viewer.restore({
    plugin: "Datagrid",
    group_by: ["model_name"],
    split_by: ["dataset_name"],
    columns: ["primary_metric", "accuracy", "score", "correct"],
    aggregates: {
      primary_metric: "avg",
      accuracy: "avg",
      score: "avg",
      correct: "avg"
    }
  });
}

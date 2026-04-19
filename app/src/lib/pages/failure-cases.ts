import type {ColDef} from "ag-grid-community";
import {mediaUrl} from "../data.ts";
import {renderGrid} from "../components/ag-grid-table.ts";
import {queryRows} from "../duckdb.ts";
import {valueClass} from "../format.ts";

export async function mountFailureCasesPage(root: HTMLElement): Promise<void> {
  const hero = document.createElement("section");
  hero.className = "hero";
  hero.innerHTML = `
    <div class="hero__eyebrow">AG Grid + local media</div>
    <h1 class="hero__title">Failure-case browser</h1>
    <p class="hero__subtitle">Thumbnail-first browsing keeps the table responsive while preserving a path back to the original run and artifact.</p>
  `;
  root.append(hero);

  const rows = await queryRows(
    `
      SELECT
        row_id,
        run_name,
        model_name,
        dataset_name,
        split,
        slice_name,
        input_text,
        prediction,
        label,
        correct,
        score,
        image_thumb_path,
        wandb_run_url
      FROM table_predictions
      ORDER BY correct ASC NULLS LAST, score DESC NULLS LAST
    `,
    ["table_predictions.parquet"]
  );

  if (rows.length === 0) {
    const empty = document.createElement("section");
    empty.className = "surface surface--padded empty-state";
    empty.innerHTML = `
      <h2 class="section-title">No table rows exported yet</h2>
      <p class="section-copy">
        Set <code>WANDB_TABLE_NAME</code> and optionally <code>WANDB_TABLE_ARTIFACT</code>, then rerun <code>make export</code>.
      </p>
    `;
    root.append(empty);
    return;
  }

  const tableContainer = document.createElement("div");
  root.append(tableContainer);

  const columnDefs: ColDef[] = [
    {
      field: "image_thumb_path",
      headerName: "Thumb",
      filter: false,
      sortable: false,
      width: 110,
      cellRenderer: ({value}) => {
        const url = mediaUrl(typeof value === "string" ? value : null);
        if (!url) {
          return "";
        }
        const image = document.createElement("img");
        image.src = url;
        image.alt = "thumbnail";
        image.loading = "lazy";
        image.className = "thumb";
        return image;
      }
    },
    {field: "run_name", headerName: "Run", minWidth: 180},
    {field: "model_name", headerName: "Model"},
    {field: "dataset_name", headerName: "Dataset"},
    {field: "slice_name", headerName: "Slice"},
    {field: "input_text", headerName: "Input", minWidth: 260, flex: 1},
    {field: "prediction", headerName: "Prediction"},
    {field: "label", headerName: "Label"},
    {
      field: "correct",
      headerName: "Correct",
      cellRenderer: ({value}) => {
        const span = document.createElement("span");
        span.className = valueClass(Boolean(value));
        span.textContent = value ? "Correct" : "Miss";
        return span;
      }
    },
    {
      field: "score",
      headerName: "Score",
      valueFormatter: ({value}) => (value == null ? "n/a" : Number(value).toFixed(3))
    },
    {
      field: "wandb_run_url",
      headerName: "Source",
      filter: false,
      sortable: false,
      cellRenderer: ({value}) => {
        if (!value) {
          return "";
        }
        const link = document.createElement("a");
        link.href = String(value);
        link.textContent = "Run";
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        return link;
      }
    }
  ];

  renderGrid({container: tableContainer, columnDefs, rowData: rows});
}

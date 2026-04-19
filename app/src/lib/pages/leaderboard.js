import {queryRows} from "../duckdb.js";
import {formatDate} from "../format.js";
import {renderGrid} from "../components/ag-grid-table.js";

export async function mountLeaderboardPage(root) {
  const hero = document.createElement("section");
  hero.className = "hero";
  hero.innerHTML = `
    <div class="hero__eyebrow">AG Grid</div>
    <h1 class="hero__title">Run leaderboard</h1>
    <p class="hero__subtitle">Virtualized table UX for the most common report-reading workflow: compare runs, filter fast, and jump to the source when needed.</p>
  `;
  root.append(hero);

  const rows = await queryRows(
    `
      SELECT
        run_name,
        model_name,
        dataset_name,
        eval_split,
        state,
        created_at,
        COALESCE(primary_metric, accuracy, 0) AS accuracy,
        loss,
        wandb_url
      FROM run_summary
      ORDER BY accuracy DESC
    `,
    ["run_summary.parquet"]
  );

  const shell = document.createElement("section");
  root.append(shell);
  const tableContainer = document.createElement("div");
  shell.append(tableContainer);

  const columnDefs = [
    {field: "run_name", headerName: "Run", pinned: "left", minWidth: 220},
    {field: "model_name", headerName: "Model"},
    {field: "dataset_name", headerName: "Dataset"},
    {field: "eval_split", headerName: "Split"},
    {field: "state", headerName: "State"},
    {
      field: "created_at",
      headerName: "Created",
      valueFormatter: ({value}) => formatDate(typeof value === "string" ? value : "")
    },
    {
      field: "accuracy",
      headerName: "Accuracy",
      sort: "desc",
      valueFormatter: ({value}) => Number(value).toFixed(3)
    },
    {
      field: "loss",
      headerName: "Loss",
      valueFormatter: ({value}) => (value == null ? "n/a" : Number(value).toFixed(3))
    },
    {
      field: "wandb_url",
      headerName: "W&B",
      filter: false,
      sortable: false,
      minWidth: 150,
      cellRenderer: ({value}) => {
        if (!value) {
          return "";
        }
        const link = document.createElement("a");
        link.href = String(value);
        link.textContent = "Open run";
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        return link;
      }
    }
  ];

  renderGrid({container: tableContainer, columnDefs, rowData: rows});
}

import {queryRows} from "../duckdb.js";
import {renderPerspective} from "../components/perspective-viewer.js";

export async function mountPivotExplorerPage(root) {
  const hero = document.createElement("section");
  hero.className = "hero";
  hero.innerHTML = `
    <div class="hero__eyebrow">Perspective</div>
    <h1 class="hero__title">Slice and pivot explorer</h1>
    <p class="hero__subtitle">AG Grid stays focused on reading, while Perspective handles self-service grouping and aggregation for ad-hoc breakdowns.</p>
  `;
  root.append(hero);

  const rows = await queryRows(
    `
      WITH failure_rollup AS (
        SELECT
          run_id,
          AVG(CASE WHEN correct THEN 1 ELSE 0 END) AS correct_rate,
          AVG(score) AS score
        FROM table_predictions
        GROUP BY 1
      )
      SELECT
        r.run_name,
        r.model_name,
        r.dataset_name,
        r.eval_split,
        r.primary_metric,
        r.accuracy,
        r.loss,
        f.correct_rate,
        f.score
      FROM run_summary r
      LEFT JOIN failure_rollup f USING (run_id)
      ORDER BY r.primary_metric DESC NULLS LAST
    `,
    ["run_summary.parquet", "table_predictions.parquet"]
  );

  if (rows.length === 0) {
    const empty = document.createElement("section");
    empty.className = "surface surface--padded empty-state";
    empty.textContent = "No rows were available for the pivot explorer.";
    root.append(empty);
    return;
  }

  const container = document.createElement("section");
  root.append(container);
  await renderPerspective(container, rows);
}

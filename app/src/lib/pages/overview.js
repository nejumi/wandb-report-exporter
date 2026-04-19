import * as Plot from "@observablehq/plot";
import {loadManifest} from "../data.js";
import {formatDate, formatInteger, formatNumber} from "../format.js";
import {metricCards} from "../components/summary-cards.js";
import {queryRows} from "../duckdb.js";

function section(title, copy) {
  const node = document.createElement("section");
  node.className = "surface surface--padded";
  node.innerHTML = `<h2 class="section-title">${title}</h2><p class="section-copy">${copy}</p>`;
  return node;
}

export async function mountOverviewPage(root) {
  const manifest = await loadManifest();
  const hero = document.createElement("section");
  hero.className = "hero";
  hero.innerHTML = `
    <div class="hero__eyebrow">Static snapshot · ${manifest.source}</div>
    <h1 class="hero__title">Fast W&B report delivery</h1>
    <p class="hero__subtitle">
      Observable handles the narrative shell, DuckDB-Wasm reads local Parquet in-browser,
      and the viewer stays offline after build.
    </p>
    <div class="page-meta">
      <span class="pill">${manifest.entity || "demo-entity"}/${manifest.project || "demo-project"}</span>
      <span class="pill">${formatDate(manifest.generated_at)}</span>
      <span class="pill">${formatInteger(manifest.counts.runs)} runs</span>
      <span class="pill">${formatInteger(manifest.counts.table_rows)} table rows</span>
    </div>
  `;
  root.append(hero);

  const [summaryStats] = await queryRows(
    `
      SELECT
        COUNT(*) AS run_count,
        MAX(COALESCE(primary_metric, accuracy, 0)) AS best_metric,
        AVG(COALESCE(primary_metric, accuracy, 0)) AS avg_metric
      FROM run_summary
    `,
    ["run_summary.parquet"]
  );

  root.append(
    metricCards({
      runCount: Number(summaryStats?.run_count ?? 0),
      bestMetric: Number(summaryStats?.best_metric ?? 0),
      avgMetric: Number(summaryStats?.avg_metric ?? 0),
      tableRows: manifest.counts.table_rows
    })
  );

  const grid = document.createElement("div");
  grid.className = "two-up";
  root.append(grid);

  const chartSection = section("Training shape", "The chart below is queried from exported Parquet at runtime, not embedded JSON.");
  const leaderboardSection = section("Top runs", "Deep links stay available, but normal browsing remains fully local.");
  grid.append(chartSection, leaderboardSection);

  const historyRows = await queryRows(
    `
      WITH top_runs AS (
        SELECT run_id
        FROM run_summary
        ORDER BY primary_metric DESC NULLS LAST
        LIMIT 4
      )
      SELECT
        r.run_name,
        h.step,
        h.metric_value
      FROM history_eval_metrics h
      JOIN run_summary r USING (run_id)
      JOIN top_runs t USING (run_id)
      WHERE h.metric_name = 'eval/accuracy'
      ORDER BY h.step
    `,
    ["history_eval_metrics.parquet", "run_summary.parquet"]
  );

  if (historyRows.length > 0) {
    const plot = Plot.plot({
      height: 320,
      marginLeft: 54,
      color: {scheme: "greens"},
      marks: [
        Plot.line(historyRows, {x: "step", y: "metric_value", stroke: "run_name"}),
        Plot.dot(historyRows.filter((_, index) => index % 10 === 0), {
          x: "step",
          y: "metric_value",
          stroke: "run_name",
          r: 2.5
        })
      ]
    });
    chartSection.append(plot);
  }

  const leaders = await queryRows(
    `
      SELECT
        run_name,
        model_name,
        dataset_name,
        COALESCE(primary_metric, accuracy, 0) AS metric,
        wandb_url
      FROM run_summary
      ORDER BY metric DESC
      LIMIT 6
    `,
    ["run_summary.parquet"]
  );

  const leaderList = document.createElement("div");
  leaderList.className = "leader-list";
  for (const row of leaders) {
    const item = document.createElement("article");
    item.className = "leader-list__item";
    item.innerHTML = `
      <div>
        <div><strong>${row.run_name}</strong></div>
        <div class="muted">${row.model_name} · ${row.dataset_name}</div>
      </div>
      <div style="text-align:right">
        <div><strong>${formatNumber(row.metric, 3)}</strong></div>
        <div><a href="${row.wandb_url}" target="_blank" rel="noopener noreferrer">Open run</a></div>
      </div>
    `;
    leaderList.append(item);
  }
  leaderboardSection.append(leaderList);
}

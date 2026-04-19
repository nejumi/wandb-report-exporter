import {formatInteger, formatNumber} from "../format.js";

export function renderSummaryCards(cards) {
  const wrapper = document.createElement("div");
  wrapper.className = "summary-grid";
  for (const card of cards) {
    const node = document.createElement("article");
    node.className = "summary-card";
    node.innerHTML = `
      <div class="summary-card__label">${card.label}</div>
      <div class="summary-card__value">${card.value}</div>
      <div class="summary-card__hint">${card.hint}</div>
    `;
    wrapper.append(node);
  }
  return wrapper;
}

export function metricCards(stats) {
  return renderSummaryCards([
    {
      label: "Runs captured",
      value: formatInteger(stats.runCount),
      hint: "Build-time snapshot only. No runtime W&B API calls."
    },
    {
      label: "Best accuracy",
      value: formatNumber(stats.bestMetric, 3),
      hint: "Top run in run_summary.parquet."
    },
    {
      label: "Average accuracy",
      value: formatNumber(stats.avgMetric, 3),
      hint: "Across all runs in the exported snapshot."
    },
    {
      label: "Prediction rows",
      value: formatInteger(stats.tableRows),
      hint: "Rows available for failure-case triage."
    }
  ]);
}

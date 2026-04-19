import {loadReport, loadPanelTable, mediaUrl} from "../data.js";
import {queryRows} from "../duckdb.js";
import {renderGrid} from "../components/ag-grid-table.js";

const SVG_NS = "http://www.w3.org/2000/svg";
const RADAR_COLORS = ["#0f766e", "#2563eb", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#65a30d", "#db2777"];
const GRID_ROW_HEIGHT = 20;
let vegaEmbedPromise = null;
let plotlyPromise = null;
let historyRowsPromise = null;
let tablePredictionsRowsPromise = null;
let imageDialog = null;

function sortPanels(panels) {
  return [...(panels || [])].sort((left, right) => {
    const ly = Number(left?.layout?.y || 0);
    const ry = Number(right?.layout?.y || 0);
    if (ly !== ry) return ly - ry;
    const lx = Number(left?.layout?.x || 0);
    const rx = Number(right?.layout?.x || 0);
    if (lx !== rx) return lx - rx;
    return 0;
  });
}

function parseMarkdownInline(text) {
  return String(text || "")
    .replace(/^##\s+(.+)$/gm, "<h2>$1</h2>")
    .replace(/^#\s+(.+)$/gm, "<h1>$1</h1>")
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
}

function summarizeMetricTitle(metrics) {
  const values = (metrics || []).map((value) => String(value)).filter(Boolean);
  if (!values.length) return "";
  if (values.length === 1) return values[0];
  const prefixMatch = values[0].match(/^(.*?)(\d+)(\..*)$/u);
  if (prefixMatch) {
    const [, prefix, , suffix] = prefixMatch;
    const sameFamily = values.every((value) => value.startsWith(prefix) && value.endsWith(suffix));
    if (sameFamily) {
      return `${prefix}*${suffix} (${values.length} series)`;
    }
  }
  return `${values[0]} + ${values.length - 1} more`;
}

function humanizeLabel(value) {
  return String(value || "")
    .replace(/[_-]+/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function flattenSelectionTree(node) {
  if (Array.isArray(node)) {
    return node.flatMap((item) => flattenSelectionTree(item));
  }
  if (typeof node === "string") {
    return [node];
  }
  if (!node || typeof node !== "object") {
    return [];
  }
  const values = [];
  for (const key of ["children", "items", "tree", "value", "values"]) {
    if (key in node) {
      values.push(...flattenSelectionTree(node[key]));
    }
  }
  return values;
}

function matchedRunsets(report, runsetNames = []) {
  return (report?.runsets || []).filter((runset) => {
    const runsetId = String(runset?.id || "");
    const runsetName = String(runset?.name || "");
    return runsetNames.some((token) => {
      const value = String(token || "");
      return value && (value === runsetId || value === runsetName);
    });
  });
}

function runsetSelectionRoot(runset) {
  const root = runset?.selections?.root;
  return root === 0 || root === 1 ? Number(root) : null;
}

function runsetSelectionMode(runset) {
  const explicitValues = flattenSelectionTree(runset?.selections?.tree);
  if (runset?.only_show_selected || runset?.single_run_only) {
    return "include";
  }
  if (!explicitValues.length) {
    return null;
  }
  const root = runsetSelectionRoot(runset);
  if (root === 0) return "include";
  if (root === 1) return "exclude";
  return null;
}

function runsetSelectionValues(report, runsetNames = []) {
  const values = new Set();
  for (const runset of matchedRunsets(report, runsetNames)) {
    for (const value of flattenSelectionTree(runset?.selections?.tree)) {
      if (typeof value === "string" && value.trim()) {
        values.add(value.trim());
      }
    }
  }
  if (!values.size && runsetNames.includes(report?.selected_runset?.name)) {
    for (const value of report?.selected_runset?.selection_run_ids || []) {
      if (value) values.add(String(value));
    }
    for (const value of report?.selected_runset?.selection_names || []) {
      if (value) values.add(String(value));
    }
  }
  return values;
}

function inferBlockVisibleValues(report, block) {
  const values = new Set();
  if (!block || block.type !== "panel-grid") {
    return values;
  }
  for (const value of block.visible_run_ids || []) {
    if (value) values.add(String(value));
  }
  for (const value of block.visible_run_names || []) {
    if (value) values.add(String(value));
  }
  if (values.size) {
    return values;
  }

  for (const runset of matchedRunsets(report, block.runsets || [])) {
    for (const value of runset?.visible_run_ids || []) {
      if (value) values.add(String(value));
    }
    for (const value of runset?.visible_run_names || []) {
      if (value) values.add(String(value));
    }
  }
  if (values.size) {
    return values;
  }

  const candidateRunIds = new Set();
  const candidateRunNames = new Set();
  for (const panel of block.panels || []) {
    const tableKey = panel?.table_key;
    if (tableKey) {
      const tableMeta = report?.panel_tables?.[tableKey] || {};
      for (const value of tableMeta?.run_ids || []) {
        if (value) candidateRunIds.add(String(value));
      }
      for (const value of tableMeta?.run_names || []) {
        if (value) candidateRunNames.add(String(value));
      }
    }
    for (const item of panel?.media_items || []) {
      if (item?.run_id) candidateRunIds.add(String(item.run_id));
      if (item?.run_name) candidateRunNames.add(String(item.run_name));
    }
  }

  for (const runset of matchedRunsets(report, block.runsets || [])) {
    const selectionValues = runsetSelectionValues(report, [runset?.id || runset?.name].filter(Boolean));
    const selectionMode = runsetSelectionMode(runset);
    if (!selectionValues.size || !selectionMode) {
      continue;
    }
    if (selectionMode === "include") {
      for (const value of candidateRunIds) {
        if (selectionValues.has(value)) values.add(value);
      }
      for (const value of candidateRunNames) {
        if (selectionValues.has(value)) values.add(value);
      }
      continue;
    }
    const selectedCandidateIds = [...candidateRunIds].filter((value) => selectionValues.has(value));
    const selectedCandidateNames = [...candidateRunNames].filter((value) => selectionValues.has(value));
    if (!selectedCandidateIds.length && !selectedCandidateNames.length) {
      continue;
    }
    if (candidateRunIds.size) {
      for (const value of candidateRunIds) {
        if (!selectionValues.has(value)) values.add(value);
      }
      continue;
    }
    for (const value of candidateRunNames) {
      if (!selectionValues.has(value)) values.add(value);
    }
  }
  return values;
}

function shouldUseSelectionFallback(report, runsetNames = []) {
  return matchedRunsets(report, runsetNames).some((runset) => Boolean(runsetSelectionMode(runset)));
}

function filterRowsByRunsetSelection(rows, report, runsetNames = [], options = {}) {
  if (!Array.isArray(rows)) {
    return [];
  }
  if (!runsetNames.length) {
    return rows;
  }
  const visible = options.visibleValues instanceof Set ? options.visibleValues : new Set();
  if (visible.size) {
    const filteredRows = rows.filter((row) => {
      const runId = row?.__run_id ?? row?.run_id;
      const runName = row?.__run_name ?? row?.run_name;
      return visible.has(String(runId || "")) || visible.has(String(runName || ""));
    });
    if (filteredRows.length) {
      return filteredRows;
    }
    return rows;
  }
  if (!options.useSelectionFallback) {
    return rows;
  }
  const selected = runsetSelectionValues(report, runsetNames);
  if (!selected.size) {
    return rows;
  }
  const matched = matchedRunsets(report, runsetNames);
  const includeMode = matched.some((runset) => runsetSelectionMode(runset) === "include");
  const selectedRows = rows.filter((row) => {
    const runId = row?.__run_id ?? row?.run_id;
    const runName = row?.__run_name ?? row?.run_name;
    return selected.has(String(runId || "")) || selected.has(String(runName || ""));
  });
  if (includeMode) {
    return selectedRows.length ? selectedRows : rows;
  }
  const remainingRows = rows.filter((row) => {
    const runId = row?.__run_id ?? row?.run_id;
    const runName = row?.__run_name ?? row?.run_name;
    return !selected.has(String(runId || "")) && !selected.has(String(runName || ""));
  });
  return remainingRows.length ? remainingRows : rows;
}

function panelNeedsOwnRow(panel) {
  return panel.view_type === "Media Browser" || panel.mode === "table";
}

function blockPanelTableKeys(block) {
  if (!block || block.type !== "panel-grid") {
    return [];
  }
  return [...new Set((block.panels || []).map((panel) => panel.table_key).filter(Boolean))];
}

function blockHasPanelMode(block, mode) {
  return Boolean(block?.type === "panel-grid" && (block.panels || []).some((panel) => panel.mode === mode));
}

function isReorderableTableBlock(block) {
  if (block?.type !== "panel-grid") {
    return false;
  }
  const panels = block.panels || [];
  const dataPanels = panels.filter((panel) => panel?.view_type && panel.view_type !== "Markdown Panel");
  if (!dataPanels.length) {
    return false;
  }
  return dataPanels.every((panel) => panel.mode === "table");
}

function isReorderablePlotBlock(block) {
  if (block?.type !== "panel-grid") {
    return false;
  }
  const panels = block.panels || [];
  const dataPanels = panels.filter((panel) => panel?.view_type && panel.view_type !== "Markdown Panel");
  if (!dataPanels.length) {
    return false;
  }
  return dataPanels.every((panel) => panel.mode === "plot");
}

function reorderReportBlocks(blocks) {
  const reordered = [...(blocks || [])];
  for (let index = 0; index < reordered.length; index += 1) {
    const block = reordered[index];
    if (!isReorderableTableBlock(block)) continue;
    const tableKeys = blockPanelTableKeys(block);
    if (!tableKeys.length) continue;
    for (let earlier = index - 1; earlier >= 0; earlier -= 1) {
      const candidate = reordered[earlier];
      if (!isReorderablePlotBlock(candidate)) continue;
      const candidateKeys = blockPanelTableKeys(candidate);
      if (!candidateKeys.some((key) => tableKeys.includes(key))) continue;
      const [moved] = reordered.splice(index, 1);
      reordered.splice(earlier, 0, moved);
      index = earlier;
      break;
    }
  }
  return reordered;
}

function expressionLabel(expr) {
  if (!expr || typeof expr !== "object" || expr.kind !== "op") {
    return null;
  }
  if (expr.name === "pick") {
    const value = expr.inputs?.key?.value;
    return value == null ? null : String(value);
  }
  if (expr.name === "run-name") return "__run_name";
  if (expr.name === "run-id") return "__run_id";
  return expr.name || null;
}

function evaluateExpression(expr, row) {
  if (!expr || typeof expr !== "object") {
    return null;
  }
  if (expr.kind === "const") {
    return expr.value;
  }
  if (expr.kind !== "op") {
    return null;
  }
  const inputs = expr.inputs || {};
  if (expr.name === "pick") {
    const key = evaluateExpression(inputs.key, row);
    return key == null ? null : row[String(key)];
  }
  if (expr.name === "run-name") return row.__run_name;
  if (expr.name === "run-id") return row.__run_id;
  const lhs = evaluateExpression(inputs.lhs, row);
  const rhs = evaluateExpression(inputs.rhs, row);
  try {
    if (expr.name === "number-add") return Number(lhs) + Number(rhs);
    if (expr.name === "number-sub") return Number(lhs) - Number(rhs);
    if (expr.name === "number-mult") return Number(lhs) * Number(rhs);
    if (expr.name === "number-div") return Number(rhs) === 0 ? null : Number(lhs) / Number(rhs);
  } catch {
    return null;
  }
  return null;
}

function sortValue(value) {
  if (value == null || value === "") {
    return [2, ""];
  }
  if (typeof value === "number") {
    return [0, value];
  }
  if (isTimestampLike(value, "")) {
    return [0, Number(new Date(String(value)).getTime()) || Number(value) || 0];
  }
  return [1, String(value)];
}

function titleForReport(report) {
  return report.title || "W&B Report";
}

function loadVegaEmbed() {
  if (!vegaEmbedPromise) {
    vegaEmbedPromise = import("vega-embed").then((module) => module.default);
  }
  return vegaEmbedPromise;
}

function loadPlotly() {
  if (!plotlyPromise) {
    plotlyPromise = import("plotly.js-dist-min").then((module) => module.default || module);
  }
  return plotlyPromise;
}

async function loadHistoryRows() {
  if (!historyRowsPromise) {
    historyRowsPromise = queryRows("SELECT * FROM history_eval_metrics", ["history_eval_metrics.parquet"]);
  }
  return historyRowsPromise;
}

async function loadTablePredictionRows() {
  if (!tablePredictionsRowsPromise) {
    tablePredictionsRowsPromise = queryRows(
      "SELECT run_id, run_name, wandb_run_url, meta_json FROM table_predictions",
      ["table_predictions.parquet"]
    ).then((rows) => rows.map((row) => {
      try {
        const payload = JSON.parse(String(row.meta_json || "{}"));
        return {
          __run_id: row.run_id,
          __run_name: row.run_name,
          __wandb_url: row.wandb_run_url,
          ...payload
        };
      } catch {
        return null;
      }
    }).filter(Boolean));
  }
  return tablePredictionsRowsPromise;
}

function isImageCell(value) {
  return value && typeof value === "object" && value._kind === "image" && value.path;
}

function rowHasImage(row) {
  return Object.values(row || {}).some((value) => isImageCell(value));
}

function rowImageCell(row) {
  return Object.values(row || {}).find((value) => isImageCell(value)) || null;
}

function panelRowsNeedHydration(rows) {
  if (!rows.length) {
    return false;
  }
  if (rows.some((row) => rowHasImage(row))) {
    return false;
  }
  const first = rows[0];
  if ("Image" in first && first.Image === "Image") {
    return true;
  }
  return Object.entries(first).some(([key, value]) => typeof value === "string" && value === key);
}

async function hydratePanelRowsIfNeeded(rows) {
  if (!panelRowsNeedHydration(rows)) {
    return rows;
  }
  const hydrated = await loadTablePredictionRows();
  return hydrated.length ? hydrated : rows;
}

function ensureImageDialog() {
  if (imageDialog) {
    return imageDialog;
  }
  const dialog = document.createElement("dialog");
  dialog.className = "image-lightbox";
  dialog.innerHTML = `
    <form method="dialog" class="image-lightbox__header">
      <div class="image-lightbox__title">Image Preview</div>
      <button type="submit" class="image-lightbox__close">Close</button>
    </form>
    <div class="image-lightbox__controls"></div>
    <div class="image-lightbox__stage"></div>
  `;
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) {
      dialog.close();
    }
  });
  document.body.append(dialog);
  imageDialog = dialog;
  return dialog;
}

function showImageDialog(value, label = "Image") {
  if (!isImageCell(value)) {
    return;
  }
  const dialog = ensureImageDialog();
  const title = dialog.querySelector(".image-lightbox__title");
  const controls = dialog.querySelector(".image-lightbox__controls");
  const stage = dialog.querySelector(".image-lightbox__stage");
  title.textContent = label;
  controls.replaceChildren();
  stage.replaceChildren();

  const figure = document.createElement("figure");
  figure.className = "image-lightbox__figure";
  const base = document.createElement("img");
  base.src = mediaUrl(value.path);
  base.alt = label;
  base.className = "image-lightbox__base";
  figure.append(base);

  const masks = Object.entries(value.masks || {});
  let leftMask = masks[0]?.[0] || null;
  let rightMask = masks[1]?.[0] || null;
  let mode = "overlay";
  const maskLayer = document.createElement("div");
  maskLayer.className = "image-lightbox__masks";
  figure.append(maskLayer);
  const compareShell = document.createElement("div");
  compareShell.className = "image-lightbox__compare";

  const redrawMasks = () => {
    maskLayer.replaceChildren();
    compareShell.replaceChildren();
    if (mode === "side-by-side") {
      for (const selectedMask of [leftMask, rightMask]) {
        const cell = document.createElement("div");
        cell.className = "image-lightbox__compare-cell";
        const img = document.createElement("img");
        img.src = mediaUrl(value.path);
        img.alt = label;
        img.className = "image-lightbox__base";
        cell.append(img);
        const maskName = selectedMask;
        const maskValue = Object.fromEntries(masks)[maskName];
        if (maskValue?.overlay_path) {
          const overlay = document.createElement("img");
          overlay.src = mediaUrl(maskValue.overlay_path);
          overlay.alt = maskName;
          overlay.className = "image-lightbox__mask";
          cell.append(overlay);
        }
        const caption = document.createElement("div");
        caption.className = "image-lightbox__compare-label";
        caption.textContent = maskName ? maskName.replaceAll("_", " ") : "Image";
        cell.append(caption);
        compareShell.append(cell);
      }
      if (!compareShell.parentElement) {
        stage.append(compareShell);
      }
      return;
    }
    compareShell.remove();
    const selectedMasks = mode === "overlay-both" ? [leftMask, rightMask] : [leftMask];
    for (const maskName of selectedMasks) {
      const maskValue = Object.fromEntries(masks)[maskName];
      if (!maskValue?.overlay_path) continue;
      const img = document.createElement("img");
      img.src = mediaUrl(maskValue.overlay_path);
      img.alt = maskName;
      img.className = "image-lightbox__mask";
      maskLayer.append(img);
    }
  };

  for (const option of [
    ["overlay", "Overlay"],
    ["overlay-both", "Both"],
    ["side-by-side", "Compare"]
  ]) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `image-mask-chip${mode === option[0] ? " image-mask-chip--active" : ""}`;
    button.textContent = option[1];
    button.dataset.mode = option[0];
    button.addEventListener("click", () => {
      mode = option[0];
      for (const chip of controls.querySelectorAll("[data-mode]")) {
        chip.classList.toggle("image-mask-chip--active", chip.dataset.mode === mode);
      }
      redrawMasks();
    });
    controls.append(button);
  }

  for (const [maskName] of masks) {
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.mask = maskName;
    button.className = `image-mask-chip${leftMask === maskName ? " image-mask-chip--active" : ""}`;
    button.textContent = maskName.replaceAll("_", " ");
    button.addEventListener("click", () => {
      if (mode === "side-by-side") {
        rightMask = maskName;
      } else {
        leftMask = maskName;
      }
      redrawMasks();
      for (const chip of controls.querySelectorAll("button")) {
        if (!chip.dataset.mask) continue;
        chip.classList.toggle("image-mask-chip--active", chip.dataset.mask === (mode === "side-by-side" ? rightMask : leftMask));
      }
    });
    controls.append(button);
  }

  redrawMasks();
  stage.append(figure);
  if (!dialog.open) {
    dialog.showModal();
  }
}

function normalizeTableRows(rows, panel) {
  if (!Array.isArray(rows)) {
    return [];
  }
  let nextRows = [...rows];
  const filter = panel.simple_filter;
  if (filter && filter.column) {
    nextRows = nextRows.filter((row) => {
      const rawValue = row[filter.column];
      if (rawValue == null || rawValue === "") {
        return false;
      }
      const value = Number(rawValue);
      if (Number.isNaN(value)) {
        return false;
      }
      if (filter.op === "number-lessEqual") return value <= filter.value;
      if (filter.op === "number-greaterEqual") return value >= filter.value;
      if (filter.op === "number-lessThan") return value < filter.value;
      if (filter.op === "number-greaterThan") return value > filter.value;
      if (filter.op === "number-equal") return value === filter.value;
      return true;
    });
  }
  if (nextRows.length && "TOTAL_SCORE" in nextRows[0]) {
    nextRows.sort((a, b) => Number(b.TOTAL_SCORE || 0) - Number(a.TOTAL_SCORE || 0));
  }
  return nextRows;
}

function materializeTableRows(rows, panel) {
  if (!rows.length) {
    return [];
  }
  const columns = Array.isArray(panel.table_columns) ? panel.table_columns : [];
  if (!columns.length) {
    return rows.map((row) => Object.fromEntries(Object.entries(row).filter(([key]) => !key.startsWith("__"))));
  }
  const projectedRows = rows.map((row) => {
    const projected = {};
    for (const column of columns) {
      const label = String(column.label || expressionLabel(column.expression) || column.id || "");
      projected[label] = evaluateExpression(column.expression, row);
    }
    return projected;
  });
  const labelByColumnId = Object.fromEntries(
    columns.map((column) => [
      String(column.id),
      String(column.label || expressionLabel(column.expression) || column.id || "")
    ])
  );
  for (const sortSpec of [...(panel.table_sort || [])].reverse()) {
    const label = labelByColumnId[String(sortSpec.column_id)];
    if (!label) continue;
    projectedRows.sort((left, right) => {
      const a = sortValue(left[label]);
      const b = sortValue(right[label]);
      if (a[0] !== b[0]) return a[0] - b[0];
      if (a[1] < b[1]) return sortSpec.direction === "desc" ? 1 : -1;
      if (a[1] > b[1]) return sortSpec.direction === "desc" ? -1 : 1;
      return 0;
    });
  }
  return projectedRows;
}

function columnDefsForRows(rows) {
  const first = rows[0] || {};
  return Object.keys(first)
    .filter((key) => !key.startsWith("__"))
    .map((key, index) => {
      const sample = rows.find((row) => row[key] != null)?.[key];
      if (isImageCell(sample)) {
        return {
          field: key,
          headerName: key,
          pinned: index === 0 ? "left" : undefined,
          minWidth: 320,
          sortable: false,
          filter: false,
          cellRenderer: ({value}) => {
            if (!isImageCell(value)) return "";
            const shell = document.createElement("button");
            shell.type = "button";
            shell.className = "image-table-cell";
            const thumb = document.createElement("img");
            thumb.src = mediaUrl(value.path);
            thumb.alt = key;
            shell.append(thumb);
            const meta = document.createElement("div");
            meta.className = "image-table-cell__meta";
            meta.textContent = Object.keys(value.masks || {}).length ? Object.keys(value.masks).join(" · ") : "Image";
            shell.append(meta);
            shell.addEventListener("click", () => showImageDialog(value, key));
            return shell;
          }
        };
      }
      return {
        field: key,
        headerName: key,
        pinned: index === 0 ? "left" : undefined,
        minWidth: typeof sample === "number" ? 120 : 180,
        valueFormatter: typeof sample === "number"
          ? ({value}) => (value == null ? "" : Number(value).toFixed(4).replace(/\.?0+$/u, ""))
          : typeof sample === "object"
            ? ({value}) => (value == null ? "" : JSON.stringify(value))
            : undefined
      };
    });
}

function formatDateValue(value) {
  if (value == null || value === "") {
    return "";
  }
  const number = Number(value);
  if (Number.isFinite(number) && number > 10_000_000_000) {
    return new Date(number / 1_000_000).toISOString().slice(0, 10);
  }
  const date = new Date(String(value));
  if (!Number.isNaN(date.getTime())) {
    return date.toISOString().slice(0, 10);
  }
  return String(value);
}

function isTimestampLike(value, key) {
  return String(key).toLowerCase().includes("date") || (Number.isFinite(Number(value)) && Number(value) > 10_000_000_000);
}

function toPlotNumber(value, treatAsTimestamp = false) {
  if (value == null || value === "") {
    return null;
  }
  if (!treatAsTimestamp) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    if (Math.abs(numeric) > 1e17) return numeric / 1e9;
    if (Math.abs(numeric) > 1e14) return numeric / 1e6;
    if (Math.abs(numeric) > 1e11) return numeric / 1e3;
    return numeric;
  }
  const parsed = new Date(String(value)).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function pickFirstMatchingKey(rows, preferredKeys, predicate = () => true) {
  const first = rows[0] || {};
  const rowKeys = Object.keys(first);
  for (const key of preferredKeys) {
    if (!rowKeys.includes(key)) {
      continue;
    }
    if (rows.some((row) => predicate(row[key], key))) {
      return key;
    }
  }
  for (const key of rowKeys) {
    if (rows.some((row) => predicate(row[key], key))) {
      return key;
    }
  }
  return null;
}

function inferVegaFields(rows) {
  return {
    x: pickFirstMatchingKey(rows, ["category", "label", "x", "__step"], (value) => typeof value === "string" && value.length > 0),
    y: pickFirstMatchingKey(rows, ["score", "TOTAL_SCORE", "y", "value"], (value) => Number.isFinite(Number(value))),
    name: pickFirstMatchingKey(rows, ["model_name", "__run_name", "name", "series"], (value) => typeof value === "string" && value.length > 0),
    id: pickFirstMatchingKey(rows, ["__run_id", "id", "__run_name", "model_name"], (value) => value != null && value !== "")
  };
}

function materializeVegaRows(rows, fields) {
  return rows.map((row, index) => ({
    ...row,
    name: row[fields.name] ?? row.__run_name ?? row.model_name ?? `Series ${index + 1}`,
    id: row[fields.id] ?? row.__run_id ?? row.__run_name ?? row.model_name ?? `series-${index + 1}`
  }));
}

function interpolateVegaSpec(specText, fields) {
  if (!specText) {
    return null;
  }
  const mapping = {
    x: fields.x || "category",
    y: fields.y || "score",
    name: fields.name || "name",
    id: fields.id || "id"
  };
  const replaced = String(specText).replace(/\$\{field:([a-zA-Z0-9_]+)\}/gu, (_, key) => mapping[key] || key);
  return normalizeVegaSeriesFields(JSON.parse(replaced), mapping);
}

function normalizeVegaSeriesFields(node, fields) {
  const seriesField = fields.id || "id";
  if (Array.isArray(node)) {
    return node.map((item) => normalizeVegaSeriesFields(item, fields));
  }
  if (!node || typeof node !== "object") {
    return node;
  }
  const normalized = {};
  for (const [key, value] of Object.entries(node)) {
    if (key === "field" && value === "id" && seriesField !== "id") {
      normalized[key] = seriesField;
      continue;
    }
    if (key === "groupby" && Array.isArray(value) && seriesField !== "id") {
      normalized[key] = value.map((item) => (item === "id" ? seriesField : item));
      continue;
    }
    normalized[key] = normalizeVegaSeriesFields(value, fields);
  }
  return normalized;
}

function isRadialVegaSpec(spec) {
  if (!spec || typeof spec !== "object") {
    return false;
  }
  const serialized = JSON.stringify(spec);
  return serialized.includes("cos(scale(")
    && serialized.includes("sin(scale(")
    && serialized.includes("\"radius\"")
    && (serialized.includes("\"angular\"") || serialized.includes("\"radial\""));
}

function isCategoryScoreMatrix(rows) {
  return Boolean(rows.length > 0 && "category" in rows[0] && "score" in rows[0]);
}

function averageScore(rows) {
  const values = rows.map((row) => Number(row.score)).filter((value) => Number.isFinite(value));
  if (!values.length) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function buildRadarModelGroups(rows) {
  const groups = new Map();
  const categories = [];
  const seenCategories = new Set();
  for (const row of rows) {
    const category = String(row.category || "");
    if (category && !seenCategories.has(category)) {
      seenCategories.add(category);
      categories.push(category);
    }
    const runName = String(row.__run_name || row.model_name || "Unknown");
    if (!groups.has(runName)) {
      groups.set(runName, []);
    }
    groups.get(runName).push(row);
  }
  return {
    categories,
    groups: [...groups.entries()]
      .map(([name, groupRows]) => ({name, rows: groupRows, average: averageScore(groupRows)}))
      .sort((a, b) => b.average - a.average)
  };
}

function buildScatterGroups(rows, plotConfig) {
  const xKey = plotConfig?.x;
  const yKey = plotConfig?.y;
  const labelKey = plotConfig?.label || "__run_name";
  const colorKey = plotConfig?.color || "__run_name";
  const xIsDate = rows.some((row) => isTimestampLike(row[xKey], xKey));
  const points = rows.map((row) => ({
    row,
    x: toPlotNumber(row[xKey], xIsDate),
    y: toPlotNumber(row[yKey], false),
    label: isImageCell(row[labelKey]) ? String(row.__run_name || row.__run_id || "") : String(row[labelKey] || row.__run_name || ""),
    color: colorKey ? String(row[colorKey] ?? row.__run_name ?? row.__run_id ?? "Other") : "All"
  })).filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
  const colorDomain = [...new Set(points.map((point) => point.color))];
  return {xKey, yKey, labelKey, points, colorDomain, xIsDate};
}

function createSvgNode(name, attributes = {}) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attributes)) {
    node.setAttribute(key, String(value));
  }
  return node;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function tooltipBoxGeometry(x, y, lines, bounds) {
  const safeLines = lines.map((line) => String(line ?? ""));
  const charWidth = 7.2;
  const lineHeight = 16;
  const paddingX = 10;
  const paddingY = 9;
  const width = Math.max(92, Math.max(...safeLines.map((line) => line.length), 0) * charWidth + paddingX * 2);
  const height = Math.max(36, safeLines.length * lineHeight + paddingY * 2 - 2);
  const preferLeft = x > bounds.width * 0.6;
  const baseLeft = preferLeft ? x - width - 16 : x + 16;
  const baseTop = y - height - 14 < 8 ? y + 16 : y - height - 14;
  return {
    left: clamp(baseLeft, 8, Math.max(8, bounds.width - width - 8)),
    top: clamp(baseTop, 8, Math.max(8, bounds.height - height - 8)),
    width,
    height
  };
}

function createTooltipGroup(x, y, lines, bounds) {
  const metrics = tooltipBoxGeometry(x, y, lines, bounds);
  const group = createSvgNode("g", {class: "chart-tooltip"});
  const bubble = createSvgNode("rect", {
    x: metrics.left,
    y: metrics.top,
    width: metrics.width,
    height: metrics.height,
    rx: 12,
    ry: 12,
    class: "chart-tooltip__bubble"
  });
  group.append(bubble);

  const text = createSvgNode("text", {
    x: metrics.left + 10,
    y: metrics.top + 20,
    class: "chart-tooltip__text"
  });
  lines.forEach((line, index) => {
    const tspan = createSvgNode("tspan", {
      x: metrics.left + 10,
      dy: index === 0 ? 0 : 16
    });
    tspan.textContent = String(line ?? "");
    text.append(tspan);
  });
  group.append(text);
  return group;
}

function createInteractivePoint({x, y, radius, color, lines, bounds, stroke = "#fff", strokeWidth = 1.5, opacity = 0.9}) {
  const group = createSvgNode("g", {class: "chart-point", tabindex: 0});
  const tooltip = createTooltipGroup(x, y, lines, bounds);
  tooltip.style.visibility = "hidden";
  tooltip.style.opacity = "0";
  const target = createSvgNode("circle", {
    cx: x,
    cy: y,
    r: Math.max(radius + 7, 11),
    class: "chart-point__target",
    fill: "rgba(255,255,255,0.001)",
    "pointer-events": "all"
  });
  const dot = createSvgNode("circle", {
    cx: x,
    cy: y,
    r: radius,
    fill: color,
    "fill-opacity": opacity,
    stroke,
    "stroke-width": strokeWidth,
    class: "chart-point__dot"
  });
  group.append(target, dot, tooltip);
  const bringToFront = () => {
    const parent = group.parentNode;
    if (parent) {
      parent.appendChild(group);
    }
    tooltip.style.visibility = "visible";
    tooltip.style.opacity = "1";
  };
  const hideTooltip = () => {
    tooltip.style.opacity = "0";
    tooltip.style.visibility = "hidden";
  };
  target.addEventListener("mouseenter", bringToFront);
  target.addEventListener("pointerenter", bringToFront);
  target.addEventListener("mousemove", bringToFront);
  target.addEventListener("pointermove", bringToFront);
  target.addEventListener("mouseleave", hideTooltip);
  target.addEventListener("pointerleave", hideTooltip);
  group.addEventListener("focusin", bringToFront);
  group.addEventListener("focusout", hideTooltip);
  return group;
}

function renderCategoryScoreChart(rows) {
  const shell = document.createElement("div");
  shell.className = "radar-shell";

  const {categories, groups} = buildRadarModelGroups(rows);
  if (!categories.length || !groups.length) {
    shell.append(emptyNote("Radar chart data was empty."));
    return shell;
  }

  const selected = new Set(groups.slice(0, Math.min(groups.length, 5)).map((group) => group.name));

  const controls = document.createElement("div");
  controls.className = "radar-controls";
  shell.append(controls);

  const chartHost = document.createElement("div");
  chartHost.className = "radar-chart";
  shell.append(chartHost);

  const redraw = () => {
    controls.replaceChildren();
    chartHost.replaceChildren();

    for (const group of groups) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `radar-chip${selected.has(group.name) ? " radar-chip--active" : ""}`;
      button.textContent = group.name;
      button.addEventListener("click", () => {
        if (selected.has(group.name)) {
          selected.delete(group.name);
        } else {
          selected.add(group.name);
        }
        redraw();
      });
      controls.append(button);
    }

    const activeGroups = groups.filter((group) => selected.has(group.name));
    if (!activeGroups.length) {
      chartHost.append(emptyNote("Select at least one model."));
      return;
    }

    const size = 760;
    const center = size / 2;
    const radius = 250;
    const svg = createSvgNode("svg", {viewBox: `0 0 ${size} ${size}`, class: "radar-svg"});

    for (let ring = 1; ring <= 4; ring += 1) {
      const ringRadius = (radius * ring) / 4;
      const points = categories.map((_, index) => {
        const angle = (-Math.PI / 2) + (index * Math.PI * 2) / categories.length;
        return `${center + Math.cos(angle) * ringRadius},${center + Math.sin(angle) * ringRadius}`;
      }).join(" ");
      svg.append(createSvgNode("polygon", {
        points,
        fill: "none",
        stroke: "rgba(0,0,0,0.12)",
        "stroke-width": 1
      }));
    }

    categories.forEach((category, index) => {
      const angle = (-Math.PI / 2) + (index * Math.PI * 2) / categories.length;
      const x = center + Math.cos(angle) * radius;
      const y = center + Math.sin(angle) * radius;
      svg.append(createSvgNode("line", {
        x1: center,
        y1: center,
        x2: x,
        y2: y,
        stroke: "rgba(0,0,0,0.16)",
        "stroke-width": 1
      }));
      const label = createSvgNode("text", {
        x: center + Math.cos(angle) * (radius + 22),
        y: center + Math.sin(angle) * (radius + 22),
        "text-anchor": x >= center ? "start" : "end",
        "dominant-baseline": Math.abs(y - center) < 12 ? "middle" : y >= center ? "hanging" : "auto",
        class: "radar-label"
      });
      label.textContent = category;
      svg.append(label);
    });

    activeGroups.forEach((group, index) => {
      const color = RADAR_COLORS[index % RADAR_COLORS.length];
      const scoreByCategory = Object.fromEntries(group.rows.map((row) => [String(row.category), Number(row.score || 0)]));
      const points = categories.map((category, categoryIndex) => {
        const angle = (-Math.PI / 2) + (categoryIndex * Math.PI * 2) / categories.length;
        const score = Math.max(0, Math.min(1, Number(scoreByCategory[category] || 0)));
        return {
          category,
          score,
          x: center + Math.cos(angle) * radius * score,
          y: center + Math.sin(angle) * radius * score
        };
      });

      svg.append(createSvgNode("polygon", {
        points: points.map((point) => `${point.x},${point.y}`).join(" "),
        fill: color,
        "fill-opacity": 0.12,
        stroke: color,
        "stroke-width": 2
      }));

      for (const point of points) {
        svg.append(createInteractivePoint({
          x: point.x,
          y: point.y,
          radius: 3.5,
          color,
          stroke: "#fff",
          strokeWidth: 1,
          opacity: 1,
          bounds: {width: size, height: size},
          lines: [
            group.name,
            point.category,
            `score: ${point.score.toFixed(3).replace(/\.?0+$/u, "")}`
          ]
        }));
      }
    });

    chartHost.append(svg);
  };

  redraw();
  return shell;
}

async function renderVegaPanel(rows, panel) {
  const shell = document.createElement("div");
  shell.className = "vega-shell";
  const host = document.createElement("div");
  host.className = "vega-host";
  shell.append(host);

  const fields = inferVegaFields(rows);
  const spec = interpolateVegaSpec(panel.vega_spec, fields);
  if (!spec) {
    shell.append(emptyNote("Custom chart spec was empty."));
    return shell;
  }
  if (isRadialVegaSpec(spec)) {
    host.classList.add("vega-host--square");
  }

  spec.data = (spec.data || []).map((item) => (item.name === "wandb" ? {...item, values: materializeVegaRows(rows, fields)} : item));
  spec.autosize = {type: "fit", contains: "padding"};
  if (!spec.padding) {
    spec.padding = {top: 24, right: 36, bottom: 42, left: 48};
  }

  const embed = await loadVegaEmbed();
  await embed(host, spec, {
    actions: false,
    renderer: "svg"
  });
  return shell;
}

function renderScatterPlot(rows, panel) {
  const plot = panel.plot || {};
  const {xKey, yKey, points, colorDomain, xIsDate} = buildScatterGroups(rows, plot);
  const shell = document.createElement("div");
  shell.className = "scatter-shell";
  if (!xKey || !yKey || !points.length) {
    shell.append(emptyNote("Combined Plot data was empty."));
    return shell;
  }

  const frame = document.createElement("div");
  frame.className = "scatter-frame";
  shell.append(frame);

  const size = {width: 980, height: 560, left: 92, right: 38, top: 28, bottom: 78};
  const minX = Math.min(...points.map((point) => point.x));
  const maxX = Math.max(...points.map((point) => point.x));
  const minY = Math.min(...points.map((point) => point.y));
  const maxY = Math.max(...points.map((point) => point.y));
  const scaleX = (value) => {
    if (maxX === minX) return size.left;
    return size.left + ((value - minX) / (maxX - minX)) * (size.width - size.left - size.right);
  };
  const scaleY = (value) => {
    if (maxY === minY) return size.height - size.bottom;
    return size.height - size.bottom - ((value - minY) / (maxY - minY)) * (size.height - size.top - size.bottom);
  };
  const colorFor = (name) => RADAR_COLORS[colorDomain.indexOf(name) % RADAR_COLORS.length];

  const header = document.createElement("div");
  header.className = "scatter-meta";
  header.textContent = `${xKey} vs ${yKey}`;
  shell.append(header);

  const svg = createSvgNode("svg", {viewBox: `0 0 ${size.width} ${size.height}`, class: "scatter-svg"});
  svg.append(createSvgNode("line", {
    x1: size.left,
    y1: size.height - size.bottom,
    x2: size.width - size.right,
    y2: size.height - size.bottom,
    stroke: "rgba(0,0,0,0.22)",
    "stroke-width": 1.5
  }));
  svg.append(createSvgNode("line", {
    x1: size.left,
    y1: size.top,
    x2: size.left,
    y2: size.height - size.bottom,
    stroke: "rgba(0,0,0,0.22)",
    "stroke-width": 1.5
  }));

  for (let i = 0; i <= 4; i += 1) {
    const t = i / 4;
    const x = size.left + t * (size.width - size.left - size.right);
    const y = size.height - size.bottom - t * (size.height - size.top - size.bottom);
    svg.append(createSvgNode("line", {
      x1: x,
      y1: size.top,
      x2: x,
      y2: size.height - size.bottom,
      stroke: "rgba(0,0,0,0.05)",
      "stroke-width": 1
    }));
    svg.append(createSvgNode("line", {
      x1: size.left,
      y1: y,
      x2: size.width - size.right,
      y2: y,
      stroke: "rgba(0,0,0,0.05)",
      "stroke-width": 1
    }));
  }

  const xLabel = createSvgNode("text", {
    x: size.width / 2,
    y: size.height - 16,
    "text-anchor": "middle",
    class: "scatter-axis-label"
  });
  xLabel.textContent = xKey;
  svg.append(xLabel);

  const yLabel = createSvgNode("text", {
    x: 22,
    y: size.height / 2,
    transform: `rotate(-90 22 ${size.height / 2})`,
    "text-anchor": "middle",
    class: "scatter-axis-label"
  });
  yLabel.textContent = yKey;
  svg.append(yLabel);

  for (let i = 0; i <= 4; i += 1) {
    const t = i / 4;
    const xValue = minX + (maxX - minX) * t;
    const yValue = minY + (maxY - minY) * t;
    const xTick = createSvgNode("text", {
      x: size.left + t * (size.width - size.left - size.right),
      y: size.height - size.bottom + 22,
      "text-anchor": "middle",
      class: "scatter-tick-label"
    });
    xTick.textContent = xIsDate ? formatDateValue(xValue) : Number(xValue).toFixed(2).replace(/\.?0+$/u, "");
    svg.append(xTick);
    const yTick = createSvgNode("text", {
      x: size.left - 12,
      y: size.height - size.bottom - t * (size.height - size.top - size.bottom) + 4,
      "text-anchor": "end",
      class: "scatter-tick-label"
    });
    yTick.textContent = Number(yValue).toFixed(2).replace(/\.?0+$/u, "");
    svg.append(yTick);
  }

  for (const point of points) {
    svg.append(createInteractivePoint({
      x: scaleX(point.x),
      y: scaleY(point.y),
      radius: 5.2,
      color: colorFor(point.color),
      bounds: {width: size.width, height: size.height},
      lines: [
        point.label,
        `${xKey}: ${xIsDate ? formatDateValue(point.x) : Number(point.x).toFixed(3).replace(/\.?0+$/u, "")}`,
        `${yKey}: ${Number(point.y).toFixed(3).replace(/\.?0+$/u, "")}`
      ]
    }));
  }

  frame.append(svg);

  const legend = document.createElement("div");
  legend.className = "scatter-legend";
  for (const item of colorDomain) {
    const entry = document.createElement("div");
    entry.className = "scatter-legend__item";
    entry.innerHTML = `<span class="scatter-legend__swatch" style="background:${colorFor(item)}"></span><span>${item}</span>`;
    legend.append(entry);
  }
  shell.append(legend);

  return shell;
}

function panelTitle(panel) {
  if (panel.view_type === "Run History Line Plot") {
    return summarizeMetricTitle(panel.metrics) || panel.view_type;
  }
  if (panel.chart_title) {
    return panel.chart_title;
  }
  if (panel.table_key && panel.mode === "plot") {
    return `${humanizeLabel(panel.table_key) || "Panel"} Plot`;
  }
  if (panel.table_key && panel.mode === "table") {
    return `${humanizeLabel(panel.table_key) || "Panel"} Table`;
  }
  return panel.table_key || panel.view_type || "Panel";
}

function historyAxisField(panel) {
  if (panel.x_axis === "_step") return "step";
  if (panel.x_axis === "_runtime") return "runtime";
  if (panel.x_axis === "_timestamp") return "timestamp_value";
  if (panel.x_axis === "epoch") return "epoch";
  return panel.x_axis || "step";
}

function formatRuntimeValue(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) return String(value ?? "");
  if (seconds >= 3600) return `${(seconds / 3600).toFixed(1)}h`;
  if (seconds >= 60) return `${(seconds / 60).toFixed(1)}m`;
  return `${seconds.toFixed(0)}s`;
}

async function renderHistoryLinePlot(panel, report, block) {
  const runsetNames = block?.runsets || [];
  const rows = filterRowsByRunsetSelection(await loadHistoryRows(), report, runsetNames, {
    visibleValues: inferBlockVisibleValues(report, block),
    useSelectionFallback: shouldUseSelectionFallback(report, runsetNames)
  });
  const metricNames = new Set((panel.metrics || []).map((value) => String(value)));
  const axisField = historyAxisField(panel);
  const filtered = rows.filter((row) => metricNames.has(String(row.metric_name)));
  const shell = document.createElement("div");
  shell.className = "history-line-shell";
  if (!filtered.length) {
    shell.append(emptyNote("No offline history rows were exported for this plot."));
    return shell;
  }

  const seriesMap = new Map();
  const axisIsTimestamp = axisField === "timestamp_value";
  for (const row of filtered) {
    const x = toPlotNumber(row[axisField], axisIsTimestamp);
    const y = toPlotNumber(row.metric_value, false);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    const label = metricNames.size > 1
      ? `${row.run_name || row.run_id} · ${row.metric_name}`
      : (row.run_name || row.run_id || row.metric_name);
    if (!seriesMap.has(label)) {
      seriesMap.set(label, []);
    }
    seriesMap.get(label).push({x, y, row});
  }
  const series = [...seriesMap.entries()]
    .map(([label, points]) => ({label, points: points.sort((a, b) => a.x - b.x)}))
    .filter((entry) => entry.points.length > 1);
  if (!series.length) {
    shell.append(emptyNote("This plot did not have enough exported history points."));
    return shell;
  }

  const width = 980;
  const height = 420;
  const left = 72;
  const right = 24;
  const top = 20;
  const bottom = 58;
  const allPoints = series.flatMap((entry) => entry.points);
  const minX = Math.min(...allPoints.map((point) => point.x));
  const maxX = Math.max(...allPoints.map((point) => point.x));
  const minY = Math.min(...allPoints.map((point) => point.y));
  const maxY = Math.max(...allPoints.map((point) => point.y));
  const scaleX = (value) => maxX === minX ? left : left + ((value - minX) / (maxX - minX)) * (width - left - right);
  const scaleY = (value) => maxY === minY ? height - bottom : height - bottom - ((value - minY) / (maxY - minY)) * (height - top - bottom);
  const xFormatter = panel.x_axis === "_timestamp"
    ? formatDateValue
    : panel.x_axis === "_runtime"
      ? formatRuntimeValue
      : (value) => Number(value).toFixed(2).replace(/\.?0+$/u, "");

  const svg = createSvgNode("svg", {viewBox: `0 0 ${width} ${height}`, class: "history-line-svg"});
  svg.append(createSvgNode("line", {x1: left, y1: height - bottom, x2: width - right, y2: height - bottom, stroke: "rgba(0,0,0,0.22)", "stroke-width": 1.5}));
  svg.append(createSvgNode("line", {x1: left, y1: top, x2: left, y2: height - bottom, stroke: "rgba(0,0,0,0.22)", "stroke-width": 1.5}));
  for (let index = 0; index <= 4; index += 1) {
    const t = index / 4;
    const x = left + t * (width - left - right);
    const y = height - bottom - t * (height - top - bottom);
    svg.append(createSvgNode("line", {x1: x, y1: top, x2: x, y2: height - bottom, stroke: "rgba(0,0,0,0.05)", "stroke-width": 1}));
    svg.append(createSvgNode("line", {x1: left, y1: y, x2: width - right, y2: y, stroke: "rgba(0,0,0,0.05)", "stroke-width": 1}));
    const xTick = createSvgNode("text", {x, y: height - bottom + 22, "text-anchor": "middle", class: "scatter-tick-label"});
    xTick.textContent = xFormatter(minX + (maxX - minX) * t);
    svg.append(xTick);
    const yTick = createSvgNode("text", {x: left - 10, y: y + 4, "text-anchor": "end", class: "scatter-tick-label"});
    yTick.textContent = (minY + (maxY - minY) * t).toFixed(2).replace(/\.?0+$/u, "");
    svg.append(yTick);
  }

  series.forEach((entry, index) => {
    const color = RADAR_COLORS[index % RADAR_COLORS.length];
    const path = createSvgNode("path", {
      d: entry.points.map((point, pointIndex) => `${pointIndex === 0 ? "M" : "L"} ${scaleX(point.x)} ${scaleY(point.y)}`).join(" "),
      fill: "none",
      stroke: color,
      "stroke-width": 2.2,
      "stroke-linejoin": "round",
      "stroke-linecap": "round"
    });
    svg.append(path);

    const step = Math.max(1, Math.ceil(entry.points.length / 160));
    entry.points.forEach((point, pointIndex) => {
      if (pointIndex % step !== 0 && pointIndex !== entry.points.length - 1) return;
      svg.append(createInteractivePoint({
        x: scaleX(point.x),
        y: scaleY(point.y),
        radius: 3.2,
        color,
        strokeWidth: 1,
        bounds: {width, height},
        lines: [
          entry.label,
          `${axisField}: ${xFormatter(point.x)}`,
          `value: ${Number(point.y).toFixed(4).replace(/\.?0+$/u, "")}`
        ]
      }));
    });
  });
  shell.append(svg);

  if (series.length <= 10) {
    const legend = document.createElement("div");
    legend.className = "scatter-legend";
    for (const [index, entry] of series.entries()) {
      const item = document.createElement("div");
      item.className = "scatter-legend__item";
      item.innerHTML = `<span class="scatter-legend__swatch" style="background:${RADAR_COLORS[index % RADAR_COLORS.length]}"></span><span>${entry.label}</span>`;
      legend.append(item);
    }
    shell.append(legend);
  } else {
    const summary = document.createElement("div");
    summary.className = "scatter-meta";
    summary.textContent = `${series.length} series`;
    shell.append(summary);
  }
  return shell;
}

function panelMinHeight(panel) {
  const layoutHeight = Math.max(Number(panel.layout?.h || 0) * 24, 0);
  if (panel.view_type === "Markdown Panel") {
    return Math.max(layoutHeight, 52);
  }
  if (panel.view_type === "Run History Line Plot") {
    return Math.max(layoutHeight, 320);
  }
  if (panel.view_type === "Media Browser") {
    return Math.max(layoutHeight, 420);
  }
  if (panel.view_type === "Vega2") {
    return Math.max(layoutHeight, 420);
  }
  if (panel.mode === "plot") {
    return Math.max(layoutHeight, 420);
  }
  if (panel.mode === "table") {
    return Math.max(layoutHeight, 460);
  }
  return Math.max(layoutHeight, 380);
}

function panelGroupWidth(group) {
  return Math.max(Number(group.layout?.w || 24), 1);
}

function panelGroupNeedsOwnRow(group) {
  return group.panels.some((panel) => panelNeedsOwnRow(panel));
}

function buildPanelRows(panels) {
  const rows = [];
  let currentY = null;
  let currentGroups = [];
  const flushCurrentGroups = () => {
    if (!currentGroups.length) {
      return;
    }
    let carry = [];
    for (const group of currentGroups) {
      if (panelGroupNeedsOwnRow(group)) {
        if (carry.length) {
          rows.push({groups: carry});
          carry = [];
        }
        rows.push({groups: [group], wide: true});
      } else {
        carry.push(group);
      }
    }
    if (carry.length) {
      rows.push({groups: carry});
    }
    currentGroups = [];
  };

  for (const group of groupPanelsByLayout(panels)) {
    const y = Number(group.layout?.y || 0);
    if (currentY == null || y === currentY) {
      currentGroups.push(group);
      currentY = y;
      continue;
    }
    flushCurrentGroups();
    currentGroups = [group];
    currentY = y;
  }
  flushCurrentGroups();
  return rows;
}

async function renderPlotlyMediaItem(item) {
  const shell = document.createElement("div");
  shell.className = "plotly-shell";
  const host = document.createElement("div");
  host.className = "plotly-host";
  shell.append(host);

  const response = await fetch(mediaUrl(item.path));
  if (!response.ok) {
    throw new Error(`Unable to load media asset at ${item.path}.`);
  }
  const figure = await response.json();
  const Plotly = await loadPlotly();
  await Plotly.newPlot(host, figure.data || [], {
    autosize: true,
    ...figure.layout,
    margin: figure.layout?.margin || {l: 24, r: 24, t: 48, b: 24},
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)"
  }, {
    displayModeBar: false,
    responsive: true
  });
  if (typeof ResizeObserver !== "undefined") {
    const observer = new ResizeObserver(() => {
      Plotly.Plots.resize(host);
    });
    observer.observe(host);
  }
  return shell;
}

async function renderMediaBrowserPanel(panel, report, block) {
  const shell = document.createElement("div");
  shell.className = "media-browser-shell";
  const runsetNames = block?.runsets || [];
  const items = filterRowsByRunsetSelection(Array.isArray(panel.media_items) ? panel.media_items : [], report, runsetNames, {
    visibleValues: inferBlockVisibleValues(report, block),
    useSelectionFallback: shouldUseSelectionFallback(report, runsetNames)
  });
  if (!items.length) {
    shell.append(emptyNote("No local media was exported for this media browser panel."));
    return shell;
  }

  for (const item of items) {
    const title = document.createElement("div");
    title.className = "media-browser-title";
    title.textContent = item.title || item.key || item.run_name || "Media";
    shell.append(title);

    if (String(item.kind).includes("plotly")) {
      shell.append(await renderPlotlyMediaItem(item));
      continue;
    }

    const figure = document.createElement("figure");
    figure.className = "media-browser-card";
    const image = document.createElement("img");
    image.src = mediaUrl(item.path);
    image.alt = item.title || item.key || "Media";
    figure.append(image);
    shell.append(figure);
  }
  return shell;
}

async function renderDataPanel(panel, report, options = {}) {
  const {useLayout = true, forceWide = false, block = null} = options;
  const blockRunsets = block?.runsets || [];
  const visibleValues = inferBlockVisibleValues(report, block);
  const useSelectionFallback = shouldUseSelectionFallback(report, blockRunsets);
  const wrapper = document.createElement("section");
  wrapper.className = "report-panel surface surface--padded";
  if (forceWide) {
    wrapper.classList.add("report-panel--wide");
  }
  if (useLayout && panel.layout) {
    const x = Number(panel.layout.x || 0);
    const y = Number(panel.layout.y || 0);
    const w = Number(panel.layout.w || 24);
    const h = Math.max(Math.ceil(panelMinHeight(panel) / GRID_ROW_HEIGHT), Number(panel.layout.h || 1));
    wrapper.style.gridColumn = `${x + 1} / span ${w}`;
    wrapper.style.gridRow = `${y + 1} / span ${h}`;
  }
  wrapper.style.minHeight = `${panelMinHeight(panel)}px`;

  if (panel.view_type === "Markdown Panel") {
    wrapper.classList.add("report-panel--markdown");
    wrapper.innerHTML = `<div class="report-html">${parseMarkdownInline(panel.markdown)}</div>`;
    return wrapper;
  }

  const title = document.createElement("div");
  title.className = "report-panel__meta";
  title.textContent = panelTitle(panel);
  wrapper.append(title);

  if (panel.view_type === "Media Browser") {
    wrapper.classList.add("report-panel--visual");
    wrapper.append(await renderMediaBrowserPanel(panel, report, block));
    return wrapper;
  }

  if (panel.view_type === "Run History Line Plot") {
    wrapper.classList.add("report-panel--visual");
    wrapper.append(await renderHistoryLinePlot(panel, report, block));
    return wrapper;
  }

  if (!panel.table_key) {
    wrapper.append(emptyNote("This embedded panel needs a W&B-specific runtime that is not exported yet."));
    return wrapper;
  }

  try {
    const tableMeta = report.panel_tables?.[panel.table_key];
    if (!tableMeta?.path) {
      throw new Error(`Offline data for ${panel.table_key} was not exported.`);
    }
    const tableRows = await hydratePanelRowsIfNeeded(await loadPanelTable(tableMeta.path));
    const sourceRows = normalizeTableRows(filterRowsByRunsetSelection(tableRows, report, blockRunsets, {
      visibleValues,
      useSelectionFallback
    }), panel);
    if (!sourceRows.length) {
      wrapper.append(emptyNote("No offline rows were exported for this panel."));
      return wrapper;
    }
    if (panel.view_type === "Vega2") {
      wrapper.classList.add("report-panel--visual");
      if (panel.vega_spec) {
        wrapper.append(await renderVegaPanel(sourceRows, panel));
      } else if (isCategoryScoreMatrix(sourceRows)) {
        wrapper.append(renderCategoryScoreChart(sourceRows));
      } else {
        wrapper.append(emptyNote("Custom chart spec was not exported for this panel."));
      }
      return wrapper;
    }
    if (panel.mode === "plot") {
      wrapper.classList.add("report-panel--visual");
      wrapper.append(renderScatterPlot(sourceRows, panel));
      return wrapper;
    }
    const rows = materializeTableRows(sourceRows, panel);
    const grid = document.createElement("div");
    const hasImages = rows.some((row) => rowHasImage(row));
    renderGrid({
      container: grid,
      columnDefs: columnDefsForRows(rows),
      rowData: rows,
      height: Math.max(panelMinHeight(panel) - 44, hasImages ? 680 : 300),
      getRowHeight: ({data}) => rowHasImage(data) ? 156 : undefined
    });
    if (hasImages) {
      wrapper.classList.add("report-panel--image-table");
      wrapper.style.minHeight = `${Math.max(panelMinHeight(panel), 700)}px`;
    }
    wrapper.append(grid);
  } catch (error) {
    wrapper.append(emptyNote(error instanceof Error ? error.message : "Failed to render panel."));
  }
  return wrapper;
}

function emptyNote(message) {
  const note = document.createElement("div");
  note.className = "empty-state";
  note.textContent = message;
  return note;
}

function loadingBlock(message = "Loading report block...") {
  const section = document.createElement("section");
  section.className = "report-block surface surface--padded loading-state";
  section.textContent = message;
  return section;
}

function estimatePanelGroupMinHeight(group) {
  const panels = group?.panels || [];
  if (!panels.length) {
    return 96;
  }
  if (shouldStackPanelGroup(panels)) {
    return panels.reduce((total, panel) => total + panelMinHeight(panel) + 16, 0);
  }
  return Math.max(...panels.map((panel) => panelMinHeight(panel)));
}

function estimatePanelRowMinHeight(row) {
  const heights = (row?.groups || []).map((group) => estimatePanelGroupMinHeight(group));
  return Math.max(...heights, 96);
}

function createRowLoadingPlaceholder(row, message = "Loading panels...") {
  const placeholder = document.createElement("div");
  placeholder.className = "loading-state loading-state--row surface surface--padded";
  placeholder.style.minHeight = `${estimatePanelRowMinHeight(row)}px`;
  placeholder.textContent = message;
  return placeholder;
}

function scheduleLazyRender(host, render, {eager = false, delay = 0} = {}) {
  let started = false;
  const start = () => {
    if (started) return;
    started = true;
    void render();
  };
  if (eager) {
    queueMicrotask(start);
    return;
  }
  setTimeout(start, Math.max(0, delay));
}

function renderArtifactPanel(block) {
  const section = document.createElement("section");
  section.className = "report-block surface surface--padded artifact-lineage";
  const title = document.createElement("h2");
  title.className = "artifact-lineage__title";
  title.textContent = `${block.artifact_name || "Artifact"} ${block.artifact_version || ""}`.trim();
  section.append(title);

  const lineage = block.lineage;
  if (!lineage?.nodes?.length) {
    section.append(emptyNote("Artifact lineage data was not exported for this panel."));
    return section;
  }

  const layers = [...new Set(lineage.nodes.map((node) => Number(node.layer || 0)))].sort((a, b) => a - b);
  const grouped = new Map(layers.map((layer) => [layer, lineage.nodes.filter((node) => Number(node.layer || 0) === layer)]));
  const width = 1080;
  const height = Math.max(360, Math.max(...layers.map((layer) => (grouped.get(layer)?.length || 1) * 110)));
  const xStep = layers.length > 1 ? (width - 220) / (layers.length - 1) : 0;
  const positions = new Map();
  const svg = createSvgNode("svg", {viewBox: `0 0 ${width} ${height}`, class: "artifact-lineage__svg"});

  layers.forEach((layer, layerIndex) => {
    const nodes = grouped.get(layer) || [];
    nodes.forEach((node, nodeIndex) => {
      const x = 110 + layerIndex * xStep;
      const y = ((nodeIndex + 1) / (nodes.length + 1)) * height;
      positions.set(node.id, {x, y});
    });
  });

  for (const edge of lineage.edges || []) {
    const source = positions.get(edge.source);
    const target = positions.get(edge.target);
    if (!source || !target) continue;
    svg.append(createSvgNode("path", {
      d: `M ${source.x} ${source.y} C ${(source.x + target.x) / 2} ${source.y}, ${(source.x + target.x) / 2} ${target.y}, ${target.x} ${target.y}`,
      fill: "none",
      stroke: "rgba(15,23,42,0.18)",
      "stroke-width": 2
    }));
  }

  for (const node of lineage.nodes) {
    const position = positions.get(node.id);
    if (!position) continue;
    const rect = createSvgNode("rect", {
      x: position.x - 74,
      y: position.y - 28,
      width: 148,
      height: 56,
      rx: 16,
      fill: node.kind === "run" ? "#eff6ff" : "#f5f3ff",
      stroke: node.kind === "run" ? "#93c5fd" : "#c4b5fd",
      "stroke-width": 1.5
    });
    svg.append(rect);
    const label = createSvgNode("text", {
      x: position.x,
      y: position.y - 2,
      "text-anchor": "middle",
      class: "artifact-lineage__label"
    });
    label.textContent = String(node.label || node.id);
    svg.append(label);
    const kind = createSvgNode("text", {
      x: position.x,
      y: position.y + 16,
      "text-anchor": "middle",
      class: "artifact-lineage__kind"
    });
    kind.textContent = String(node.kind || "");
    svg.append(kind);
  }
  section.append(svg);

  const centerNode = lineage.nodes.find((node) => node.kind === "artifact" && Number(node.layer || 0) === 2);
  if (centerNode?.meta?.metadata) {
    const meta = document.createElement("div");
    meta.className = "artifact-lineage__meta";
    for (const [key, value] of Object.entries(centerNode.meta.metadata).slice(0, 8)) {
      const item = document.createElement("div");
      item.className = "artifact-lineage__meta-item";
      item.innerHTML = `<span>${key}</span><strong>${value}</strong>`;
      meta.append(item);
    }
    section.append(meta);
  }
  return section;
}

function groupPanelsByLayout(panels) {
  const groups = [];
  const byKey = new Map();
  for (const panel of sortPanels(panels)) {
    const layout = panel?.layout || {};
    const key = [layout.x || 0, layout.y || 0, layout.w || 24, layout.h || 1].join(":");
    if (!byKey.has(key)) {
      const group = {key, layout, panels: []};
      byKey.set(key, group);
      groups.push(group);
    }
    byKey.get(key).panels.push(panel);
  }
  return groups;
}

function shouldStackPanelGroup(panels) {
  if ((panels || []).length !== 2) {
    return false;
  }
  const tableKeys = [...new Set((panels || []).map((panel) => panel?.table_key).filter(Boolean))];
  if (tableKeys.length !== 1) {
    return false;
  }
  const modes = new Set((panels || []).map((panel) => panel?.mode).filter(Boolean));
  return modes.has("table") && modes.has("plot");
}

async function renderPanelGroup(group, report, block) {
  const uniquePanels = [];
  const seen = new Set();
  for (const panel of group.panels) {
    const signature = JSON.stringify({...panel, layout: undefined});
    if (seen.has(signature)) continue;
    seen.add(signature);
    uniquePanels.push(panel);
  }
  const stackedPanels = shouldStackPanelGroup(uniquePanels)
    ? [...uniquePanels].sort((left, right) => {
        const order = {table: 0, plot: 1};
        return (order[left.mode] ?? 9) - (order[right.mode] ?? 9);
      })
    : null;
  if (uniquePanels.length === 1) {
    const single = await renderDataPanel(uniquePanels[0], report, {
      useLayout: false,
      forceWide: panelGroupNeedsOwnRow({panels: uniquePanels, layout: group.layout}),
      block
    });
    single.style.gridColumn = panelGroupNeedsOwnRow({panels: uniquePanels, layout: group.layout})
      ? "1 / -1"
      : `${Number(group.layout?.x || 0) + 1} / span ${panelGroupWidth(group)}`;
    return single;
  }
  if (stackedPanels) {
    const wrapper = document.createElement("section");
    wrapper.className = "report-panel-group report-panel-group--stacked";
    wrapper.style.gridColumn = `${Number(group.layout?.x || 0) + 1} / span ${panelGroupWidth(group)}`;
    const renderedPanels = await Promise.all(stackedPanels.map((panel) => renderDataPanel(panel, report, {
      useLayout: false,
      block
    })));
    for (const panelNode of renderedPanels) {
      wrapper.append(panelNode);
    }
    return wrapper;
  }
  const wrapper = document.createElement("section");
  wrapper.className = "report-panel surface surface--padded report-panel-group";
  wrapper.style.minHeight = `${Math.max(...uniquePanels.map((panel) => panelMinHeight(panel)))}px`;
  wrapper.style.gridColumn = `${Number(group.layout?.x || 0) + 1} / span ${panelGroupWidth(group)}`;

  const tabs = document.createElement("div");
  tabs.className = "report-panel-group__tabs";
  const body = document.createElement("div");
  body.className = "report-panel-group__body";
  wrapper.append(tabs, body);

  const renderedPanels = await Promise.all(uniquePanels.map((panel) => renderDataPanel(panel, report, {
    useLayout: false,
    block
  })));
  let activeIndex = 0;
  const redraw = () => {
    body.replaceChildren(renderedPanels[activeIndex]);
    for (const [index, button] of [...tabs.children].entries()) {
      button.classList.toggle("report-panel-group__tab--active", index === activeIndex);
    }
  };
  uniquePanels.forEach((panel, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `report-panel-group__tab${index === 0 ? " report-panel-group__tab--active" : ""}`;
    button.textContent = panelTitle(panel);
    button.addEventListener("click", () => {
      activeIndex = index;
      redraw();
    });
    tabs.append(button);
  });
  redraw();
  return wrapper;
}

function renderPanelRowShell(row) {
  const rowElement = document.createElement("div");
  rowElement.className = "report-panel-row";
  rowElement.append(createRowLoadingPlaceholder(row));
  return rowElement;
}

async function fillPanelRow(rowElement, row, report, block) {
  const renderedGroups = [];
  for (const group of row.groups) {
    renderedGroups.push(await renderPanelGroup(group, report, block));
  }
  rowElement.replaceChildren(...renderedGroups);
}

async function renderBlock(block, report) {
  if (block.type === "html") {
    const section = document.createElement("section");
    section.className = "report-block report-html";
    section.innerHTML = block.html || "";
    return section;
  }
  if (block.type === "image") {
    const figure = document.createElement("figure");
    figure.className = "report-block report-figure";
    const img = document.createElement("img");
    img.src = mediaUrl(block.src);
    img.alt = block.alt || "";
    figure.append(img);
    if (block.caption_html) {
      const figcaption = document.createElement("figcaption");
      figcaption.innerHTML = block.caption_html;
      figure.append(figcaption);
    }
    return figure;
  }
  if (block.type === "details") {
    const details = document.createElement("details");
    details.className = "report-block report-details surface surface--padded";
    details.open = true;
    const summary = document.createElement("summary");
    summary.innerHTML = block.summary_html || "Details";
    details.append(summary);
    const body = document.createElement("div");
      body.className = "report-details__body";
    for (const child of block.children || []) {
      body.append(await renderBlock(child, report));
    }
    details.append(body);
    return details;
  }
  if (block.type === "artifact-panel") {
    return renderArtifactPanel(block);
  }
  if (block.type === "panel-grid") {
    const section = document.createElement("section");
    section.className = "report-block report-panel-layout";
    const rows = buildPanelRows(block.panels || []);
    for (const [index, row] of rows.entries()) {
      const rowElement = renderPanelRowShell(row);
      section.append(rowElement);
      scheduleLazyRender(rowElement, async () => {
        await fillPanelRow(rowElement, row, report, block);
      }, {
        eager: index < 2,
        delay: Math.min(index * 40, 480)
      });
    }
    if (!(block.panels || []).length) {
      section.append(emptyNote("This W&B panel grid was exported without panel metadata."));
    }
    return section;
  }
  if (block.type === "code") {
    const pre = document.createElement("pre");
    pre.className = "report-block report-code surface surface--padded";
    const code = document.createElement("code");
    code.textContent = block.code || "";
    pre.append(code);
    return pre;
  }
  return emptyNote(`Unsupported report block: ${block.type}`);
}

export async function mountReportPage(root) {
  const report = await loadReport();
  root.className = "report-root";

  const article = document.createElement("article");
  article.className = "report-article";

  const header = document.createElement("header");
  header.className = "report-header";
  header.innerHTML = `
    <p class="report-kicker">Exported W&B Report</p>
    <h1>${titleForReport(report)}</h1>
    ${report.report_url ? `<p class="report-source"><a href="${report.report_url}" target="_blank" rel="noopener noreferrer">Open original report on W&B</a></p>` : ""}
  `;
  article.append(header);
  root.replaceChildren(article);

  const blocks = report.blocks || [];
  const placeholders = blocks.map((block) => {
    const placeholder = loadingBlock(block.type === "panel-grid" ? "Loading report panels..." : "Loading report block...");
    article.append(placeholder);
    return placeholder;
  });

  for (const [index, block] of blocks.entries()) {
    const rendered = await renderBlock(block, report);
    placeholders[index].replaceWith(rendered);
    if (index < blocks.length - 1) {
      await new Promise((resolve) => requestAnimationFrame(() => resolve()));
    }
  }
}

# W&B Report Exporter
![W&B Report Exporter screenshots](./wandb-report-exporter_screenshots_tiled.png)

This repository currently focuses on exporting a W&B Report into a `marimo HTML-WASM` viewer for local, self-contained exploration.

The goal is report-first export: preserve the original W&B report structure, panels, tables, media, and chart behavior as much as possible while serving everything from local files.

## What It Exports

`scripts/export_wandb_snapshot.py` builds a snapshot with:

- report blocks and panel metadata
- the latest accessible saved draft state when W&B exposes one
- report-scoped runs only, inherited from the report runset/filter when available
- run unions across report runsets when a report mixes multiple panel-level runsets
- run summaries
- offline history rows for report charts
- panel tables as JSON
- local media assets for images, masks, and supported Plotly payloads
- artifact lineage metadata for lineage panels
- persistent per-run history caching to speed up repeated exports

The exported snapshot is then consumed by:

- `marimo_viewer/dist/` for the marimo HTML-WASM viewer
- `dist/` for the legacy Observable reference viewer

## Setup

### Python

```bash
uv sync
```

If you want report export extras and marimo export support:

```bash
uv sync --extra reports --extra marimo
```

### Frontend

```bash
npm install
```

## Configuration

Copy `.env.example` to `.env` if you want environment-based configuration.

Important variables:

- `WANDB_API_KEY`: required for live export from W&B
- `WANDB_REPORT_URL`: the W&B report URL to export
- `WANDB_ENTITY`, `WANDB_PROJECT`: optional when the report URL already contains them
- `WANDB_HISTORY_KEYS`: optional extra history keys to force-export
- `WANDB_MAX_RUNS`: safety cap for exported runs
- `WANDB_TABLE_NAME`, `WANDB_TABLE_ARTIFACT`: optional manual overrides for difficult table resolution cases
- `WANDB_EXPORT_WORKERS`: optional concurrency override for history/table/media export work
- `WANDB_ENABLE_PRIMARY_TABLE_SCAN=1`: opt into the slower legacy primary-table crawl used by the old Observable fallback

In most cases, the simplest workflow is to pass the report URL directly on the command line.

## Quick Start

### 1. Export a report snapshot

```bash
python3 scripts/export_wandb_snapshot.py "https://wandb.ai/<entity>/<project>/reports/..."
```

If credentials are missing, the exporter falls back to sample data so you can still build and test the marimo viewer.

The first live export can take a while. For larger reports, it is normal for this step to take several minutes while W&B history, tables, artifacts, and media are being fetched.

Recent versions of the exporter print per-phase progress, so you should now see which stage is active even during long downloads.

### 2. Build and serve the marimo viewer

```bash
make marimo-build
make marimo-serve
```

Open `http://localhost:8124`.

## Common Workflows

### Full marimo flow

```bash
make export
make verify-export
make marimo-build
make marimo-serve
```

### Stop the local marimo server

```bash
make marimo-stop
```

## Output Layout

- `extracted/processed/`: canonical exported snapshot
- `marimo_viewer/wandb_report.py`: generated marimo notebook
- `marimo_viewer/dist/`: final marimo HTML-WASM site
- `app/src/data/`: data bundle for the legacy Observable reference viewer
- `app/src/media/`: media bundle for the legacy Observable reference viewer
- `dist/`: final Observable static site

## Git Hygiene

Exported content and local build artifacts are meant to stay out of version control.

The repository's `.gitignore` is set up so that the following stay local by default:

- `extracted/processed/` and `extracted/raw/`
- `app/src/data/` and `app/src/media/`
- `marimo_viewer/dist/`, `marimo_viewer/generated_assets/`, and generated marimo notebook payload files
- `dist/`
- `artifacts/`
- `output/`
- local editor and Codex metadata such as `.vscode/` and `.codex/`

In normal use, you should commit source files, tests, and docs, but not downloaded W&B content or generated export output.

## Current Coverage

The exporter and marimo viewer currently support a substantial subset of W&B report content, including:

- rich HTML and markdown-like report blocks
- code blocks
- image blocks
- panel grids
- Run History Line Plot panels
- Combined Plot / Weave scatter-style panels
- many table panels, including computed columns and sort metadata when exported
- image tables with segmentation mask overlays
- supported Plotly payloads exported through media panels
- artifact lineage panels
- Vega-based custom charts where the exported spec and offline data are sufficient
- square/polar Vega charts rendered with series-aware offline field mapping
- per-panel series selection for multi-series scatter/radar-style charts in marimo
- hover tooltips for custom scatter/history/radar-style points
- runset-aware panel filtering, including inferred visible runs from exported panel/media rows when the published report spec only contains hidden/selected state

## Notes and Limitations

- The viewer is meant to be served over HTTP. Do not open it via `file://`.
- Some W&B panel types still depend on W&B-specific runtime behavior that is difficult to reproduce perfectly offline.
- Advanced mask interaction in W&B itself is richer than the current offline implementation.
- W&B's published report spec can lag behind the browser-visible draft state; when that happens, offline export can only reproduce the state exposed by the fetched report spec.
- Export speed is still bounded mostly by W&B API/history/media fetch time, although the exporter now uses safer staging and some concurrency.
- The first export of a large report can take long enough to feel stalled. That is expected: the exporter may spend minutes downloading run history, artifacts, tables, and media before the next visible log line appears.
- For marimo-first workflows, the exporter skips the old global primary-table crawl by default because it can dominate runtime on large leaderboard reports. Panel tables referenced by the report are still exported. If you explicitly need the legacy `table_predictions.parquet` path, set `WANDB_ENABLE_PRIMARY_TABLE_SCAN=1`.
- Large reports can take noticeable time to hydrate in the browser when many offline charts and tables are present.
- If a report cannot be resolved to a report-scoped runset, the exporter avoids silently falling back to a full-project crawl.

## Development Notes

- `python3 scripts/generate_marimo_report.py` regenerates the marimo notebook from the current snapshot
- `python3 scripts/export_marimo_wasm.py` regenerates the HTML-WASM marimo site
- `python3 scripts/verify_export.py` validates that the exported snapshot has the files and offline rows needed by the marimo renderer
- `npm run build` still builds the Observable reference viewer and syncs static assets into `dist/`

## Observable Reference Viewer

The Observable viewer is still in the repository as an experimental reference path, but it is not the current product focus.

If you want to try it anyway:

```bash
make build
make serve
```

Open `http://localhost:8000`.

If you are testing against a remote machine, serve the built output over HTTP and use SSH port forwarding or your private network overlay to access it locally.

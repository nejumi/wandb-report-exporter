#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_marimo_report import (
    filter_rows_by_report_runsets,
    infer_block_visible_values,
    render_history_svg,
    should_use_selection_fallback,
)
PROCESSED_DIR = ROOT / "extracted" / "processed"


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def collect_media_paths(value: object) -> set[str]:
    paths: set[str] = set()
    if isinstance(value, dict):
        direct_path = value.get("path")
        overlay_path = value.get("overlay_path")
        if isinstance(direct_path, str) and direct_path:
            paths.add(direct_path)
        if isinstance(overlay_path, str) and overlay_path:
            paths.add(overlay_path)
        for nested in value.values():
            paths.update(collect_media_paths(nested))
    elif isinstance(value, list):
        for item in value:
            paths.update(collect_media_paths(item))
    return paths


def main() -> int:
    issues: list[str] = []
    warnings: list[str] = []

    report_path = PROCESSED_DIR / "report_content.json"
    history_path = PROCESSED_DIR / "history_eval_metrics.parquet"
    if not report_path.exists():
        print("[fail] missing extracted/processed/report_content.json")
        return 1
    if not history_path.exists():
        print("[fail] missing extracted/processed/history_eval_metrics.parquet")
        return 1

    report = load_json(report_path)
    if not isinstance(report, dict):
        print("[fail] report_content.json is not a JSON object")
        return 1

    history_rows = pd.read_parquet(history_path).to_dict(orient="records")
    panel_tables = report.get("panel_tables", {}) or {}

    for table_key, meta in panel_tables.items():
        if not isinstance(meta, dict):
            warnings.append(f"panel table metadata for {table_key} is malformed")
            continue
        relative_path = meta.get("path")
        if not isinstance(relative_path, str) or not relative_path:
            issues.append(f"panel table {table_key} is missing a path")
            continue
        path = PROCESSED_DIR / relative_path
        if not path.exists():
            issues.append(f"panel table file is missing: {relative_path}")
            continue
        try:
            rows = load_json(path)
        except Exception as exc:
            issues.append(f"panel table {table_key} failed to load: {exc}")
            continue
        if not isinstance(rows, list):
            issues.append(f"panel table {table_key} is not a JSON list")
            continue
        for media_path in sorted(collect_media_paths(rows)):
            if not (PROCESSED_DIR / media_path).exists():
                issues.append(f"referenced media is missing for {table_key}: {media_path}")

    history_panel_count = 0
    for block_index, block in enumerate(report.get("blocks", []) or []):
        if not isinstance(block, dict) or block.get("type") != "panel-grid":
            continue
        runset_names = [str(value) for value in block.get("runsets", []) or [] if value]
        visible_values = infer_block_visible_values(report, block)
        filtered_history_rows = filter_rows_by_report_runsets(
            history_rows,
            report,
            runset_names,
            visible_values=visible_values,
            use_selection_fallback=should_use_selection_fallback(report, runset_names),
        )
        for panel_index, panel in enumerate(block.get("panels", []) or []):
            if not isinstance(panel, dict):
                continue
            if panel.get("view_type") == "Run History Line Plot":
                history_panel_count += 1
                if not render_history_svg(panel, filtered_history_rows):
                    issues.append(
                        f"history panel block={block_index} panel={panel_index} could not be rendered from offline rows "
                        f"for metrics={panel.get('metrics')}"
                    )
            for media_path in sorted(collect_media_paths(panel)):
                if media_path.startswith("http"):
                    continue
                if not (PROCESSED_DIR / media_path).exists():
                    issues.append(f"panel block={block_index} panel={panel_index} references missing media: {media_path}")

    print(f"[info] verified {len(panel_tables)} panel tables and {history_panel_count} history panels")
    for warning in warnings:
        print(f"[warn] {warning}")
    if issues:
        for issue in issues:
            print(f"[fail] {issue}")
        return 1
    print("[ok] export snapshot passed validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

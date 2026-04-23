#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import html
import json
import math
import os
import re
import shutil
import subprocess
import textwrap
import zlib
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = Path(os.environ.get("WANDB_PROCESSED_DIR", ROOT / "extracted" / "processed")).resolve()
MARIMO_VIEWER_DIR = ROOT / "marimo_viewer"
NOTEBOOK_PATH = MARIMO_VIEWER_DIR / "wandb_report.py"
GENERATED_ASSETS_DIR = MARIMO_VIEWER_DIR / "generated_assets"
VEGA_RENDER_SCRIPT = ROOT / "scripts" / "render_vega_svg.mjs"


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def row_has_media_placeholders(row: dict[str, object]) -> bool:
    for key, value in row.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        normalized_key = re.sub(r"[^a-z0-9]+", "", key.lower())
        normalized_value = re.sub(r"[^a-z0-9]+", "", value.lower())
        if normalized_key in {"image", "images", "media", "file", "files"} and normalized_value == normalized_key:
            return True
    return False


def hydrate_media_items(node: object) -> object:
    if isinstance(node, dict):
        media_items = node.get("media_items")
        if isinstance(media_items, list):
            hydrated_items = []
            for item in media_items:
                if not isinstance(item, dict):
                    hydrated_items.append(item)
                    continue
                hydrated = dict(item)
                relative_path = hydrated.get("path")
                if isinstance(relative_path, str) and "plotly" in str(hydrated.get("kind", "")):
                    source_path = PROCESSED_DIR / relative_path
                    if source_path.exists():
                        hydrated["figure"] = load_json(source_path)
                hydrated_items.append(hydrated)
            node = {**node, "media_items": hydrated_items}
        return {key: hydrate_media_items(value) for key, value in node.items()}
    if isinstance(node, list):
        return [hydrate_media_items(item) for item in node]
    return node


def flatten_selection_tree(node: object) -> list[str]:
    if isinstance(node, list):
        values: list[str] = []
        for item in node:
            values.extend(flatten_selection_tree(item))
        return values
    if isinstance(node, str):
        return [node]
    if not isinstance(node, dict):
        return []
    values: list[str] = []
    for key in ("children", "items", "tree", "value", "values"):
        if key in node:
            values.extend(flatten_selection_tree(node.get(key)))
    return values


def runset_matches_identifier(runset: dict[str, object], identifier: str) -> bool:
    token = str(identifier or "")
    if not token:
        return False
    return token in {str(runset.get("id") or ""), str(runset.get("name") or "")}


def matched_runsets(report: dict[str, object], runset_names: list[str]) -> list[dict[str, object]]:
    return [
        runset
        for runset in report.get("runsets", []) or []
        if isinstance(runset, dict) and any(runset_matches_identifier(runset, identifier) for identifier in runset_names)
    ]


def runset_selection_values(report: dict[str, object], runset_names: list[str]) -> set[str]:
    values: set[str] = set()
    for runset in matched_runsets(report, runset_names):
        for value in flatten_selection_tree((runset.get("selections") or {}).get("tree")):
            if isinstance(value, str) and value.strip():
                values.add(value.strip())
    selected_runset = report.get("selected_runset") or {}
    if not values and isinstance(selected_runset, dict) and any(
        token in {str(selected_runset.get("id") or ""), str(selected_runset.get("name") or "")}
        for token in runset_names
    ):
        for value in selected_runset.get("selection_run_ids", []) or []:
            if value:
                values.add(str(value))
        for value in selected_runset.get("selection_names", []) or []:
            if value:
                values.add(str(value))
    return values


def runset_selection_root(runset: dict[str, object]) -> int | None:
    root = ((runset.get("selections") or {}).get("root"))
    if root in {0, 1}:
        return int(root)
    return None


def runset_selection_mode(runset: dict[str, object]) -> str | None:
    explicit_values = flatten_selection_tree((runset.get("selections") or {}).get("tree"))
    if runset.get("only_show_selected") or runset.get("single_run_only"):
        return "include"
    if not explicit_values:
        return None
    root = runset_selection_root(runset)
    if root == 0:
        return "include"
    if root == 1:
        return "exclude"
    return None


def infer_block_visible_values(report: dict[str, object], block: dict[str, object]) -> set[str]:
    values: set[str] = set()
    if block.get("type") != "panel-grid":
        return values
    for value in block.get("visible_run_ids", []) or []:
        if value:
            values.add(str(value))
    for value in block.get("visible_run_names", []) or []:
        if value:
            values.add(str(value))
    if values:
        return values

    for runset in matched_runsets(report, [str(value) for value in block.get("runsets", []) or [] if value]):
        for value in runset.get("visible_run_ids", []) or []:
            if value:
                values.add(str(value))
        for value in runset.get("visible_run_names", []) or []:
            if value:
                values.add(str(value))
    if values:
        return values

    candidate_run_ids: set[str] = set()
    candidate_run_names: set[str] = set()
    panel_tables = report.get("panel_tables", {}) or {}
    for panel in block.get("panels", []) or []:
        if not isinstance(panel, dict):
            continue
        table_key = panel.get("table_key")
        if isinstance(table_key, str):
            table_meta = panel_tables.get(table_key) or {}
            for value in table_meta.get("run_ids", []) or []:
                if value:
                    candidate_run_ids.add(str(value))
            for value in table_meta.get("run_names", []) or []:
                if value:
                    candidate_run_names.add(str(value))
        for item in panel.get("media_items", []) or []:
            if not isinstance(item, dict):
                continue
            if item.get("run_id"):
                candidate_run_ids.add(str(item.get("run_id")))
            if item.get("run_name"):
                candidate_run_names.add(str(item.get("run_name")))

    for runset in matched_runsets(report, [str(value) for value in block.get("runsets", []) or [] if value]):
        selection_values = runset_selection_values(report, [str(runset.get("id") or runset.get("name") or "")])
        selection_mode = runset_selection_mode(runset)
        if not selection_values or not selection_mode:
            continue
        if selection_mode == "include":
            values.update(value for value in candidate_run_ids if value in selection_values)
            values.update(value for value in candidate_run_names if value in selection_values)
            continue
        selected_candidate_ids = [value for value in candidate_run_ids if value in selection_values]
        selected_candidate_names = [value for value in candidate_run_names if value in selection_values]
        if not selected_candidate_ids and not selected_candidate_names:
            continue
        if candidate_run_ids:
            values.update(value for value in candidate_run_ids if value not in selection_values)
            continue
        values.update(value for value in candidate_run_names if value not in selection_values)
    return values


def should_use_selection_fallback(report: dict[str, object], runset_names: list[str]) -> bool:
    return any(
        bool(runset_selection_mode(runset))
        for runset in matched_runsets(report, runset_names)
    )


def filter_rows_by_report_runsets(
    rows: list[dict[str, object]],
    report: dict[str, object],
    runset_names: list[str],
    *,
    visible_values: set[str] | None = None,
    use_selection_fallback: bool = False,
) -> list[dict[str, object]]:
    if not rows or not runset_names:
        return rows
    visible = visible_values or set()
    if visible:
        filtered = [
            row
            for row in rows
            if str(row.get("__run_id") or row.get("run_id") or "") in visible
            or str(row.get("__run_name") or row.get("run_name") or "") in visible
        ]
        if filtered:
            return filtered
        return rows
    if not use_selection_fallback:
        return rows
    selected = runset_selection_values(report, runset_names)
    if not selected:
        return rows
    include_mode = any(
        runset_selection_mode(runset) == "include"
        for runset in matched_runsets(report, runset_names)
    )
    filtered = [
        row
        for row in rows
        if str(row.get("__run_id") or row.get("run_id") or "") in selected
        or str(row.get("__run_name") or row.get("run_name") or "") in selected
    ]
    if include_mode:
        return filtered or rows
    remaining = [
        row
        for row in rows
        if str(row.get("__run_id") or row.get("run_id") or "") not in selected
        and str(row.get("__run_name") or row.get("run_name") or "") not in selected
    ]
    return remaining or rows


def pick_first_matching_key(rows: list[dict[str, object]], preferred_keys: list[str], predicate) -> str | None:
    if not rows:
        return None
    row_keys = list(rows[0].keys())
    for key in preferred_keys:
        if key in row_keys and any(predicate(row.get(key), key) for row in rows):
            return key
    for key in row_keys:
        if any(predicate(row.get(key), key) for row in rows):
            return key
    return None


def infer_vega_fields(rows: list[dict[str, object]]) -> dict[str, str]:
    def is_non_empty_string(value: object, _key: str) -> bool:
        return isinstance(value, str) and bool(value)

    def is_number(value: object, _key: str) -> bool:
        try:
            float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return False
        return True

    def is_present(value: object, _key: str) -> bool:
        return value not in (None, "")

    return {
        "x": pick_first_matching_key(rows, ["category", "label", "x", "__step"], is_non_empty_string) or "category",
        "y": pick_first_matching_key(rows, ["score", "TOTAL_SCORE", "y", "value"], is_number) or "score",
        "name": pick_first_matching_key(rows, ["model_name", "__run_name", "name", "series"], is_non_empty_string) or "name",
        "id": pick_first_matching_key(rows, ["__run_id", "id", "__run_name", "model_name"], is_present) or "id",
    }


def materialize_vega_rows(rows: list[dict[str, object]], fields: dict[str, str]) -> list[dict[str, object]]:
    materialized: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        materialized.append(
            {
                **row,
                "name": row.get(fields["name"]) or row.get("__run_name") or row.get("model_name") or f"Series {index}",
                "id": row.get(fields["id"]) or row.get("__run_id") or row.get("__run_name") or row.get("model_name") or f"series-{index}",
            }
        )
    return materialized


def interpolate_vega_spec(spec_text: str | None, fields: dict[str, str]) -> dict[str, object] | None:
    if not spec_text:
        return None
    mapping = {
        "x": fields.get("x") or "category",
        "y": fields.get("y") or "score",
        "name": fields.get("name") or "name",
        "id": fields.get("id") or "id",
    }
    replaced = str(spec_text)
    for key, value in mapping.items():
        replaced = replaced.replace(f"${{field:{key}}}", value)
    return normalize_vega_series_fields(json.loads(replaced), mapping)


def normalize_vega_series_fields(node: object, fields: dict[str, str]) -> object:
    series_field = fields.get("id") or "id"
    if isinstance(node, dict):
        normalized: dict[str, object] = {}
        for key, value in node.items():
            if key == "field" and value == "id" and series_field != "id":
                normalized[key] = series_field
                continue
            if key == "groupby" and isinstance(value, list) and series_field != "id":
                normalized[key] = [series_field if item == "id" else item for item in value]
                continue
            normalized[key] = normalize_vega_series_fields(value, fields)
        return normalized
    if isinstance(node, list):
        return [normalize_vega_series_fields(item, fields) for item in node]
    return node


def is_radial_vega_spec(spec: dict[str, object] | None) -> bool:
    if not isinstance(spec, dict):
        return False
    serialized = json.dumps(spec, ensure_ascii=False)
    return (
        "cos(scale(" in serialized
        and "sin(scale(" in serialized
        and '"radius"' in serialized
        and ('"angular"' in serialized or '"radial"' in serialized)
    )


def apply_simple_filter_rows(rows: list[dict[str, object]], filter_spec: object) -> list[dict[str, object]]:
    if not isinstance(filter_spec, dict):
        return rows
    column = filter_spec.get("column")
    op = filter_spec.get("op")
    value = filter_spec.get("value")
    if not column or op is None:
        return rows
    filtered: list[dict[str, object]] = []
    for row in rows:
        raw_value = row.get(str(column))
        if raw_value in (None, ""):
            continue
        try:
            cell = float(raw_value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if op == "number-lessEqual" and cell <= value:
            filtered.append(row)
        elif op == "number-greaterEqual" and cell >= value:
            filtered.append(row)
        elif op == "number-lessThan" and cell < value:
            filtered.append(row)
        elif op == "number-greaterThan" and cell > value:
            filtered.append(row)
        elif op == "number-equal" and cell == value:
            filtered.append(row)
    return filtered


def render_vega_svg(spec_text: str | None, rows: list[dict[str, object]]) -> tuple[str | None, str | None]:
    fields = infer_vega_fields(rows)
    spec = interpolate_vega_spec(spec_text, fields)
    if spec is None:
        return None, "Custom chart spec was empty."
    data = spec.get("data")
    if isinstance(data, list):
        spec["data"] = [
            {**item, "values": materialize_vega_rows(rows, fields)}
            if isinstance(item, dict) and item.get("name") == "wandb"
            else item
            for item in data
        ]
    spec["autosize"] = {"type": "fit", "contains": "padding"}
    spec.setdefault("padding", {"top": 24, "right": 36, "bottom": 42, "left": 48})
    radial = is_radial_vega_spec(spec)
    result = subprocess.run(
        ["node", str(VEGA_RENDER_SCRIPT)],
        input=json.dumps(
            {
                "spec": spec,
                "width": 820 if radial else 720,
                "height": 820 if radial else 480,
            },
            ensure_ascii=False,
        ),
        text=True,
        capture_output=True,
        cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        return None, (result.stderr or "Failed to render Vega chart.").strip()
    return result.stdout, None


def write_svg_asset(svg_text: str, prefix: str) -> str:
    GENERATED_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(svg_text.encode("utf-8")).hexdigest()[:16]
    filename = f"{prefix}-{digest}.svg"
    path = GENERATED_ASSETS_DIR / filename
    path.write_text(svg_text, encoding="utf-8")
    return f"assets/generated_assets/{filename}"


def write_png_asset(image: Image.Image, prefix: str) -> str:
    GENERATED_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    buffer = image.tobytes()
    digest = hashlib.sha256(buffer).hexdigest()[:16]
    filename = f"{prefix}-{digest}.png"
    path = GENERATED_ASSETS_DIR / filename
    image.save(path, format="PNG")
    return f"assets/generated_assets/{filename}"


def write_json_asset(payload: object, prefix: str) -> str:
    GENERATED_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    filename = f"{prefix}-{digest}.json"
    path = GENERATED_ASSETS_DIR / filename
    path.write_text(text, encoding="utf-8")
    return f"assets/generated_assets/{filename}"


def clamp(value: float, lower: float, upper: float) -> float:
    return min(max(value, lower), upper)


def tooltip_box_geometry(
    x: float,
    y: float,
    lines: list[str],
    width: float,
    height: float,
    *,
    detail_width: float = 0.0,
    detail_height: float = 0.0,
) -> tuple[float, float, float, float]:
    char_width = 7.2
    line_height = 16
    padding_x = 10
    padding_y = 9
    text_width = max((len(line) for line in lines), default=0) * char_width + padding_x * 2
    text_height = max(36.0, len(lines) * line_height + padding_y * 2 - 2)
    box_width = max(92.0, text_width, detail_width + padding_x * 2)
    box_height = max(36.0, text_height + detail_height)
    prefer_left = x > width * 0.6
    base_left = x - box_width - 16 if prefer_left else x + 16
    base_top = y + 16 if y - box_height - 14 < 8 else y - box_height - 14
    left = clamp(base_left, 8.0, max(8.0, width - box_width - 8.0))
    top = clamp(base_top, 8.0, max(8.0, height - box_height - 8.0))
    return left, top, box_width, box_height


def svg_interactive_point(
    *,
    x: float,
    y: float,
    radius: float,
    color: str,
    lines: list[str],
    width: float,
    height: float,
    stroke: str = "white",
    stroke_width: float = 1.5,
    opacity: float = 0.9,
) -> str:
    left, top, box_width, box_height = tooltip_box_geometry(x, y, lines, width, height)
    tspans = "".join(
        f"<tspan x='{left + 10:.2f}' dy='{0 if index == 0 else 16}'>{html.escape(line)}</tspan>"
        for index, line in enumerate(lines)
    )
    hide_all = 'const root=this.ownerSVGElement||this.closest("svg");if(root){for(const other of root.querySelectorAll(".chart-tooltip")){other.style.opacity="0";other.style.visibility="hidden";}}'
    return (
        "<g class='chart-point' tabindex='0' "
        + f"""onfocusin='this.parentNode.appendChild(this);{hide_all};const tip=this.querySelector(".chart-tooltip");if(tip){{tip.style.visibility="visible";tip.style.opacity="1";}}' """
        + """onfocusout='const tip=this.querySelector(".chart-tooltip");if(tip){tip.style.opacity="0";tip.style.visibility="hidden";}'>"""
        + f"""<circle cx='{x:.2f}' cy='{y:.2f}' r='{max(radius + 7, 11):.2f}' class='chart-point__target' fill='rgba(255,255,255,0.001)' pointer-events='all' onmouseover='this.parentNode.parentNode.appendChild(this.parentNode);{hide_all};const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.visibility="visible";tip.style.opacity="1";}}' onmousemove='{hide_all};const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.visibility="visible";tip.style.opacity="1";}}' onmouseout='const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.opacity="0";tip.style.visibility="hidden";}}' onmouseleave='const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.opacity="0";tip.style.visibility="hidden";}}'></circle>"""
        + f"<circle cx='{x:.2f}' cy='{y:.2f}' r='{radius:.2f}' fill='{html.escape(color)}' fill-opacity='{opacity}' stroke='{html.escape(stroke)}' stroke-width='{stroke_width}' class='chart-point__dot'></circle>"
        + "<g class='chart-tooltip'>"
        + f"<rect x='{left:.2f}' y='{top:.2f}' width='{box_width:.2f}' height='{box_height:.2f}' rx='12' ry='12' class='chart-tooltip__bubble'></rect>"
        + f"<text x='{left + 10:.2f}' y='{top + 20:.2f}' class='chart-tooltip__text'>{tspans}</text>"
        + "</g></g>"
    )


def svg_interactive_rect(
    *,
    x: float,
    y: float,
    rect_width: float,
    rect_height: float,
    fill: str,
    opacity: float,
    lines: list[str],
    width: float,
    height: float,
    detail_svg: str = "",
    detail_width: float = 0.0,
    detail_height: float = 0.0,
) -> str:
    left, top, box_width, box_height = tooltip_box_geometry(
        x + rect_width / 2,
        y + rect_height / 2,
        lines,
        width,
        height,
        detail_width=detail_width,
        detail_height=detail_height,
    )
    line_height = 16
    padding_y = 9
    text_height = max(36.0, len(lines) * line_height + padding_y * 2 - 2)
    tspans = "".join(
        f"<tspan x='{left + 10:.2f}' dy='{0 if index == 0 else 16}'>{html.escape(line)}</tspan>"
        for index, line in enumerate(lines)
    )
    hide_all = 'const root=this.ownerSVGElement||this.closest("svg");if(root){for(const other of root.querySelectorAll(".chart-tooltip")){other.style.opacity="0";other.style.visibility="hidden";}}'
    detail_group = (
        f"<g transform='translate({left + 10:.2f},{top + text_height:.2f})'>{detail_svg}</g>"
        if detail_svg
        else ""
    )
    return (
        "<g class='chart-point' tabindex='0' "
        + f"""onfocusin='this.parentNode.appendChild(this);{hide_all};const tip=this.querySelector(".chart-tooltip");if(tip){{tip.style.visibility="visible";tip.style.opacity="1";}}' """
        + """onfocusout='const tip=this.querySelector(".chart-tooltip");if(tip){tip.style.opacity="0";tip.style.visibility="hidden";}'>"""
        + f"""<rect x='{x:.2f}' y='{y:.2f}' width='{rect_width:.2f}' height='{rect_height:.2f}' fill='{html.escape(fill)}' fill-opacity='{opacity:.3f}' stroke='none' pointer-events='all' onmouseover='this.parentNode.parentNode.appendChild(this.parentNode);{hide_all};const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.visibility="visible";tip.style.opacity="1";}}' onmousemove='{hide_all};const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.visibility="visible";tip.style.opacity="1";}}' onmouseout='const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.opacity="0";tip.style.visibility="hidden";}}' onmouseleave='const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{tip.style.opacity="0";tip.style.visibility="hidden";}}'></rect>"""
        + "<g class='chart-tooltip'>"
        + f"<rect x='{left:.2f}' y='{top:.2f}' width='{box_width:.2f}' height='{box_height:.2f}' rx='12' ry='12' class='chart-tooltip__bubble'></rect>"
        + f"<text x='{left + 10:.2f}' y='{top + 20:.2f}' class='chart-tooltip__text'>{tspans}</text>"
        + detail_group
        + "</g></g>"
    )


def attach_pre_rendered_vega(node: object, panel_tables: dict[str, object], report: dict[str, object]) -> object:
    if isinstance(node, dict):
        if node.get("type") == "panel-grid":
            runset_names = [str(value) for value in node.get("runsets", []) or [] if value]
            visible_values = infer_block_visible_values(report, node)
            use_selection_fallback = should_use_selection_fallback(report, runset_names)
            hydrated_panels = []
            for panel in node.get("panels", []) or []:
                if not isinstance(panel, dict):
                    hydrated_panels.append(panel)
                    continue
                hydrated = dict(panel)
                if hydrated.get("view_type") == "Vega2" and hydrated.get("vega_spec") and hydrated.get("table_key"):
                    table_rows = panel_tables.get(str(hydrated.get("table_key")), [])
                    if isinstance(table_rows, list):
                        filtered_rows = filter_rows_by_report_runsets(
                            table_rows,
                            report,
                            runset_names,
                            visible_values=visible_values,
                            use_selection_fallback=use_selection_fallback,
                        )
                        filtered_rows = apply_simple_filter_rows(filtered_rows, hydrated.get("simple_filter"))
                        spec = interpolate_vega_spec(str(hydrated.get("vega_spec")), infer_vega_fields(filtered_rows or table_rows))
                        if is_radial_vega_spec(spec):
                            hydrated["vega_aspect"] = "square"
                        svg, error = render_vega_svg(str(hydrated.get("vega_spec")), filtered_rows)
                        if svg:
                            hydrated["vega_svg_path"] = write_svg_asset(svg, "vega")
                        if error:
                            hydrated["vega_error"] = error
                hydrated_panels.append(hydrated)
            node = {**node, "panels": hydrated_panels}
        return {key: attach_pre_rendered_vega(value, panel_tables, report) for key, value in node.items()}
    if isinstance(node, list):
        return [attach_pre_rendered_vega(item, panel_tables, report) for item in node]
    return node


def format_date_value(value: object) -> str:
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def format_runtime_value(value: object) -> str:
    try:
        total = int(float(value))
    except Exception:
        return str(value)
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m"
    return f"{seconds}s"


def to_plot_number(value: object, treat_as_timestamp: bool = False) -> float | None:
    if value in (None, ""):
        return None
    if not treat_as_timestamp:
        try:
            number = float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None
    try:
        numeric = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        numeric = None
    if numeric is not None and math.isfinite(numeric):
        if abs(numeric) > 1e17:
            return numeric / 1e9
        if abs(numeric) > 1e14:
            return numeric / 1e6
        if abs(numeric) > 1e11:
            return numeric / 1e3
        return numeric
    try:
        parsed = pd.to_datetime(value).timestamp() * 1000
    except Exception:
        return None
    return parsed if math.isfinite(parsed) else None


def history_axis_field(panel: dict[str, object]) -> str:
    axis = panel.get("x_axis") or "_step"
    if axis == "_step":
        return "step"
    if axis == "_runtime":
        return "runtime"
    if axis == "_timestamp":
        return "timestamp_value"
    if axis == "epoch":
        return "epoch"
    return str(axis)


def history_metric_aliases(metric_name: str) -> list[str]:
    value = str(metric_name or "")
    aliases = [value]
    process_match = re.match(r"^system/gpu\.process\.(\d+)\.(.+)$", value)
    if process_match:
        aliases.append(f"system/gpu.{process_match.group(1)}.{process_match.group(2)}")
    gpu_match = re.match(r"^system/gpu\.(\d+)\.(.+)$", value)
    if gpu_match:
        aliases.append(f"system/gpu.process.{gpu_match.group(1)}.{gpu_match.group(2)}")
    return list(dict.fromkeys(aliases))


def history_tooltip_clear_attrs() -> str:
    return """onmouseleave='for (const tip of this.querySelectorAll(".chart-tooltip")) { tip.style.opacity = "0"; tip.style.visibility = "hidden"; }' onblur='for (const tip of this.querySelectorAll(".chart-tooltip")) { tip.style.opacity = "0"; tip.style.visibility = "hidden"; }' """


def format_plot_value(value: float) -> str:
    return f"{float(value):.4f}".rstrip("0").rstrip(".")


def weighted_quantile(points: list[tuple[float, float]], quantile: float) -> float | None:
    if not points:
        return None
    total_weight = sum(weight for _, weight in points)
    if total_weight <= 0:
        return None
    threshold = total_weight * quantile
    cumulative = 0.0
    for value, weight in points:
        cumulative += weight
        if cumulative >= threshold:
            return value
    return points[-1][0]


def histogram_payload_from_row(row: dict[str, object]) -> dict[str, object] | None:
    payload = row.get("metric_value_json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = None
    if isinstance(payload, dict) and str(payload.get("_type") or "") == "histogram":
        return payload
    return None


def histogram_cells_from_payload(payload: object) -> list[dict[str, float]]:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return []
    if not isinstance(payload, dict) or str(payload.get("_type") or "") != "histogram":
        return []
    packed_bins = payload.get("packedBins") or {}
    counts = payload.get("values")
    if not isinstance(packed_bins, dict) or not isinstance(counts, list) or not counts:
        return []
    base = to_plot_number(packed_bins.get("min"), False)
    size = to_plot_number(packed_bins.get("size"), False)
    if base is None or size is None or size <= 0:
        return []
    cells: list[dict[str, float]] = []
    for index, raw_count in enumerate(counts):
        count = to_plot_number(raw_count, False)
        if count is None or count <= 0:
            continue
        low = base + index * size
        high = low + size
        cells.append(
            {
                "low": low,
                "high": high,
                "center": low + size / 2,
                "count": count,
            }
        )
    return cells


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    value = str(color or "#0f766e").lstrip("#")
    if len(value) != 6:
        return (15, 118, 110)
    return tuple(int(value[index : index + 2], 16) for index in (0, 2, 4))


def histogram_summary_from_payload(payload: object) -> dict[str, float] | None:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None
    if not isinstance(payload, dict) or str(payload.get("_type") or "") != "histogram":
        return None
    packed_bins = payload.get("packedBins") or {}
    counts = payload.get("values")
    if not isinstance(packed_bins, dict) or not isinstance(counts, list) or not counts:
        return None
    base = to_plot_number(packed_bins.get("min"), False)
    size = to_plot_number(packed_bins.get("size"), False)
    if base is None or size is None or size <= 0:
        return None
    weighted_points: list[tuple[float, float]] = []
    first_nonzero: int | None = None
    last_nonzero: int | None = None
    total = 0.0
    weighted_sum = 0.0
    for index, raw_count in enumerate(counts):
        count = to_plot_number(raw_count, False)
        if count is None or count <= 0:
            continue
        if first_nonzero is None:
            first_nonzero = index
        last_nonzero = index
        center = base + (index + 0.5) * size
        weighted_points.append((center, count))
        total += count
        weighted_sum += center * count
    if not weighted_points or total <= 0:
        return None
    mean = weighted_sum / total
    variance = sum(weight * ((value - mean) ** 2) for value, weight in weighted_points) / total
    summary = {
        "count": total,
        "mean": mean,
        "std": math.sqrt(max(variance, 0.0)),
        "min": base + first_nonzero * size if first_nonzero is not None else mean,
        "max": base + (last_nonzero + 1) * size if last_nonzero is not None else mean,
        "q10": weighted_quantile(weighted_points, 0.10) or mean,
        "q25": weighted_quantile(weighted_points, 0.25) or mean,
        "q50": weighted_quantile(weighted_points, 0.50) or mean,
        "q75": weighted_quantile(weighted_points, 0.75) or mean,
        "q90": weighted_quantile(weighted_points, 0.90) or mean,
    }
    return summary


def histogram_summary_from_row(row: dict[str, object]) -> dict[str, float] | None:
    if str(row.get("metric_value_kind") or "") == "histogram":
        keys = {
            "count": "metric_histogram_count",
            "mean": "metric_histogram_mean",
            "std": "metric_histogram_std",
            "min": "metric_histogram_min",
            "max": "metric_histogram_max",
            "q10": "metric_histogram_q10",
            "q25": "metric_histogram_q25",
            "q50": "metric_histogram_q50",
            "q75": "metric_histogram_q75",
            "q90": "metric_histogram_q90",
        }
        summary = {
            key: to_plot_number(row.get(source_key), False)
            for key, source_key in keys.items()
        }
        if summary["q50"] is not None:
            return {key: float(value) for key, value in summary.items() if value is not None}
        payload = row.get("metric_value_json")
        fallback = histogram_summary_from_payload(payload)
        if fallback:
            return fallback
    payload = row.get("metric_value_json")
    return histogram_summary_from_payload(payload)


def histogram_cross_section_detail_svg(
    cells: list[dict[str, float]],
    *,
    highlight_low: float,
    highlight_high: float,
) -> tuple[str, float, float]:
    ordered = sorted(cells, key=lambda cell: (float(cell["low"]), float(cell["high"])))
    if not ordered:
        return "", 0.0, 0.0
    width = 176.0
    height = 78.0
    left = 6.0
    right = 6.0
    top = 4.0
    bottom = 16.0
    plot_width = width - left - right
    plot_height = height - top - bottom
    min_low = min(float(cell["low"]) for cell in ordered)
    max_high = max(float(cell["high"]) for cell in ordered)
    max_count = max(float(cell["count"]) for cell in ordered)
    if max_high <= min_low or max_count <= 0:
        return "", 0.0, 0.0

    def scale_x(value: float) -> float:
        return left + (value - min_low) / (max_high - min_low) * plot_width

    def scale_y(value: float) -> float:
        return top + plot_height - value / max_count * plot_height

    area_points = [f"M {scale_x(min_low):.2f} {top + plot_height:.2f}"]
    for cell in ordered:
        x0 = scale_x(float(cell["low"]))
        x1 = scale_x(float(cell["high"]))
        y = scale_y(float(cell["count"]))
        area_points.append(f"L {x0:.2f} {y:.2f}")
        area_points.append(f"L {x1:.2f} {y:.2f}")
    area_points.append(f"L {scale_x(max_high):.2f} {top + plot_height:.2f} Z")
    highlight_x = scale_x(highlight_low)
    highlight_width = max(2.0, scale_x(highlight_high) - highlight_x)
    axis_y = top + plot_height
    labels = (
        f"<text x='{left:.2f}' y='{height - 3:.2f}' class='chart-tooltip__text' style='font-size:10px;fill:rgba(255,255,255,0.72)'>{html.escape(format_plot_value(min_low))}</text>"
        + f"<text x='{left + plot_width:.2f}' y='{height - 3:.2f}' text-anchor='end' class='chart-tooltip__text' style='font-size:10px;fill:rgba(255,255,255,0.72)'>{html.escape(format_plot_value(max_high))}</text>"
    )
    svg = (
        f"<rect x='{left:.2f}' y='{top:.2f}' width='{plot_width:.2f}' height='{plot_height:.2f}' fill='rgba(255,255,255,0.02)' stroke='rgba(255,255,255,0.08)' stroke-width='1' rx='8' ry='8'></rect>"
        + f"<rect x='{highlight_x:.2f}' y='{top:.2f}' width='{highlight_width:.2f}' height='{plot_height:.2f}' fill='rgba(255,255,255,0.16)' rx='4' ry='4'></rect>"
        + f"<path d='{' '.join(area_points)}' fill='rgba(96,165,250,0.35)' stroke='rgba(147,197,253,0.95)' stroke-width='1.5' stroke-linejoin='round'></path>"
        + f"<line x1='{left:.2f}' y1='{axis_y:.2f}' x2='{left + plot_width:.2f}' y2='{axis_y:.2f}' stroke='rgba(255,255,255,0.18)' stroke-width='1'></line>"
        + labels
    )
    return svg, width, height


def render_history_histogram_inline(panel: dict[str, object], history_rows: list[dict[str, object]]) -> str | None:
    metric_names = [str(metric) for metric in panel.get("metrics", []) if metric]
    if not metric_names:
        return None
    axis_field = history_axis_field(panel)
    metric_name_set = {alias for metric in metric_names for alias in history_metric_aliases(metric)}
    filtered = [row for row in history_rows if str(row.get("metric_name")) in metric_name_set]
    if not filtered:
        return None

    heatmap_points: list[dict[str, object]] = []
    for row in filtered:
        x = to_plot_number(row.get(axis_field), axis_field == "timestamp_value")
        if x is None:
            continue
        cells = histogram_cells_from_payload(histogram_payload_from_row(row))
        if not cells:
            continue
        heatmap_points.append({"x": x, "cells": cells})

    if len(heatmap_points) < 2:
        return None

    width = 940
    height = 420
    left, right, top, bottom = 72, 18, 16, 48
    strip_height = height - top - bottom
    min_x = min(float(point["x"]) for point in heatmap_points)
    max_x = max(float(point["x"]) for point in heatmap_points)
    all_cells = [cell for point in heatmap_points for cell in point["cells"]]
    min_y = min(float(cell["low"]) for cell in all_cells)
    max_y = max(float(cell["high"]) for cell in all_cells)
    aggregated_cells: dict[tuple[float, float, float], float] = {}
    for point in heatmap_points:
        x_value = round(float(point["x"]), 9)
        for cell in point["cells"]:
            key = (x_value, round(float(cell["low"]), 9), round(float(cell["high"]), 9))
            aggregated_cells[key] = aggregated_cells.get(key, 0.0) + float(cell["count"])
    max_count = max(aggregated_cells.values()) if aggregated_cells else 0.0

    def scale_x(value: float) -> float:
        if max_x == min_x:
            return float(left)
        return left + (value - min_x) / (max_x - min_x) * (width - left - right)

    def scale_y(value: float) -> float:
        if max_y == min_y:
            return top + strip_height
        return top + strip_height - (value - min_y) / (max_y - min_y) * strip_height

    image = Image.new("RGBA", (width, height), (255, 255, 255, 255))
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    axis_color = (96, 96, 96, 255)
    grid_color = (0, 0, 0, 14)
    border_color = (0, 0, 0, 56)
    base_cell_color = (235, 239, 244, 255)
    accent_color = (78, 152, 223)

    if axis_field == "timestamp_value":
        axis_formatter = format_date_value
    elif axis_field == "runtime":
        axis_formatter = format_runtime_value
    else:
        axis_formatter = format_plot_value

    strip_top = top
    strip_bottom = top + strip_height
    draw.line((left, strip_top, left, strip_bottom), fill=border_color, width=1)
    draw.line((left, strip_bottom, width - right, strip_bottom), fill=border_color, width=1)

    for tick_index in range(5):
        t = tick_index / 4
        x = left + t * (width - left - right)
        y = strip_bottom - t * strip_height
        draw.line((x, strip_top, x, strip_bottom), fill=grid_color, width=1)
        draw.line((left, y, width - right, y), fill=grid_color, width=1)
        y_tick = min_y + (max_y - min_y) * t
        draw.text((left - 56, y - 6), format_plot_value(y_tick), fill=axis_color, font=font)
        x_tick = min_x + (max_x - min_x) * t
        draw.text((x - 12, strip_bottom + 10), axis_formatter(x_tick), fill=axis_color, font=font)

    draw.text((width - 46, strip_bottom - 20), "Step", fill=axis_color, font=font)

    unique_x = sorted({round(float(point["x"]), 9) for point in heatmap_points})
    step_width = max(4.0, (width - left - right) / max(len(unique_x), 1))
    for (x_value, low, high), count in aggregated_cells.items():
        x = scale_x(x_value)
        cell_left = x - step_width / 2
        y_top = scale_y(high)
        y_bottom = scale_y(low)
        rect = (
            int(round(cell_left)),
            int(round(y_top)),
            int(round(cell_left + step_width)),
            int(round(max(y_top + 1, y_bottom))),
        )
        draw.rectangle(rect, fill=base_cell_color)
        normalized = 0.0 if max_count <= 0 else math.sqrt(count / max_count)
        fill = (
            int(base_cell_color[0] + (accent_color[0] - base_cell_color[0]) * normalized),
            int(base_cell_color[1] + (accent_color[1] - base_cell_color[1]) * normalized),
            int(base_cell_color[2] + (accent_color[2] - base_cell_color[2]) * normalized),
            255,
        )
        draw.rectangle(rect, fill=fill)

    flattened = Image.new("RGB", image.size, (255, 255, 255))
    flattened.paste(image, mask=image.getchannel("A"))
    png_path = write_png_asset(flattened, "history")
    png_filename = Path(png_path).name
    tooltip_style = (
        ".chart-point{cursor:crosshair}"
        ".chart-point__dot{pointer-events:none}"
        ".chart-tooltip{visibility:hidden;opacity:0;pointer-events:none;transition:opacity 120ms ease}"
        ".chart-tooltip__bubble{fill:rgba(17,24,39,0.94);stroke:rgba(255,255,255,0.16);stroke-width:1}"
        ".chart-tooltip__text{fill:#fff;font-size:12px;font-family:sans-serif}"
    )
    overlay = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' class='marimo-chart-svg' role='img' {history_tooltip_clear_attrs()}>",
        f"<style>{tooltip_style}</style>",
        f"<image href='{html.escape(png_filename)}' x='0' y='0' width='{width}' height='{height}' />",
    ]
    step_cells: dict[float, list[dict[str, float]]] = {}
    for (x_value, low, high), count in aggregated_cells.items():
        step_cells.setdefault(x_value, []).append({"low": low, "high": high, "count": count})
    for (x_value, low, high), count in aggregated_cells.items():
        x = scale_x(x_value)
        y_top = scale_y(high)
        y_bottom = scale_y(low)
        detail_svg, detail_width, detail_height = histogram_cross_section_detail_svg(
            step_cells.get(x_value, []),
            highlight_low=low,
            highlight_high=high,
        )
        overlay.append(
            svg_interactive_rect(
                x=x - step_width / 2,
                y=y_top,
                rect_width=step_width,
                rect_height=max(1.0, y_bottom - y_top),
                fill="#ffffff",
                opacity=0.001,
                lines=[
                    axis_formatter(x_value),
                    f"bin: {format_plot_value(low)} - {format_plot_value(high)}",
                    f"count: {format_plot_value(count)}",
                ],
                width=width,
                height=height,
                detail_svg=detail_svg,
                detail_width=detail_width,
                detail_height=detail_height,
            )
        )
    overlay.append("</svg>")
    return "".join(overlay)


def render_history_svg(panel: dict[str, object], history_rows: list[dict[str, object]]) -> str | None:
    metric_names = [str(metric) for metric in panel.get("metrics", []) if metric]
    if not metric_names:
        return None
    axis_field = history_axis_field(panel)
    metric_name_set = {alias for metric in metric_names for alias in history_metric_aliases(metric)}
    filtered = [row for row in history_rows if str(row.get("metric_name")) in metric_name_set]
    if not filtered:
        return None

    scalar_series_map: dict[str, list[dict[str, float]]] = {}
    histogram_series_map: dict[str, list[dict[str, object]]] = {}
    for row in filtered:
        x = to_plot_number(row.get(axis_field), axis_field == "timestamp_value")
        if x is None:
            continue
        label = (
            f"{row.get('run_name') or row.get('run_id')} · {row.get('metric_name')}"
            if len(metric_names) > 1
            else str(row.get("run_name") or row.get("run_id") or row.get("metric_name"))
        )
        histogram_payload = histogram_payload_from_row(row)
        histogram_cells = histogram_cells_from_payload(histogram_payload)
        if histogram_cells:
            histogram_series_map.setdefault(label, []).append({"x": x, "cells": histogram_cells})
            continue
        y = to_plot_number(row.get("metric_value"), False)
        if y is None:
            continue
        scalar_series_map.setdefault(label, []).append({"x": x, "y": y})

    width, height = 940, 420
    left, right, top, bottom = 74, 24, 20, 58
    palette = ["#0f766e", "#2563eb", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#65a30d", "#db2777"]

    if axis_field == "timestamp_value":
        axis_formatter = format_date_value
    elif axis_field == "runtime":
        axis_formatter = format_runtime_value
    else:
        axis_formatter = format_plot_value

    axis_text_style = "fill:#606060;font-size:12px;font-family:sans-serif"
    tooltip_style = (
        ".chart-point{cursor:crosshair}"
        ".chart-point__dot{pointer-events:none}"
        ".chart-tooltip{visibility:hidden;opacity:0;pointer-events:none;transition:opacity 120ms ease}"
        ".chart-tooltip__bubble{fill:rgba(17,24,39,0.94);stroke:rgba(255,255,255,0.16);stroke-width:1}"
        ".chart-tooltip__text{fill:#fff;font-size:12px;font-family:sans-serif}"
    )

    scalar_series = [
        {"label": label, "points": sorted(points, key=lambda point: point["x"])}
        for label, points in scalar_series_map.items()
        if len(points) > 1
    ]
    histogram_series = [
        {"label": label, "points": sorted(points, key=lambda point: float(point["x"]))}
        for label, points in histogram_series_map.items()
        if len(points) > 1
    ]
    if not scalar_series and not histogram_series:
        return None

    if histogram_series and not scalar_series:
        all_hist_points = [point for entry in histogram_series for point in entry["points"]]
        all_cells = [cell for entry in histogram_series for point in entry["points"] for cell in point["cells"]]
        if not all_cells:
            return None
        run_gap = 30
        strip_height = 180
        title_pad = 28
        width = 940
        height = max(220, len(histogram_series) * (strip_height + run_gap) + 56)
        left, right, top, bottom = 88, 24, 18, 42
        min_x = min(float(point["x"]) for point in all_hist_points)
        max_x = max(float(point["x"]) for point in all_hist_points)
        min_y = min(float(cell["low"]) for cell in all_cells)
        max_y = max(float(cell["high"]) for cell in all_cells)
        max_count = max(float(cell["count"]) for cell in all_cells)

        def scale_x(value: float) -> float:
            if max_x == min_x:
                return float(left)
            return left + (value - min_x) / (max_x - min_x) * (width - left - right)

        def scale_y(value: float, strip_top: float) -> float:
            if max_y == min_y:
                return strip_top + strip_height
            return strip_top + strip_height - (value - min_y) / (max_y - min_y) * strip_height

        def cell_color(series_index: int, count: float) -> tuple[str, float]:
            base = palette[series_index % len(palette)]
            normalized = 0.15 if max_count <= 0 else 0.15 + 0.85 * math.sqrt(count / max_count)
            return base, normalized

        svg = [
            f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' role='img' {history_tooltip_clear_attrs()}>",
            f"<style>{tooltip_style}</style>",
        ]

        for series_index, entry in enumerate(histogram_series):
            strip_top = top + series_index * (strip_height + run_gap) + title_pad
            strip_bottom = strip_top + strip_height
            svg.append(f"<text x='{left}' y='{strip_top - 10}' text-anchor='start' style='{axis_text_style};font-weight:600'>{html.escape(str(entry['label']))}</text>")
            svg.append(f"<line x1='{left}' y1='{strip_bottom}' x2='{width-right}' y2='{strip_bottom}' stroke='rgba(0,0,0,0.22)' stroke-width='1.2' />")
            svg.append(f"<line x1='{left}' y1='{strip_top}' x2='{left}' y2='{strip_bottom}' stroke='rgba(0,0,0,0.22)' stroke-width='1.2' />")

            for tick_index in range(5):
                t = tick_index / 4
                x = left + t * (width - left - right)
                y = strip_bottom - t * strip_height
                svg.append(f"<line x1='{x}' y1='{strip_top}' x2='{x}' y2='{strip_bottom}' stroke='rgba(0,0,0,0.05)' />")
                svg.append(f"<line x1='{left}' y1='{y}' x2='{width-right}' y2='{y}' stroke='rgba(0,0,0,0.05)' />")
                if series_index == len(histogram_series) - 1:
                    x_tick = min_x + (max_x - min_x) * t
                    svg.append(f"<text x='{x}' y='{strip_bottom + 22}' text-anchor='middle' style='{axis_text_style}'>{html.escape(axis_formatter(x_tick))}</text>")
                y_tick = min_y + (max_y - min_y) * t
                svg.append(f"<text x='{left - 10}' y='{y + 4}' text-anchor='end' style='{axis_text_style}'>{html.escape(format_plot_value(y_tick))}</text>")

            points = entry["points"]
            if len(points) > 1:
                step_width = max(4.0, (width - left - right) / len(points))
            else:
                step_width = width - left - right
            for point in points:
                x = scale_x(float(point["x"]))
                cell_left = x - step_width / 2
                for cell in point["cells"]:
                    y_top = scale_y(float(cell["high"]), strip_top)
                    y_bottom = scale_y(float(cell["low"]), strip_top)
                    rect_height = max(1.0, y_bottom - y_top)
                    color, opacity = cell_color(series_index, float(cell["count"]))
                    svg.append(
                        svg_interactive_rect(
                            x=cell_left,
                            y=y_top,
                            rect_width=step_width,
                            rect_height=rect_height,
                            fill=color,
                            opacity=opacity,
                            lines=[
                                str(entry["label"]),
                                f"{axis_field}: {axis_formatter(float(point['x']))}",
                                f"bin: {format_plot_value(float(cell['low']))} - {format_plot_value(float(cell['high']))}",
                                f"count: {format_plot_value(float(cell['count']))}",
                            ],
                            width=width,
                            height=height,
                        )
                    )
        svg.append("</svg>")
        return "".join(svg)

    all_points = [point for entry in scalar_series for point in entry["points"]]
    min_x = min(point["x"] for point in all_points)
    max_x = max(point["x"] for point in all_points)
    min_y = min(point["y"] for point in all_points)
    max_y = max(point["y"] for point in all_points)

    def scale_x(value: float) -> float:
        if max_x == min_x:
            return float(left)
        return left + (value - min_x) / (max_x - min_x) * (width - left - right)

    def scale_y(value: float) -> float:
        if max_y == min_y:
            return float(height - bottom)
        return height - bottom - (value - min_y) / (max_y - min_y) * (height - top - bottom)

    svg = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' role='img' {history_tooltip_clear_attrs()}>",
        f"<style>{tooltip_style}</style>",
        f"<line x1='{left}' y1='{height-bottom}' x2='{width-right}' y2='{height-bottom}' stroke='rgba(0,0,0,0.22)' stroke-width='1.5' />",
        f"<line x1='{left}' y1='{top}' x2='{left}' y2='{height-bottom}' stroke='rgba(0,0,0,0.22)' stroke-width='1.5' />",
    ]
    for index in range(5):
        x = left + index / 4 * (width - left - right)
        y = height - bottom - index / 4 * (height - top - bottom)
        svg.append(f"<line x1='{x}' y1='{top}' x2='{x}' y2='{height-bottom}' stroke='rgba(0,0,0,0.06)' />")
        svg.append(f"<line x1='{left}' y1='{y}' x2='{width-right}' y2='{y}' stroke='rgba(0,0,0,0.06)' />")
        x_tick = min_x + (max_x - min_x) * (index / 4)
        y_tick = min_y + (max_y - min_y) * (index / 4)
        svg.append(f"<text x='{x}' y='{height-bottom+24}' text-anchor='middle' style='{axis_text_style}'>{html.escape(axis_formatter(x_tick))}</text>")
        svg.append(f"<text x='{left-12}' y='{y+4}' text-anchor='end' style='{axis_text_style}'>{html.escape(f'{y_tick:.2f}'.rstrip('0').rstrip('.'))}</text>")
    for index, entry in enumerate(scalar_series):
        color = palette[index % len(palette)]
        path = " ".join(
            f"{'M' if point_index == 0 else 'L'} {scale_x(point['x'])} {scale_y(point['y'])}"
            for point_index, point in enumerate(entry["points"])
        )
        svg.append(f"<path d='{path}' fill='none' stroke='{color}' stroke-width='2.2' stroke-linejoin='round' stroke-linecap='round'></path>")
        step = max(1, math.ceil(len(entry["points"]) / 160))
        for point_index, point in enumerate(entry["points"]):
            if point_index % step != 0 and point_index != len(entry["points"]) - 1:
                continue
            svg.append(
                svg_interactive_point(
                    x=scale_x(point["x"]),
                    y=scale_y(point["y"]),
                    radius=3.2,
                    color=color,
                    width=width,
                    height=height,
                    stroke_width=1,
                        lines=[
                            str(entry["label"]),
                            f"{axis_field}: {axis_formatter(point['x'])}",
                            f"value: {format_plot_value(point['y'])}",
                        ],
                    )
                )
    svg.append("</svg>")
    return "".join(svg)


def attach_pre_rendered_history(node: object, history_rows: list[dict[str, object]], report: dict[str, object]) -> object:
    if isinstance(node, dict):
        if node.get("type") == "panel-grid":
            runset_names = [str(value) for value in node.get("runsets", []) or [] if value]
            visible_values = infer_block_visible_values(report, node)
            use_selection_fallback = should_use_selection_fallback(report, runset_names)
            hydrated_panels = []
            for panel in node.get("panels", []) or []:
                if not isinstance(panel, dict):
                    hydrated_panels.append(panel)
                    continue
                hydrated = dict(panel)
                if hydrated.get("view_type") == "Run History Line Plot":
                    filtered_history_rows = filter_rows_by_report_runsets(
                        history_rows,
                        report,
                        runset_names,
                        visible_values=visible_values,
                        use_selection_fallback=use_selection_fallback,
                    )
                    history_inline_svg = render_history_histogram_inline(hydrated, filtered_history_rows)
                    if history_inline_svg:
                        hydrated["history_asset_path"] = write_svg_asset(history_inline_svg, "history")
                        hydrated["history_asset_type"] = "image/svg+xml"
                    else:
                        svg = render_history_svg(hydrated, filtered_history_rows)
                        if svg:
                            hydrated["history_asset_path"] = write_svg_asset(svg, "history")
                            hydrated["history_asset_type"] = "image/svg+xml"
                hydrated_panels.append(hydrated)
            node = {**node, "panels": hydrated_panels}
        return {key: attach_pre_rendered_history(value, history_rows, report) for key, value in node.items()}
    if isinstance(node, list):
        return [attach_pre_rendered_history(item, history_rows, report) for item in node]
    return node


def load_payload() -> dict[str, object]:
    manifest = load_json(PROCESSED_DIR / "report_manifest.json")
    report = manifest.get("report") or load_json(PROCESSED_DIR / "report_content.json")
    report = hydrate_media_items(report)
    panel_tables_meta = report.get("panel_tables", {}) if isinstance(report, dict) else {}
    table_prediction_rows: list[dict[str, object]] = []
    table_predictions_path = PROCESSED_DIR / "table_predictions.parquet"
    if table_predictions_path.exists():
        table_predictions = pd.read_parquet(table_predictions_path).to_dict(orient="records")
        for row in table_predictions:
            try:
                payload = json.loads(str(row.get("meta_json") or "{}"))
            except Exception:
                continue
            table_prediction_rows.append(
                {
                    "__run_id": row.get("run_id"),
                    "__run_name": row.get("run_name"),
                    "__wandb_url": row.get("wandb_run_url"),
                    **payload,
                }
            )
    panel_tables: dict[str, object] = {}
    for table_key, meta in panel_tables_meta.items():
        if not isinstance(meta, dict):
            continue
        relative_path = meta.get("path")
        if not isinstance(relative_path, str):
            continue
        source_path = PROCESSED_DIR / relative_path
        if source_path.exists():
            rows = load_json(source_path)
            if (
                isinstance(rows, list)
                and rows
                and isinstance(rows[0], dict)
                and row_has_media_placeholders(rows[0])
                and table_prediction_rows
            ):
                panel_tables[table_key] = table_prediction_rows
            else:
                panel_tables[table_key] = rows
    history_rows = []
    history_path = PROCESSED_DIR / "history_eval_metrics.parquet"
    if history_path.exists():
        history_rows = pd.read_parquet(history_path).to_dict(orient="records")
    if isinstance(report, dict):
        report = attach_pre_rendered_vega(report, panel_tables, report)
        if history_rows:
            report = attach_pre_rendered_history(report, history_rows, report)
    return {
        "title": report.get("title", manifest.get("title", "W&B Report")) if isinstance(report, dict) else "W&B Report",
        "report_url": report.get("report_url", manifest.get("report_url")) if isinstance(report, dict) else manifest.get("report_url"),
        "report": report,
        "panel_tables": panel_tables,
    }


def encode_payload(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    compressed = zlib.compress(raw, level=9)
    return base64.b64encode(compressed).decode("ascii")


def notebook_source(encoded_payload: str) -> str:
    return textwrap.dedent(
        f"""
        import marimo

        __generated_with = "codex"
        app = marimo.App(width="full")

        @app.cell
        def __():
            PAYLOAD_B64 = "{encoded_payload}"
            return (PAYLOAD_B64,)


        @app.cell
        def __(PAYLOAD_B64):
            import base64
            import hashlib
            import html
            import json
            import math
            import zlib
            from datetime import datetime, timezone
            import marimo as mo

            payload = json.loads(zlib.decompress(base64.b64decode(PAYLOAD_B64)).decode("utf-8"))
            report = payload["report"]
            panel_tables = payload["panel_tables"]

            def render_html(body: str):
                html_ctor = getattr(mo, "Html", None) or getattr(mo, "html", None)
                if html_ctor is not None:
                    return html_ctor(body)
                return mo.md(body)

            def vstack(items):
                children = [item for item in items if item is not None]
                if hasattr(mo, "vstack"):
                    return mo.vstack(children)
                return children

            def hstack(items, widths=None):
                children = [item for item in items if item is not None]
                if hasattr(mo, "hstack"):
                    kwargs = {{}}
                    if widths is not None:
                        kwargs["widths"] = widths
                    return mo.hstack(children, gap=1.0, align="stretch", **kwargs)
                return vstack(children)

            def sort_panels(panels):
                return sorted(
                    list(panels or []),
                    key=lambda panel: (
                        float((panel or {{}}).get("layout", {{}}).get("y", 0) or 0),
                        float((panel or {{}}).get("layout", {{}}).get("x", 0) or 0),
                    ),
                )

            def panel_needs_own_row(panel):
                return panel.get("view_type") == "Media Browser" or panel.get("mode") == "table"

            def block_panel_table_keys(block):
                if block.get("type") != "panel-grid":
                    return []
                return list(dict.fromkeys(panel.get("table_key") for panel in (block.get("panels") or []) if panel.get("table_key")))

            def block_has_panel_mode(block, mode):
                return block.get("type") == "panel-grid" and any(panel.get("mode") == mode for panel in (block.get("panels") or []))

            def is_reorderable_table_block(block):
                if block.get("type") != "panel-grid":
                    return False
                data_panels = [panel for panel in (block.get("panels") or []) if panel.get("view_type") and panel.get("view_type") != "Markdown Panel"]
                if not data_panels:
                    return False
                return all(panel.get("mode") == "table" for panel in data_panels)

            def is_reorderable_plot_block(block):
                if block.get("type") != "panel-grid":
                    return False
                data_panels = [panel for panel in (block.get("panels") or []) if panel.get("view_type") and panel.get("view_type") != "Markdown Panel"]
                if not data_panels:
                    return False
                return all(panel.get("mode") == "plot" for panel in data_panels)

            def reorder_report_blocks(blocks):
                reordered = list(blocks or [])
                index = 0
                while index < len(reordered):
                    block = reordered[index]
                    if not is_reorderable_table_block(block):
                        index += 1
                        continue
                    table_keys = block_panel_table_keys(block)
                    moved = False
                    for earlier in range(index - 1, -1, -1):
                        candidate = reordered[earlier]
                        if not is_reorderable_plot_block(candidate):
                            continue
                        if not any(key in table_keys for key in block_panel_table_keys(candidate)):
                            continue
                        item = reordered.pop(index)
                        reordered.insert(earlier, item)
                        index = earlier
                        moved = True
                        break
                    if not moved:
                        index += 1
                return reordered

            def summarize_metric_title(metrics):
                values = [str(metric) for metric in (metrics or []) if metric]
                if not values:
                    return ""
                if len(values) == 1:
                    return values[0]
                import re
                match = re.match(r"^(.*?)(\\d+)(\\..*)$", values[0])
                if match:
                    prefix, _index, suffix = match.groups()
                    if all(value.startswith(prefix) and value.endswith(suffix) for value in values):
                        return f"{{prefix}}*{{suffix}} ({{len(values)}} series)"
                return f"{{values[0]}} + {{len(values) - 1}} more"

            def humanize_label(value):
                return " ".join(str(value or "").replace("_", " ").replace("-", " ").split())

            def build_panel_rows(panels):
                rows = []
                current_y = None
                current_row = []

                def flush_current():
                    nonlocal current_row
                    if not current_row:
                        return
                    carry = []
                    for panel in current_row:
                        if panel_needs_own_row(panel):
                            if carry:
                                rows.append(carry)
                                carry = []
                            rows.append([panel])
                        else:
                            carry.append(panel)
                    if carry:
                        rows.append(carry)
                    current_row = []

                for panel in sort_panels(panels):
                    panel_y = float((panel.get("layout") or {{}}).get("y", 0) or 0)
                    if current_y is None or panel_y == current_y:
                        current_row.append(panel)
                        current_y = panel_y
                        continue
                    flush_current()
                    current_row = [panel]
                    current_y = panel_y
                flush_current()
                return rows

            def row_items_with_spacers(row_panels, block):
                items = []
                widths = []
                current = 0.0
                for panel in row_panels:
                    layout = panel.get("layout") or {{}}
                    x = max(float(layout.get("x", 0) or 0), 0.0)
                    w = max(float(layout.get("w", 1) or 1), 1.0)
                    if x > current:
                        items.append(render_html("<div class='marimo-spacer'></div>"))
                        widths.append(x - current)
                    items.append(render_panel(panel, block))
                    widths.append(w)
                    current = x + w
                if current < 24:
                    items.append(render_html("<div class='marimo-spacer'></div>"))
                    widths.append(24 - current)
                return items, widths

            def dedupe_panels(panels):
                unique = []
                seen = set()
                for panel in panels:
                    signature = json.dumps({{k: v for k, v in panel.items() if k != "layout"}}, sort_keys=True, ensure_ascii=False)
                    if signature in seen:
                        continue
                    seen.add(signature)
                    unique.append(panel)
                return unique

            def should_stack_panel_group(panels):
                if len(panels or []) != 2:
                    return False
                table_keys = list(dict.fromkeys(panel.get("table_key") for panel in (panels or []) if panel.get("table_key")))
                if len(table_keys) != 1:
                    return False
                modes = {{panel.get("mode") for panel in (panels or []) if panel.get("mode")}}
                return "table" in modes and "plot" in modes

            def expression_label(expr):
                if not isinstance(expr, dict):
                    return None
                if expr.get("kind") != "op":
                    return None
                op_name = expr.get("name")
                inputs = expr.get("inputs", {{}})
                if op_name == "pick":
                    key = inputs.get("key", {{}})
                    if isinstance(key, dict) and key.get("kind") == "const":
                        value = key.get("value")
                        return str(value) if value is not None else None
                if op_name == "run-name":
                    return "__run_name"
                if op_name == "run-id":
                    return "__run_id"
                return str(op_name) if op_name else None

            def evaluate_expression(expr, row):
                if not isinstance(expr, dict):
                    return None
                kind = expr.get("kind")
                if kind == "const":
                    return expr.get("value")
                if kind != "op":
                    return None
                name = expr.get("name")
                inputs = expr.get("inputs", {{}})
                if name == "pick":
                    key = evaluate_expression(inputs.get("key"), row)
                    if key is None:
                        return None
                    return row.get(str(key))
                if name == "run-name":
                    return row.get("__run_name")
                if name == "run-id":
                    return row.get("__run_id")
                lhs = evaluate_expression(inputs.get("lhs"), row)
                rhs = evaluate_expression(inputs.get("rhs"), row)
                try:
                    if name == "number-add":
                        return float(lhs) + float(rhs)
                    if name == "number-sub":
                        return float(lhs) - float(rhs)
                    if name == "number-mult":
                        return float(lhs) * float(rhs)
                    if name == "number-div":
                        return float(lhs) / float(rhs) if float(rhs) != 0 else None
                except (TypeError, ValueError, ZeroDivisionError):
                    return None
                return None

            def sort_value(value):
                if value is None:
                    return (2, "")
                if isinstance(value, (int, float)):
                    return (0, float(value))
                if is_timestamp_like(value, ""):
                    return (0, to_plot_number(value, True))
                return (1, str(value))

            def apply_simple_filter(rows, filter_spec):
                if not filter_spec:
                    return rows
                column = filter_spec.get("column")
                op = filter_spec.get("op")
                value = filter_spec.get("value")
                if not column or op is None:
                    return rows

                def keep(row):
                    raw_value = row.get(column)
                    if raw_value in (None, ""):
                        return False
                    try:
                        cell = float(raw_value)
                    except (TypeError, ValueError):
                        return False
                    if op == "number-lessEqual":
                        return cell <= value
                    if op == "number-greaterEqual":
                        return cell >= value
                    if op == "number-lessThan":
                        return cell < value
                    if op == "number-greaterThan":
                        return cell > value
                    if op == "number-equal":
                        return cell == value
                    return True

                return [row for row in rows if keep(row)]

            def materialize_table_rows(rows, panel):
                table_columns = panel.get("table_columns") or []
                if not table_columns:
                    visible = [key for key in rows[0].keys() if not str(key).startswith("__")] if rows else []
                    return [{{key: row.get(key) for key in visible}} for row in rows]

                projected_rows = []
                for row in rows:
                    projected_row = {{}}
                    for column in table_columns:
                        label = str(column.get("label") or expression_label(column.get("expression")) or column.get("id") or "")
                        value = evaluate_expression(column.get("expression"), row)
                        projected_row[label] = value
                    projected_rows.append(projected_row)

                column_id_to_label = {{
                    str(column.get("id")): str(column.get("label") or expression_label(column.get("expression")) or column.get("id") or "")
                    for column in table_columns
                }}
                for sort_spec in reversed(panel.get("table_sort") or []):
                    label = column_id_to_label.get(str(sort_spec.get("column_id")))
                    if not label:
                        continue
                    projected_rows.sort(
                        key=lambda item: sort_value(item.get(label)),
                        reverse=str(sort_spec.get("direction")) == "desc",
                    )
                return projected_rows

            def is_timestamp_like(value, key):
                key_text = str(key or "").lower()
                if "date" in key_text or "time" in key_text:
                    return True
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    return False
                return abs(number) > 10_000_000_000

            def to_plot_number(value, treat_as_timestamp):
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    return None
                if not treat_as_timestamp:
                    return number
                abs_number = abs(number)
                if abs_number > 1e17:
                    return number / 1e9
                if abs_number > 1e14:
                    return number / 1e6
                if abs_number > 1e11:
                    return number / 1e3
                return number

            def format_date_value(value):
                number = to_plot_number(value, True)
                if number is None:
                    return str(value)
                try:
                    dt = datetime.fromtimestamp(number, tz=timezone.utc)
                except (OverflowError, OSError, ValueError):
                    return str(value)
                return dt.strftime("%Y-%m-%d")

            def format_runtime_value(value):
                try:
                    seconds = float(value)
                except (TypeError, ValueError):
                    return str(value)
                if seconds >= 3600:
                    return f"{{seconds / 3600:.1f}}h"
                if seconds >= 60:
                    return f"{{seconds / 60:.1f}}m"
                return f"{{seconds:.0f}}s"

            def clamp(value, lower, upper):
                return min(max(value, lower), upper)

            def panel_chart_identity(panel, extra=None):
                payload = {{
                    "view_type": panel.get("view_type"),
                    "table_key": panel.get("table_key"),
                    "panel_table_key": panel.get("panel_table_key"),
                    "title": panel.get("title"),
                    "layout": panel.get("layout"),
                }}
                if extra is not None:
                    payload["extra"] = extra
                return zlib.adler32(
                    json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
                ) & 0xFFFFFFFF

            def flatten_selection_tree(node):
                if isinstance(node, list):
                    values = []
                    for item in node:
                        values.extend(flatten_selection_tree(item))
                    return values
                if isinstance(node, str):
                    return [node]
                if not isinstance(node, dict):
                    return []
                values = []
                for key in ("children", "items", "tree", "value", "values"):
                    if key in node:
                        values.extend(flatten_selection_tree(node.get(key)))
                return values

            def runset_matches_identifier(runset, identifier):
                token = str(identifier or "")
                if not token:
                    return False
                return token in {{str(runset.get("id") or ""), str(runset.get("name") or "")}}

            def runset_selection_values(runset_names):
                values = set()
                if not runset_names:
                    return values
                for runset in report.get("runsets", []):
                    if not any(runset_matches_identifier(runset, identifier) for identifier in runset_names):
                        continue
                    for value in flatten_selection_tree((runset.get("selections") or {{}}).get("tree")):
                        if isinstance(value, str) and value.strip():
                            values.add(value.strip())
                selected_runset = report.get("selected_runset") or {{}}
                if not values and any(
                    token in {{str(selected_runset.get("id") or ""), str(selected_runset.get("name") or "")}}
                    for token in runset_names
                ):
                    for value in selected_runset.get("selection_run_ids", []) or []:
                        if value:
                            values.add(str(value))
                    for value in selected_runset.get("selection_names", []) or []:
                        if value:
                            values.add(str(value))
                return values

            def runset_selection_root(runset):
                root = ((runset.get("selections") or {{}}).get("root"))
                if root in {{0, 1}}:
                    return int(root)
                return None

            def runset_selection_mode(runset):
                explicit_values = flatten_selection_tree((runset.get("selections") or {{}}).get("tree"))
                if runset.get("only_show_selected") or runset.get("single_run_only"):
                    return "include"
                if not explicit_values:
                    return None
                root = runset_selection_root(runset)
                if root == 0:
                    return "include"
                if root == 1:
                    return "exclude"
                return None

            def infer_block_visible_values(block):
                values = set()
                if not isinstance(block, dict) or block.get("type") != "panel-grid":
                    return values
                for value in block.get("visible_run_ids", []) or []:
                    if value:
                        values.add(str(value))
                for value in block.get("visible_run_names", []) or []:
                    if value:
                        values.add(str(value))
                if values:
                    return values

                runset_names = [str(value) for value in block.get("runsets", []) or [] if value]
                for runset in report.get("runsets", []):
                    if not any(runset_matches_identifier(runset, identifier) for identifier in runset_names):
                        continue
                    for value in runset.get("visible_run_ids", []) or []:
                        if value:
                            values.add(str(value))
                    for value in runset.get("visible_run_names", []) or []:
                        if value:
                            values.add(str(value))
                if values:
                    return values

                candidate_run_ids = set()
                candidate_run_names = set()
                for panel in block.get("panels", []) or []:
                    table_key = panel.get("table_key") if isinstance(panel, dict) else None
                    if table_key:
                        table_meta = (report.get("panel_tables") or {{}}).get(table_key) or {{}}
                        for value in table_meta.get("run_ids", []) or []:
                            if value:
                                candidate_run_ids.add(str(value))
                        for value in table_meta.get("run_names", []) or []:
                            if value:
                                candidate_run_names.add(str(value))
                    for item in (panel.get("media_items") or []) if isinstance(panel, dict) else []:
                        if not isinstance(item, dict):
                            continue
                        if item.get("run_id"):
                            candidate_run_ids.add(str(item.get("run_id")))
                        if item.get("run_name"):
                            candidate_run_names.add(str(item.get("run_name")))

                for runset in report.get("runsets", []):
                    if not any(runset_matches_identifier(runset, identifier) for identifier in runset_names):
                        continue
                    selection_values = runset_selection_values([str(runset.get("id") or runset.get("name") or "")])
                    selection_mode = runset_selection_mode(runset)
                    if not selection_values or not selection_mode:
                        continue
                    if selection_mode == "include":
                        values.update(value for value in candidate_run_ids if value in selection_values)
                        values.update(value for value in candidate_run_names if value in selection_values)
                        continue
                    selected_candidate_ids = [value for value in candidate_run_ids if value in selection_values]
                    selected_candidate_names = [value for value in candidate_run_names if value in selection_values]
                    if not selected_candidate_ids and not selected_candidate_names:
                        continue
                    if candidate_run_ids:
                        values.update(value for value in candidate_run_ids if value not in selection_values)
                        continue
                    values.update(value for value in candidate_run_names if value not in selection_values)
                return values

            def should_use_selection_fallback(runset_names):
                return any(
                    bool(runset_selection_mode(runset))
                    for runset in report.get("runsets", [])
                    if any(runset_matches_identifier(runset, identifier) for identifier in runset_names)
                )

            def filter_rows_by_runset_selection(rows, runset_names, visible_values=None, use_selection_fallback=False):
                if not rows or not runset_names:
                    return rows
                visible = visible_values or set()
                if visible:
                    filtered = []
                    for row in rows:
                        run_id = row.get("__run_id", row.get("run_id")) if isinstance(row, dict) else None
                        run_name = row.get("__run_name", row.get("run_name")) if isinstance(row, dict) else None
                        if str(run_id or "") in visible or str(run_name or "") in visible:
                            filtered.append(row)
                    if filtered:
                        return filtered
                    return rows
                if not use_selection_fallback:
                    return rows
                selected = runset_selection_values(runset_names)
                if not selected:
                    return rows
                include_mode = any(
                    runset_selection_mode(runset) == "include"
                    for runset in report.get("runsets", [])
                    if any(runset_matches_identifier(runset, identifier) for identifier in runset_names)
                )
                filtered = []
                for row in rows:
                    run_id = row.get("__run_id", row.get("run_id")) if isinstance(row, dict) else None
                    run_name = row.get("__run_name", row.get("run_name")) if isinstance(row, dict) else None
                    if str(run_id or "") in selected or str(run_name or "") in selected:
                        filtered.append(row)
                if include_mode:
                    return filtered or rows
                remaining = []
                for row in rows:
                    run_id = row.get("__run_id", row.get("run_id")) if isinstance(row, dict) else None
                    run_name = row.get("__run_name", row.get("run_name")) if isinstance(row, dict) else None
                    if str(run_id or "") not in selected and str(run_name or "") not in selected:
                        remaining.append(row)
                return remaining or rows

            def tooltip_box_geometry(x, y, lines, width, height):
                char_width = 7.2
                line_height = 16
                padding_x = 10
                padding_y = 9
                box_width = max(92.0, max((len(str(line)) for line in lines), default=0) * char_width + padding_x * 2)
                box_height = max(36.0, len(lines) * line_height + padding_y * 2 - 2)
                prefer_left = x > width * 0.6
                base_left = x - box_width - 16 if prefer_left else x + 16
                base_top = y + 16 if y - box_height - 14 < 8 else y - box_height - 14
                left = clamp(base_left, 8.0, max(8.0, width - box_width - 8.0))
                top = clamp(base_top, 8.0, max(8.0, height - box_height - 8.0))
                return left, top, box_width, box_height

            def tooltip_root_attrs():
                return '''onmouseleave='for (const tip of this.querySelectorAll(".chart-tooltip")) {{ tip.style.opacity = "0"; tip.style.visibility = "hidden"; }}' onblur='for (const tip of this.querySelectorAll(".chart-tooltip")) {{ tip.style.opacity = "0"; tip.style.visibility = "hidden"; }}' '''

            def interactive_svg_point(x, y, radius, color, lines, width, height, stroke="white", stroke_width=1.5, opacity=0.9):
                left, top, box_width, box_height = tooltip_box_geometry(x, y, lines, width, height)
                tspans = "".join(
                    f"<tspan x='{{left + 10:.2f}}' dy='{{0 if index == 0 else 16}}'>{{html.escape(str(line))}}</tspan>"
                    for index, line in enumerate(lines)
                )
                hide_all = 'const root=this.ownerSVGElement||this.closest("svg");if(root){{for(const other of root.querySelectorAll(".chart-tooltip")){{other.style.opacity="0";other.style.visibility="hidden";}}}}'
                return (
                    "<g class='chart-point' "
                    + f'''onfocusin='this.parentNode.appendChild(this);{{hide_all}};const tip=this.querySelector(".chart-tooltip");if(tip){{{{tip.style.visibility="visible";tip.style.opacity="1";}}}}' '''
                    + '''onfocusout='const tip=this.querySelector(".chart-tooltip");if(tip){{tip.style.opacity="0";tip.style.visibility="hidden";}}' '''
                    + "tabindex='0'>"
                    + f'''<circle cx='{{x:.2f}}' cy='{{y:.2f}}' r='{{max(radius + 7, 11):.2f}}' class='chart-point__target' fill='rgba(255,255,255,0.001)' pointer-events='all' onmouseover='this.parentNode.parentNode.appendChild(this.parentNode);{{hide_all}};const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{{{tip.style.visibility="visible";tip.style.opacity="1";}}}}' onmousemove='{{hide_all}};const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{{{tip.style.visibility="visible";tip.style.opacity="1";}}}}' onmouseout='const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{{{tip.style.opacity="0";tip.style.visibility="hidden";}}}}' onmouseleave='const tip=this.parentNode.querySelector(".chart-tooltip");if(tip){{{{tip.style.opacity="0";tip.style.visibility="hidden";}}}}'></circle>'''
                    + f"<circle cx='{{x:.2f}}' cy='{{y:.2f}}' r='{{radius:.2f}}' fill='{{html.escape(str(color))}}' fill-opacity='{{opacity}}' stroke='{{html.escape(str(stroke))}}' stroke-width='{{stroke_width}}' class='chart-point__dot'></circle>"
                    + "<g class='chart-tooltip'>"
                    + f"<rect x='{{left:.2f}}' y='{{top:.2f}}' width='{{box_width:.2f}}' height='{{box_height:.2f}}' rx='12' ry='12' class='chart-tooltip__bubble'></rect>"
                    + f"<text x='{{left + 10:.2f}}' y='{{top + 20:.2f}}' class='chart-tooltip__text'>{{tspans}}</text>"
                    + "</g></g>"
                )

            def is_image_cell(value):
                return isinstance(value, dict) and value.get("_kind") == "image" and value.get("path")

            def contains_image_cells(rows):
                return any(is_image_cell(value) for row in rows for value in row.values())

            def render_image_cell(value, cell_key=""):
                if not is_image_cell(value):
                    return ""
                mask_names = ", ".join(str(name).replace("_", " ") for name in (value.get("masks") or {{}}).keys())
                caption = html.escape(mask_names or "image")
                src = html.escape(str(value.get("path") or ""))
                digest = hashlib.sha1(f"{{cell_key}}::{{src}}".encode("utf-8")).hexdigest()[:12]
                lightbox_id = f"lightbox-{{digest}}"
                return (
                    "<div class='marimo-image-thumb'>"
                    + f"<input class='marimo-lightbox__toggle' type='checkbox' id='{{lightbox_id}}' />"
                    + f"<label class='marimo-image-thumb__link' for='{{lightbox_id}}' aria-label='Open image preview'>"
                    + f"<img src='{{src}}' alt='image' />"
                    + "<span class='marimo-image-thumb__zoom' aria-hidden='true'>Zoom</span>"
                    + "</label>"
                    + "<div class='marimo-lightbox' role='dialog' aria-modal='true'>"
                    + f"<label class='marimo-lightbox__backdrop' for='{{lightbox_id}}' aria-label='Close image preview'></label>"
                    + "<div class='marimo-lightbox__dialog'>"
                    + "<div class='marimo-lightbox__chrome'>"
                    + f"<div class='marimo-lightbox__caption'>{{caption}}</div>"
                    + f"<label class='marimo-lightbox__close' for='{{lightbox_id}}'>Close</label>"
                    + "</div>"
                    + f"<img class='marimo-lightbox__image' src='{{src}}' alt='image' />"
                    + "</div>"
                    + "</div>"
                    + f"<div class='marimo-image-thumb__meta'>{{caption}}</div>"
                    + "</div>"
                )

            def render_table(rows, panel):
                if not rows:
                    return render_html("<div class='marimo-note'>No offline rows were exported for this panel.</div>")
                materialized_rows = materialize_table_rows(rows, panel)
                if not materialized_rows:
                    return render_html("<div class='marimo-note'>No offline rows were exported for this panel.</div>")
                if contains_image_cells(materialized_rows):
                    headers = list(materialized_rows[0].keys())
                    body = []
                    for row_index, row in enumerate(materialized_rows[:50]):
                        cells = []
                        for header in headers:
                            value = row.get(header)
                            if is_image_cell(value):
                                row_key = row.get("__run_id") or row.get("run_id") or row.get("id") or row_index
                                cell_key = f"{{row_key}}:{{header}}:{{row_index}}"
                                cells.append(f"<td>{{render_image_cell(value, cell_key)}}</td>")
                            else:
                                cells.append(f"<td>{{html.escape('' if value is None else str(value))}}</td>")
                        body.append("<tr>" + "".join(cells) + "</tr>")
                    table_html = (
                        "<div class='marimo-table-shell'><table class='marimo-table'>"
                        + "<thead><tr>"
                        + "".join(f"<th>{{html.escape(str(header))}}</th>" for header in headers)
                        + "</tr></thead><tbody>"
                        + "".join(body)
                        + "</tbody></table></div>"
                    )
                    return render_html(table_html)
                first_column = next(iter(materialized_rows[0].keys()), None)
                wrapped = [key for key in materialized_rows[0].keys() if len(str(key)) > 18]
                format_mapping = {{}}
                for key, value in materialized_rows[0].items():
                    if isinstance(value, (int, float)):
                        format_mapping[key] = lambda value, _key=key: "" if value is None else f"{{float(value):.4f}}".rstrip("0").rstrip(".")
                table_kwargs = {{
                    "pagination": True,
                    "page_size": 25,
                    "show_download": True,
                    "show_data_types": False,
                    "max_height": 920,
                    "wrapped_columns": wrapped or None,
                    "format_mapping": format_mapping or None,
                    "freeze_columns_left": [first_column] if first_column else None,
                }}
                table_ctor = getattr(mo.ui, "table", None)
                if table_ctor is None:
                    return render_html("<div class='marimo-note'>marimo table UI is unavailable.</div>")
                return table_ctor(materialized_rows, **table_kwargs)

            def infer_series_names(rows, preferred_key="__run_name"):
                names = []
                seen = set()
                for row in rows or []:
                    if not isinstance(row, dict):
                        continue
                    value = row.get(preferred_key) or row.get("__run_name") or row.get("model_name") or row.get("name")
                    if value in (None, ""):
                        continue
                    label = str(value)
                    if label in seen:
                        continue
                    seen.add(label)
                    names.append(label)
                return names

            def render_series_toggles(series_names, palette, chart_id):
                if len(series_names) <= 1:
                    return "", "", ""
                states = []
                items = []
                rules = []
                for index, name in enumerate(series_names):
                    series_id = f"series-{{index}}"
                    color = palette[index % len(palette)]
                    input_id = f"{{chart_id}}-{{series_id}}"
                    states.append(
                        f"<input id='{{input_id}}' class='marimo-series-state' type='checkbox' checked />"
                    )
                    items.append(
                        f"<label class='marimo-series-toggle' for='{{input_id}}'>"
                        + f"<span class='marimo-series-toggle__swatch' style='background:{{color}}'></span>"
                        + f"<span class='marimo-series-toggle__label'>{{html.escape(str(name))}}</span>"
                        + "</label>"
                    )
                    rules.append(
                        f".marimo-chart-shell--{{chart_id}} > #{{input_id}}:not(:checked) ~ .marimo-chart-svg [data-series='{{series_id}}'], "
                        + f".marimo-chart-shell--{{chart_id}} > #{{input_id}}:not(:checked) ~ .marimo-legend [data-series='{{series_id}}'] "
                        + "{{ display: none; }}"
                    )
                    rules.append(
                        f".marimo-chart-shell--{{chart_id}} > #{{input_id}}:not(:checked) ~ .marimo-series-toggles label[for='{{input_id}}'] "
                        + "{{ opacity: 0.45; background: rgba(15, 23, 42, 0.02); }}"
                    )
                style = "<style>" + "".join(rules) + "</style>"
                return "".join(states), "<div class='marimo-series-toggles'>" + "".join(items) + "</div>", style

            def render_scatter(rows, plot, panel):
                x_key = plot.get("x")
                y_key = plot.get("y")
                if not x_key or not y_key:
                    return render_html("<div class='marimo-note'>Combined Plot metadata was not exported.</div>")
                x_is_date = any(is_timestamp_like(row.get(x_key), x_key) for row in rows)
                color_key = plot.get("color") or "__run_name"
                label_key = plot.get("label") or "__run_name"
                points = []
                for row in rows:
                    x = to_plot_number(row.get(x_key), x_is_date)
                    y = to_plot_number(row.get(y_key), False)
                    if x is None or y is None:
                        continue
                    group_value = row.get(color_key) or row.get("__run_name") or row.get("__run_id") or "All"
                    points.append(
                        {{
                            "x": x,
                            "y": y,
                            "label": row.get(label_key) or row.get("__run_name") or row.get("__run_id") or "",
                            "group": group_value,
                        }}
                    )
                if not points:
                    return render_html("<div class='marimo-note'>Combined Plot data was empty.</div>")
                width, height = 940, 540
                left, right, top, bottom = 92, 34, 28, 72
                min_x = min(point["x"] for point in points)
                max_x = max(point["x"] for point in points)
                min_y = min(point["y"] for point in points)
                max_y = max(point["y"] for point in points)
                palette = ["#0f766e", "#2563eb", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#65a30d", "#db2777"]
                groups = list(dict.fromkeys(point["group"] for point in points))
                series_lookup = {{group: f"series-{{index}}" for index, group in enumerate(groups)}}

                def scale_x(value):
                    if max_x == min_x:
                        return left
                    return left + (value - min_x) / (max_x - min_x) * (width - left - right)

                def scale_y(value):
                    if max_y == min_y:
                        return height - bottom
                    return height - bottom - (value - min_y) / (max_y - min_y) * (height - top - bottom)

                def tick_value(min_value, max_value, index):
                    return min_value + (max_value - min_value) * (index / 4)

                chart_id = f"scatter-{{panel_chart_identity(panel, [x_key, y_key, groups]):x}}"
                svg = [
                    f"<svg viewBox='0 0 {{width}} {{height}}' class='marimo-chart-svg' role='img' {{tooltip_root_attrs()}}>",
                    f"<line x1='{{left}}' y1='{{height-bottom}}' x2='{{width-right}}' y2='{{height-bottom}}' stroke='rgba(0,0,0,0.22)' stroke-width='1.5' />",
                    f"<line x1='{{left}}' y1='{{top}}' x2='{{left}}' y2='{{height-bottom}}' stroke='rgba(0,0,0,0.22)' stroke-width='1.5' />",
                ]
                for index in range(5):
                    x = left + index / 4 * (width - left - right)
                    y = height - bottom - index / 4 * (height - top - bottom)
                    svg.append(f"<line x1='{{x}}' y1='{{top}}' x2='{{x}}' y2='{{height-bottom}}' stroke='rgba(0,0,0,0.06)' />")
                    svg.append(f"<line x1='{{left}}' y1='{{y}}' x2='{{width-right}}' y2='{{y}}' stroke='rgba(0,0,0,0.06)' />")
                    x_tick_value = tick_value(min_x, max_x, index)
                    svg.append(
                        f"<text x='{{x}}' y='{{height-bottom+24}}' text-anchor='middle' class='marimo-axis-text'>{{html.escape(format_date_value(x_tick_value) if x_is_date else f'{{x_tick_value:.2f}}'.rstrip('0').rstrip('.'))}}</text>"
                    )
                    svg.append(
                        f"<text x='{{left-12}}' y='{{y+4}}' text-anchor='end' class='marimo-axis-text'>{{html.escape(f'{{tick_value(min_y, max_y, index):.2f}}'.rstrip('0').rstrip('.'))}}</text>"
                    )
                svg.append(f"<text x='{{width/2}}' y='{{height-18}}' text-anchor='middle' class='marimo-axis-label'>{{html.escape(str(x_key))}}</text>")
                svg.append(f"<text x='24' y='{{height/2}}' transform='rotate(-90 24 {{height/2}})' text-anchor='middle' class='marimo-axis-label'>{{html.escape(str(y_key))}}</text>")
                for point in points:
                    color = palette[groups.index(point["group"]) % len(palette)]
                    x_display = format_date_value(point["x"]) if x_is_date else f"{{point['x']:.3f}}".rstrip("0").rstrip(".")
                    y_display = f"{{point['y']:.3f}}".rstrip("0").rstrip(".")
                    point_markup = interactive_svg_point(
                        scale_x(point["x"]),
                        scale_y(point["y"]),
                        5.5,
                        color,
                        [
                            str(point["label"]),
                            f"{{x_key}}: {{x_display}}",
                            f"{{y_key}}: {{y_display}}",
                        ],
                        width,
                        height,
                    )
                    point_markup = point_markup.replace(
                        "<g class='chart-point'",
                        f"<g class='chart-point' data-series='{{series_lookup.get(point['group'], 'series-0')}}'",
                        1,
                    )
                    svg.append(point_markup)
                svg.append("</svg>")
                legend = "".join(
                    f"<span class='marimo-legend-item' data-series='series-{{index}}'><span class='marimo-legend-swatch' style='background: {{palette[index % len(palette)]}}'></span>{{html.escape(str(group))}}</span>"
                    for index, group in enumerate(groups[:12])
                )
                legend_html = (
                    f"<div class='marimo-legend'>{{legend}}</div>"
                    if len(groups) <= 12
                    else f"<div class='marimo-chart-meta'>{{len(groups)}} series</div>"
                )
                toggle_states, toggle_labels, toggle_style = render_series_toggles(groups, palette, chart_id)
                body = (
                    f"<div class='marimo-chart-shell marimo-chart-shell--{{chart_id}}'>"
                    + toggle_states
                    + f"<div class='marimo-chart-meta'>{{html.escape(str(x_key))}} vs {{html.escape(str(y_key))}}</div>"
                    + toggle_labels
                    + "".join(svg)
                    + legend_html
                    + toggle_style
                    + "</div>"
                )
                return render_html(body)

            def history_axis_field(panel):
                axis = panel.get("x_axis") or "_step"
                if axis == "_step":
                    return "step"
                if axis == "_runtime":
                    return "runtime"
                if axis == "_timestamp":
                    return "timestamp_value"
                if axis == "epoch":
                    return "epoch"
                return str(axis)

            def render_history_line_plot(panel):
                inline_svg = panel.get("history_inline_svg")
                if inline_svg:
                    return render_html(
                        "<div class='marimo-chart-shell'>"
                        + str(inline_svg)
                        + "</div>"
                    )
                asset_path = panel.get("history_asset_path")
                asset_type = str(panel.get("history_asset_type") or "")
                if asset_path:
                    if asset_type == "image/svg+xml":
                        return render_html(
                            "<div class='marimo-chart-shell marimo-loading-shell'>"
                            + "<div class='marimo-loading-badge'>Loading chart…</div>"
                            + f'''<iframe src='{{html.escape(str(asset_path))}}' loading='lazy' class='marimo-chart-frame' title='History chart' onload="this.closest('.marimo-loading-shell')?.classList.add('is-loaded')"></iframe>'''
                            + "</div>"
                        )
                    return render_html(
                        "<div class='marimo-chart-shell marimo-loading-shell'>"
                        + "<div class='marimo-loading-badge'>Loading panel…</div>"
                        + f'''<object data='{{html.escape(str(asset_path))}}' type='{{html.escape(asset_type or 'image/svg+xml')}}' class='marimo-chart-object' onload="this.closest('.marimo-loading-shell')?.classList.add('is-loaded')"></object>'''
                        + "</div>"
                    )
                return render_html("<div class='marimo-note'>No offline history rows were exported for this plot.</div>")

            def render_artifact_panel(block):
                lineage = block.get("lineage") or {{}}
                nodes = lineage.get("nodes") or []
                edges = lineage.get("edges") or []
                if not nodes:
                    return render_html("<div class='marimo-note'>Artifact lineage data was not exported for this panel.</div>")
                layers = sorted({{int(node.get("layer", 0) or 0) for node in nodes}})
                grouped = {{layer: [node for node in nodes if int(node.get('layer', 0) or 0) == layer] for layer in layers}}
                width, height = 980, max(320, max(len(grouped[layer]) for layer in layers) * 110)
                x_step = (width - 220) / max(len(layers) - 1, 1)
                positions = {{}}
                for layer_index, layer in enumerate(layers):
                    layer_nodes = grouped.get(layer) or []
                    for node_index, node in enumerate(layer_nodes):
                        positions[node["id"]] = {{
                            "x": 110 + layer_index * x_step,
                            "y": (node_index + 1) / (len(layer_nodes) + 1) * height,
                        }}
                svg = [f"<svg viewBox='0 0 {{width}} {{height}}' class='marimo-chart-svg' role='img' {{tooltip_root_attrs()}}>"]
                for edge in edges:
                    source = positions.get(edge.get("source"))
                    target = positions.get(edge.get("target"))
                    if not source or not target:
                        continue
                    svg.append(
                        f"<path d='M {{source['x']}} {{source['y']}} C {{(source['x'] + target['x']) / 2}} {{source['y']}}, {{(source['x'] + target['x']) / 2}} {{target['y']}}, {{target['x']}} {{target['y']}}' fill='none' stroke='rgba(15,23,42,0.18)' stroke-width='2'></path>"
                    )
                for node in nodes:
                    point = positions.get(node["id"])
                    if not point:
                        continue
                    fill = "#eff6ff" if node.get("kind") == "run" else "#f5f3ff"
                    stroke = "#93c5fd" if node.get("kind") == "run" else "#c4b5fd"
                    svg.append(f"<rect x='{{point['x'] - 74}}' y='{{point['y'] - 28}}' width='148' height='56' rx='16' fill='{{fill}}' stroke='{{stroke}}' stroke-width='1.5'></rect>")
                    svg.append(f"<text x='{{point['x']}}' y='{{point['y'] - 2}}' text-anchor='middle' class='marimo-axis-label'>{{html.escape(str(node.get('label') or node.get('id')))}}</text>")
                    svg.append(f"<text x='{{point['x']}}' y='{{point['y'] + 16}}' text-anchor='middle' class='marimo-axis-text'>{{html.escape(str(node.get('kind') or ''))}}</text>")
                svg.append("</svg>")
                return render_html("<div class='marimo-chart-shell'>" + "".join(svg) + "</div>")

            def is_category_score_matrix(rows):
                return bool(rows and isinstance(rows[0], dict) and "category" in rows[0] and "score" in rows[0])

            def render_category_score_chart(rows, panel):
                categories = []
                seen_categories = set()
                grouped = {{}}
                for row in rows:
                    category = str(row.get("category", ""))
                    if category and category not in seen_categories:
                        seen_categories.add(category)
                        categories.append(category)
                    name = str(row.get("__run_name") or row.get("model_name") or "Unknown")
                    grouped.setdefault(name, []).append(row)
                if not categories or not grouped:
                    return render_html("<div class='marimo-note'>Category/score chart data was empty.</div>")
                active_names = list(grouped)
                width, height = 780, 780
                center = width / 2
                radius = 250
                palette = ["#0f766e", "#2563eb", "#dc2626", "#9333ea", "#ea580c", "#0891b2", "#65a30d", "#db2777"]

                def polar(score, index):
                    angle = -math.pi / 2 + index * math.tau / len(categories)
                    return (
                        center + math.cos(angle) * radius * score,
                        center + math.sin(angle) * radius * score,
                    )

                chart_id = f"radar-{{panel_chart_identity(panel, [active_names, categories]):x}}"
                svg = [f"<svg viewBox='0 0 {{width}} {{height}}' class='marimo-chart-svg' role='img'>"]
                for ring in range(1, 5):
                    ring_radius = radius * ring / 4
                    points = []
                    for index, _category in enumerate(categories):
                        angle = -math.pi / 2 + index * math.tau / len(categories)
                        points.append(f"{{center + math.cos(angle) * ring_radius}},{{center + math.sin(angle) * ring_radius}}")
                    svg.append(f"<polygon points='{{' '.join(points)}}' fill='none' stroke='rgba(0,0,0,0.12)' stroke-width='1' />")
                for index, category in enumerate(categories):
                    angle = -math.pi / 2 + index * math.tau / len(categories)
                    x = center + math.cos(angle) * radius
                    y = center + math.sin(angle) * radius
                    label_x = center + math.cos(angle) * (radius + 28)
                    label_y = center + math.sin(angle) * (radius + 28)
                    anchor = "start" if x >= center else "end"
                    svg.append(f"<line x1='{{center}}' y1='{{center}}' x2='{{x}}' y2='{{y}}' stroke='rgba(0,0,0,0.16)' stroke-width='1' />")
                    svg.append(f"<text x='{{label_x}}' y='{{label_y}}' text-anchor='{{anchor}}' class='marimo-axis-text'>{{html.escape(category)}}</text>")
                for index, name in enumerate(active_names):
                    color = palette[index % len(palette)]
                    series_id = f"series-{{index}}"
                    score_map = {{str(row.get("category")): max(0.0, min(1.0, float(row.get("score") or 0.0))) for row in grouped[name]}}
                    polygon = []
                    for category_index, category in enumerate(categories):
                        score = score_map.get(category, 0.0)
                        x, y = polar(score, category_index)
                        polygon.append(f"{{x}},{{y}}")
                        point_markup = interactive_svg_point(
                            x,
                            y,
                            4.0,
                            color,
                            [
                                str(name),
                                str(category),
                                f"score: {{score:.3f}}".rstrip("0").rstrip("."),
                            ],
                            width,
                            height,
                            stroke_width=1,
                            opacity=1,
                        )
                        point_markup = point_markup.replace("<g class='chart-point'", f"<g class='chart-point' data-series='{{series_id}}'", 1)
                        svg.append(point_markup)
                    svg.append(
                        f"<polygon data-series='{{series_id}}' points='{{' '.join(polygon)}}' fill='{{color}}' fill-opacity='0.12' stroke='{{color}}' stroke-width='2'></polygon>"
                    )
                svg.append("</svg>")
                legend = "".join(
                    f"<span class='marimo-legend-item' data-series='series-{{index}}'><span class='marimo-legend-swatch' style='background: {{palette[index % len(palette)]}}'></span>{{html.escape(name)}}</span>"
                    for index, name in enumerate(active_names)
                )
                toggle_states, toggle_labels, toggle_style = render_series_toggles(active_names, palette, chart_id)
                return render_html(
                    f"<div class='marimo-chart-shell marimo-chart-shell--{{chart_id}}'>"
                    + toggle_states
                    + toggle_labels
                    + "".join(svg)
                    + f"<div class='marimo-legend'>{{legend}}</div>"
                    + toggle_style
                    + "</div>"
                )

            def polar_to_cartesian(cx, cy, radius, angle):
                return (cx + math.cos(angle) * radius, cy + math.sin(angle) * radius)

            def annular_sector_path(cx, cy, inner_radius, outer_radius, start_angle, end_angle):
                large_arc = 1 if end_angle - start_angle > math.pi else 0
                x0, y0 = polar_to_cartesian(cx, cy, outer_radius, start_angle)
                x1, y1 = polar_to_cartesian(cx, cy, outer_radius, end_angle)
                x2, y2 = polar_to_cartesian(cx, cy, inner_radius, end_angle)
                x3, y3 = polar_to_cartesian(cx, cy, inner_radius, start_angle)
                return (
                    f"M {{x0}} {{y0}} "
                    + f"A {{outer_radius}} {{outer_radius}} 0 {{large_arc}} 1 {{x1}} {{y1}} "
                    + f"L {{x2}} {{y2}} "
                    + f"A {{inner_radius}} {{inner_radius}} 0 {{large_arc}} 0 {{x3}} {{y3}} Z"
                )

            def render_plotly_figure(figure):
                if not isinstance(figure, dict):
                    return render_html("<div class='marimo-note'>Plotly figure payload was missing.</div>")
                traces = figure.get("data") or []
                if not traces:
                    return render_html("<div class='marimo-note'>Plotly figure had no traces.</div>")
                trace = traces[0]
                if trace.get("type") != "sunburst":
                    return render_html("<div class='marimo-note'>This Plotly chart type is not rendered in marimo yet.</div>")

                ids = list(trace.get("ids") or [])
                labels = list(trace.get("labels") or [])
                parents = list(trace.get("parents") or [])
                values = list(trace.get("values") or [])
                colors = list((trace.get("marker") or {{}}).get("colors") or [])
                if not ids or not labels or not parents or not values:
                    return render_html("<div class='marimo-note'>Sunburst data was incomplete.</div>")

                nodes = []
                by_id = {{}}
                roots = []
                for index, node_id in enumerate(ids):
                    parent_id = parents[index] or ""
                    node = {{
                        "id": str(node_id),
                        "label": str(labels[index]),
                        "parent": str(parent_id),
                        "value": float(values[index] or 0.0),
                        "color": colors[index] if index < len(colors) else "#3b82f6",
                        "children": [],
                    }}
                    nodes.append(node)
                    by_id[node["id"]] = node
                for node in nodes:
                    if node["parent"] and node["parent"] in by_id:
                        by_id[node["parent"]]["children"].append(node)
                    else:
                        roots.append(node)
                if not roots:
                    return render_html("<div class='marimo-note'>Sunburst hierarchy was empty.</div>")

                def node_depth(node):
                    if not node["children"]:
                        return 1
                    return 1 + max(node_depth(child) for child in node["children"])

                max_depth = max(node_depth(root) for root in roots)
                width, height = 900, 900
                cx, cy = width / 2, height / 2
                inner_padding = 70
                ring_width = max(60, min(120, (min(width, height) / 2 - inner_padding - 48) / max(max_depth, 1)))
                svg = [
                    f"<svg viewBox='0 0 {{width}} {{height}}' class='marimo-chart-svg marimo-sunburst-svg' role='img'>"
                ]

                def draw_node(node, start_angle, end_angle, depth):
                    inner_radius = inner_padding + (depth - 1) * ring_width
                    outer_radius = inner_radius + ring_width - 8
                    path = annular_sector_path(cx, cy, inner_radius, outer_radius, start_angle, end_angle)
                    svg.append(
                        f"<path d='{{path}}' fill='{{html.escape(str(node['color']))}}' stroke='rgba(255,255,255,0.92)' stroke-width='2'>"
                        + f"<title>{{html.escape(node['label'])}}: {{node['value']:.3f}}</title></path>"
                    )
                    angle = (start_angle + end_angle) / 2
                    span = end_angle - start_angle
                    if span > 0.28:
                        label_radius = (inner_radius + outer_radius) / 2
                        lx, ly = polar_to_cartesian(cx, cy, label_radius, angle)
                        svg.append(
                            f"<text x='{{lx}}' y='{{ly}}' text-anchor='middle' dominant-baseline='middle' class='marimo-sunburst-label'>{{html.escape(node['label'])}}</text>"
                        )
                    total = sum(max(child["value"], 0.0) for child in node["children"])
                    cursor = start_angle
                    for child in node["children"]:
                        child_value = max(child["value"], 0.0)
                        child_span = span * (child_value / total) if total > 0 else 0
                        if child_span <= 0:
                            continue
                        draw_node(child, cursor, cursor + child_span, depth + 1)
                        cursor += child_span

                total_root_value = sum(max(root["value"], 0.0) for root in roots)
                cursor = -math.pi / 2
                for root in roots:
                    root_span = math.tau * (max(root["value"], 0.0) / total_root_value) if total_root_value > 0 else 0
                    if root_span <= 0:
                        continue
                    draw_node(root, cursor, cursor + root_span, 1)
                    cursor += root_span
                svg.append("</svg>")
                title = (((figure.get("layout") or {{}}).get("title") or {{}}).get("text") or "").strip()
                title_html = f"<div class='marimo-chart-meta'>{{html.escape(title)}}</div>" if title else ""
                return render_html("<div class='marimo-chart-shell'>" + title_html + "".join(svg) + "</div>")

            def render_media_browser(panel, block):
                runset_names = block.get("runsets") or []
                items = filter_rows_by_runset_selection(
                    panel.get("media_items") or [],
                    runset_names,
                    visible_values=infer_block_visible_values(block),
                    use_selection_fallback=should_use_selection_fallback(runset_names),
                )
                if not items:
                    return render_html("<div class='marimo-note'>No local media was exported for this media browser panel.</div>")
                blocks = []
                for item in items:
                    title = html.escape(str(item.get("title") or item.get("key") or item.get("run_name") or "Media"))
                    blocks.append(f"<div class='marimo-chart-meta'>{{title}}</div>")
                    if "plotly" in str(item.get("kind", "")):
                        blocks.append(render_plotly_figure(item.get("figure")))
                    else:
                        src = html.escape(str(item.get("path") or ""))
                        blocks.append(render_html("<figure class='marimo-figure'>" + f"<img src='{{src}}' alt='{{title}}' />" + "</figure>"))
                return vstack(blocks)

            def render_panel(panel, block):
                try:
                    if panel.get("view_type") == "Markdown Panel":
                        return mo.md(panel.get("markdown", ""))
                    title = panel.get("table_key") or panel.get("view_type") or "Panel"
                    if panel.get("table_key") and panel.get("mode") == "plot":
                        title = f"{{humanize_label(panel.get('table_key')) or 'Panel'}} Plot"
                    elif panel.get("table_key") and panel.get("mode") == "table":
                        title = f"{{humanize_label(panel.get('table_key')) or 'Panel'}} Table"
                    if panel.get("view_type") == "Media Browser":
                        heading = mo.md(f"### {{humanize_label(title)}}")
                        return vstack([heading, render_media_browser(panel, block)])
                    if panel.get("view_type") == "Run History Line Plot":
                        heading = mo.md(f"### {{humanize_label(summarize_metric_title(panel.get('metrics') or [title]))}}")
                        return vstack([heading, render_history_line_plot(panel)])
                    runset_names = block.get("runsets") or []
                    rows = filter_rows_by_runset_selection(
                        panel_tables.get(panel.get("table_key"), []),
                        runset_names,
                        visible_values=infer_block_visible_values(block),
                        use_selection_fallback=should_use_selection_fallback(runset_names),
                    )
                    rows = apply_simple_filter(rows, panel.get("simple_filter"))
                    if panel.get("view_type") == "Vega2":
                        if is_category_score_matrix(rows):
                            body = render_category_score_chart(rows, panel)
                        elif panel.get("vega_svg_path"):
                            frame_class = "marimo-chart-frame"
                            if panel.get("vega_aspect") == "square":
                                frame_class += " marimo-chart-frame--square"
                            body = render_html(
                                "<div class='marimo-chart-shell marimo-loading-shell'>"
                                + "<div class='marimo-loading-badge'>Loading chart…</div>"
                                + f'''<iframe src='{{html.escape(str(panel.get('vega_svg_path')))}}' loading='lazy' class='{{frame_class}}' title='Custom chart' onload="this.closest('.marimo-loading-shell')?.classList.add('is-loaded')"></iframe>'''
                                + "</div>"
                            )
                        elif panel.get("vega_error"):
                            body = render_html("<div class='marimo-note'>" + html.escape(str(panel.get("vega_error"))) + "</div>")
                        else:
                            body = render_html("<div class='marimo-note'>Custom chart data could not be rendered generically in marimo.</div>")
                    elif panel.get("mode") == "plot":
                        plot = panel.get("plot", {{}})
                        body = render_scatter(rows, plot, panel)
                    else:
                        body = render_table(rows, panel)
                    heading = mo.md(f"### {{humanize_label(title)}}")
                    return vstack([heading, body])
                except Exception as exc:
                    title = panel.get("table_key") or panel.get("view_type") or "Panel"
                    heading = mo.md(f"### {{humanize_label(title)}}")
                    note = render_html(
                        "<div class='marimo-note'>"
                        + "This panel could not be rendered offline. "
                        + html.escape(str(exc))
                        + "</div>"
                    )
                    return vstack([heading, note])

            def render_block(block):
                block_type = block.get("type")
                if block_type == "html":
                    return mo.md(block.get("html", ""))
                if block_type == "image":
                    caption = block.get("caption_html") or ""
                    src = block.get("src", "")
                    alt = html.escape(str(block.get("alt", "")))
                    return render_html(
                        "<figure class='marimo-figure'>"
                        + f"<img src='{{html.escape(src)}}' alt='{{alt}}' />"
                        + (f"<figcaption>{{caption}}</figcaption>" if caption else "")
                        + "</figure>"
                    )
                if block_type == "panel-grid":
                    rendered_rows = []
                    for row_panels in build_panel_rows(block.get("panels", [])):
                        row_panels = dedupe_panels(row_panels)
                        if not row_panels:
                            continue
                        if should_stack_panel_group(row_panels):
                            order = {{"table": 0, "plot": 1}}
                            for panel in sorted(row_panels, key=lambda panel: order.get(panel.get("mode"), 9)):
                                rendered_rows.append(render_panel(panel, block))
                            continue
                        if len(row_panels) == 1 and panel_needs_own_row(row_panels[0]):
                            rendered_rows.append(render_panel(row_panels[0], block))
                            continue
                        items, widths = row_items_with_spacers(row_panels, block)
                        rendered_rows.append(hstack(items, widths=widths))
                    return vstack(rendered_rows)
                if block_type == "details":
                    children = [render_block(child) for child in block.get("children", [])]
                    return vstack([mo.md(block.get("summary_html", "## Details")), *children])
                if block_type == "artifact-panel":
                    title = block.get("artifact_name") or "Artifact"
                    return vstack([mo.md(f"## {{title}}"), render_artifact_panel(block)])
                if block_type == "code":
                    return render_html("<pre class='marimo-code'><code>" + html.escape(block.get("code", "")) + "</code></pre>")
                return render_html(f"<div class='marimo-note'>Unsupported block: {{html.escape(str(block_type))}}</div>")

            styles = render_html(
                '''
                <style>
                  .marimo-chart-shell, .marimo-table-shell, .marimo-figure img, .marimo-code {{
                    border: 1px solid rgba(0,0,0,0.1);
                    border-radius: 18px;
                    background: rgba(255,255,255,0.92);
                    box-shadow: 0 10px 30px rgba(0,0,0,0.05);
                  }}
                  .marimo-chart-shell, .marimo-table-shell, .marimo-code {{
                    padding: 1rem;
                    overflow-x: auto;
                  }}
                  .marimo-chart-shell {{
                    content-visibility: auto;
                    contain-intrinsic-size: 540px;
                  }}
                  .marimo-chart-svg {{
                    width: 100%;
                    min-width: 0;
                    display: block;
                  }}
                  .marimo-chart-frame {{
                    width: 100%;
                    min-height: 420px;
                    border: 0;
                    display: block;
                    background: transparent;
                  }}
                  .marimo-loading-shell {{
                    position: relative;
                    min-height: 420px;
                  }}
                  .marimo-loading-badge {{
                    position: absolute;
                    inset: 1rem auto auto 1rem;
                    z-index: 3;
                    display: inline-flex;
                    align-items: center;
                    gap: 0.55rem;
                    padding: 0.55rem 0.85rem;
                    border-radius: 999px;
                    background: rgba(255,255,255,0.94);
                    border: 1px solid rgba(59,130,246,0.18);
                    box-shadow: 0 8px 22px rgba(15,23,42,0.08);
                    color: rgba(15,23,42,0.86);
                    font-size: 0.92rem;
                    font-weight: 600;
                    letter-spacing: 0.01em;
                    pointer-events: none;
                    transition: opacity 160ms ease, transform 160ms ease;
                  }}
                  .marimo-loading-badge::before {{
                    content: "";
                    width: 0.9rem;
                    height: 0.9rem;
                    border-radius: 999px;
                    border: 2px solid rgba(59,130,246,0.24);
                    border-top-color: rgba(37,99,235,0.9);
                    animation: marimo-loading-spin 0.9s linear infinite;
                  }}
                  .marimo-loading-shell.is-loaded > .marimo-loading-badge {{
                    opacity: 0;
                    transform: translateY(-4px);
                  }}
                  .marimo-loading-shell.is-loaded > .marimo-loading-badge::before {{
                    animation: none;
                  }}
                  .marimo-chart-object {{
                    width: 100%;
                    min-height: 420px;
                    border: 0;
                    display: block;
                  }}
                  .marimo-heatmap-shell {{
                    position: relative;
                    overflow: hidden;
                  }}
                  .marimo-heatmap-image {{
                    user-select: none;
                    -webkit-user-drag: none;
                  }}
                  .marimo-heatmap-tooltip {{
                    position: absolute;
                    left: 0;
                    top: 0;
                    visibility: hidden;
                    opacity: 0;
                    pointer-events: none;
                    transition: opacity 120ms ease;
                    max-width: 240px;
                    padding: 0.6rem 0.72rem;
                    border-radius: 12px;
                    background: rgba(17, 24, 39, 0.94);
                    color: #fff;
                    font-size: 12px;
                    line-height: 1.45;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.18);
                    z-index: 20;
                  }}
                  .marimo-chart-object--square {{
                    aspect-ratio: 1 / 1;
                    min-height: 620px;
                    height: auto;
                  }}
                  .marimo-chart-frame--square {{
                    aspect-ratio: 1 / 1;
                    min-height: 620px;
                    height: auto;
                  }}
                  @keyframes marimo-loading-spin {{
                    from {{ transform: rotate(0deg); }}
                    to {{ transform: rotate(360deg); }}
                  }}
                  .marimo-sunburst-svg {{
                    min-width: 0;
                  }}
                  .marimo-sunburst-label {{
                    fill: rgba(17,17,17,0.92);
                    font-size: 12px;
                    font-family: sans-serif;
                    pointer-events: none;
                  }}
                  .marimo-axis-text {{
                    fill: #606060;
                    font-size: 12px;
                    font-family: sans-serif;
                  }}
                  .marimo-axis-label {{
                    fill: #151515;
                    font-size: 13px;
                    font-family: sans-serif;
                  }}
                  .chart-point {{
                    cursor: crosshair;
                  }}
                  .chart-point__target {{
                    fill: transparent;
                  }}
                  .chart-point__dot {{
                    pointer-events: none;
                  }}
                  .chart-tooltip {{
                    visibility: hidden;
                    opacity: 0;
                    pointer-events: none;
                    transition: opacity 120ms ease;
                  }}
                  .chart-tooltip__bubble {{
                    fill: rgba(17, 24, 39, 0.94);
                    stroke: rgba(255, 255, 255, 0.16);
                    stroke-width: 1;
                  }}
                  .chart-tooltip__text {{
                    fill: #fff;
                    font-size: 12px;
                    font-family: sans-serif;
                  }}
                  .marimo-legend {{
                    display: flex;
                    gap: 0.6rem 0.9rem;
                    flex-wrap: wrap;
                    margin-top: 0.85rem;
                    color: #606060;
                    font-size: 0.9rem;
                  }}
                  .marimo-legend-item {{
                    display: inline-flex;
                    gap: 0.42rem;
                    align-items: center;
                  }}
                  .marimo-legend-swatch {{
                    width: 10px;
                    height: 10px;
                    border-radius: 999px;
                    display: inline-block;
                  }}
                  .marimo-series-toggles {{
                    display: flex;
                    gap: 0.5rem 0.75rem;
                    flex-wrap: wrap;
                    margin: 0.35rem 0 0.95rem;
                  }}
                  .marimo-series-state {{
                    position: absolute;
                    inline-size: 1px;
                    block-size: 1px;
                    margin: 0;
                    padding: 0;
                    border: 0;
                    opacity: 0;
                    pointer-events: none;
                  }}
                  .marimo-series-toggle {{
                    display: inline-flex;
                    align-items: center;
                    gap: 0.45rem;
                    padding: 0.32rem 0.58rem;
                    border-radius: 999px;
                    background: rgba(15, 23, 42, 0.05);
                    color: #334155;
                    font-size: 0.88rem;
                    cursor: pointer;
                    user-select: none;
                  }}
                  .marimo-series-toggle__swatch {{
                    width: 10px;
                    height: 10px;
                    border-radius: 999px;
                    display: inline-block;
                    flex: 0 0 auto;
                  }}
                  .marimo-chart-meta, .marimo-table-note {{
                    color: #606060;
                    margin-bottom: 0.7rem;
                    font-size: 0.9rem;
                  }}
                  .marimo-note {{
                    padding: 0.9rem 1rem;
                    border-radius: 14px;
                    border: 1px dashed rgba(0,0,0,0.18);
                    background: rgba(255,255,255,0.65);
                    color: #606060;
                  }}
                  .marimo-table {{
                    width: 100%;
                    min-width: 720px;
                    border-collapse: collapse;
                    font-size: 0.92rem;
                  }}
                  .marimo-table th, .marimo-table td {{
                    padding: 0.55rem 0.7rem;
                    border-bottom: 1px solid rgba(0,0,0,0.08);
                    text-align: left;
                    vertical-align: top;
                  }}
                  .marimo-table thead th {{
                    position: sticky;
                    top: 0;
                    background: rgba(245, 248, 242, 0.95);
                  }}
                  .marimo-figure {{
                    margin: 0;
                    display: grid;
                    gap: 0.5rem;
                  }}
                  .marimo-figure img {{
                    width: 100%;
                    padding: 0;
                  }}
                  .marimo-image-thumb {{
                    display: grid;
                    gap: 0.35rem;
                  }}
                  .marimo-image-thumb__link {{
                    display: block;
                    cursor: zoom-in;
                    position: relative;
                  }}
                  .marimo-lightbox__toggle {{
                    position: absolute;
                    opacity: 0;
                    pointer-events: none;
                  }}
                  .marimo-image-thumb img {{
                    width: 100%;
                    height: 120px;
                    object-fit: contain;
                    border-radius: 12px;
                    border: 1px solid rgba(0,0,0,0.1);
                    background: rgba(245,248,242,0.9);
                  }}
                  .marimo-image-thumb__zoom {{
                    position: absolute;
                    right: 0.55rem;
                    bottom: 0.55rem;
                    padding: 0.18rem 0.45rem;
                    border-radius: 999px;
                    background: rgba(26, 32, 29, 0.76);
                    color: #f7fbf4;
                    font-size: 0.7rem;
                    font-weight: 700;
                    letter-spacing: 0.03em;
                    text-transform: uppercase;
                  }}
                  .marimo-image-thumb__meta {{
                    font-size: 0.78rem;
                    color: #606060;
                  }}
                  .marimo-lightbox {{
                    position: fixed;
                    inset: 0;
                    display: none;
                    z-index: 9999;
                  }}
                  .marimo-lightbox__toggle:checked + .marimo-image-thumb__link + .marimo-lightbox {{
                    display: block;
                  }}
                  .marimo-lightbox__backdrop {{
                    position: absolute;
                    inset: 0;
                    background: rgba(18, 22, 20, 0.82);
                    backdrop-filter: blur(4px);
                  }}
                  .marimo-lightbox__dialog {{
                    position: relative;
                    z-index: 1;
                    display: grid;
                    gap: 0.75rem;
                    width: min(92vw, 880px);
                    max-height: 90vh;
                    margin: 5vh auto;
                    padding: 1rem;
                    border-radius: 18px;
                    background: #fbfcf8;
                    box-shadow: 0 24px 64px rgba(0,0,0,0.28);
                  }}
                  .marimo-lightbox__chrome {{
                    display: flex;
                    gap: 1rem;
                    align-items: center;
                    justify-content: space-between;
                    flex-wrap: wrap;
                  }}
                  .marimo-lightbox__caption {{
                    font-size: 0.92rem;
                    color: #2d3a31;
                  }}
                  .marimo-lightbox__close {{
                    color: #2d3a31;
                    cursor: pointer;
                    font-size: 0.84rem;
                    font-weight: 700;
                    letter-spacing: 0.02em;
                    text-transform: uppercase;
                  }}
                  .marimo-lightbox__image {{
                    width: 100%;
                    max-height: calc(90vh - 5rem);
                    object-fit: contain;
                    border-radius: 14px;
                    background: rgba(240, 244, 236, 0.92);
                  }}
                  .marimo-spacer {{
                    min-height: 1px;
                  }}
                  .marimo-code {{
                    overflow: auto;
                  }}
                </style>
                '''
            )

            return apply_simple_filter, hstack, mo, panel_tables, payload, render_block, render_html, render_panel, report, styles, vstack


        @app.cell
        def __(mo, payload, render_block, report, styles, vstack):
            title = report.get("title") or payload.get("title") or "W&B Report"
            report_url = report.get("report_url") or payload.get("report_url")
            header = [styles, mo.md(f"# {{title}}"), mo.md("Standalone marimo viewer exported from the same W&B report snapshot.")]
            if report_url:
                header.append(mo.md(f"[Open original report on W&B]({{report_url}})"))
            blocks = [render_block(block) for block in report.get("blocks", [])]
            page = vstack(header + blocks)
            page
            return


        if __name__ == "__main__":
            app.run()
        """
    ).strip() + "\n"


def main() -> None:
    MARIMO_VIEWER_DIR.mkdir(parents=True, exist_ok=True)
    if GENERATED_ASSETS_DIR.exists():
        shutil.rmtree(GENERATED_ASSETS_DIR, ignore_errors=True)
    payload = load_payload()
    NOTEBOOK_PATH.write_text(notebook_source(encode_payload(payload)), encoding="utf-8")
    print(f"[ok] wrote {NOTEBOOK_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

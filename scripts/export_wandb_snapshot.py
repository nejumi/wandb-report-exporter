from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import html
import json
import math
import mimetypes
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

import pandas as pd
from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

try:
    import wandb
except Exception:  # pragma: no cover - wandb is declared in pyproject
    wandb = None

try:
    from wandb_gql import gql
except Exception:  # pragma: no cover - provided by wandb in normal installs
    gql = None


ROOT = Path(__file__).resolve().parents[1]
FINAL_RAW_DIR = ROOT / "extracted" / "raw"
FINAL_PROCESSED_DIR = ROOT / "extracted" / "processed"
FINAL_APP_DATA_DIR = ROOT / "app" / "src" / "data"
FINAL_APP_MEDIA_DIR = ROOT / "app" / "src" / "media"
STAGING_ROOT_DIR = ROOT / ".snapshot-staging"
CACHE_ROOT_DIR = ROOT / ".snapshot-cache"
HISTORY_CACHE_DIR = CACHE_ROOT_DIR / "history"
RAW_DIR = FINAL_RAW_DIR
PROCESSED_DIR = FINAL_PROCESSED_DIR
APP_DATA_DIR = FINAL_APP_DATA_DIR
APP_MEDIA_DIR = FINAL_APP_MEDIA_DIR
PANEL_TABLES_DIRNAME = "panel_tables"
HISTORY_CACHE_VERSION = "v2"
EXPORT_TIMER_START: float | None = None

VIEWS2_RAW_VIEW_QUERY = """
query Views2RawView($id: ID!) {
  view(id: $id) {
    id
    name
    displayName
    updatedAt
    specObject
    children {
      edges {
        node {
          id
          displayName
          updatedAt
          user {
            id
            username
          }
        }
      }
    }
  }
}
"""


@dataclass
class ExportConfig:
    base_url: str | None
    entity: str | None
    project: str | None
    report_url: str | None
    history_keys: list[str]
    table_name: str | None
    table_artifact: str | None
    table_artifact_type: str | None
    max_runs: int
    sample_data: bool
    enable_primary_table_scan: bool


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def infer_base_url(report_url: str | None, explicit_base_url: str | None = None) -> str | None:
    if explicit_base_url:
        return explicit_base_url
    if not report_url:
        return None
    parsed = urlparse(report_url)
    if not (parsed.scheme and parsed.netloc):
        return None
    host = parsed.netloc.lower()
    if host in {"wandb.ai", "www.wandb.ai"}:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def parse_args() -> ExportConfig:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Export a local snapshot for the W&B fast report viewer.")
    parser.add_argument("report_url_arg", nargs="?", help="Optional positional W&B report URL.")
    parser.add_argument("--base-url", default=os.getenv("WANDB_BASE_URL"))
    parser.add_argument("--entity", default=os.getenv("WANDB_ENTITY"))
    parser.add_argument("--project", default=os.getenv("WANDB_PROJECT"))
    parser.add_argument("--report-url", default=os.getenv("WANDB_REPORT_URL"))
    parser.add_argument(
        "--history-keys",
        default=os.getenv("WANDB_HISTORY_KEYS", "eval/accuracy,train/loss"),
        help="Comma-separated list of history metrics to export.",
    )
    parser.add_argument("--table-name", default=os.getenv("WANDB_TABLE_NAME"))
    parser.add_argument("--table-artifact", default=os.getenv("WANDB_TABLE_ARTIFACT"))
    parser.add_argument("--table-artifact-type", default=os.getenv("WANDB_TABLE_ARTIFACT_TYPE"))
    parser.add_argument("--max-runs", type=int, default=int(os.getenv("WANDB_MAX_RUNS", "25")))
    parser.add_argument(
        "--enable-primary-table-scan",
        action="store_true",
        help="Opt into the slower legacy primary-table scan used by the old Observable viewer fallback.",
    )
    parser.add_argument("--sample-data", action="store_true", help="Force sample snapshot generation.")
    args = parser.parse_args()
    report_url = args.report_url_arg or args.report_url
    base_url = infer_base_url(report_url, args.base_url)
    if base_url:
        os.environ["WANDB_BASE_URL"] = base_url
    history_keys = [item.strip() for item in args.history_keys.split(",") if item.strip()]
    has_live_target = bool(report_url or (args.entity and args.project))
    sample_data = args.sample_data or not (has_live_target and os.getenv("WANDB_API_KEY"))
    enable_primary_table_scan = (
        args.enable_primary_table_scan
        or env_flag("WANDB_ENABLE_PRIMARY_TABLE_SCAN", False)
        or bool(args.table_name or args.table_artifact)
    )
    return ExportConfig(
        base_url=base_url,
        entity=args.entity,
        project=args.project,
        report_url=report_url,
        history_keys=history_keys,
        table_name=args.table_name,
        table_artifact=args.table_artifact,
        table_artifact_type=args.table_artifact_type,
        max_runs=args.max_runs,
        sample_data=sample_data,
        enable_primary_table_scan=enable_primary_table_scan,
    )


def begin_export_timer() -> None:
    global EXPORT_TIMER_START
    EXPORT_TIMER_START = time.perf_counter()


def format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    minutes, remainder = divmod(seconds, 60)
    if minutes >= 1:
        return f"{int(minutes)}m {remainder:.1f}s"
    return f"{remainder:.1f}s"


def log_info(message: str) -> None:
    if EXPORT_TIMER_START is None:
        print(f"[info] {message}")
        return
    elapsed = time.perf_counter() - EXPORT_TIMER_START
    print(f"[info {format_duration(elapsed)}] {message}")


def history_cache_exists(run: Any, metric_requests: dict[str, list[str]], extra_scan_keys: list[str] | None = None) -> bool:
    return history_cache_path(run, metric_requests, extra_scan_keys).exists()


def ensure_directories() -> None:
    for directory in (RAW_DIR, PROCESSED_DIR, APP_DATA_DIR, APP_MEDIA_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def reset_output_roots() -> None:
    global RAW_DIR, PROCESSED_DIR, APP_DATA_DIR, APP_MEDIA_DIR
    RAW_DIR = FINAL_RAW_DIR
    PROCESSED_DIR = FINAL_PROCESSED_DIR
    APP_DATA_DIR = FINAL_APP_DATA_DIR
    APP_MEDIA_DIR = FINAL_APP_MEDIA_DIR


def begin_snapshot_output() -> Path:
    global RAW_DIR, PROCESSED_DIR, APP_DATA_DIR, APP_MEDIA_DIR
    stage_root = STAGING_ROOT_DIR / datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    RAW_DIR = stage_root / "raw"
    PROCESSED_DIR = stage_root / "processed"
    APP_DATA_DIR = stage_root / "app_data"
    APP_MEDIA_DIR = stage_root / "app_media"
    ensure_directories()
    return stage_root


def replace_directory(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        shutil.rmtree(target)
    os.replace(source, target)


def commit_snapshot_output(stage_root: Path) -> None:
    replace_directory(PROCESSED_DIR, FINAL_PROCESSED_DIR)
    replace_directory(APP_DATA_DIR, FINAL_APP_DATA_DIR)
    replace_directory(APP_MEDIA_DIR, FINAL_APP_MEDIA_DIR)
    if RAW_DIR.exists():
        replace_directory(RAW_DIR, FINAL_RAW_DIR)
    shutil.rmtree(stage_root, ignore_errors=True)
    reset_output_roots()


def cleanup_staging_output(stage_root: Path) -> None:
    shutil.rmtree(stage_root, ignore_errors=True)
    reset_output_roots()


def export_worker_count(task_count: int) -> int:
    try:
        configured = int(os.getenv("WANDB_EXPORT_WORKERS", "6"))
    except ValueError:
        configured = 6
    return max(1, min(max(configured, 1), max(task_count, 1)))


def chunked(values: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        return [values]
    return [values[index : index + size] for index in range(0, len(values), size)]


def normalize_system_history_key(key: str) -> str:
    text = str(key)
    if text.startswith("system."):
        return "system/" + text[len("system.") :]
    return text


def history_metric_aliases(metric_name: str) -> list[str]:
    value = str(metric_name or "")
    aliases = [value]
    process_match = re.match(r"^system/gpu\.process\.(\d+)\.(.+)$", value)
    if process_match:
        aliases.append(f"system/gpu.{process_match.group(1)}.{process_match.group(2)}")
    gpu_match = re.match(r"^system/gpu\.(\d+)\.(.+)$", value)
    if gpu_match:
        aliases.append(f"system/gpu.process.{gpu_match.group(1)}.{gpu_match.group(2)}")
    return dedupe_strings(aliases)


def build_history_metric_requests(metric_keys: list[str]) -> dict[str, list[str]]:
    requests: dict[str, list[str]] = {}
    for key in dedupe_strings([str(metric) for metric in metric_keys if metric]):
        requests[key] = history_metric_aliases(key)
    return requests


def dump_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(sanitize_json_value(payload), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    frame = pd.DataFrame(rows)
    frame.to_parquet(path, index=False)


def write_parquet_with_columns(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    if rows:
        frame = pd.DataFrame(rows)
    else:
        frame = pd.DataFrame(columns=columns)
    frame.to_parquet(path, index=False)


def history_request_hash(metric_requests: dict[str, list[str]], extra_scan_keys: list[str] | None = None) -> str:
    payload = {
        "version": HISTORY_CACHE_VERSION,
        "metric_requests": {key: list(value) for key, value in sorted(metric_requests.items())},
        "extra_scan_keys": sorted(extra_scan_keys or []),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()[:16]


def slugify(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")


def make_thumb(path: Path, label: str, color: tuple[int, int, int]) -> None:
    image = Image.new("RGB", (320, 200), color=color)
    draw = ImageDraw.Draw(image)
    accent = tuple(max(channel - 40, 0) for channel in color)
    draw.rounded_rectangle((18, 18, 302, 182), radius=22, outline=(255, 255, 255), width=3)
    draw.rectangle((28, 28, 292, 88), fill=accent)
    font = ImageFont.load_default()
    draw.text((36, 38), label[:28], fill=(255, 255, 255), font=font)
    draw.text((36, 116), "Local snapshot thumbnail", fill=(245, 245, 245), font=font)
    image.save(path, format="PNG")


def sample_snapshot(config: ExportConfig) -> None:
    now = datetime.now(timezone.utc)
    models = ["vision-transformer-b", "clip-lite", "siglip-pro", "convnext-s"]
    datasets = ["imagenet-val", "coco-minival"]
    runs: list[dict[str, Any]] = []
    history_rows: list[dict[str, Any]] = []
    table_rows: list[dict[str, Any]] = []
    media_manifest: list[dict[str, Any]] = []
    thumbs_dir = PROCESSED_DIR / "media" / "thumbs"
    thumbs_dir.mkdir(parents=True, exist_ok=True)

    run_counter = 0
    for dataset_index, dataset_name in enumerate(datasets):
        for model_index, model_name in enumerate(models):
            run_counter += 1
            run_id = f"sample-{run_counter:02d}"
            run_name = f"{model_name}-{dataset_name}"
            accuracy = round(0.68 + model_index * 0.045 - dataset_index * 0.025 + (run_counter % 3) * 0.01, 4)
            loss = round(0.95 - model_index * 0.08 + dataset_index * 0.06, 4)
            created_at = now - timedelta(days=10 - run_counter)
            wandb_url = f"https://wandb.ai/demo/{dataset_name}/runs/{run_id}"
            runs.append(
                {
                    "run_id": run_id,
                    "run_name": run_name,
                    "project": config.project or "demo-project",
                    "entity": config.entity or "demo-entity",
                    "state": "finished",
                    "created_at": created_at.isoformat(),
                    "updated_at": (created_at + timedelta(hours=3)).isoformat(),
                    "tags_json": json.dumps([dataset_name, model_name, "sample-data"]),
                    "config_json": json.dumps({"lr": round(3e-4 + model_index * 1e-4, 6), "batch_size": 64}),
                    "summary_json": json.dumps({"eval/accuracy": accuracy, "train/loss": loss}),
                    "wandb_url": wandb_url,
                    "group_key": dataset_name,
                    "model_name": model_name,
                    "dataset_name": dataset_name,
                    "eval_split": "validation",
                    "primary_metric": accuracy,
                    "loss": loss,
                    "accuracy": accuracy,
                }
            )

            for step in range(1, 41):
                eval_accuracy = accuracy - 0.14 + step * 0.003 + math.sin(step / 7.0) * 0.01
                train_loss = loss + 0.35 - step * 0.012 + math.cos(step / 6.0) * 0.015
                timestamp = created_at + timedelta(minutes=step * 4)
                history_rows.append(
                    {
                        "run_id": run_id,
                        "step": step,
                        "epoch": round(step / 4, 2),
                        "metric_name": "eval/accuracy",
                        "metric_value": round(eval_accuracy, 4),
                        "timestamp": timestamp.isoformat(),
                    }
                )
                history_rows.append(
                    {
                        "run_id": run_id,
                        "step": step,
                        "epoch": round(step / 4, 2),
                        "metric_name": "train/loss",
                        "metric_value": round(max(train_loss, 0.09), 4),
                        "timestamp": timestamp.isoformat(),
                    }
                )

            for example_index in range(6):
                row_id = f"{run_id}-ex-{example_index + 1}"
                thumb_name = f"{row_id}.png"
                thumb_rel = f"media/thumbs/{thumb_name}"
                make_thumb(
                    thumbs_dir / thumb_name,
                    f"{model_name} #{example_index + 1}",
                    (70 + model_index * 30, 120 + dataset_index * 45, 150 + example_index * 8),
                )
                is_correct = (example_index + model_index + dataset_index) % 4 != 0
                score = round(0.45 + example_index * 0.07 + model_index * 0.03, 4)
                table_rows.append(
                    {
                        "row_id": row_id,
                        "run_id": run_id,
                        "run_name": run_name,
                        "model_name": model_name,
                        "dataset_name": dataset_name,
                        "split": "validation",
                        "slice_name": "hard-negative" if example_index % 2 else "standard",
                        "input_text": f"Sample prompt {example_index + 1} for {dataset_name}",
                        "prediction": f"class_{(example_index + model_index) % 5}",
                        "label": f"class_{(example_index + dataset_index) % 5}",
                        "correct": is_correct,
                        "score": score,
                        "image_thumb_path": thumb_rel,
                        "image_full_path": thumb_rel,
                        "wandb_run_url": wandb_url,
                        "wandb_artifact_url": f"{wandb_url}/artifacts",
                        "meta_json": json.dumps({"difficulty": "high" if not is_correct else "medium"}),
                    }
                )
                media_manifest.append(
                    {
                        "id": row_id,
                        "thumbnail_path": thumb_rel,
                        "full_path": thumb_rel,
                        "kind": "image",
                    }
                )

    manifest = build_manifest(
        config=config,
        run_rows=runs,
        history_rows=history_rows,
        table_rows=table_rows,
        media_items=media_manifest,
        source="sample",
        report_data={
            "report_url": config.report_url,
            "title": "Sample imported report",
            "selected_table_name": "sample_predictions_output_table",
            "selected_table_artifact": None,
            "selected_runset": None,
            "runsets": [],
            "table_candidates": ["sample_predictions_output_table"],
            "blocks": [
                {"type": "html", "html": "<p>This sample snapshot mimics an imported W&B report narrative.</p>"},
                {
                    "type": "details",
                    "summary_html": "<strong>What is preserved</strong>",
                    "children": [
                        {"type": "html", "html": "<p>Headings, paragraphs, images, lists, and code blocks can be carried into the static viewer.</p>"},
                        {"type": "code", "language": "bash", "code": "make export\nmake build\nmake serve"},
                    ],
                },
            ],
        },
    )
    persist_snapshot(runs, history_rows, table_rows, media_manifest, manifest)


def safe_json(value: Any) -> str:
    try:
        return json.dumps(sanitize_json_value(value), ensure_ascii=False, default=str, allow_nan=False)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


def sanitize_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): sanitize_json_value(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, str) and value.strip().lower() in {"nan", "none", "null"}:
        return None
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    return value


def coerce_history_numeric_value(value: Any) -> float | None:
    sanitized = sanitize_json_value(value)
    if sanitized is None or isinstance(sanitized, (dict, list)):
        return None
    if isinstance(sanitized, bool):
        return float(int(sanitized))
    if isinstance(sanitized, (int, float)):
        number = float(sanitized)
        return number if math.isfinite(number) else None
    if isinstance(sanitized, str):
        try:
            number = float(sanitized)
        except ValueError:
            return None
        return number if math.isfinite(number) else None
    return None


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


def histogram_history_summary(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or str(value.get("_type") or "") != "histogram":
        return None
    packed_bins = value.get("packedBins") or {}
    counts = value.get("values")
    if not isinstance(packed_bins, dict) or not isinstance(counts, list) or not counts:
        return None
    base = coerce_history_numeric_value(packed_bins.get("min"))
    size = coerce_history_numeric_value(packed_bins.get("size"))
    if base is None or size is None or size <= 0:
        return None
    weighted_points: list[tuple[float, float]] = []
    first_nonzero_index: int | None = None
    last_nonzero_index: int | None = None
    total_weight = 0.0
    weighted_sum = 0.0
    for index, raw_count in enumerate(counts):
        count = coerce_history_numeric_value(raw_count)
        if count is None or count <= 0:
            continue
        if first_nonzero_index is None:
            first_nonzero_index = index
        last_nonzero_index = index
        center = base + (index + 0.5) * size
        weighted_points.append((center, count))
        total_weight += count
        weighted_sum += center * count
    if not weighted_points or total_weight <= 0:
        return None
    mean = weighted_sum / total_weight
    variance = sum(weight * ((point - mean) ** 2) for point, weight in weighted_points) / total_weight
    min_value = base + first_nonzero_index * size if first_nonzero_index is not None else None
    max_value = base + (last_nonzero_index + 1) * size if last_nonzero_index is not None else None
    return {
        "metric_value_kind": "histogram",
        "metric_histogram_count": total_weight,
        "metric_histogram_mean": mean,
        "metric_histogram_std": math.sqrt(max(variance, 0.0)),
        "metric_histogram_min": min_value,
        "metric_histogram_max": max_value,
        "metric_histogram_q10": weighted_quantile(weighted_points, 0.10),
        "metric_histogram_q25": weighted_quantile(weighted_points, 0.25),
        "metric_histogram_q50": weighted_quantile(weighted_points, 0.50),
        "metric_histogram_q75": weighted_quantile(weighted_points, 0.75),
        "metric_histogram_q90": weighted_quantile(weighted_points, 0.90),
    }


def normalize_history_metric_value(value: Any) -> dict[str, Any]:
    sanitized = sanitize_json_value(value)
    numeric_value = coerce_history_numeric_value(sanitized)
    if numeric_value is not None:
        return {
            "metric_value": numeric_value,
            "metric_value_kind": "scalar",
            "metric_value_json": None,
        }
    histogram = histogram_history_summary(sanitized)
    if histogram:
        return {
            "metric_value": None,
            "metric_value_kind": "histogram",
            "metric_value_json": safe_json(sanitized),
            **histogram,
        }
    return {
        "metric_value": None,
        "metric_value_kind": "json" if isinstance(sanitized, (dict, list)) else "text",
        "metric_value_json": safe_json(sanitized),
    }


def parse_report_url(report_url: str | None) -> tuple[str | None, str | None, str | None]:
    if not report_url:
        return None, None, None
    parsed = urlparse(report_url)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 4 or parts[2] != "reports":
        return None, None, None
    report_id = parts[3].rsplit("--", 1)[-1].replace("=", "")
    return parts[0], parts[1], report_id


def iter_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from iter_nodes(nested)
    elif isinstance(value, list):
        for item in value:
            yield from iter_nodes(item)


def normalize_inline(nodes: list[dict[str, Any]] | None) -> str:
    parts: list[str] = []
    for node in nodes or []:
        if not isinstance(node, dict):
            continue
        if node.get("type") == "link":
            inner = normalize_inline(node.get("children"))
            url = html.escape(str(node.get("url", "")), quote=True)
            label = inner or html.escape(str(node.get("url", "")))
            parts.append(f'<a href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>')
            continue
        if "text" in node:
            text = html.escape(str(node.get("text", ""))).replace("\n", "<br>")
            if node.get("code"):
                text = f"<code>{text}</code>"
            if node.get("strong"):
                text = f"<strong>{text}</strong>"
            if node.get("italic"):
                text = f"<em>{text}</em>"
            if node.get("underline"):
                text = f"<u>{text}</u>"
            if node.get("strikethrough"):
                text = f"<s>{text}</s>"
            parts.append(text)
            continue
        parts.append(normalize_inline(node.get("children")))
    return "".join(parts)


def plain_text(nodes: list[dict[str, Any]] | None) -> str:
    return html.unescape(normalize_inline(nodes).replace("<br>", " ")).strip()


def normalize_list(block: dict[str, Any], ordered: bool | None = None) -> str:
    is_ordered = bool(block.get("ordered")) if ordered is None else ordered
    tag = "ol" if is_ordered else "ul"
    items: list[str] = []
    for child in block.get("children", []):
        if not isinstance(child, dict):
            continue
        if child.get("type") == "list-item":
            fragments: list[str] = []
            for grandchild in child.get("children", []):
                if isinstance(grandchild, dict) and grandchild.get("type") == "list":
                    fragments.append(normalize_list(grandchild))
                elif isinstance(grandchild, dict):
                    fragments.append(normalize_block_to_html(grandchild))
            items.append(f"<li>{''.join(fragment for fragment in fragments if fragment)}</li>")
    return f"<{tag}>{''.join(items)}</{tag}>"


def normalize_block_to_html(block: dict[str, Any]) -> str:
    block_type = block.get("type", "default")
    if block_type in {"paragraph", "default"}:
        content = normalize_inline(block.get("children"))
        return f"<p>{content}</p>" if content else ""
    if block_type == "heading":
        level = min(max(int(block.get("level", 2)), 1), 6)
        content = normalize_inline(block.get("children"))
        return f"<h{level}>{content}</h{level}>" if content else ""
    if block_type == "list":
        return normalize_list(block)
    if block_type in {"blockquote", "quote"}:
        content = normalize_inline(block.get("children"))
        return f"<blockquote>{content}</blockquote>" if content else ""
    if block_type in {"code-block", "code"}:
        code = html.escape(plain_text(block.get("children")))
        return f"<pre><code>{code}</code></pre>" if code else ""
    content = normalize_inline(block.get("children"))
    return f"<div>{content}</div>" if content else ""


def is_table_descriptor(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and isinstance(value.get("wbObjectType"), dict)
        and value["wbObjectType"].get("type") == "table"
    )


def contains_table_descriptor(value: Any) -> bool:
    if is_table_descriptor(value):
        return True
    if isinstance(value, dict):
        members = value.get("members")
        if isinstance(members, list):
            return any(contains_table_descriptor(member) for member in members)
    return False


def table_name_score(name: str) -> tuple[int, int]:
    lowered = name.lower()
    score = 0
    if "output_table" in lowered:
        score += 120
    if lowered == "leaderboard_table":
        score += 110
    elif "leaderboard_table" in lowered:
        score += 80
    if lowered.endswith("_table"):
        score += 20
    if "radar" in lowered or "hierarchy" in lowered:
        score -= 15
    if "subcategory" in lowered:
        score -= 10
    return score, -len(name)


def dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def extract_table_candidates_from_report(report_spec: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    for node in iter_nodes(report_spec):
        for key, value in node.items():
            if isinstance(key, str) and "table" in key.lower() and contains_table_descriptor(value):
                candidates.append(key)
    return sorted(dedupe_strings(candidates), key=table_name_score, reverse=True)


def normalize_order(sort_value: dict[str, Any] | None) -> str:
    if wandb is None or not sort_value:
        return "-createdAt"
    query_generator = wandb.apis.public.QueryGenerator()
    if sort_value.get("keys"):
        orders = query_generator.keys_to_order(sort_value)
        return orders[0] if orders else "-createdAt"
    if sort_value.get("key"):
        order = query_generator.key_to_server_path(sort_value["key"])
        return f"+{order}" if sort_value.get("ascending") else f"-{order}"
    return "-createdAt"


def append_name_selection(filters: dict[str, Any] | None, selected_names: list[str]) -> dict[str, Any]:
    if not selected_names:
        return filters or {}
    selection_filter = {"name": {"$in": selected_names}}
    if not filters:
        return selection_filter
    if "$or" in filters and filters["$or"]:
        first = filters["$or"][0]
        if isinstance(first, dict) and "$and" in first and isinstance(first["$and"], list):
            first["$and"].append(selection_filter)
            return filters
    return {"$and": [filters, selection_filter]}


RUN_ID_PATTERN = re.compile(r"^[a-z0-9]{8}$")


def flatten_selection_tree(node: Any) -> list[str]:
    if isinstance(node, str):
        return [node]
    if isinstance(node, list):
        values: list[str] = []
        for item in node:
            values.extend(flatten_selection_tree(item))
        return values
    if isinstance(node, dict):
        values: list[str] = []
        for key in ("tree", "children", "value"):
            if key in node:
                values.extend(flatten_selection_tree(node.get(key)))
        return values
    return []


def extract_runset_selections(runset: dict[str, Any]) -> tuple[list[str], list[str]]:
    raw_values = [value.strip() for value in flatten_selection_tree((runset.get("selections") or {}).get("tree")) if isinstance(value, str)]
    deduped = dedupe_strings([value for value in raw_values if value])
    run_ids = [value for value in deduped if RUN_ID_PATTERN.fullmatch(value)]
    names = [value for value in deduped if value not in run_ids]
    return run_ids, names


def runset_selection_root(runset: dict[str, Any]) -> int | None:
    selections = runset.get("selections") or {}
    root = selections.get("root")
    if root in {0, 1}:
        return int(root)
    return None


def runset_selection_mode(runset: dict[str, Any]) -> str | None:
    selected_run_ids, selected_names = extract_runset_selections(runset)
    has_explicit_selection = bool(selected_run_ids or selected_names)
    if runset.get("only_show_selected") or runset.get("single_run_only"):
        return "include"
    if not has_explicit_selection:
        return None
    root = runset_selection_root(runset)
    if root == 0:
        return "include"
    if root == 1:
        return "exclude"
    return None


def normalize_runset_filter_spec(node: Any) -> dict[str, Any] | None:
    if not isinstance(node, dict):
        return None
    if node.get("disabled"):
        return None
    op = node.get("op")
    if op in {"AND", "OR"}:
        children = [normalized for child in node.get("filters", []) if (normalized := normalize_runset_filter_spec(child))]
        if not children:
            return None
        return {
            **node,
            "filters": children,
        }
    return dict(node)


def runset_is_single_run(runset: dict[str, Any]) -> bool:
    selected_run_ids, selected_names = extract_runset_selections(runset)
    if len(selected_run_ids) + len(selected_names) == 1:
        return True
    filters = runset.get("filters") or {}
    if runset.get("selections", {}).get("tree"):
        return False
    if filters.get("op") != "OR":
        return False
    groups = filters.get("filters") or []
    if len(groups) != 1:
        return False
    and_group = groups[0]
    if and_group.get("op") != "AND":
        return False
    predicates = and_group.get("filters") or []
    return len(predicates) == 1 and predicates[0].get("key", {}).get("name") == "name" and predicates[0].get("op") == "="


def collect_runsets_from_report(report_spec: dict[str, Any]) -> list[dict[str, Any]]:
    runsets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for block in report_spec.get("blocks", []):
        if not isinstance(block, dict):
            continue
        for node in iter_nodes(block):
            raw_runsets = node.get("runSets")
            if not isinstance(raw_runsets, list):
                continue
            for index, runset in enumerate(raw_runsets):
                if not isinstance(runset, dict) or not runset.get("enabled", True):
                    continue
                runset_id = str(runset.get("id") or runset.get("ref", {}).get("id") or f"runset-{index}")
                if runset_id in seen:
                    continue
                seen.add(runset_id)
                runsets.append(
                    {
                        "id": runset_id,
                        "name": runset.get("name") or f"Run set {index + 1}",
                        "filters": runset.get("filters"),
                        "filters_ref": runset.get("filtersRef"),
                        "sort": runset.get("sort"),
                        "sort_ref": runset.get("sortRef"),
                        "search": runset.get("search", {}),
                        "only_show_selected": bool((runset.get("runFeed") or {}).get("onlyShowSelected")),
                        "selections": runset.get("selections", {}),
                        "group_selections_ref": runset.get("groupSelectionsRef"),
                        "project": runset.get("project", {}),
                        "block_type": block.get("type"),
                        "single_run_only": runset_is_single_run(runset),
                    }
                )
    return runsets


def download_report_asset(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    extension = Path(parsed.path).suffix
    target_dir = PROCESSED_DIR / "media" / "report"
    target_dir.mkdir(parents=True, exist_ok=True)
    asset_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    if not extension:
        extension = mimetypes.guess_extension(mimetypes.guess_type(url)[0] or "") or ".bin"
    target_path = target_dir / f"{asset_hash}{extension}"
    if not target_path.exists():
        request = Request(url, headers={"User-Agent": "wandb-report-exporter/0.1"})
        with urlopen(request, timeout=60) as response:  # noqa: S310
            target_path.write_bytes(response.read())
    return f"media/report/{target_path.name}"


def normalize_report_block(block: dict[str, Any]) -> dict[str, Any] | None:
    block_type = block.get("type", "default")
    if block_type == "image":
        local_path = download_report_asset(block.get("url"))
        return {
            "type": "image",
            "src": local_path,
            "remote_url": block.get("url"),
            "alt": plain_text(block.get("children")) or "Report image",
            "caption_html": normalize_inline(block.get("children")),
        }
    if block.get("collapsedChildren"):
        return {
            "type": "details",
            "summary_html": normalize_inline(block.get("children")) or html.escape(block_type.title()),
            "children": [normalized for child in block.get("collapsedChildren", []) if (normalized := normalize_report_block(child))],
        }
    if block_type == "weave-panel":
        if artifact_panel := normalize_artifact_panel(block.get("config", {}) or {}):
            return artifact_panel
    if block_type == "panel-grid":
        node_runsets = None
        metadata = block.get("metadata", {})
        raw_runsets = metadata.get("runSets")
        if isinstance(raw_runsets, list):
            node_runsets = [
                str(runset.get("id") or runset.get("ref", {}).get("id") or runset.get("name") or "")
                for runset in raw_runsets
                if isinstance(runset, dict) and runset.get("enabled", True)
            ]
            node_runsets = [value for value in node_runsets if value]
        node_tables = extract_table_candidates_from_report({"blocks": [block]})
        return {
            "type": "panel-grid",
            "title": metadata.get("name") or "Embedded W&B panel",
            "runsets": node_runsets or [],
            "table_candidates": node_tables,
            "panels": normalize_panel_grid_panels(metadata.get("panelBankSectionConfig", {}).get("panels", [])),
        }
    if block_type in {"code-block", "code"}:
        return {
            "type": "code",
            "language": block.get("language") or "",
            "code": plain_text(block.get("children")),
        }
    block_html = normalize_block_to_html(block)
    if block_html:
        return {"type": "html", "html": block_html}
    return None


def normalize_report_blocks(report_spec: dict[str, Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for block in report_spec.get("blocks", []):
        if not isinstance(block, dict):
            continue
        block_payload = normalize_report_block(block)
        if block_payload:
            normalized.append(block_payload)
    return normalized


def normalize_artifact_panel(config: dict[str, Any]) -> dict[str, Any] | None:
    panel_config = config.get("panelConfig", {}) or {}
    exp = panel_config.get("exp")
    if not isinstance(exp, dict):
        return None
    expr = normalize_weave_expression(exp)
    if not isinstance(expr, dict):
        return None
    if expr.get("name") != "project-artifact":
        return None
    inputs = expr.get("inputs", {})
    artifact_name = None
    artifact_version = panel_config.get("selectedMembershipIdentifier")
    selected_tab = (((panel_config.get("tabConfigs") or {}).get("overview") or {}).get("selectedTab"))
    if isinstance(inputs, dict):
        artifact_expr = inputs.get("artifactName")
        if isinstance(artifact_expr, dict) and artifact_expr.get("kind") == "const":
            artifact_name = artifact_expr.get("value")
    return {
        "type": "artifact-panel",
        "artifact_name": str(artifact_name or ""),
        "artifact_version": str(artifact_version or ""),
        "selected_tab": str(selected_tab or ""),
    }


def extract_history_metrics_from_blocks(blocks: list[dict[str, Any]]) -> list[str]:
    metrics: list[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "panel-grid":
            for panel in block.get("panels", []) or []:
                if not isinstance(panel, dict):
                    continue
                for metric in panel.get("metrics", []) or []:
                    if isinstance(metric, str):
                        metrics.append(metric)
        for child in block.get("children", []) or []:
            if isinstance(child, dict):
                metrics.extend(extract_history_metrics_from_blocks([child]))
    return dedupe_strings(metrics)


def extract_history_axes_from_blocks(blocks: list[dict[str, Any]]) -> list[str]:
    axes: list[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "panel-grid":
            for panel in block.get("panels", []) or []:
                if not isinstance(panel, dict):
                    continue
                axis = panel.get("x_axis")
                if isinstance(axis, str):
                    axes.append(axis)
        for child in block.get("children", []) or []:
            if isinstance(child, dict):
                axes.extend(extract_history_axes_from_blocks([child]))
    return dedupe_strings(axes)


def extract_table_key_from_expression(node: Any) -> str | None:
    if isinstance(node, dict):
        if node.get("name") == "tableKey" and "value" in node:
            value = str(node.get("value", ""))
            if "table" in value.lower():
                return value
        if node.get("nodeType") == "const" and node.get("type") == "string":
            value = str(node.get("val", ""))
            if "table" in value.lower():
                return value
        from_op = node.get("fromOp")
        if isinstance(from_op, dict):
            inputs = from_op.get("inputs", {})
            if isinstance(inputs, dict):
                for value in inputs.values():
                    if table_key := extract_table_key_from_expression(value):
                        return table_key
        for value in node.values():
            if table_key := extract_table_key_from_expression(value):
                return table_key
    elif isinstance(node, list):
        for item in node:
            if table_key := extract_table_key_from_expression(item):
                return table_key
    return None


def extract_table_key_from_panel_state(node: Any) -> str | None:
    if isinstance(node, dict):
        key_type = str(node.get("keyType") or node.get("type") or "")
        if key_type == "table-file" and node.get("key"):
            return str(node.get("key"))
        working_key_and_type = node.get("workingKeyAndType")
        if isinstance(working_key_and_type, dict):
            if str(working_key_and_type.get("type") or "") == "table-file" and working_key_and_type.get("key"):
                return str(working_key_and_type.get("key"))
        for value in node.values():
            if table_key := extract_table_key_from_panel_state(value):
                return table_key
    elif isinstance(node, list):
        for item in node:
            if table_key := extract_table_key_from_panel_state(item):
                return table_key
    return None


def extract_pick_column_name(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None
    from_op = node.get("fromOp")
    if isinstance(from_op, dict) and from_op.get("name") == "pick":
        key = from_op.get("inputs", {}).get("key")
        if isinstance(key, dict) and key.get("nodeType") == "const" and key.get("type") == "string":
            return str(key.get("val", ""))
    for value in node.values():
        if column_name := extract_pick_column_name(value):
            return column_name
    return None


def parse_simple_weave_filter(node: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(node, dict):
        return None
    from_op = node.get("fromOp", {})
    op_name = from_op.get("name")
    inputs = from_op.get("inputs", {})
    if op_name not in {"number-lessEqual", "number-greaterEqual", "number-lessThan", "number-greaterThan", "number-equal"}:
        return None
    lhs = inputs.get("lhs")
    rhs = inputs.get("rhs")
    if not isinstance(lhs, dict) or not isinstance(rhs, dict):
        return None
    rhs_value = rhs.get("val")
    lhs_key = extract_pick_column_name(lhs)
    if lhs_key and isinstance(rhs_value, (int, float)):
        return {"column": lhs_key, "op": op_name, "value": rhs_value}
    return None


def normalize_weave_expression(node: Any) -> dict[str, Any] | None:
    if not isinstance(node, dict):
        return None
    if node.get("nodeType") == "const":
        return {"kind": "const", "value": sanitize_json_value(node.get("val"))}
    from_op = node.get("fromOp")
    if not isinstance(from_op, dict):
        return None
    inputs = from_op.get("inputs", {})
    normalized_inputs: dict[str, Any] = {}
    if isinstance(inputs, dict):
        for key, value in inputs.items():
            normalized_value = normalize_weave_expression(value)
            if normalized_value is not None:
                normalized_inputs[str(key)] = normalized_value
    return {
        "kind": "op",
        "name": str(from_op.get("name") or ""),
        "inputs": normalized_inputs,
    }


def expression_label(expr: dict[str, Any] | None) -> str | None:
    if not isinstance(expr, dict):
        return None
    if expr.get("kind") != "op":
        return None
    op_name = expr.get("name")
    inputs = expr.get("inputs", {})
    if op_name == "pick":
        key = inputs.get("key", {})
        if isinstance(key, dict) and key.get("kind") == "const":
            value = key.get("value")
            return str(value) if value is not None else None
    if op_name == "run-name":
        return "__run_name"
    if op_name == "run-id":
        return "__run_id"
    return op_name or None


def panel_column_sort_key(column_id: str) -> tuple[int, str]:
    suffix = str(column_id).split("col-")[-1]
    return (int(suffix), str(column_id)) if suffix.isdigit() else (9999, str(column_id))


def normalize_table_columns(table_state: dict[str, Any]) -> list[dict[str, Any]]:
    column_selects = table_state.get("columnSelectFunctions", {})
    column_names = table_state.get("columnNames", {})
    columns_meta = table_state.get("columns", {})
    normalized: list[dict[str, Any]] = []
    if not isinstance(column_selects, dict):
        return normalized
    ordered_column_ids = sorted(column_selects.keys(), key=panel_column_sort_key)
    for column_id in ordered_column_ids:
        expr = normalize_weave_expression(column_selects.get(column_id))
        column_name = ""
        if isinstance(column_names, dict):
            column_name = str(column_names.get(column_id) or "").strip()
        original_key = ""
        if isinstance(columns_meta, dict):
            original_key = str((columns_meta.get(column_id) or {}).get("originalKey") or "").strip()
        label = column_name or original_key or expression_label(expr) or str(column_id)
        normalized.append(
            {
                "id": str(column_id),
                "label": label,
                "expression": expr,
            }
        )
    return normalized


def normalize_table_sort(sort_spec: Any) -> list[dict[str, Any]]:
    if not isinstance(sort_spec, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in sort_spec:
        if not isinstance(item, dict):
            continue
        column_id = item.get("columnId")
        direction = item.get("dir")
        if column_id and direction in {"asc", "desc"}:
            normalized.append({"column_id": str(column_id), "direction": str(direction)})
    return normalized


def normalize_panel_grid_panels(panels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for panel in panels:
        if not isinstance(panel, dict):
            continue
        view_type = panel.get("viewType") or "Unknown"
        config = panel.get("config", {}) or {}
        payload: dict[str, Any] = {
            "view_type": view_type,
            "layout": panel.get("layout", {}),
        }
        if view_type == "Markdown Panel":
            payload["markdown"] = config.get("value", "")
            normalized.append(payload)
            continue
        if view_type == "Media Browser":
            payload["media_keys"] = [str(item) for item in config.get("mediaKeys", []) if item]
            payload["chart_title"] = config.get("chartTitle")
            normalized.append(payload)
            continue
        if view_type == "Run History Line Plot":
            payload["metrics"] = [str(metric) for metric in config.get("metrics", []) if metric]
            payload["x_axis"] = str(config.get("xAxis") or "_step")
            payload["legend_fields"] = [str(field) for field in config.get("legendFields", []) if field]
            payload["smoothing_type"] = str(config.get("smoothingType") or "")
            payload["smoothing_weight"] = sanitize_json_value(config.get("smoothingWeight"))
            normalized.append(payload)
            continue

        table_key = None
        if view_type == "Vega2":
            user_query = config.get("userQuery", {})
            table_key = extract_table_key_from_expression(user_query)
            payload["vega_spec"] = config.get("customPanelDef", {}).get("spec")
        elif view_type == "Weave":
            panel2_config = config.get("panel2Config", {})
            table_key = extract_table_key_from_expression(panel2_config.get("exp"))
            child_config = panel2_config.get("panelConfig", {}).get("childConfig", {})
            state_table_key = extract_table_key_from_panel_state(panel2_config.get("panelConfig"))
            if state_table_key is None:
                state_table_key = extract_table_key_from_panel_state(config.get("defaultWorkspaceState", {}))
            if state_table_key:
                table_key = state_table_key
            payload["simple_filter"] = parse_simple_weave_filter(
                child_config.get("tableState", {}).get("preFilterFunction")
            )
            payload["panel_id"] = str(panel2_config.get("panelId") or "")
            payload.update(
                extract_weave_panel_config(
                    child_config,
                    config.get("defaultWorkspaceState", {}) or {},
                    payload["panel_id"],
                )
            )
        if table_key:
            payload["table_key"] = table_key
        normalized.append(payload)
    return normalized


def weave_plot_payload(child_config: dict[str, Any]) -> dict[str, Any]:
    series = child_config.get("series") or []
    first_series = series[0] if series else {}
    table = first_series.get("table", {})
    column_selects = table.get("columnSelectFunctions", {})
    return {
        "mode": "plot",
        "plot": {
            "x": extract_pick_key_from_column(column_selects.get("col-0")),
            "y": extract_pick_key_from_column(column_selects.get("col-1")),
            "color": extract_pick_key_from_column(column_selects.get("col-3")),
            "label": extract_pick_key_from_column(column_selects.get("col-4")),
        },
    }


def weave_table_payload(child_config: dict[str, Any]) -> dict[str, Any]:
    table_state = child_config.get("tableState", {}) or {}
    return {
        "mode": "table",
        "table_columns": normalize_table_columns(table_state),
        "table_sort": normalize_table_sort(table_state.get("sort")),
    }


def extract_weave_panel_config(
    child_config: dict[str, Any],
    workspace_state: dict[str, Any] | None = None,
    panel_id: str | None = None,
) -> dict[str, Any]:
    normalized_panel_id = str(panel_id or "").lower()
    if normalized_panel_id.endswith(".table") or ".table" in normalized_panel_id:
        if "tableState" in child_config:
            return weave_table_payload(child_config)
        return {"mode": "table"}
    if normalized_panel_id.endswith(".plot") or ".plot" in normalized_panel_id:
        if "series" in child_config:
            return weave_plot_payload(child_config)
        return {"mode": "plot"}
    if "series" in child_config:
        return weave_plot_payload(child_config)
    if "tableState" in child_config:
        return weave_table_payload(child_config)
    if (workspace_state or {}).get("keyType") == "table-file":
        return {"mode": "table"}
    return {}


def runset_should_resolve_selected_runs(runset: dict[str, Any]) -> bool:
    return runset_selection_mode(runset) == "include"


def extract_pick_key_from_column(node: dict[str, Any] | None) -> str | None:
    if not isinstance(node, dict):
        return None
    from_op = node.get("fromOp", {})
    inputs = from_op.get("inputs", {})
    key = inputs.get("key", {})
    if isinstance(key, dict) and key.get("nodeType") == "const":
        value = key.get("val")
        return str(value) if value is not None else None
    if from_op.get("name") == "run-name":
        return "__run_name"
    return None


def resolve_report(api: Any, config: ExportConfig) -> tuple[Any | None, list[dict[str, Any]], list[str]]:
    report_entity, report_project, report_id = parse_report_url(config.report_url)
    effective_entity = report_entity or config.entity
    effective_project = report_project or config.project
    if effective_entity:
        config.entity = effective_entity
    if effective_project:
        config.project = effective_project
    if not (config.report_url and effective_entity and effective_project):
        return None, [], []

    if gql is not None and report_id:
        try:
            canonical_report_id = report_id if report_id.endswith("==") else f"{report_id}=="
            root_result = api.client.execute(gql(VIEWS2_RAW_VIEW_QUERY), variable_values={"id": canonical_report_id})
            root_view = (root_result or {}).get("view")
            if root_view:
                latest_view = root_view
                for edge in ((root_view.get("children") or {}).get("edges") or []):
                    child = (edge or {}).get("node") or {}
                    child_id = child.get("id")
                    if not child_id:
                        continue
                    try:
                        child_result = api.client.execute(gql(VIEWS2_RAW_VIEW_QUERY), variable_values={"id": child_id})
                    except Exception:
                        continue
                    child_view = (child_result or {}).get("view")
                    if not child_view:
                        continue
                    if str(child_view.get("updatedAt") or "") > str(latest_view.get("updatedAt") or ""):
                        latest_view = child_view
                spec_object = latest_view.get("specObject")
                if isinstance(spec_object, dict):
                    report = SimpleNamespace(
                        id=latest_view.get("id"),
                        url=config.report_url,
                        display_name=latest_view.get("displayName"),
                        spec=spec_object,
                    )
                    runsets = collect_runsets_from_report(spec_object)
                    table_candidates = extract_table_candidates_from_report(spec_object)
                    return report, runsets, table_candidates
        except Exception as exc:
            print(f"[warn] failed to load report via GraphQL Views2RawView: {exc}")

    report_path = f"{effective_entity}/{effective_project}"
    reports = list(api.reports(report_path, per_page=100))
    for report in reports:
        if report.url == config.report_url or report.id.replace("=", "") == (report_id or ""):
            runsets = collect_runsets_from_report(report.spec)
            table_candidates = extract_table_candidates_from_report(report.spec)
            return report, runsets, table_candidates
    print(f"[warn] report {config.report_url} was not found via Public API; falling back to project-wide export")
    return None, [], []


def collect_runs_for_runset(api: Any, fallback_entity: str, fallback_project: str, runset: dict[str, Any], max_runs: int) -> tuple[list[Any], dict[str, Any]]:
    run_project = runset.get("project", {}) or {}
    entity = run_project.get("entityName") or fallback_entity
    project = run_project.get("name") or fallback_project
    selected_run_ids, selected_names = extract_runset_selections(runset)
    selection_mode = runset_selection_mode(runset)
    if selected_run_ids and runset_should_resolve_selected_runs(runset):
        runs: list[Any] = []
        for run_id in selected_run_ids:
            try:
                runs.append(api.run(f"{entity}/{project}/{run_id}"))
            except Exception as exc:
                print(f"[warn] failed to resolve selected run {run_id} in {entity}/{project}: {exc}")
        return runs, {
            "id": runset.get("id"),
            "name": runset.get("name"),
            "entity": entity,
            "project": project,
            "order": normalize_order(runset.get("sort")),
            "filters": normalize_runset_filter_spec(runset.get("filters")),
            "search": runset.get("search", {}),
            "matched_run_count": len(runs),
            "single_run_only": runset.get("single_run_only", False),
            "selection_run_ids": selected_run_ids,
            "selection_names": selected_names,
            "selection_mode": selection_mode,
            "resolution": "selected-run-ids",
        }
    filters = None
    normalized_filter_spec = normalize_runset_filter_spec(runset.get("filters"))
    if normalized_filter_spec and wandb is not None:
        filters = wandb.apis.public.QueryGenerator().filter_to_mongo(normalized_filter_spec)
    if selected_names and runset_should_resolve_selected_runs(runset):
        filters = append_name_selection(filters, selected_names)
    order = normalize_order(runset.get("sort"))
    runs_iter = api.runs(f"{entity}/{project}", filters=filters or None, order=order, per_page=100)
    runs: list[Any] = []
    for index, run in enumerate(runs_iter, start=1):
        if index > max_runs:
            break
        runs.append(run)
    return runs, {
        "id": runset.get("id"),
        "name": runset.get("name"),
        "entity": entity,
        "project": project,
        "order": order,
        "filters": normalized_filter_spec,
        "search": runset.get("search", {}),
        "matched_run_count": len(runs),
        "single_run_only": runset.get("single_run_only", False),
        "selection_run_ids": selected_run_ids,
        "selection_names": selected_names,
        "selection_mode": selection_mode,
        "resolution": "filters",
    }


def derive_table_candidate_names(config: ExportConfig, report_table_candidates: list[str]) -> list[str]:
    names = [config.table_name] if config.table_name else []
    names.extend(report_table_candidates)
    return sorted(dedupe_strings([name for name in names if name]), key=table_name_score, reverse=True)


def artifact_candidate_names(artifact: Any, preferred_names: list[str]) -> list[str]:
    candidates = list(preferred_names)
    try:
        for entry_name in artifact.manifest.entries.keys():
            entry_str = str(entry_name)
            if "table" not in entry_str.lower():
                continue
            candidates.append(entry_str)
            path = Path(entry_str)
            candidates.append(path.stem)
            if path.stem.endswith(".table"):
                candidates.append(path.stem[: -len(".table")])
    except Exception as exc:
        print(f"[warn] failed to inspect artifact manifest for {getattr(artifact, 'source_name', '<artifact>')}: {exc}")
    return sorted(dedupe_strings(candidates), key=table_name_score, reverse=True)


def table_to_rows(frame: pd.DataFrame, artifact: Any, run_map: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    media_manifest: list[dict[str, Any]] = []
    for index, (_, raw_row) in enumerate(frame.iterrows(), start=1):
        normalized = {column: serialise_cell(raw_row[column]) for column in frame.columns}
        run_id = normalized.get("run_id") or next(iter(run_map.keys()), "")
        run_meta = run_map.get(str(run_id), {})
        image_path = None
        for column in frame.columns:
            value = raw_row[column]
            if is_media_like(value):
                serialised_value = serialise_cell(value)
                if isinstance(serialised_value, dict) and serialised_value.get("_kind") == "image":
                    image_path = serialised_value.get("path")
                elif isinstance(serialised_value, str):
                    image_path = serialised_value
                break
        row_id = normalized.get("row_id") or f"{run_id}-row-{index}"
        rows.append(
            {
                "row_id": row_id,
                "run_id": run_id,
                "run_name": run_meta.get("run_name"),
                "model_name": run_meta.get("model_name"),
                "dataset_name": run_meta.get("dataset_name"),
                "split": normalized.get("split") or run_meta.get("eval_split") or "validation",
                "slice_name": normalized.get("slice_name") or "unknown",
                "input_text": normalized.get("input_text") or normalized.get("input") or normalized.get("prompt"),
                "prediction": normalized.get("prediction") or normalized.get("pred"),
                "label": normalized.get("label") or normalized.get("target"),
                "correct": normalized.get("correct"),
                "score": normalized.get("score"),
                "image_thumb_path": image_path,
                "image_full_path": image_path,
                "wandb_run_url": run_meta.get("wandb_url"),
                "wandb_artifact_url": getattr(artifact, "url", None),
                "meta_json": safe_json(normalized),
            }
        )
        if image_path:
            media_manifest.append(
                {
                    "id": row_id,
                    "thumbnail_path": image_path,
                    "full_path": image_path,
                    "kind": "image",
                }
            )
    return rows, media_manifest


def parse_table_prediction_meta_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        frame = pd.read_parquet(path)
    except Exception:
        return []
    return table_prediction_meta_rows_from_records(frame.to_dict(orient="records"))


def table_prediction_meta_rows_from_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        try:
            payload = json.loads(str(record.get("meta_json") or "{}"))
        except Exception:
            continue
        rows.append(
            {
                "__run_id": record.get("run_id"),
                "__run_name": record.get("run_name"),
                "__wandb_url": record.get("wandb_run_url"),
                **payload,
            }
        )
    return rows


def flatten_run(run: Any) -> dict[str, Any]:
    summary = dict(getattr(run, "summary", {}) or {})
    config = dict(getattr(run, "config", {}) or {})
    created_at = getattr(run, "created_at", None)
    updated_at = getattr(run, "updated_at", None)
    project_value = getattr(run, "project", "")
    project_name = project_value.name if hasattr(project_value, "name") else str(project_value or "")
    accuracy = summary.get("eval/accuracy", summary.get("accuracy"))
    loss = summary.get("train/loss", summary.get("loss"))
    model_name = config.get("model_name") or config.get("model") or getattr(run, "name", "unknown-model")
    dataset_name = config.get("dataset_name") or config.get("dataset") or "unknown-dataset"
    return {
        "run_id": getattr(run, "id", ""),
        "run_name": getattr(run, "name", ""),
        "project": project_name,
        "entity": getattr(run, "entity", ""),
        "state": getattr(run, "state", ""),
        "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at or ""),
        "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at or ""),
        "tags_json": safe_json(getattr(run, "tags", [])),
        "config_json": safe_json(config),
        "summary_json": safe_json(summary),
        "wandb_url": getattr(run, "url", ""),
        "group_key": config.get("group") or dataset_name,
        "model_name": str(model_name),
        "dataset_name": str(dataset_name),
        "eval_split": str(config.get("split", "validation")),
        "primary_metric": accuracy,
        "loss": loss,
        "accuracy": accuracy,
    }


def flatten_history(run: Any, metric_requests: dict[str, list[str]], extra_scan_keys: list[str] | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    default_metrics = {metric: aliases for metric, aliases in metric_requests.items() if not str(metric).startswith("system/")}
    system_metrics = {metric: aliases for metric, aliases in metric_requests.items() if str(metric).startswith("system/")}

    if default_metrics:
        try:
            iterator = run.scan_history(page_size=1000)
        except Exception as exc:
            print(f"[warn] failed to read default history for run {getattr(run, 'id', '<unknown>')}: {exc}")
            iterator = []
        for item in iterator:
            step = item.get("_step")
            epoch = item.get("epoch")
            runtime = item.get("_runtime")
            timestamp = item.get("_timestamp")
            for requested_key, aliases in default_metrics.items():
                source_key = next((alias for alias in aliases if alias in item), None)
                if source_key is None:
                    continue
                metric_payload = normalize_history_metric_value(item.get(source_key))
                row = {
                    "run_id": getattr(run, "id", ""),
                    "run_name": getattr(run, "name", ""),
                    "step": sanitize_json_value(step),
                    "epoch": sanitize_json_value(epoch),
                    "runtime": sanitize_json_value(runtime),
                    "metric_name": requested_key,
                    "timestamp": datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                    if isinstance(timestamp, (int, float))
                    else str(timestamp or ""),
                    "timestamp_value": sanitize_json_value(timestamp),
                    "source_metric_name": source_key,
                    **metric_payload,
                }
                for extra_key in extra_scan_keys or []:
                    if extra_key in {requested_key, source_key, "_step", "epoch", "_runtime", "_timestamp"}:
                        continue
                    if extra_key in item:
                        row[extra_key] = sanitize_json_value(item.get(extra_key))
                rows.append(row)

    if system_metrics:
        try:
            system_frame = run.history(samples=5000, stream="system", pandas=True)
        except Exception as exc:
            print(f"[warn] failed to read system history for run {getattr(run, 'id', '<unknown>')}: {exc}")
            system_frame = None
        if system_frame is not None and len(system_frame.index):
            renamed = {column: normalize_system_history_key(column) for column in system_frame.columns}
            system_frame = system_frame.rename(columns=renamed)
            for record in system_frame.to_dict(orient="records"):
                runtime = record.get("_runtime")
                timestamp = record.get("_timestamp")
                for requested_key, aliases in system_metrics.items():
                    source_key = next((alias for alias in aliases if alias in record), None)
                    if source_key is None:
                        continue
                    metric_payload = normalize_history_metric_value(record.get(source_key))
                    rows.append(
                        {
                            "run_id": getattr(run, "id", ""),
                            "run_name": getattr(run, "name", ""),
                            "step": sanitize_json_value(record.get("_step")),
                            "epoch": sanitize_json_value(record.get("epoch")),
                            "runtime": sanitize_json_value(runtime),
                            "metric_name": requested_key,
                            "timestamp": datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                            if isinstance(timestamp, (int, float))
                            else str(timestamp or ""),
                            "timestamp_value": sanitize_json_value(timestamp),
                            "source_metric_name": source_key,
                            **metric_payload,
                        }
                    )
    return rows


def history_cache_path(run: Any, metric_requests: dict[str, list[str]], extra_scan_keys: list[str] | None = None) -> Path:
    run_id = str(getattr(run, "id", "") or "unknown-run")
    updated_at = getattr(run, "updated_at", None)
    updated_token = updated_at.isoformat() if hasattr(updated_at, "isoformat") else str(updated_at or "")
    updated_hash = hashlib.sha1(updated_token.encode("utf-8")).hexdigest()[:10]
    request_hash = history_request_hash(metric_requests, extra_scan_keys)
    return HISTORY_CACHE_DIR / f"{run_id}-{updated_hash}-{request_hash}.parquet"


def cached_flatten_history(run: Any, metric_requests: dict[str, list[str]], extra_scan_keys: list[str] | None = None) -> list[dict[str, Any]]:
    cache_path = history_cache_path(run, metric_requests, extra_scan_keys)
    if cache_path.exists():
        try:
            return pd.read_parquet(cache_path).to_dict(orient="records")
        except Exception as exc:
            print(f"[warn] failed to read history cache {cache_path.name}: {exc}")
    rows = flatten_history(run, metric_requests, extra_scan_keys)
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        if rows:
            pd.DataFrame(rows).to_parquet(cache_path, index=False)
        else:
            pd.DataFrame(
                columns=[
                    "run_id",
                    "run_name",
                    "step",
                    "epoch",
                    "runtime",
                    "metric_name",
                    "metric_value",
                    "metric_value_kind",
                    "metric_value_json",
                    "metric_histogram_count",
                    "metric_histogram_mean",
                    "metric_histogram_std",
                    "metric_histogram_min",
                    "metric_histogram_max",
                    "metric_histogram_q10",
                    "metric_histogram_q25",
                    "metric_histogram_q50",
                    "metric_histogram_q75",
                    "metric_histogram_q90",
                    "timestamp",
                    "timestamp_value",
                    "source_metric_name",
                ]
            ).to_parquet(cache_path, index=False)
    except Exception as exc:
        print(f"[warn] failed to write history cache {cache_path.name}: {exc}")
    return rows


def is_media_like(value: Any) -> bool:
    if hasattr(value, "_path"):
        return True
    if isinstance(value, dict):
        media_type = str(value.get("_type", ""))
        return "image" in media_type or "file" in media_type
    return False


def copy_local_media_file(source_path: str | Path | None, category: str) -> str | None:
    if not source_path:
        return None
    path = Path(str(source_path))
    if not path.exists():
        return None
    category_slug = slugify(category) or "media"
    target_dir = PROCESSED_DIR / "media" / category_slug
    target_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()[:12]
    suffix = path.suffix or ".bin"
    target_path = target_dir / f"{path.stem[:48]}-{digest}{suffix}"
    if not target_path.exists():
        shutil.copy2(path, target_path)
    return f"media/{category_slug}/{target_path.name}"


def class_color(class_id: int) -> tuple[int, int, int]:
    palette = [
        (17, 94, 89),
        (37, 99, 235),
        (220, 38, 38),
        (217, 119, 6),
        (147, 51, 234),
        (5, 150, 105),
        (219, 39, 119),
        (2, 132, 199),
    ]
    return palette[class_id % len(palette)]


def build_mask_overlay(mask_path: str | Path | None, classes: list[dict[str, Any]] | None) -> str | None:
    if not mask_path:
        return None
    path = Path(str(mask_path))
    if not path.exists():
        return None
    class_map = {int(item.get("id", 0)): item for item in classes or [] if isinstance(item, dict) and item.get("id") is not None}
    image = Image.open(path).convert("L")
    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    pixels = image.load()
    overlay_pixels = overlay.load()
    for y in range(image.height):
        for x in range(image.width):
            class_id = int(pixels[x, y] or 0)
            if class_id == 0 and class_id not in class_map:
                continue
            red, green, blue = class_color(class_id)
            alpha = 118 if class_id != 0 else 72
            overlay_pixels[x, y] = (red, green, blue, alpha)
    target_dir = PROCESSED_DIR / "media" / "mask-overlays"
    target_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()[:12]
    target_path = target_dir / f"{path.stem[:48]}-{digest}.png"
    if not target_path.exists():
        overlay.save(target_path, format="PNG")
    return f"media/mask-overlays/{target_path.name}"


def serialise_image_mask(mask: Any, classes: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
    mask_path = copy_local_media_file(getattr(mask, "_path", None), "masks")
    if not mask_path:
        return None
    payload: dict[str, Any] = {
        "_kind": "mask",
        "path": mask_path,
    }
    overlay_path = build_mask_overlay(getattr(mask, "_path", None), classes)
    if overlay_path:
        payload["overlay_path"] = overlay_path
    class_labels = getattr(mask, "_class_labels", None)
    if class_labels:
        payload["class_labels"] = sanitize_json_value(class_labels)
    return payload


def serialise_wandb_image(value: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "_kind": "image",
        "path": copy_local_media_file(getattr(value, "_path", None), "images"),
    }
    caption = getattr(value, "_caption", None)
    if caption:
        payload["caption"] = str(caption)
    classes = getattr(value, "_classes", None)
    class_set = getattr(classes, "_class_set", None) if classes is not None else None
    if class_set:
        payload["classes"] = sanitize_json_value(class_set)
    masks: dict[str, Any] = {}
    for name, mask in (getattr(value, "_masks", {}) or {}).items():
        serialised_mask = serialise_image_mask(mask, sanitize_json_value(class_set) if class_set else None)
        if serialised_mask is not None:
            masks[str(name)] = serialised_mask
    if masks:
        payload["masks"] = masks
    boxes = getattr(value, "_boxes", None)
    if boxes:
        payload["boxes"] = sanitize_json_value(boxes)
    return payload


def serialise_cell(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and not math.isfinite(value):
            return None
        return value
    if hasattr(value, "_path"):
        return serialise_wandb_image(value)
    if isinstance(value, dict):
        return sanitize_json_value(value)
    if isinstance(value, list):
        return sanitize_json_value(value)
    return str(value)


def extract_table_rows(
    api: Any,
    config: ExportConfig,
    run_map: dict[str, dict[str, Any]],
    report_table_candidates: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None, str | None]:
    preferred_names = derive_table_candidate_names(config, report_table_candidates)
    artifacts_to_try: list[Any] = []
    seen_artifacts: set[str] = set()

    if config.table_artifact:
        try:
            artifact = api.artifact(config.table_artifact, type=config.table_artifact_type)
            artifacts_to_try.append(artifact)
            seen_artifacts.add(str(getattr(artifact, "id", config.table_artifact)))
        except Exception as exc:
            print(f"[warn] failed to resolve artifact {config.table_artifact}: {exc}")

    for run_id in run_map:
        run_obj = run_map[run_id]["_run_object"]
        try:
            for artifact in run_obj.logged_artifacts():
                artifact_key = str(getattr(artifact, "id", getattr(artifact, "source_name", "")))
                if artifact_key and artifact_key not in seen_artifacts:
                    seen_artifacts.add(artifact_key)
                    artifacts_to_try.append(artifact)
        except Exception as exc:
            print(f"[warn] failed to inspect artifacts for run {run_id}: {exc}")

    combined_rows: list[dict[str, Any]] = []
    combined_media_manifest: list[dict[str, Any]] = []
    selected_candidate_name: str | None = None
    selected_artifact_name: str | None = None
    seen_row_keys: set[tuple[Any, Any, Any]] = set()
    tasks: list[tuple[int, Any, str]] = []
    task_index = 0
    for artifact in artifacts_to_try:
        for candidate_name in artifact_candidate_names(artifact, preferred_names):
            tasks.append((task_index, artifact, candidate_name))
            task_index += 1

    if tasks:
        log_info(
            "legacy primary table scan: "
            f"{len(artifacts_to_try)} artifacts, {len(tasks)} candidate lookups, "
            f"{export_worker_count(len(tasks))} workers"
        )
        results: list[tuple[int, str, str | None, list[dict[str, Any]], list[dict[str, Any]]]] = []
        completed = 0
        with ThreadPoolExecutor(max_workers=export_worker_count(len(tasks))) as executor:
            future_map = {
                executor.submit(_extract_rows_from_artifact_candidate, artifact, candidate_name, run_map): (index, artifact, candidate_name)
                for index, artifact, candidate_name in tasks
            }
            for future in as_completed(future_map):
                index, artifact, candidate_name = future_map[future]
                completed += 1
                try:
                    rows, media_manifest = future.result()
                except Exception as exc:
                    print(f"[warn] failed to materialize table {candidate_name} from {getattr(artifact, 'source_name', '<artifact>')}: {exc}")
                    continue
                if rows:
                    results.append((index, candidate_name, getattr(artifact, "source_name", None), rows, media_manifest))
                if completed == 1 or completed == len(tasks) or completed % 10 == 0:
                    log_info(
                        "legacy primary table scan progress: "
                        f"{completed}/{len(tasks)} candidates, {len(results)} matching tables"
                    )

        for index, candidate_name, artifact_name, rows, media_manifest in sorted(results, key=lambda item: item[0]):
            if selected_candidate_name is None:
                selected_candidate_name = candidate_name
            if selected_artifact_name is None:
                selected_artifact_name = artifact_name
            for row in rows:
                row_key = (row.get("run_id"), row.get("row_id"), row.get("image_full_path"))
                if row_key in seen_row_keys:
                    continue
                seen_row_keys.add(row_key)
                combined_rows.append(row)
            combined_media_manifest.extend(media_manifest)

    if combined_rows:
        log_info(
            "legacy primary table scan complete: "
            f"{len(combined_rows)} rows from {selected_candidate_name or '<unknown-table>'}"
        )
        return combined_rows, combined_media_manifest, selected_candidate_name, selected_artifact_name

    if preferred_names or artifacts_to_try:
        print("[warn] no compatible W&B table could be exported; continuing without table_predictions.parquet")
    return [], [], None, None


def _extract_rows_from_artifact_candidate(
    artifact: Any,
    candidate_name: str,
    run_map: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        table = artifact.get(candidate_name)
    except Exception:
        return [], []
    if table is None or not hasattr(table, "get_dataframe"):
        return [], []
    frame = table.get_dataframe()
    return table_to_rows(frame, artifact, run_map)


def load_table_file_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    columns = payload.get("columns", [])
    data = payload.get("data", [])
    if not isinstance(columns, list) or not isinstance(data, list):
        return []
    rows: list[dict[str, Any]] = []
    for row in data:
        if not isinstance(row, list):
            continue
        rows.append({str(column): serialise_cell(value) for column, value in zip(columns, row)})
    return rows


def materialize_table_rows_from_artifact(artifact: Any, candidate_names: list[str], run_id: str, run_meta: dict[str, Any]) -> list[dict[str, Any]]:
    for candidate_name in candidate_names:
        try:
            table = artifact.get(candidate_name)
        except Exception:
            continue
        if table is None or not hasattr(table, "get_dataframe"):
            continue
        try:
            frame = table.get_dataframe()
        except Exception as exc:
            print(
                f"[warn] failed to materialize panel table {candidate_name} "
                f"from {getattr(artifact, 'source_name', '<artifact>')}: {exc}"
            )
            continue
        rows: list[dict[str, Any]] = []
        for _, raw_row in frame.iterrows():
            serialized_row = {str(column): serialise_cell(raw_row[column]) for column in frame.columns}
            rows.append(
                {
                    "__run_id": run_id,
                    "__run_name": run_meta.get("run_name"),
                    "__wandb_url": run_meta.get("wandb_url"),
                    **serialized_row,
                }
            )
        if rows:
            return rows
    return []


def artifact_matches_table_key(artifact: Any, table_key: str, artifact_path_hint: str) -> bool:
    lowered_table_key = table_key.lower()
    normalized_table_key = re.sub(r"[^a-z0-9]+", "", lowered_table_key)
    source_name = str(getattr(artifact, "source_name", "") or "").lower()
    normalized_source_name = re.sub(r"[^a-z0-9]+", "", source_name)
    if lowered_table_key in source_name or (normalized_table_key and normalized_table_key in normalized_source_name):
        return True
    if artifact_path_hint:
        hint_name = artifact_path_hint.rsplit("/", 1)[-1].split(":", 1)[0].lower()
        normalized_hint_name = re.sub(r"[^a-z0-9]+", "", hint_name)
        if hint_name and (hint_name in source_name or (normalized_hint_name and normalized_hint_name in normalized_source_name)):
            return True
    try:
        entries = artifact.manifest.entries.keys()
    except Exception:
        return False
    for entry in entries:
        entry_name = str(entry).lower()
        normalized_entry_name = re.sub(r"[^a-z0-9]+", "", entry_name)
        if lowered_table_key in entry_name or (normalized_table_key and normalized_table_key in normalized_entry_name):
            return True
        if artifact_path_hint:
            hint_name = artifact_path_hint.rsplit("/", 1)[-1].split(":", 1)[0].lower()
            normalized_hint_name = re.sub(r"[^a-z0-9]+", "", hint_name)
            if hint_name and (hint_name in entry_name or (normalized_hint_name and normalized_hint_name in normalized_entry_name)):
                return True
    return False


def iter_report_blocks(blocks: list[dict[str, Any]]):
    for block in blocks:
        if not isinstance(block, dict):
            continue
        yield block
        children = block.get("children") or []
        if isinstance(children, list):
            yield from iter_report_blocks(children)


def build_artifact_lineage(api: Any, entity: str, project: str, artifact_name: str, artifact_version: str | None) -> dict[str, Any] | None:
    if not entity or not project or not artifact_name:
        return None
    artifact_ref = f"{entity}/{project}/{artifact_name}:{artifact_version or 'latest'}"
    try:
        artifact = api.artifact(artifact_ref)
    except Exception as exc:
        print(f"[warn] failed to resolve artifact {artifact_ref}: {exc}")
        return None

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()

    def add_node(node_id: str, kind: str, label: str, layer: int, url: str | None = None, meta: dict[str, Any] | None = None) -> None:
        if node_id in seen_nodes:
            return
        seen_nodes.add(node_id)
        payload: dict[str, Any] = {
            "id": node_id,
            "kind": kind,
            "label": label,
            "layer": layer,
        }
        if url:
            payload["url"] = url
        if meta:
            payload["meta"] = sanitize_json_value(meta)
        nodes.append(payload)

    artifact_node_id = f"artifact:{getattr(artifact, 'source_name', artifact_ref)}"
    add_node(
        artifact_node_id,
        "artifact",
        str(getattr(artifact, "source_name", artifact_ref)),
        2,
        getattr(artifact, "url", None),
        {
            "aliases": getattr(artifact, "aliases", None),
            "metadata": getattr(artifact, "metadata", None),
            "description": getattr(artifact, "description", None),
        },
    )

    run = None
    try:
        run = artifact.logged_by()
    except Exception as exc:
        print(f"[warn] failed to resolve logged_by() for {artifact_ref}: {exc}")
    if run is not None:
        run_node_id = f"run:{getattr(run, 'id', '')}"
        add_node(run_node_id, "run", str(getattr(run, "name", getattr(run, "id", "run"))), 1, getattr(run, "url", None))
        edges.append({"source": run_node_id, "target": artifact_node_id, "label": "logged"})
        try:
            for upstream in list(run.used_artifacts())[:24]:
                upstream_id = f"artifact:{getattr(upstream, 'source_name', getattr(upstream, 'name', 'artifact'))}"
                add_node(
                    upstream_id,
                    "artifact",
                    str(getattr(upstream, "source_name", getattr(upstream, "name", "artifact"))),
                    0,
                    getattr(upstream, "url", None),
                )
                edges.append({"source": upstream_id, "target": run_node_id, "label": "used"})
        except Exception as exc:
            print(f"[warn] failed to inspect used_artifacts() for run {getattr(run, 'id', '<unknown>')}: {exc}")
        try:
            for downstream_artifact in list(run.logged_artifacts())[:24]:
                downstream_id = f"artifact:{getattr(downstream_artifact, 'source_name', getattr(downstream_artifact, 'name', 'artifact'))}"
                add_node(
                    downstream_id,
                    "artifact",
                    str(getattr(downstream_artifact, "source_name", getattr(downstream_artifact, "name", "artifact"))),
                    3 if downstream_id != artifact_node_id else 2,
                    getattr(downstream_artifact, "url", None),
                )
                if downstream_id != artifact_node_id:
                    edges.append({"source": run_node_id, "target": downstream_id, "label": "logged"})
        except Exception as exc:
            print(f"[warn] failed to inspect logged_artifacts() for run {getattr(run, 'id', '<unknown>')}: {exc}")

    try:
        for consumer in list(artifact.used_by() or [])[:24]:
            if hasattr(consumer, "id"):
                consumer_id = f"run:{getattr(consumer, 'id', '')}"
                add_node(consumer_id, "run", str(getattr(consumer, "name", getattr(consumer, "id", "run"))), 4, getattr(consumer, "url", None))
                edges.append({"source": artifact_node_id, "target": consumer_id, "label": "used by"})
    except Exception as exc:
        print(f"[warn] failed to inspect used_by() for {artifact_ref}: {exc}")

    return {
        "artifact_ref": artifact_ref,
        "nodes": nodes,
        "edges": edges,
    }


def export_artifact_panels(api: Any, blocks: list[dict[str, Any]], entity: str, project: str) -> None:
    for block in iter_report_blocks(blocks):
        if block.get("type") != "artifact-panel":
            continue
        lineage = build_artifact_lineage(
            api,
            entity,
            project,
            str(block.get("artifact_name") or ""),
            str(block.get("artifact_version") or ""),
        )
        if lineage is not None:
            block["lineage"] = lineage


def collect_panel_table_keys(blocks: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    for block in blocks:
        if block.get("type") != "panel-grid":
            continue
        for panel in block.get("panels", []):
            table_key = panel.get("table_key")
            if isinstance(table_key, str):
                keys.append(table_key)
    return dedupe_strings(keys)


def infer_block_visible_runs(block: dict[str, Any], panel_tables: dict[str, dict[str, Any]]) -> tuple[set[str], set[str]]:
    run_ids: set[str] = set()
    run_names: set[str] = set()
    if block.get("type") != "panel-grid":
        return run_ids, run_names
    for panel in block.get("panels", []):
        if not isinstance(panel, dict):
            continue
        table_key = panel.get("table_key")
        if isinstance(table_key, str):
            table_meta = panel_tables.get(table_key) or {}
            run_ids.update(str(value) for value in table_meta.get("run_ids", []) or [] if value)
            run_names.update(str(value) for value in table_meta.get("run_names", []) or [] if value)
        for item in panel.get("media_items", []) or []:
            if not isinstance(item, dict):
                continue
            run_id = item.get("run_id")
            run_name = item.get("run_name")
            if run_id:
                run_ids.add(str(run_id))
            if run_name:
                run_names.add(str(run_name))
    return run_ids, run_names


def runset_matches_identifier(runset: dict[str, Any], identifier: str) -> bool:
    token = str(identifier or "")
    if not token:
        return False
    runset_id = str(runset.get("id") or "")
    runset_name = str(runset.get("name") or "")
    return token == runset_id or token == runset_name


def enrich_block_visible_runs(
    normalized_blocks: list[dict[str, Any]],
    report_runsets: list[dict[str, Any]],
    panel_tables: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched_blocks: list[dict[str, Any]] = []
    for block in normalized_blocks:
        if block.get("type") != "panel-grid":
            enriched_blocks.append(block)
            continue

        candidate_run_ids, candidate_run_names = infer_block_visible_runs(block, panel_tables)
        visible_run_ids: set[str] = set()
        visible_run_names: set[str] = set()

        matching_runsets = [
            runset
            for runset in report_runsets
            if isinstance(runset, dict)
            and any(runset_matches_identifier(runset, identifier) for identifier in block.get("runsets", []) or [])
        ]
        for runset in matching_runsets:
            selected_run_ids, selected_names = extract_runset_selections(runset)
            selected_run_ids_set = {str(value) for value in selected_run_ids if value}
            selected_names_set = {str(value) for value in selected_names if value}
            selection_mode = runset_selection_mode(runset)
            if selection_mode == "include":
                visible_run_ids.update(candidate_run_ids & selected_run_ids_set)
                visible_run_names.update(candidate_run_names & selected_names_set)
                continue
            if selection_mode == "exclude" and candidate_run_ids and selected_run_ids_set.intersection(candidate_run_ids):
                visible_run_ids.update(candidate_run_ids - selected_run_ids_set)
            if selection_mode == "exclude" and candidate_run_names and selected_names_set.intersection(candidate_run_names):
                visible_run_names.update(candidate_run_names - selected_names_set)

        if visible_run_ids or visible_run_names:
            enriched_blocks.append(
                {
                    **block,
                    "visible_run_ids": sorted(visible_run_ids),
                    "visible_run_names": sorted(visible_run_names),
                }
            )
        else:
            enriched_blocks.append(block)
    return enriched_blocks


def enrich_runset_visible_runs(
    report_runsets: list[dict[str, Any]],
    normalized_blocks: list[dict[str, Any]],
    panel_tables: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if not report_runsets:
        return report_runsets
    enriched: list[dict[str, Any]] = []
    for runset in report_runsets:
        candidate_run_ids: set[str] = set()
        candidate_run_names: set[str] = set()
        for block in iter_report_blocks(normalized_blocks):
            identifiers = [str(value) for value in block.get("runsets", []) or [] if value]
            if not any(runset_matches_identifier(runset, identifier) for identifier in identifiers):
                continue
            block_run_ids, block_run_names = infer_block_visible_runs(block, panel_tables)
            candidate_run_ids.update(block_run_ids)
            candidate_run_names.update(block_run_names)

        selected_run_ids, selected_names = extract_runset_selections(runset)
        selected_run_ids_set = {str(value) for value in selected_run_ids if value}
        selected_names_set = {str(value) for value in selected_names if value}
        selection_mode = runset_selection_mode(runset)

        visible_run_ids: list[str] = []
        visible_run_names: list[str] = []
        if selection_mode == "include":
            if candidate_run_ids:
                visible_run_ids = sorted(candidate_run_ids & selected_run_ids_set)
            if candidate_run_names:
                visible_run_names = sorted(candidate_run_names & selected_names_set)
        elif selection_mode == "exclude":
            if candidate_run_ids and selected_run_ids_set.intersection(candidate_run_ids):
                remaining_run_ids = sorted(candidate_run_ids - selected_run_ids_set)
                if remaining_run_ids:
                    visible_run_ids = remaining_run_ids
            if candidate_run_names and selected_names_set.intersection(candidate_run_names):
                remaining_run_names = sorted(candidate_run_names - selected_names_set)
                if remaining_run_names:
                    visible_run_names = remaining_run_names

        enriched.append(
            {
                **runset,
                "visible_run_ids": visible_run_ids,
                "visible_run_names": visible_run_names,
            }
        )
    return enriched


def iter_panel_grids(blocks: list[dict[str, Any]]):
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "panel-grid":
            yield block
        for child in block.get("children", []) or []:
            if isinstance(child, dict):
                yield from iter_panel_grids([child])


def download_run_media_file(run_obj: Any, remote_path: str) -> str:
    source_name = Path(remote_path).name
    category = Path(remote_path).parent.name or "media"
    target_dir = PROCESSED_DIR / "media" / category
    target_dir.mkdir(parents=True, exist_ok=True)
    downloaded = run_obj.file(remote_path).download(root=str(PROCESSED_DIR), replace=True)
    source_path = Path(downloaded.name)
    target_path = target_dir / source_name
    if source_path.resolve() != target_path.resolve():
        shutil.copy2(source_path, target_path)
    return f"media/{category}/{target_path.name}"


def export_media_browser_panels(
    api: Any,
    blocks: list[dict[str, Any]],
    report_runsets: list[dict[str, Any]],
    fallback_entity: str,
    fallback_project: str,
) -> None:
    runset_cache: dict[str, list[Any]] = {}

    for block in iter_panel_grids(blocks):
        runset_names = set(block.get("runsets") or [])
        if not runset_names:
            continue
        matching_runsets = [
            runset
            for runset in report_runsets
            if any(runset_matches_identifier(runset, identifier) for identifier in runset_names)
        ]
        if not matching_runsets:
            continue

        for panel in block.get("panels", []):
            if panel.get("view_type") != "Media Browser":
                continue
            media_keys = [key for key in panel.get("media_keys", []) if isinstance(key, str)]
            if not media_keys:
                continue

            tasks: list[tuple[Any, str]] = []
            for runset in matching_runsets:
                cache_key = str(runset.get("id") or runset.get("name"))
                if cache_key not in runset_cache:
                    runs, _summary = collect_runs_for_runset(api, fallback_entity, fallback_project, runset, max_runs=5)
                    runset_cache[cache_key] = runs
                for run in runset_cache[cache_key]:
                    for media_key in media_keys:
                        tasks.append((run, media_key))
            media_items: list[dict[str, Any]] = []
            if tasks:
                log_info(
                    "media browser export: "
                    f"{panel.get('chart_title') or ', '.join(media_keys[:2])} "
                    f"({len(tasks)} assets, {export_worker_count(len(tasks))} workers)"
                )
            with ThreadPoolExecutor(max_workers=export_worker_count(len(tasks))) as executor:
                future_map = {
                    executor.submit(_export_media_browser_item, run, media_key, panel.get("chart_title")): (run, media_key)
                    for run, media_key in tasks
                }
                for future in as_completed(future_map):
                    run, media_key = future_map[future]
                    try:
                        item = future.result()
                    except Exception as exc:
                        print(f"[warn] failed to export media browser asset {media_key} from run {getattr(run, 'id', '<unknown>')}: {exc}")
                        continue
                    if item is not None:
                        media_items.append(item)
            if media_items:
                panel["media_items"] = media_items
                log_info(
                    "media browser export complete: "
                    f"{panel.get('chart_title') or ', '.join(media_keys[:2])} -> {len(media_items)} assets"
                )


def _export_media_browser_item(run: Any, media_key: str, chart_title: str | None) -> dict[str, Any] | None:
    media_ref = getattr(run, "summary", {}).get(media_key)
    if not hasattr(media_ref, "get"):
        return None
    remote_path = media_ref.get("path")
    media_type = str(media_ref.get("_type") or "")
    if not remote_path:
        return None
    local_path = download_run_media_file(run, remote_path)
    return {
        "key": media_key,
        "kind": media_type or "file",
        "path": local_path,
        "run_id": getattr(run, "id", None),
        "run_name": getattr(run, "name", None),
        "title": chart_title,
    }


def export_panel_tables(
    run_map: dict[str, dict[str, Any]],
    panel_table_keys: list[str],
    hydrated_rows: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    if not panel_table_keys:
        return {}
    target_dir = PROCESSED_DIR / PANEL_TABLES_DIRNAME
    target_dir.mkdir(parents=True, exist_ok=True)
    exports: dict[str, dict[str, Any]] = {}
    for table_key in panel_table_keys:
        log_info(f"panel table export: {table_key} across {len(run_map)} runs")
        combined_rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=export_worker_count(len(run_map))) as executor:
            future_map = {
                executor.submit(_load_panel_table_rows_for_run, table_key, run_id, run_meta): run_id
                for run_id, run_meta in run_map.items()
            }
            for future in as_completed(future_map):
                run_id = future_map[future]
                try:
                    rows = future.result()
                except Exception as exc:
                    print(f"[warn] failed to download panel table {table_key} for run {run_id}: {exc}")
                    continue
                combined_rows.extend(rows)
        if not combined_rows:
            continue
        needs_hydration = False
        if any(isinstance(row, dict) and row.get("Image") == "Image" for row in combined_rows):
            fallback_rows = hydrated_rows or parse_table_prediction_meta_rows(PROCESSED_DIR / "table_predictions.parquet")
            if fallback_rows:
                combined_rows = fallback_rows
            else:
                needs_hydration = True
        target_path = target_dir / f"{slugify(table_key)}.json"
        dump_json(target_path, combined_rows)
        run_ids = sorted(
            {
                str(row.get("__run_id"))
                for row in combined_rows
                if isinstance(row, dict) and row.get("__run_id") not in (None, "")
            }
        )
        run_names = sorted(
            {
                str(row.get("__run_name"))
                for row in combined_rows
                if isinstance(row, dict) and row.get("__run_name") not in (None, "")
            }
        )
        exports[table_key] = {
            "path": f"{PANEL_TABLES_DIRNAME}/{target_path.name}",
            "rows": len(combined_rows),
            "run_ids": run_ids,
            "run_names": run_names,
            "needs_hydration": needs_hydration,
        }
        log_info(f"panel table export complete: {table_key} -> {len(combined_rows)} rows")
    return exports


def _load_panel_table_rows_for_run(table_key: str, run_id: str, run_meta: dict[str, Any]) -> list[dict[str, Any]]:
    run_obj = run_meta["_run_object"]
    table_ref = getattr(run_obj, "summary", {}).get(table_key)
    artifact_path_hint = ""
    preferred_names = [table_key, slugify(table_key), slugify(table_key).replace("-", "_")]
    direct_path = None
    direct_type = ""
    if hasattr(table_ref, "get"):
        artifact_path_hint = str(table_ref.get("artifact_path") or table_ref.get("_latest_artifact_path") or "")
        direct_path = table_ref.get("path")
        direct_type = str(table_ref.get("_type") or "")
        artifact_hint_name = Path(artifact_path_hint).name if artifact_path_hint else ""
        if artifact_hint_name:
            preferred_names = dedupe_strings(
                [
                    artifact_hint_name,
                    Path(artifact_hint_name).stem,
                    Path(artifact_hint_name).stem.removesuffix(".table"),
                    *preferred_names,
                ]
            )
    try:
        for artifact in run_obj.logged_artifacts():
            if not artifact_matches_table_key(artifact, table_key, artifact_path_hint):
                continue
            rows = materialize_table_rows_from_artifact(
                artifact,
                artifact_candidate_names(artifact, preferred_names),
                run_id,
                run_meta,
            )
            if rows:
                return rows
    except Exception as exc:
        print(f"[warn] failed to materialize panel table {table_key} for run {run_id}: {exc}")
    if direct_path and direct_type == "table-file":
        try:
            downloaded = run_obj.file(direct_path).download(root=str(PROCESSED_DIR), replace=True)
            table_rows = load_table_file_rows(Path(downloaded.name))
            return [
                {
                    "__run_id": run_id,
                    "__run_name": run_meta.get("run_name"),
                    "__wandb_url": run_meta.get("wandb_url"),
                    **row,
                }
                for row in table_rows
            ]
        except Exception as exc:
            print(f"[warn] failed to download direct table-file {table_key} for run {run_id}: {exc}")
    if not hasattr(table_ref, "get"):
        return []
    path = table_ref.get("path")
    if not path:
        return []
    downloaded = run_obj.file(path).download(root=str(PROCESSED_DIR), replace=True)
    table_rows = load_table_file_rows(Path(downloaded.name))
    return [
        {
            "__run_id": run_id,
            "__run_name": run_meta.get("run_name"),
            "__wandb_url": run_meta.get("wandb_url"),
            **row,
        }
        for row in table_rows
    ]


def build_manifest(
    config: ExportConfig,
    run_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    table_rows: list[dict[str, Any]],
    media_items: list[dict[str, Any]],
    source: str,
    report_data: dict[str, Any] | None = None,
    panel_tables: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    overview_description = (
        f"{len(run_rows)} runs, {len(history_rows)} history points, "
        f"{len(table_rows)} table rows captured at build-time."
    )
    return {
        "generated_at": generated_at,
        "source": source,
        "entity": config.entity,
        "project": config.project,
        "report_url": config.report_url,
        "counts": {
            "runs": len(run_rows),
            "history_rows": len(history_rows),
            "table_rows": len(table_rows),
            "media_items": len(media_items),
        },
        "datasets": {
            "run_summary": "run_summary.parquet",
            "history_eval_metrics": "history_eval_metrics.parquet",
            "table_predictions": "table_predictions.parquet",
            "media_manifest": "media_manifest.json",
            "report_content": "report_content.json",
            "panel_tables": panel_tables or {},
        },
        "report": report_data or {"blocks": []},
        "pages": [
            {
                "slug": "overview",
                "title": "Overview",
                "description": overview_description,
                "datasets": ["run_summary.parquet", "history_eval_metrics.parquet", "report_content.json"],
                "widgets": ["report-content", "summary-cards", "line-chart", "leaderboard-preview"],
            },
            {
                "slug": "leaderboard",
                "title": "Leaderboard",
                "description": "Fast run comparison with client-side sorting and filtering.",
                "datasets": ["run_summary.parquet"],
                "widgets": ["ag-grid"],
            },
            {
                "slug": "failure-cases",
                "title": "Failure Cases",
                "description": "Per-example prediction browsing with local thumbnails.",
                "datasets": ["table_predictions.parquet"],
                "widgets": ["ag-grid", "media"],
            },
            {
                "slug": "pivot-explorer",
                "title": "Pivot Explorer",
                "description": "Perspective-powered slice analysis for self-service breakdowns.",
                "datasets": ["run_summary.parquet", "table_predictions.parquet"],
                "widgets": ["perspective"],
            },
        ],
    }


def persist_snapshot(
    run_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    table_rows: list[dict[str, Any]],
    media_items: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    write_parquet_with_columns(
        PROCESSED_DIR / "run_summary.parquet",
        run_rows,
        [
            "run_id",
            "run_name",
            "project",
            "entity",
            "state",
            "created_at",
            "updated_at",
            "tags_json",
            "config_json",
            "summary_json",
            "wandb_url",
            "group_key",
            "model_name",
            "dataset_name",
            "eval_split",
            "primary_metric",
            "loss",
            "accuracy",
        ],
    )
    write_parquet_with_columns(
        PROCESSED_DIR / "history_eval_metrics.parquet",
        history_rows,
        [
            "run_id",
            "run_name",
            "step",
            "epoch",
            "runtime",
            "metric_name",
            "metric_value",
            "metric_value_kind",
            "metric_value_json",
            "metric_histogram_count",
            "metric_histogram_mean",
            "metric_histogram_std",
            "metric_histogram_min",
            "metric_histogram_max",
            "metric_histogram_q10",
            "metric_histogram_q25",
            "metric_histogram_q50",
            "metric_histogram_q75",
            "metric_histogram_q90",
            "timestamp",
            "timestamp_value",
            "source_metric_name",
        ],
    )
    write_parquet_with_columns(
        PROCESSED_DIR / "table_predictions.parquet",
        table_rows,
        [
            "row_id",
            "run_id",
            "run_name",
            "model_name",
            "dataset_name",
            "split",
            "slice_name",
            "input_text",
            "prediction",
            "label",
            "correct",
            "score",
            "image_thumb_path",
            "image_full_path",
            "wandb_run_url",
            "wandb_artifact_url",
            "meta_json",
        ],
    )
    dump_json(PROCESSED_DIR / "media_manifest.json", media_items)
    dump_json(PROCESSED_DIR / "report_manifest.json", manifest)
    dump_json(PROCESSED_DIR / "report_content.json", manifest.get("report", {}))

    for file_name in (
        "run_summary.parquet",
        "history_eval_metrics.parquet",
        "table_predictions.parquet",
        "media_manifest.json",
        "report_manifest.json",
        "report_content.json",
    ):
        shutil.copy2(PROCESSED_DIR / file_name, APP_DATA_DIR / file_name)

    media_src = PROCESSED_DIR / "media"
    if media_src.exists():
        shutil.copytree(media_src, APP_MEDIA_DIR, dirs_exist_ok=True)
    panel_tables_src = PROCESSED_DIR / PANEL_TABLES_DIRNAME
    if panel_tables_src.exists():
        shutil.copytree(panel_tables_src, APP_DATA_DIR / PANEL_TABLES_DIRNAME, dirs_exist_ok=True)


def real_snapshot(config: ExportConfig) -> None:
    if wandb is None:
        raise RuntimeError("wandb is not installed; run `uv sync` first.")

    api = wandb.Api(timeout=60)
    phase_started = time.perf_counter()
    report, report_runsets, report_table_candidates = resolve_report(api, config)
    log_info(
        "resolved report metadata in "
        f"{format_duration(time.perf_counter() - phase_started)}: "
        f"{len(report_runsets)} runsets, {len(report_table_candidates)} table candidates"
    )

    selected_runset_summary = None
    selected_runs: list[Any] = []
    resolved_report_runs: list[Any] = []
    if report_runsets:
        log_info(f"resolving runs from {len(report_runsets)} report runsets")
        evaluated: list[tuple[list[Any], dict[str, Any]]] = []
        for runset in report_runsets:
            runs, summary = collect_runs_for_runset(api, config.entity or "", config.project or "", runset, config.max_runs)
            evaluated.append((runs, summary))
        seen_run_ids: set[str] = set()
        for runs, _summary in evaluated:
            for run in runs:
                run_id = str(getattr(run, "id", "") or "")
                if run_id and run_id not in seen_run_ids:
                    seen_run_ids.add(run_id)
                    resolved_report_runs.append(run)
        if evaluated:
            selected_runs, selected_runset_summary = max(
                evaluated,
                key=lambda item: (item[1]["matched_run_count"], 0 if item[1]["single_run_only"] else 1),
            )
    if selected_runset_summary is not None:
        log_info(
            "selected runset: "
            f"{selected_runset_summary.get('name') or '<unnamed>'} "
            f"({selected_runset_summary.get('resolution')}, {len(selected_runs)} runs)"
        )

    if resolved_report_runs:
        selected_runs = resolved_report_runs
        log_info(f"resolved {len(selected_runs)} unique runs across all report runsets")

    if not selected_runs:
        if config.report_url:
            raise RuntimeError(
                "No runs were resolved from WANDB_REPORT_URL. "
                "Refusing to fall back to the whole project because that can explode export time. "
                "Check the report URL or increase WANDB_MAX_RUNS if the report selection is larger than the current cap."
            )
        project_path = f"{config.entity}/{config.project}"
        runs_iter = api.runs(project_path, per_page=50, order="-created_at")
        selected_runs = []
        for index, run in enumerate(runs_iter, start=1):
            if index > config.max_runs:
                break
            selected_runs.append(run)
        log_info(f"fell back to project scan: {len(selected_runs)} runs")

    normalized_blocks = normalize_report_blocks(report.spec) if report is not None else []
    history_metric_keys = dedupe_strings(config.history_keys + extract_history_metrics_from_blocks(normalized_blocks))
    history_metric_requests = build_history_metric_requests(history_metric_keys)
    history_axis_keys = [
        axis for axis in extract_history_axes_from_blocks(normalized_blocks) if axis not in {"_step", "epoch", "_runtime", "_timestamp"}
    ]

    run_rows: list[dict[str, Any]] = []
    history_rows: list[dict[str, Any]] = []
    run_map: dict[str, dict[str, Any]] = {}

    for run in selected_runs:
        flat = flatten_run(run)
        run_rows.append(flat)
        run_map[flat["run_id"]] = {**flat, "_run_object": run}
    log_info(f"prepared {len(run_rows)} run summaries")
    if history_metric_requests:
        history_scan_keys = dedupe_strings(history_axis_keys)
        history_workers = export_worker_count(len(selected_runs))
        history_phase_started = time.perf_counter()
        history_cache_hits = 0
        completed_history = 0
        log_info(
            "history export: "
            f"{len(selected_runs)} runs, {len(history_metric_requests)} requested metrics, {history_workers} workers"
        )
        with ThreadPoolExecutor(max_workers=history_workers) as executor:
            future_map = {
                executor.submit(cached_flatten_history, run, history_metric_requests, history_scan_keys): (
                    run,
                    history_cache_exists(run, history_metric_requests, history_scan_keys),
                )
                for run in selected_runs
            }
            for future in as_completed(future_map):
                run, cache_hit = future_map[future]
                completed_history += 1
                if cache_hit:
                    history_cache_hits += 1
                try:
                    history_rows.extend(future.result())
                except Exception as exc:
                    print(f"[warn] failed to flatten history for run {getattr(run, 'id', '<unknown>')}: {exc}")
                else:
                    log_info(
                        "history export progress: "
                        f"{completed_history}/{len(selected_runs)} runs complete "
                        f"({'cache' if cache_hit else 'live'})"
                    )
        log_info(
            "history export complete: "
            f"{len(history_rows)} rows in {format_duration(time.perf_counter() - history_phase_started)} "
            f"({history_cache_hits}/{len(selected_runs)} cache hits)"
        )

    if config.enable_primary_table_scan:
        table_phase_started = time.perf_counter()
        table_rows, media_items, selected_table_name, selected_table_artifact = extract_table_rows(
            api,
            config,
            run_map,
            report_table_candidates,
        )
        log_info(
            "legacy primary table phase complete: "
            f"{len(table_rows)} rows in {format_duration(time.perf_counter() - table_phase_started)}"
        )
    else:
        log_info(
            "skipping legacy primary table scan; marimo uses panel tables directly. "
            "Set WANDB_ENABLE_PRIMARY_TABLE_SCAN=1 or pass --enable-primary-table-scan to opt in."
        )
        table_rows, media_items, selected_table_name, selected_table_artifact = [], [], None, None
    if report is not None:
        phase_started = time.perf_counter()
        export_media_browser_panels(api, normalized_blocks, report_runsets, config.entity or "", config.project or "")
        export_artifact_panels(api, normalized_blocks, config.entity or "", config.project or "")
        log_info(f"report media/artifact export complete in {format_duration(time.perf_counter() - phase_started)}")
    phase_started = time.perf_counter()
    panel_table_keys = collect_panel_table_keys(normalized_blocks)
    hydrated_panel_rows = table_prediction_meta_rows_from_records(table_rows) if table_rows else []
    panel_tables = export_panel_tables(run_map, panel_table_keys, hydrated_rows=hydrated_panel_rows)
    log_info(f"panel table export complete in {format_duration(time.perf_counter() - phase_started)}")
    normalized_blocks = enrich_block_visible_runs(normalized_blocks, report_runsets, panel_tables)
    report_runsets = enrich_runset_visible_runs(report_runsets, normalized_blocks, panel_tables)
    report_data = {
        "report_url": config.report_url,
        "title": getattr(report, "display_name", None),
        "selected_table_name": selected_table_name,
        "selected_table_artifact": selected_table_artifact,
        "selected_runset": selected_runset_summary,
        "runsets": report_runsets,
        "table_candidates": report_table_candidates,
        "blocks": normalized_blocks,
        "panel_tables": panel_tables,
    }
    manifest = build_manifest(
        config=config,
        run_rows=run_rows,
        history_rows=history_rows,
        table_rows=table_rows,
        media_items=media_items,
        source="wandb",
        report_data=report_data,
        panel_tables=panel_tables,
    )
    phase_started = time.perf_counter()
    persist_snapshot(run_rows, history_rows, table_rows, media_items, manifest)
    log_info(f"persisted snapshot files in {format_duration(time.perf_counter() - phase_started)}")


def main() -> None:
    config = parse_args()
    begin_export_timer()
    stage_root = begin_snapshot_output()
    try:
        if config.sample_data:
            log_info("generating sample snapshot (set WANDB_API_KEY + entity/project to export real data)")
            sample_snapshot(config)
        else:
            if config.report_url:
                log_info(f"exporting snapshot for {config.report_url}")
            else:
                log_info(f"exporting snapshot from {config.entity}/{config.project}")
            real_snapshot(config)
        commit_snapshot_output(stage_root)
    except Exception:
        cleanup_staging_output(stage_root)
        raise
    print(f"[done] snapshot available in {FINAL_PROCESSED_DIR}")


if __name__ == "__main__":
    main()

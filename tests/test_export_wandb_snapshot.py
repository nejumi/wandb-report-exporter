from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.export_wandb_snapshot import (
    build_history_metric_requests,
    enrich_block_visible_runs,
    collect_runsets_from_report,
    collect_runs_for_runset,
    enrich_runset_visible_runs,
    extract_weave_panel_config,
    flatten_history,
    extract_runset_selections,
    normalize_runset_filter_spec,
    runset_is_single_run,
    runset_should_resolve_selected_runs,
    export_panel_tables,
    table_prediction_meta_rows_from_records,
    normalize_history_metric_value,
    extract_table_candidates_from_report,
    extract_table_key_from_panel_state,
    infer_base_url,
    load_table_file_rows,
    find_matching_snapshot_archive,
    row_has_media_placeholders,
    snapshot_cache_metadata,
    snapshot_archive_dir,
)


class ExportWandbSnapshotTests(unittest.TestCase):
    def test_collect_runsets_from_report_reads_metadata_runsets(self) -> None:
        spec = {
            "blocks": [
                {
                    "type": "panel-grid",
                    "metadata": {
                        "runSets": [
                            {
                                "id": "abc123xyz",
                                "name": "Run set",
                                "enabled": True,
                                "project": {"entityName": "entity", "name": "project"},
                                "selections": {"tree": ["0thca7ud", "b8pzxvg0"]},
                                "filters": {
                                    "op": "OR",
                                    "filters": [
                                        {
                                            "op": "AND",
                                            "filters": [
                                                {
                                                    "key": {"section": "run", "name": ""},
                                                    "op": "=",
                                                    "value": "",
                                                    "disabled": True,
                                                }
                                            ],
                                        }
                                    ],
                                },
                            }
                        ]
                    },
                }
            ]
        }
        runsets = collect_runsets_from_report(spec)
        self.assertEqual(len(runsets), 1)
        self.assertEqual(runsets[0]["id"], "abc123xyz")
        self.assertEqual(runsets[0]["project"]["entityName"], "entity")
        self.assertFalse(runsets[0]["only_show_selected"])

    def test_extract_runset_selections_treats_tree_values_as_run_ids(self) -> None:
        runset = {
            "selections": {
                "tree": [
                    "0thca7ud",
                    {"children": ["b8pzxvg0", "friendly-name"]},
                ]
            }
        }
        run_ids, names = extract_runset_selections(runset)
        self.assertEqual(run_ids, ["0thca7ud", "b8pzxvg0"])
        self.assertEqual(names, ["friendly-name"])

    def test_normalize_runset_filter_spec_drops_disabled_empty_filters(self) -> None:
        filters = {
            "op": "OR",
            "filters": [
                {
                    "op": "AND",
                    "filters": [
                        {
                            "key": {"section": "run", "name": ""},
                            "op": "=",
                            "value": "",
                            "disabled": True,
                        }
                    ],
                }
            ],
        }
        self.assertIsNone(normalize_runset_filter_spec(filters))

    def test_runset_is_single_run_when_one_selected_id(self) -> None:
        runset = {"selections": {"tree": ["0thca7ud"]}, "filters": {}}
        self.assertTrue(runset_is_single_run(runset))

    def test_extract_weave_panel_config_prefers_table_mode_from_panel_id(self) -> None:
        child_config = {
            "series": [
                {
                    "table": {
                        "columnSelectFunctions": {
                            "col-0": {"fromOp": {"inputs": {"key": {"nodeType": "const", "val": "vehicle IoU"}}}},
                            "col-1": {"fromOp": {"inputs": {"key": {"nodeType": "const", "val": "road IoU"}}}},
                        }
                    }
                }
            ],
            "tableState": {
                "columnSelectFunctions": {
                    "col-0": {"fromOp": {"name": "run-name", "inputs": {}}},
                    "col-1": {"fromOp": {"inputs": {"key": {"nodeType": "const", "val": "Image"}}}},
                },
                "columnNames": {"col-0": "Run", "col-1": "Image"},
            },
        }
        payload = extract_weave_panel_config(child_config, {"keyType": "table-file"}, "merge.table")
        self.assertEqual(payload["mode"], "table")
        self.assertEqual(payload["table_columns"][0]["label"], "Run")

    def test_extract_weave_panel_config_prefers_plot_mode_from_panel_id(self) -> None:
        child_config = {
            "series": [
                {
                    "table": {
                        "columnSelectFunctions": {
                            "col-0": {"fromOp": {"inputs": {"key": {"nodeType": "const", "val": "vehicle IoU"}}}},
                            "col-1": {"fromOp": {"inputs": {"key": {"nodeType": "const", "val": "road IoU"}}}},
                            "col-3": {"fromOp": {"name": "run-name", "inputs": {}}},
                        }
                    }
                }
            ],
            "tableState": {
                "columnSelectFunctions": {
                    "col-0": {"fromOp": {"inputs": {"key": {"nodeType": "const", "val": "Image"}}}}
                }
            },
        }
        payload = extract_weave_panel_config(child_config, {"keyType": "table-file"}, "merge.plot")
        self.assertEqual(payload["mode"], "plot")
        self.assertEqual(payload["plot"]["x"], "vehicle IoU")
        self.assertEqual(payload["plot"]["color"], "__run_name")

    def test_extract_table_candidates_from_report_reads_union_table_descriptors(self) -> None:
        spec = {
            "type": "typedDict",
            "propertyTypes": {
                "validation_prediction_table": {
                    "type": "union",
                    "members": [
                        "none",
                        {
                            "type": "file",
                            "wbObjectType": {"type": "table"},
                        },
                    ],
                }
            },
        }
        self.assertIn("validation_prediction_table", extract_table_candidates_from_report(spec))

    def test_extract_table_key_from_panel_state_reads_working_key_and_type(self) -> None:
        node = {
            "panelConfig": {
                "workingKeyAndType": {
                    "key": "validation_prediction_table",
                    "type": "table-file",
                }
            }
        }
        self.assertEqual(extract_table_key_from_panel_state(node), "validation_prediction_table")

    def test_infer_base_url_skips_public_wandb_host(self) -> None:
        self.assertIsNone(
            infer_base_url("https://wandb.ai/entity/project/reports/Name--VmlldzoxMjM0")
        )

    def test_infer_base_url_uses_dedicated_host(self) -> None:
        self.assertEqual(
            infer_base_url("https://wandb.my-company.example/entity/project/reports/Name--VmlldzoxMjM0"),
            "https://wandb.my-company.example",
        )

    def test_runset_should_resolve_selected_runs_only_when_selected_view_is_active(self) -> None:
        self.assertTrue(runset_should_resolve_selected_runs({"only_show_selected": True}))
        self.assertTrue(runset_should_resolve_selected_runs({"single_run_only": True}))
        self.assertFalse(runset_should_resolve_selected_runs({"only_show_selected": False, "single_run_only": False}))

    def test_collect_runs_for_runset_uses_filters_when_only_show_selected_is_false(self) -> None:
        class DummyRun:
            def __init__(self, run_id: str) -> None:
                self.id = run_id

        class DummyApi:
            def __init__(self) -> None:
                self.run_calls: list[str] = []
                self.runs_calls: list[tuple[str, object, str, int]] = []

            def run(self, path: str):
                self.run_calls.append(path)
                return DummyRun(path.rsplit("/", 1)[-1])

            def runs(self, path: str, filters=None, order=None, per_page=100):
                self.runs_calls.append((path, filters, order, per_page))
                return [DummyRun("glorious"), DummyRun("comfy")]

        api = DummyApi()
        runs, summary = collect_runs_for_runset(
            api,
            "wandb-japan",
            "ADAS-Segmentation",
            {
                "project": {"entityName": "wandb-japan", "name": "ADAS-Segmentation"},
                "selections": {"tree": ["0thca7ud", "b8pzxvg0"]},
                "filters": {"op": "OR", "filters": []},
                "sort": {"keys": [{"key": {"section": "run", "name": "createdAt"}, "ascending": False}]},
                "only_show_selected": False,
                "single_run_only": False,
            },
            max_runs=5,
        )
        self.assertEqual(api.run_calls, [])
        self.assertEqual(len(api.runs_calls), 1)
        self.assertEqual(summary["resolution"], "filters")
        self.assertEqual([run.id for run in runs], ["glorious", "comfy"])

    def test_enrich_runset_visible_runs_prefers_panel_rows_minus_hidden_selection(self) -> None:
        runsets = [
            {
                "id": "abc123xyz",
                "name": "Run set",
                "only_show_selected": False,
                "selections": {"tree": ["0thca7ud", "b8pzxvg0"]},
            }
        ]
        blocks = [
            {
                "type": "panel-grid",
                "runsets": ["Run set"],
                "panels": [
                    {"table_key": "pred_table"},
                ],
            }
        ]
        panel_tables = {
            "pred_table": {
                "run_ids": ["0thca7ud", "b1gishis", "gtqhzr20"],
                "run_names": ["fine-firebrand-18", "glorious-lake-18", "comfy-resonance-3"],
            }
        }
        enriched = enrich_runset_visible_runs(runsets, blocks, panel_tables)
        self.assertEqual(enriched[0]["visible_run_ids"], ["b1gishis", "gtqhzr20"])

    def test_enrich_runset_visible_runs_keeps_selected_rows_when_only_show_selected(self) -> None:
        runsets = [
            {
                "id": "abc123xyz",
                "name": "Run set",
                "only_show_selected": True,
                "selections": {"tree": ["b1gishis", "gtqhzr20"]},
            }
        ]
        blocks = [
            {
                "type": "panel-grid",
                "runsets": ["Run set"],
                "panels": [
                    {"table_key": "pred_table"},
                ],
            }
        ]
        panel_tables = {
            "pred_table": {
                "run_ids": ["0thca7ud", "b1gishis", "gtqhzr20"],
                "run_names": ["fine-firebrand-18", "glorious-lake-18", "comfy-resonance-3"],
            }
        }
        enriched = enrich_runset_visible_runs(runsets, blocks, panel_tables)
        self.assertEqual(enriched[0]["visible_run_ids"], ["b1gishis", "gtqhzr20"])

    def test_flatten_history_uses_export_time_alias_resolution_for_system_metrics(self) -> None:
        class DummyRun:
            id = "run-1"
            name = "glorious-lake-18"

            def scan_history(self, page_size=1000):
                return iter([])

            def history(self, samples=5000, stream="system", pandas=True):
                import pandas as pd

                return pd.DataFrame(
                    [
                        {
                            "_runtime": 12,
                            "_timestamp": 1000,
                            "system.gpu.0.powerWatts": 111.5,
                        }
                    ]
                )

        requests = build_history_metric_requests(["system/gpu.process.0.powerWatts"])
        rows = flatten_history(DummyRun(), requests, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["metric_name"], "system/gpu.process.0.powerWatts")
        self.assertEqual(rows[0]["source_metric_name"], "system/gpu.0.powerWatts")
        self.assertEqual(rows[0]["metric_value"], 111.5)

    def test_normalize_history_metric_value_summarizes_histograms(self) -> None:
        payload = normalize_history_metric_value(
            {
                "_type": "histogram",
                "packedBins": {"count": 4, "min": -1.0, "size": 0.5},
                "values": [1, 3, 2, 0],
            }
        )
        self.assertEqual(payload["metric_value_kind"], "histogram")
        self.assertIsNone(payload["metric_value"])
        self.assertAlmostEqual(payload["metric_histogram_q50"], -0.25)
        self.assertAlmostEqual(payload["metric_histogram_q90"], 0.25)

    def test_enrich_block_visible_runs_attaches_visible_rows_to_matching_block(self) -> None:
        runsets = [
            {
                "id": "abc123xyz",
                "name": "Run set",
                "only_show_selected": False,
                "selections": {"tree": ["0thca7ud", "b8pzxvg0"]},
            }
        ]
        blocks = [
            {
                "type": "panel-grid",
                "runsets": ["Run set"],
                "panels": [
                    {"table_key": "pred_table"},
                ],
            },
            {
                "type": "panel-grid",
                "runsets": ["Run set"],
                "panels": [
                    {"view_type": "Run History Line Plot", "metrics": ["train_loss"]},
                ],
            },
        ]
        panel_tables = {
            "pred_table": {
                "run_ids": ["0thca7ud", "b1gishis", "gtqhzr20"],
                "run_names": ["fine-firebrand-18", "glorious-lake-18", "comfy-resonance-3"],
            }
        }
        enriched = enrich_block_visible_runs(blocks, runsets, panel_tables)
        self.assertEqual(enriched[0]["visible_run_ids"], ["b1gishis", "gtqhzr20"])
        self.assertNotIn("visible_run_ids", enriched[1])

    def test_table_prediction_meta_rows_from_records_restores_embedded_image_payload(self) -> None:
        rows = table_prediction_meta_rows_from_records(
            [
                {
                    "run_id": "run-1",
                    "run_name": "demo-run",
                    "wandb_run_url": "https://wandb.ai/demo/demo/runs/run-1",
                    "meta_json": json.dumps(
                        {
                            "Image": {"_kind": "image", "path": "media/images/demo.png"},
                            "score": 0.5,
                        }
                    ),
                }
            ]
        )
        self.assertEqual(rows[0]["__run_id"], "run-1")
        self.assertEqual(rows[0]["Image"]["_kind"], "image")
        self.assertEqual(rows[0]["Image"]["path"], "media/images/demo.png")

    def test_export_panel_tables_only_flags_hydration_for_image_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.export_wandb_snapshot.PROCESSED_DIR", Path(tmpdir)):
                run_map = {"run-1": {}}
                with patch(
                    "scripts.export_wandb_snapshot._load_panel_table_rows_for_run",
                    return_value=[{"__run_id": "run-1", "Image": "Image", "score": 0.5}],
                ):
                    exports = export_panel_tables(run_map, ["pred_table"])
        self.assertTrue(exports["pred_table"]["needs_hydration"])

    def test_row_has_media_placeholders_detects_lowercase_image_column(self) -> None:
        self.assertTrue(row_has_media_placeholders({"image": "Image", "score": 0.5}))
        self.assertFalse(row_has_media_placeholders({"label": "Image", "score": 0.5}))

    def test_export_panel_tables_uses_hydrated_rows_for_lowercase_image_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.export_wandb_snapshot.PROCESSED_DIR", Path(tmpdir)):
                run_map = {"run-1": {"_run_object": object()}}
                with patch(
                    "scripts.export_wandb_snapshot._load_panel_table_rows_for_run",
                    return_value=[{"__run_id": "run-1", "image": "Image", "score": 0.5}],
                ), patch(
                    "scripts.export_wandb_snapshot.hydrate_panel_table_rows",
                    return_value=[
                        {
                            "__run_id": "run-1",
                            "__run_name": "demo-run",
                            "image": {"_kind": "image", "path": "media/images/demo.png"},
                            "score": 0.5,
                        }
                    ],
                ):
                    exports = export_panel_tables(run_map, ["pred_table"])
                    exported = json.loads((Path(tmpdir) / "panel_tables" / "pred-table.json").read_text(encoding="utf-8"))
        self.assertFalse(exports["pred_table"]["needs_hydration"])
        self.assertEqual(exported[0]["image"]["path"], "media/images/demo.png")

    def test_export_panel_tables_uses_hydrated_rows_without_marking_need(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.export_wandb_snapshot.PROCESSED_DIR", Path(tmpdir)):
                run_map = {"run-1": {}}
                with patch(
                    "scripts.export_wandb_snapshot._load_panel_table_rows_for_run",
                    return_value=[{"__run_id": "run-1", "Image": "Image", "score": 0.5}],
                ):
                    exports = export_panel_tables(
                        run_map,
                        ["pred_table"],
                        hydrated_rows=[
                            {
                                "__run_id": "run-1",
                                "__run_name": "demo-run",
                                "Image": {"_kind": "image", "path": "media/images/demo.png"},
                                "score": 0.5,
                            }
                        ],
                    )
        self.assertFalse(exports["pred_table"]["needs_hydration"])

    def test_load_table_file_rows_restores_artifact_image_cells(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            artifact_root = root / "artifact"
            image_dir = artifact_root / "media" / "images"
            image_dir.mkdir(parents=True, exist_ok=True)
            image_path = image_dir / "demo.png"
            image_path.write_bytes(b"png")
            table_path = artifact_root / "demo.table.json"
            table_path.write_text(
                json.dumps(
                    {
                        "columns": ["image", "score"],
                        "data": [
                            [
                                {
                                    "_type": "image-file",
                                    "path": "media/images/demo.png",
                                    "width": 28,
                                    "height": 28,
                                },
                                0.5,
                            ]
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with patch("scripts.export_wandb_snapshot.PROCESSED_DIR", root / "processed"):
                rows = load_table_file_rows(table_path, artifact_root=artifact_root)
        self.assertEqual(rows[0]["image"]["_kind"], "image")
        self.assertTrue(str(rows[0]["image"]["path"]).startswith("media/images/"))

    def test_snapshot_archive_dir_uses_report_title_and_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            processed_dir = Path(tmpdir)
            (processed_dir / "report_manifest.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-04-23T06:20:06.341566+00:00",
                        "snapshot_cache": {"snapshot_cache_version": "v2"},
                        "report_url": "https://wandb.ai/demo/project/reports/Sample-Report--Vmlldzox",
                        "report": {"title": "Sample Report"},
                    }
                ),
                encoding="utf-8",
            )
            archive_dir = snapshot_archive_dir(processed_dir)
        self.assertIn("sample-report", str(archive_dir))
        self.assertTrue(str(archive_dir).endswith("2026-04-23-062006-341566+0000"))

    def test_find_matching_snapshot_archive_uses_snapshot_cache_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshots_dir = Path(tmpdir)
            archive_dir = snapshots_dir / "sample-report-deadbeef" / "2026-04-23-062006-341566+0000"
            processed_dir = archive_dir / "processed"
            processed_dir.mkdir(parents=True, exist_ok=True)
            report = SimpleNamespace(
                id="report-1",
                display_name="Sample Report",
                updated_at="2026-04-23T06:00:00+00:00",
                spec={"blocks": [{"type": "html", "html": "<p>demo</p>"}]},
            )
            config = SimpleNamespace(
                report_url="https://wandb.ai/demo/project/reports/sample",
                entity="demo",
                project="project",
                sample_data=False,
            )
            cache_meta = snapshot_cache_metadata(config, report)
            (processed_dir / "report_manifest.json").write_text(
                json.dumps({"snapshot_cache": cache_meta}),
                encoding="utf-8",
            )
            with patch("scripts.export_wandb_snapshot.FINAL_SNAPSHOTS_DIR", snapshots_dir):
                matched = find_matching_snapshot_archive(cache_meta)
        self.assertEqual(matched, archive_dir)


if __name__ == "__main__":
    unittest.main()

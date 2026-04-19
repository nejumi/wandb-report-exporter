from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from scripts.generate_marimo_report import load_payload, row_has_media_placeholders


class GenerateMarimoReportTests(unittest.TestCase):
    def test_row_has_media_placeholders_detects_lowercase_image_column(self) -> None:
        self.assertTrue(row_has_media_placeholders({"image": "Image", "score": 0.5}))
        self.assertFalse(row_has_media_placeholders({"label": "Image", "score": 0.5}))

    def test_load_payload_hydrates_panel_table_with_lowercase_image_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            panel_dir = root / "panel_tables"
            panel_dir.mkdir(parents=True, exist_ok=True)
            (panel_dir / "pred-table.json").write_text(
                json.dumps([{"__run_id": "run-1", "image": "Image", "score": 0.5}]),
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "run_name": "demo-run",
                        "wandb_run_url": "https://wandb.ai/demo/demo/runs/run-1",
                        "meta_json": json.dumps({"image": {"_kind": "image", "path": "media/images/demo.png"}, "score": 0.5}),
                    }
                ]
            ).to_parquet(root / "table_predictions.parquet", index=False)
            (root / "report_manifest.json").write_text(
                json.dumps(
                    {
                        "report": {
                            "title": "Demo",
                            "panel_tables": {
                                "pred_table": {
                                    "path": "panel_tables/pred-table.json",
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch("scripts.generate_marimo_report.PROCESSED_DIR", root):
                payload = load_payload()
        panel_tables = payload["panel_tables"]
        self.assertEqual(panel_tables["pred_table"][0]["image"]["path"], "media/images/demo.png")


if __name__ == "__main__":
    unittest.main()

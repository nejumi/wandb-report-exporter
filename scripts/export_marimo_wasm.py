#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = ROOT / "marimo_viewer" / "wandb_report.py"
GENERATED_ASSETS_DIR = ROOT / "marimo_viewer" / "generated_assets"
OUTPUT_DIR = ROOT / "marimo_viewer" / "dist"
MARIMO_CONFIG_DIR = ROOT / ".marimo-config"
APP_MEDIA_DIR = ROOT / "app" / "src" / "media"


def python_with_marimo() -> str | None:
    if importlib.util.find_spec("marimo") is not None:
        return sys.executable
    venv_python = ROOT / ".venv" / "bin" / "python"
    if venv_python.exists():
        probe = subprocess.run(
            [str(venv_python), "-c", "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('marimo') else 1)"],
            check=False,
        )
        if probe.returncode == 0:
            return str(venv_python)
    return None


def patch_marimo_worker_imports() -> None:
    assets_dir = OUTPUT_DIR / "assets"
    if not assets_dir.exists():
        return
    pattern = re.compile(
        r"try\{return await import\(`/wasm/controller\.js\?version=\$\{e\}`\)\}catch\{return new (?P<class>[A-Za-z0-9_]+)\}"
    )
    for script_path in assets_dir.glob("*.js"):
        text = script_path.read_text(encoding="utf-8", errors="ignore")
        replaced = pattern.sub(
            lambda match: f"return new {match.group('class')}",
            text,
        )
        if replaced != text:
            script_path.write_text(replaced, encoding="utf-8")


def main() -> None:
    python_bin = python_with_marimo()
    if python_bin is None:
        raise RuntimeError(
            "marimo is not installed. Install it with `uv sync --extra marimo` "
            "or `pip install marimo`, then rerun this command."
        )

    MARIMO_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["MARIMO_CONFIG_DIR"] = str(MARIMO_CONFIG_DIR)

    subprocess.run([python_bin, str(ROOT / "scripts" / "generate_marimo_report.py")], check=True)
    subprocess.run(
        [
            python_bin,
            "-m",
            "marimo",
            "export",
            "html-wasm",
            str(NOTEBOOK_PATH),
            "-o",
            str(OUTPUT_DIR),
            "--force",
            "--mode",
            "run",
        ],
        check=True,
        env=env,
    )
    if GENERATED_ASSETS_DIR.exists():
        generated_output_dir = OUTPUT_DIR / GENERATED_ASSETS_DIR.name
        generated_assets_output_dir = OUTPUT_DIR / "assets" / GENERATED_ASSETS_DIR.name
        if generated_output_dir.exists():
            shutil.rmtree(generated_output_dir)
        if generated_assets_output_dir.exists():
            shutil.rmtree(generated_assets_output_dir)
        shutil.copytree(GENERATED_ASSETS_DIR, OUTPUT_DIR / GENERATED_ASSETS_DIR.name, dirs_exist_ok=True)
        shutil.copytree(GENERATED_ASSETS_DIR, OUTPUT_DIR / "assets" / GENERATED_ASSETS_DIR.name, dirs_exist_ok=True)
    if APP_MEDIA_DIR.exists():
        shutil.copytree(APP_MEDIA_DIR, OUTPUT_DIR / "media", dirs_exist_ok=True)
    patch_marimo_worker_imports()
    print(f"[ok] exported marimo viewer to {OUTPUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

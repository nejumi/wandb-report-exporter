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
PROCESSED_DIR = Path(os.environ.get("WANDB_PROCESSED_DIR", ROOT / "extracted" / "processed")).resolve()
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


def inject_loading_overlay() -> None:
    index_path = OUTPUT_DIR / "index.html"
    if not index_path.exists():
        return
    text = index_path.read_text(encoding="utf-8")
    marker = "codex-marimo-loading"
    if marker in text:
        return
    overlay_style = """
    <style id="codex-marimo-loading-style">
      #codex-marimo-loading {
        position: fixed;
        inset: 0;
        z-index: 2147483647;
        display: grid;
        place-items: center;
        padding: 2rem;
        background:
          radial-gradient(circle at top, rgba(219, 234, 254, 0.78), transparent 38%),
          linear-gradient(180deg, rgba(248,250,252,0.98), rgba(241,245,249,0.98));
        color: #0f172a;
        transition: opacity 240ms ease, visibility 240ms ease;
      }
      #codex-marimo-loading.is-hidden {
        opacity: 0;
        visibility: hidden;
        pointer-events: none;
      }
      .codex-marimo-loading-card {
        max-width: 34rem;
        padding: 1.2rem 1.35rem;
        border-radius: 22px;
        border: 1px solid rgba(148, 163, 184, 0.25);
        background: rgba(255,255,255,0.92);
        box-shadow: 0 18px 44px rgba(15, 23, 42, 0.12);
        backdrop-filter: blur(10px);
      }
      .codex-marimo-loading-title {
        display: flex;
        align-items: center;
        gap: 0.85rem;
        margin: 0 0 0.55rem;
        font-size: 1.02rem;
        font-weight: 700;
      }
      .codex-marimo-loading-title::before {
        content: "";
        width: 1rem;
        height: 1rem;
        border-radius: 999px;
        border: 2px solid rgba(59,130,246,0.22);
        border-top-color: rgba(37,99,235,0.95);
        animation: codex-marimo-loading-spin 0.9s linear infinite;
      }
      .codex-marimo-loading-copy {
        margin: 0;
        color: rgba(15,23,42,0.72);
        line-height: 1.55;
        font-size: 0.95rem;
      }
      .codex-marimo-loading-copy strong {
        color: rgba(15,23,42,0.9);
      }
      @keyframes codex-marimo-loading-spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
      }
    </style>
    """
    overlay_html = """
    <div id="codex-marimo-loading" aria-live="polite" aria-busy="true">
      <div class="codex-marimo-loading-card">
        <p class="codex-marimo-loading-title">Now loading this W&B report…</p>
        <p class="codex-marimo-loading-copy">The page is preparing the report structure first and then heavier charts load lazily. On large reports this can take a little while, but the content is still on the way.</p>
      </div>
    </div>
    """
    overlay_script = """
    <script id="codex-marimo-loading-script">
      (function () {
        const root = document.getElementById("root");
        const overlay = document.getElementById("codex-marimo-loading");
        if (!root || !overlay) return;
        let hidden = false;
        const hide = () => {
          if (hidden) return;
          hidden = true;
          overlay.classList.add("is-hidden");
          window.setTimeout(() => overlay.remove(), 320);
        };
        const hasRenderedContent = () => {
          if (!root.childElementCount) return false;
          const content = root.textContent || "";
          if (content.trim().length > 24) return true;
          return Boolean(
            root.querySelector("main, section, article, .markdown, .mo-md, .marimo-chart-shell, .marimo-table-shell, .marimo-note")
          );
        };
        if (hasRenderedContent()) {
          hide();
          return;
        }
        const observer = new MutationObserver(() => {
          if (hasRenderedContent()) {
            observer.disconnect();
            hide();
          }
        });
        observer.observe(root, { childList: true, subtree: true, characterData: true });
        window.addEventListener("load", () => {
          if (hasRenderedContent()) {
            observer.disconnect();
            hide();
            return;
          }
          window.setTimeout(() => {
            const copy = overlay.querySelector(".codex-marimo-loading-copy");
            if (copy) {
              copy.innerHTML = "Still loading the report. Large W&amp;B exports can take a bit on first paint, especially with heavy charts and media panels.";
            }
          }, 5000);
        }, { once: true });
        window.setTimeout(() => {
          if (hasRenderedContent()) {
            observer.disconnect();
            hide();
          }
        }, 15000);
      })();
    </script>
    """
    text = text.replace("</head>", overlay_style + "\n  </head>")
    text = text.replace("<body>", "<body>\n" + overlay_html)
    text = text.replace("</body>", overlay_script + "\n  </body>")
    index_path.write_text(text, encoding="utf-8")


def main() -> None:
    python_bin = python_with_marimo()
    if python_bin is None:
        raise RuntimeError(
            "marimo is not installed. Install project dependencies with `uv sync` "
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
    processed_media_dir = PROCESSED_DIR / "media"
    output_media_dir = OUTPUT_DIR / "media"
    if output_media_dir.exists():
        shutil.rmtree(output_media_dir)
    if processed_media_dir.exists():
        shutil.copytree(processed_media_dir, output_media_dir, dirs_exist_ok=True)
    elif APP_MEDIA_DIR.exists():
        shutil.copytree(APP_MEDIA_DIR, output_media_dir, dirs_exist_ok=True)
    patch_marimo_worker_imports()
    inject_loading_overlay()
    print(f"[ok] exported marimo viewer to {OUTPUT_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

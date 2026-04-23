PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
PORT ?= 8000
MARIMO_PORT ?= 8124
SAMPLE_PROCESSED ?= examples/mnist-sample/processed

.PHONY: export verify-export build serve dev marimo-notebook marimo-build marimo-serve sample-build stop marimo-stop

export:
	$(PYTHON) scripts/export_wandb_snapshot.py

verify-export:
	$(PYTHON) scripts/verify_export.py

build:
	npm run build

serve:
	exec $(PYTHON) -m http.server $(PORT) -d dist

dev:
	npm run dev

marimo-notebook:
	$(PYTHON) scripts/generate_marimo_report.py

marimo-build:
	$(PYTHON) scripts/export_marimo_wasm.py

sample-build:
	WANDB_PROCESSED_DIR=$(SAMPLE_PROCESSED) $(PYTHON) scripts/export_marimo_wasm.py

marimo-serve:
	exec $(PYTHON) -m http.server $(MARIMO_PORT) -d marimo_viewer/dist

stop:
	-pkill -f "http.server $(PORT) -d dist"

marimo-stop:
	-pkill -f "http.server $(MARIMO_PORT) -d marimo_viewer/dist"

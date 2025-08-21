# Makefile – unified tasks for UC server, UI, client, and Archetype demo

SHELL := /bin/bash

# ---------- Config (override via env or CLI, e.g. `make UC_SERVER=http://127.0.0.1:8080 uc-start`) ----------
UC_SERVER      ?= http://localhost:8080
UC_ENDPOINT    ?= $(UC_SERVER)/api/2.1/unity-catalog
UC_CATALOG     ?= unity
UC_SCHEMA      ?= default

# Path to the Unity Catalog repo in this workspace
UC_DIR         ?= unitycatalog
UC_CONF_DIR    ?= $(UC_DIR)/etc/conf
UC_ADMIN_TOKEN ?= $(shell test -f $(UC_CONF_DIR)/token.txt && cat $(UC_CONF_DIR)/token.txt)

# UI (if present)
UC_UI_DIR      ?= $(UC_DIR)/ui
UI_CLIENT_ID   ?=

# Python client (generated OpenAPI)
UC_CLIENT_DIR  ?= $(UC_DIR)/clients/python/target

# Local data root for examples/benchmarks
ARCHETYPE_DATA_DIR ?= ./archetype_data

.PHONY: help
help:
	@echo "Targets:"
	@echo "  uc-start           Start the Unity Catalog server"
	@echo "  uc-admin-token     Print admin token (created on first start)"
	@echo "  uc-add-user        Add a user to UC (requires USER_EMAIL and USER_NAME)"
	@echo "  uc-grant-basic     Grant USE CATALOG and USE SCHEMA to USER_EMAIL"
	@echo "  uc-login           Login via UC CLI (Google OAuth), saves token JSON to $(UC_CONF_DIR)/user_token.json"
	@echo "  ui-start           Start the UC UI (requires UI_CLIENT_ID)"
	@echo "  client-install     Install local generated UC python client (editable)"
	@echo "  demo               Run Archetype UC+GCS demo (requires UC_TOKEN in env)"
	@echo "  test               Run tests with uv (no sync)"

# ---------- Unity Catalog server ----------
.PHONY: uc-start
uc-start:
	@cd $(UC_DIR) && bin/start-uc-server

.PHONY: uc-admin-token
uc-admin-token:
	@cat $(UC_CONF_DIR)/token.txt

.PHONY: uc-add-user
uc-add-user:
	@if [ -z "$$USER_EMAIL" ] || [ -z "$$USER_NAME" ]; then \
		echo "Set USER_EMAIL and USER_NAME env vars, e.g. make uc-add-user USER_EMAIL=user@example.com USER_NAME=\"User Name\""; \
		exit 1; \
	fi
	@cd $(UC_DIR) && bin/uc --server $(UC_SERVER) --auth_token $(UC_ADMIN_TOKEN) user create --name "$$USER_NAME" --email "$$USER_EMAIL" || true

.PHONY: uc-grant-basic
uc-grant-basic:
	@if [ -z "$$USER_EMAIL" ]; then \
		echo "Set USER_EMAIL env var, e.g. make uc-grant-basic USER_EMAIL=user@example.com"; \
		exit 1; \
	fi
	@cd $(UC_DIR) && \
	bin/uc --server $(UC_SERVER) --auth_token $(UC_ADMIN_TOKEN) permission create --securable_type catalog --name $(UC_CATALOG) --privilege "USE CATALOG" --principal "$$USER_EMAIL" || true && \
	bin/uc --server $(UC_SERVER) --auth_token $(UC_ADMIN_TOKEN) permission create --securable_type schema --name $(UC_CATALOG).$(UC_SCHEMA) --privilege "USE SCHEMA" --principal "$$USER_EMAIL" || true

.PHONY: uc-login
uc-login:
	@cd $(UC_DIR) && bin/uc auth login --server $(UC_SERVER) --output jsonPretty | tee $(UC_CONF_DIR)/user_token.json
	@echo "Export UC_TOKEN from the printed JSON (access_token). Example: export UC_TOKEN=..."

# ---------- UI ----------
.PHONY: ui-start
ui-start:
	@if [ -z "$(UI_CLIENT_ID)" ]; then \
		echo "Set UI_CLIENT_ID to your Google OAuth Client ID."; \
		exit 1; \
	fi
	@if [ ! -d "$(UC_UI_DIR)" ]; then \
		echo "UI dir $(UC_UI_DIR) not found; adjust UC_UI_DIR or skip."; \
		exit 1; \
	fi
	@cd $(UC_UI_DIR) && \
	  export REACT_APP_GOOGLE_AUTH_ENABLED=true && \
	  export REACT_APP_GOOGLE_CLIENT_ID=$(UI_CLIENT_ID) && \
	  yarn install && yarn start

# ---------- Python client (generated) ----------
.PHONY: client-install
client-install:
	@uv pip install -e $(UC_CLIENT_DIR)

# ---------- Archetype demo ----------
.PHONY: demo
demo:
	@if [ -z "$$UC_TOKEN" ]; then \
		echo "Set UC_TOKEN in your env before running demo."; \
		exit 1; \
	fi
	@export UC_ENDPOINT=$(UC_ENDPOINT) UC_CATALOG=$(UC_CATALOG) UC_SCHEMA=$(UC_SCHEMA) && \
	  UV_NO_SYNC=1 uv run examples/run_uc_gcs_demo.py

# ---------- Tests ----------
.PHONY: test
test:
	@UV_NO_SYNC=1 uv run pytest -q || true

# ---------- Dev / Release / Bench ----------
.PHONY: dev-install
dev-install:
	@UV_NO_SYNC=1 uv pip install -e .

.PHONY: build
build:
	@UV_NO_SYNC=1 uv build

.PHONY: publish
publish:
	@echo "Uploading to PyPI; requires twine and PYPI_TOKEN in env" && \
	(test -n "$$PYPI_TOKEN" || (echo "Set PYPI_TOKEN env var" && exit 1)) && \
	python -m twine upload -u __token__ -p $$PYPI_TOKEN dist/*

.PHONY: run-example
run-example:
	@# Usage: make run-example EX=examples/pyglet_example.py
	@test -n "$$EX" || (echo "Set EX to the example path, e.g. EX=examples/pyglet_example.py" && exit 1)
	@mkdir -p $(ARCHETYPE_DATA_DIR)
	@PYTHONPATH=src UV_NO_SYNC=1 uv run -q python $$EX

.PHONY: bench-broker
bench-broker:
	@PYTHONPATH=src UV_NO_SYNC=1 uv run -q python bench/app/bench_command_broker.py | cat

.PHONY: bench-core
bench-core:
	@mkdir -p .bench_out
	@PYTHONPATH=src:. UV_NO_SYNC=1 uv run -q python -m bench.core.ecs.run --steps 1 --out .bench_out/ecs.json && echo "Wrote .bench_out/ecs.json"

.PHONY: uc-bootstrap
uc-bootstrap:
	@UC_ENDPOINT=$(UC_ENDPOINT) UC_ADMIN_TOKEN=$(UC_ADMIN_TOKEN) UC_CATALOG=$(UC_CATALOG) UC_SCHEMA=$(UC_SCHEMA) \
	 UV_NO_SYNC=1 uv run -q python scripts/uc_bootstrap.py

.PHONY: profile
profile:
	@# Usage: make profile CMD="pytest -q tests/app/test_simulation_service.py"
	@test -n "$$CMD" || (echo "Set CMD to the command to run under viztracer" && exit 1)
	@PYTHONPATH=src UV_NO_SYNC=1 uv run -q viztracer --ignore site-packages $$CMD


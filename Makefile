SHELL := /bin/bash

.DEFAULT_GOAL := help

# ==============================================================================
# Archetype dev workflow (uv + ruff + pre-commit)
# ==============================================================================

PYTHONPATH ?= src

.PHONY: help
help:
	@echo "Archetype Makefile"
	@echo ""
	@echo "Setup:"
	@echo "  make sync           Install runtime deps (uv)"
	@echo "  make sync-dev       Install runtime + dev deps (uv --group dev)"
	@echo ""
	@echo "Quality:"
	@echo "  make format         ruff format (src + tests)"
	@echo "  make lint           ruff check (src + tests)"
	@echo "  make lint-fix       ruff check --fix (src + tests)"
	@echo "  make check          format + lint (src + tests)"
	@echo "  make format-all     ruff format (everything)"
	@echo "  make lint-all       ruff check (everything)"
	@echo ""
	@echo "Tests:"
	@echo "  make test           pytest (fast; uses PYTHONPATH=$(PYTHONPATH))"
	@echo "  make test-cov       pytest with coverage report"
	@echo ""
	@echo "Pre-commit:"
	@echo "  make precommit-install   install git hooks"
	@echo "  make precommit-run       run hooks on all files"
	@echo ""
	@echo "Packaging / release:"
	@echo "  make lock-check     verify uv.lock matches pyproject"
	@echo "  make build          build sdist+wheel (uv build)"
	@echo "  make release-check  sync-dev + check + test + lock-check + build"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          remove build artifacts"
	@echo "  make mcp            run MCP server (python -m archetype.mcp)"
	@echo "  make example EX=... run an example (e.g. examples/grpo_text_end_to_end.py)"


# ------------------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------------------

.PHONY: sync
sync:
	@uv sync

.PHONY: sync-dev
sync-dev:
	@uv sync --group dev


# ------------------------------------------------------------------------------
# Quality
# ------------------------------------------------------------------------------

.PHONY: format
format:
	@uv run ruff format src tests

.PHONY: lint
lint:
	@uv run ruff check src tests

.PHONY: lint-fix
lint-fix:
	@uv run ruff check src tests --fix

.PHONY: format-all
format-all:
	@uv run ruff format .

.PHONY: lint-all
lint-all:
	@uv run ruff check .

.PHONY: check
check: format lint


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

.PHONY: test
test:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q

.PHONY: test-cov
test-cov:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q --cov=src --cov-report=term-missing


# ------------------------------------------------------------------------------
# Pre-commit
# ------------------------------------------------------------------------------

.PHONY: precommit-install
precommit-install:
	@uv run pre-commit install

.PHONY: precommit-run
precommit-run:
	@uv run pre-commit run --all-files


# ------------------------------------------------------------------------------
# Packaging / release
# ------------------------------------------------------------------------------

.PHONY: lock-check
lock-check:
	@uv lock --check

.PHONY: build
build:
	@uv build

.PHONY: release-check
release-check: sync-dev check test lock-check build
	@echo "OK: release-check passed"


# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

.PHONY: clean
clean:
	@rm -rf dist build .pytest_cache .ruff_cache
	@rm -rf src/*.egg-info src/*/*.egg-info

.PHONY: mcp
mcp:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m archetype.mcp

.PHONY: example
example:
	@test -n "$(EX)" || (echo "Set EX=examples/<file>.py" && exit 1)
	@PYTHONPATH=$(PYTHONPATH) uv run python "$(EX)"

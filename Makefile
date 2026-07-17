SHELL := /bin/bash

.DEFAULT_GOAL := help

# ==============================================================================
# Archetype dev workflow (uv + ruff + pre-commit)
# ==============================================================================

PYTHONPATH ?= src
VERSION := $(shell grep -m1 'version = ' pyproject.toml | cut -d'"' -f2)

.PHONY: help
help:
	@echo "Archetype Makefile (v$(VERSION))"
	@echo ""
	@echo "Setup:"
	@echo "  make sync           Install runtime deps (uv)"
	@echo "  make sync-dev       Install runtime + dev deps"
	@echo ""
	@echo "Quality:"
	@echo "  make format         Format code (ruff)"
	@echo "  make lint           Lint code (ruff)"
	@echo "  make lint-fix       Lint and auto-fix"
	@echo "  make check          Format + lint"
	@echo "  make complexity     Cyclomatic complexity / maintainability report (radon)"
	@echo ""
	@echo "Tests:"
	@echo "  make test           Run tests (fast)"
	@echo "  make test-cov       Run tests with coverage"
	@echo "  make test-all       Run all tests verbose"
	@echo "  make ci             CI gate (format-check + lint + lock-check + test-cov)"
	@echo "  make mutmut         Run mutation tests (pilot scope; slow, on-demand)"
	@echo "  make mutmut-results Show mutmut survivors from the last run"
	@echo "  make mutmut-browse  Interactive TUI to inspect surviving mutants"
	@echo "  make mutmut-clean   Remove mutmut cache and generated mutants"
	@echo ""
	@echo "Benchmarks & Evals:"
	@echo "  make bench          Run ECS microbenchmarks (1 step)"
	@echo "  make bench-full     Run ECS microbenchmarks (3 steps)"
	@echo "  make bench-query    Run QueryService latency benchmarks"
	@echo "  make eval           Run all eval suites"
	@echo "  make eval-reg       Run regression suite only"
	@echo "  make eval-idem      Run idempotency suite only"
	@echo "  make eval-cap       Run capability suite only"
	@echo "  make test-infra     Run external-infrastructure tests (requires configured service)"
	@echo ""
	@echo "Build & Release:"
	@echo "  make build          Build sdist + wheel"
	@echo "  make release-check  Full pre-release validation"
	@echo "  make publish-test   Publish to TestPyPI"
	@echo "  make publish        Publish to PyPI"
	@echo "  make version        Show current version"
	@echo ""
	@echo "Docs:"
	@echo "  make docs           Build docs (MkDocs)"
	@echo "  make docs-serve     Preview the Pages artifact at http://localhost:8788"
	@echo "  make docs-lint      Run doc quality checks (spelling, markdown lint, link check)"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          Remove build artifacts"
	@echo "  make clean-all      Remove all generated files"

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
	@uv run ruff format src tests evals/suites/idempotency.py \
		evals/suites/idempotency_durable.py evals/suites/idempotency_process.py \
		evals/infra/idempotency_worker.py scripts/check_idempotency_contracts.py

.PHONY: lint
lint: lazy-audit api-boundary-audit idempotency-audit gate-coverage-audit
	@uv run ruff check src tests evals/suites/idempotency.py \
		evals/suites/idempotency_durable.py evals/suites/idempotency_process.py \
		evals/infra/idempotency_worker.py scripts/check_idempotency_contracts.py

.PHONY: lint-fix
lint-fix:
	@uv run ruff check src tests --fix

.PHONY: format-check
format-check:
	@uv run ruff format --check src tests evals/suites/idempotency.py \
		evals/suites/idempotency_durable.py evals/suites/idempotency_process.py \
		evals/infra/idempotency_worker.py scripts/check_idempotency_contracts.py

.PHONY: check
check: format lint

# Lazy-evaluation audit: gate .collect()/.to_pylist() call sites against
# lazy_audit.toml. Every premature materialization is a contract exception
# and must be justified in writing. See scripts/check_lazy_audit.py.
.PHONY: lazy-audit
lazy-audit:
	@uv run python scripts/check_lazy_audit.py

.PHONY: api-boundary-audit
api-boundary-audit:
	@uv run python scripts/check_api_import_boundaries.py

# Keep every normative idempotency-matrix row mapped to a registered eval.
# This is static and fast; make eval-idem executes the behavioral scenarios.
.PHONY: idempotency-audit
idempotency-audit:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/check_idempotency_contracts.py

# Command-disposition manifest + API error taxonomy. Static guard for the
# accepted-then-dropped class (#178/#368) and unmapped-500 class (#180).
.PHONY: gate-coverage-audit
gate-coverage-audit:
	@PYTHONPATH=$(PYTHONPATH) uv run python scripts/check_gate_coverage.py

# Cyclomatic complexity + maintainability report.
# Uses uvx so radon stays out of the project lock file.
# CC ranks: A (1-5) B (6-10) C (11-20) D (21-30) E (31-40) F (41+).
# Surfaces rank C+ for CC and rank B+ for MI — the "review me" set.
.PHONY: complexity
complexity:
	@echo "=== Cyclomatic complexity (functions ranked C or worse) ==="
	@uvx radon cc src -n C -s -a --total-average || true
	@echo ""
	@echo "=== Maintainability index (files ranked B or worse) ==="
	@uvx radon mi src -n B -s || true
	@echo ""
	@echo "=== Raw line counts ==="
	@uvx radon raw src -s

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

.PHONY: test
test:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q -n auto --dist loadgroup

# Narrow test target: run a specific path/file/nodeid.
# Usage: make test-mod MOD=tests/lifecycle/
# Fails fast if MOD is unset so it can't be confused with test-all.
.PHONY: test-mod
test-mod:
	@if [ -z "$(MOD)" ]; then \
		echo "Error: MOD is required for test-mod."; \
		echo "Usage: make test-mod MOD=tests/lifecycle/"; \
		exit 1; \
	fi
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -v $(MOD)

.PHONY: test-cov
test-cov:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest \
		-n auto --dist loadgroup \
		--cov=archetype \
		--cov-branch \
		--cov-report=term-missing:skip-covered \
		--cov-report=xml \
		--cov-fail-under=70

.PHONY: test-all
test-all:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -v --tb=short

.PHONY: ci
ci: format-check lint lock-check test-cov eval-reg eval-idem
	@echo "CI gate passed"

# Mutation testing (mutmut). Not part of `make ci` — each mutation runs the
# full pilot test suite, so even the narrow scope takes minutes. Run
# on-demand to probe assertion strength on the modules under [tool.mutmut].
# See docs/guide/mutation-testing.md.
.PHONY: mutmut
mutmut:
	@PYTHONPATH=$(PYTHONPATH) uv run mutmut run

.PHONY: mutmut-results
mutmut-results:
	@uv run mutmut results

.PHONY: mutmut-browse
mutmut-browse:
	@uv run mutmut browse

.PHONY: mutmut-clean
mutmut-clean:
	@rm -rf mutants/ .mutmut-cache

# ------------------------------------------------------------------------------
# Benchmarks & Evals
# ------------------------------------------------------------------------------

.PHONY: bench
bench:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m bench.core.ecs.run --steps 1 --out bench-results.json
	@echo "Benchmark results written to bench-results.json"

.PHONY: bench-full
bench-full:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m bench.core.ecs.run --steps 3 --out bench-results.json
	@echo "Benchmark results written to bench-results.json"

.PHONY: bench-query
bench-query:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m bench.core.query_latency --out query-bench-results.json
	@echo "Query benchmark results written to query-bench-results.json"

.PHONY: eval
eval:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m evals.run --out eval-results.json

.PHONY: eval-reg
eval-reg:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m evals.run --suite regression

.PHONY: eval-idem
eval-idem:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m evals.run --suite idempotency

.PHONY: eval-cap
eval-cap:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m evals.run --suite capability

.PHONY: test-infra
test-infra:
	@PYTHONPATH=$(PYTHONPATH):. uv run pytest -q tests/infrastructure

# ------------------------------------------------------------------------------
# Build & Release
# ------------------------------------------------------------------------------

.PHONY: version
version:
	@echo "$(VERSION)"

.PHONY: lock-check
lock-check:
	@bash scripts/check_uv_lock.sh

# Bump tool.uv.exclude-newer to today minus 7 days and regenerate the lock.
# Use this to advance the supply-chain quarantine cutoff over time.
.PHONY: refresh-quarantine
refresh-quarantine:
	@CUTOFF=$$(python3 -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc)-timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z"))'); \
	 echo "→ exclude-newer = $$CUTOFF"; \
	 sed -i.bak "s|^exclude-newer = .*|exclude-newer = \"$$CUTOFF\"|" pyproject.toml; \
	 rm -f pyproject.toml.bak; \
	 uv lock

.PHONY: build
build: clean
	@echo "Building archetype v$(VERSION)..."
	@uv build
	@echo ""
	@echo "Built:"
	@ls -lh dist/

.PHONY: release-check
release-check: sync-dev check test-cov lock-check build
	@echo ""
	@echo "✅ Release check passed for v$(VERSION)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. git tag v$(VERSION)"
	@echo "  2. git push origin v$(VERSION)"
	@echo "  3. make publish (or let CI handle it)"

.PHONY: publish-test
publish-test: build
	@echo "Publishing to TestPyPI..."
	@uv publish --publish-url https://test.pypi.org/legacy/

.PHONY: publish
publish: build
	@echo "Publishing to PyPI..."
	@uv publish

# ------------------------------------------------------------------------------
# Docs (Material for MkDocs)
# ------------------------------------------------------------------------------

.PHONY: docs-gen
docs-gen:
	@echo "Generating API & CLI reference docs..."
	@uv run python scripts/generate_python_api_docs.py
	@uv run python scripts/generate_api_docs.py
	@uv run python scripts/generate_cli_docs.py

.PHONY: docs
docs: docs-gen
	@rm -rf site
	@uv run --extra docs mkdocs build
	@uv run python scripts/assemble_docs_site.py

.PHONY: docs-serve
docs-serve: docs
	@npx --yes wrangler pages dev site/ --port 8788

.PHONY: docs-lint
docs-lint:
	@echo "=== Spelling (typos) ==="
	@if command -v typos >/dev/null 2>&1; then \
		typos --config ./_typos.toml .; \
	else \
		echo "typos not installed — install via: cargo install typos-cli"; \
		echo "  or: brew install typos-cli"; \
		exit 1; \
	fi
	@echo ""
	@echo "=== Markdown lint ==="
	@if command -v markdownlint-cli2 >/dev/null 2>&1; then \
		markdownlint-cli2 --config .markdownlint.yaml "docs/**/*.md" "*.md"; \
	elif command -v npx >/dev/null 2>&1; then \
		npx --yes markdownlint-cli2 --config .markdownlint.yaml "docs/**/*.md" "*.md"; \
	else \
		echo "markdownlint-cli2 not available"; \
		exit 1; \
	fi
	@echo ""
	@echo "=== Link check (lychee) ==="
	@if command -v lychee >/dev/null 2>&1; then \
		lychee --config lychee.toml "docs/" "README.md" "CONTRIBUTING.md" "AGENTS.md"; \
	else \
		echo "lychee not installed — install via: cargo install lychee"; \
		echo "  or: brew install lychee"; \
		exit 1; \
	fi
	@echo ""
	@echo "Docs lint passed"

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

.PHONY: clean
clean:
	@rm -rf dist build
	@rm -rf src/*.egg-info src/*/*.egg-info
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

.PHONY: clean-all
clean-all: clean
	@rm -rf .pytest_cache .ruff_cache .coverage coverage.xml htmlcov
	@rm -rf .venv

.PHONY: precommit-install
precommit-install:
	@uv run pre-commit install

.PHONY: precommit-run
precommit-run:
	@uv run pre-commit run --all-files

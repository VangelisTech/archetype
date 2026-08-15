SHELL := /bin/bash

.DEFAULT_GOAL := help

# ==============================================================================
# Archetype dev workflow (uv + ruff + pre-commit)
# ==============================================================================

SOURCE_ROOTS := packages/archetype-ecs/src packages/archetype-missions/src packages/archetype-physical-ai/src packages/archetype-research/src
PYTHONPATH ?= packages/archetype-ecs/src:packages/archetype-missions/src:packages/archetype-physical-ai/src:packages/archetype-research/src
VERSION := $(shell grep -m1 'version = ' packages/archetype-ecs/pyproject.toml | cut -d'"' -f2)
RUFF_PATHS := packages tests evals bench scripts quality experiments examples
SYNC_FLAGS := --all-packages --all-extras
FRAMEWORK_WHEEL := $(shell find dist -maxdepth 1 -name 'archetype_ecs-*.whl' -print -quit 2>/dev/null)

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
	@echo "  make static         All version-independent blocking validation"
	@echo "  make contract-audit Validate normative sources and executable oracles"
	@echo "  make benchmark-audit Validate benchmark ownership and policies"
	@echo "  make operational-audit Validate operational scenario ownership and policies"
	@echo "  make architecture-audit  Enforce dependency and encapsulation policy"
	@echo "  make observability-audit Enforce signal safety and family dispositions"
	@echo "  make lockfile-audit  Scan the locked dependency graph for known vulnerabilities"
	@echo "  make version-inventory-audit  Validate pinned execution-environment inventory"
	@echo "  make python-api-audit  Validate committed generated Python reference"
	@echo "  make lint-fix       Lint and auto-fix"
	@echo "  make check          Format + lint"
	@echo "  make complexity     Cyclomatic complexity / maintainability report (radon)"
	@echo ""
	@echo "Tests:"
	@echo "  make test           Run tests (fast)"
	@echo "  make test-cov       Run tests with coverage"
	@echo "  make test-all       Run all tests verbose"
	@echo "  make test-unit      Fast tests outside contract/integration/process lanes"
	@echo "  make test-contract  Approved normative contract evidence"
	@echo "  make test-integration  Multi-layer repository evidence"
	@echo "  make test-process   Subprocess/crash evidence"
	@echo "  make ci             Compatibility alias for the PR verification profile"
	@echo "  make mutmut         Run mutation tests (pilot scope; slow, on-demand)"
	@echo "  make mutmut-results Show mutmut survivors from the last run"
	@echo "  make mutmut-browse  Interactive TUI to inspect surviving mutants"
	@echo "  make mutmut-clean   Remove mutmut cache and generated mutants"
	@echo ""
	@echo "Repository Harness:"
	@echo "  make bench          Run ECS microbenchmarks (1 step)"
	@echo "  make bench-full     Run ECS microbenchmarks (3 steps)"
	@echo "  make bench-query    Run QueryService latency benchmarks"
	@echo "  make bench-daft-attribution  Characterize lazy Daft execution attribution"
	@echo "  make eval           Run all repository-check groups"
	@echo "  make eval-reg       Run regression checks only"
	@echo "  make eval-idem      Run idempotency checks only"
	@echo "  make eval-cap       Run broad capability scenarios only"
	@echo "  make eval-conformance  Blocking public-boundary conformance profile"
	@echo "  make eval-reliability  Blocking retry/crash/recovery profile"
	@echo "  make eval-capability  Blocking architectural capability profile"
	@echo "  make examples-local  Run Tier-1 semantic examples in isolated storage"
	@echo "  make operational-runtime  Run the shipped runtime/API/CLI loopback scenario"
	@echo "  make operational-wheel  Run representative scenarios against the built wheel matrix"
	@echo "  make operational-mission  Run the credential-free exact-head mission scenario"
	@echo "  make operational-external  Require selected Tier-5/6 provider evidence"
	@echo "  make operational-release  Run credential-free release evidence against the wheel matrix"
	@echo "  make operational-release-modal  Run the paid live Modal/Codex wheel evidence"
	@echo "  make test-infra     Run external-infrastructure tests (requires configured service)"
	@echo ""
	@echo "Build & Release:"
	@echo "  make build          Build all four sdists and wheels"
	@echo "  make package-smoke  Install and probe the built distribution matrix outside the checkout"
	@echo "  make verify-pr      Complete pull-request profile"
	@echo "  make verify-full    Main-branch profile"
	@echo "  make verify-release Source profile plus exact installed-artifact release evidence"
	@echo "  make release-check  Full pre-release validation"
	@echo "  make verify-test-index Verify exact TestPyPI bytes and install matrix"
	@echo "  make verify-published Verify exact PyPI bytes and install matrix"
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
	@uv sync $(SYNC_FLAGS)

.PHONY: sync-dev
sync-dev:
	@uv sync $(SYNC_FLAGS) --group dev

# ------------------------------------------------------------------------------
# Quality
# ------------------------------------------------------------------------------

.PHONY: format
format:
	@uv run ruff format $(RUFF_PATHS)

.PHONY: lint
lint: lazy-audit architecture-audit observability-audit lockfile-audit version-inventory-audit python-api-audit api-boundary-audit idempotency-audit gate-coverage-audit operational-audit
	@uv run ruff check $(RUFF_PATHS)

.PHONY: lint-fix
lint-fix:
	@uv run ruff check $(RUFF_PATHS) --fix

.PHONY: format-check
format-check:
	@uv run ruff format --check $(RUFF_PATHS)

.PHONY: check
check: format lint

.PHONY: typecheck
typecheck:
	@uvx ty@0.0.48 check --python .venv

.PHONY: contract-audit
contract-audit:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/validate_contracts.py
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/generate_contract_traceability.py --check

.PHONY: benchmark-audit
benchmark-audit:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/validate_benchmarks.py

.PHONY: operational-audit
operational-audit:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/validate_operational_scenarios.py

.PHONY: static
static: format-check lint typecheck lock-check contract-audit benchmark-audit
	@echo "Static validation passed"

.PHONY: lockfile-audit
lockfile-audit:
	@uv run python scripts/audit_lockfile.py

# Lazy-evaluation audit: gate .collect()/.to_pylist() call sites against
# lazy_audit.toml. Every premature materialization is a contract exception
# and must be justified in writing. See scripts/check_lazy_audit.py.
.PHONY: lazy-audit
lazy-audit:
	@uv run python scripts/check_lazy_audit.py

.PHONY: api-boundary-audit
api-boundary-audit:
	@uv run python scripts/check_api_import_boundaries.py

.PHONY: python-api-audit
python-api-audit:
	@PYTHONPATH=$(PYTHONPATH) uv run python scripts/generate_python_api_docs.py --check

.PHONY: architecture-audit
architecture-audit:
	@uv run python scripts/check_architecture.py

.PHONY: observability-audit
observability-audit:
	@uv run python scripts/check_observability.py

# Fail-closed load of the pinned execution-environment inventory plus a
# freshness check of its rendered operator page (#507).
.PHONY: version-inventory-audit
version-inventory-audit:
	@PYTHONPATH=$(PYTHONPATH) uv run python scripts/generate_version_inventory.py --check

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
	@uvx radon cc $(SOURCE_ROOTS) -n C -s -a --total-average || true
	@echo ""
	@echo "=== Maintainability index (files ranked B or worse) ==="
	@uvx radon mi $(SOURCE_ROOTS) -n B -s || true
	@echo ""
	@echo "=== Raw line counts ==="
	@uvx radon raw $(SOURCE_ROOTS) -s

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

.PHONY: test-unit
test-unit:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q -n auto --dist loadgroup \
		-m "not contract and not integration and not process and not external and not slow"

.PHONY: test-contract
test-contract:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q -n auto --dist loadgroup -m contract

.PHONY: test-integration
test-integration:
	@PYTHONPATH=$(PYTHONPATH) uv run pytest -q -n auto --dist loadgroup \
		-m "integration and not process and not external"

.PHONY: test-process
test-process:
	@PYTHONPATH=$(PYTHONPATH):. uv run pytest -q -m process

.PHONY: ci
ci: verify-pr

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
# Repository harness
# ------------------------------------------------------------------------------

EVAL_TRIALS ?= 1
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

.PHONY: bench-daft-attribution
bench-daft-attribution:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m bench.observability.daft_attribution --out daft-attribution-results.json
	@echo "Daft attribution results written to daft-attribution-results.json"

.PHONY: eval
eval:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m evals.run --out eval-results.json

.PHONY: eval-reg
eval-reg:
	@PYTHONPATH=$(PYTHONPATH) uv run python -m evals.run --suite regression

.PHONY: eval-idem
eval-idem: eval-reliability

.PHONY: eval-cap
eval-cap: eval-capability

.PHONY: eval-conformance
eval-conformance:
	@PYTHONPATH=$(PYTHONPATH):. uv run python -m evals.run \
		--profile conformance --out eval-conformance-results.json

.PHONY: eval-reliability
eval-reliability:
	@PYTHONPATH=$(PYTHONPATH):. uv run python -m evals.run \
		--profile reliability --trials $(EVAL_TRIALS) --out eval-reliability-results.json

.PHONY: eval-capability
eval-capability:
	@PYTHONPATH=$(PYTHONPATH):. uv run python -m evals.run \
		--profile capability --out eval-capability-results.json

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
	@uv build --all-packages --no-sources --clear --out-dir dist
	@echo ""
	@echo "Built:"
	@ls -lh dist/

.PHONY: package-smoke
package-smoke: build
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/package_smoke.py dist

.PHONY: examples-local
examples-local:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
		--mode source --cadence pr --kind example --max-tier 1 \
		--out operational-source-results.json

.PHONY: examples-smoke
examples-smoke: examples-local
	@echo "Semantic example scenarios passed"

OPERATIONAL_BUILD_COMMAND ?= $(MAKE) --no-print-directory build
OPERATIONAL_DIST_DIR ?= dist
OPERATIONAL_WHEEL_RESULTS ?= operational-results.json
OPERATIONAL_COMMANDS_RESULTS ?= operational-commands-source-results.json
OPERATIONAL_RUNTIME_RESULTS ?= operational-runtime-source-results.json
OPERATIONAL_RELEASE_RESULTS ?= operational-release-results.json
RELEASE_ARTIFACT_MANIFEST ?= release-artifact.json

.PHONY: operational-runtime
operational-runtime:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
		--mode source --cadence pr --max-tier 1 --require-run \
		--scenario dogfood.runtime.loopback \
		--out "$(OPERATIONAL_RUNTIME_RESULTS)"

.PHONY: operational-commands
operational-commands:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
		--mode source --cadence pr --max-tier 1 --require-run \
		--scenario dogfood.commands.local \
		--out "$(OPERATIONAL_COMMANDS_RESULTS)"

.PHONY: operational-wheel
operational-wheel:
	@build_status=0; \
		$(OPERATIONAL_BUILD_COMMAND) || build_status=$$?; \
		wheel=""; \
		if [ "$$build_status" -eq 0 ]; then \
			wheel=$$(find "$(OPERATIONAL_DIST_DIR)" -maxdepth 1 -name 'archetype_ecs-*.whl' -print -quit 2>/dev/null); \
		fi; \
		if [ -z "$$wheel" ]; then \
			wheel="$(OPERATIONAL_DIST_DIR)/.missing-operational-wheel.whl"; \
		fi; \
		runner_status=0; \
		PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
			--mode wheel --cadence pr --max-tier 1 --require-run \
			--scenario example.00_quickstart \
			--scenario example.01_world_mutations \
			--scenario example.02_fork_counterfactual \
			--scenario example.03_time_travel \
			--scenario example.06_trajectory_analysis \
			--scenario example.10_autoresearch \
			--scenario dogfood.runtime.loopback \
			--scenario dogfood.commands.local \
			--scenario dogfood.evaluation.durable_receipt \
			--scenario dogfood.artifacts.local \
			--wheel "$$wheel" --wheel-dir "$(OPERATIONAL_DIST_DIR)" \
			--out "$(OPERATIONAL_WHEEL_RESULTS)" || runner_status=$$?; \
		if [ "$$build_status" -ne 0 ]; then \
			exit "$$build_status"; \
		fi; \
		exit "$$runner_status"

.PHONY: operational-wheel-existing
operational-wheel-existing:
	@$(MAKE) --no-print-directory operational-wheel OPERATIONAL_BUILD_COMMAND=true

.PHONY: operational-mission
operational-mission:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
		--mode source --cadence pr --max-tier 1 --require-run \
		--scenario dogfood.agent_mission.modal_activity_contracts \
		--out operational-mission-results.json

.PHONY: operational-external
operational-external:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
		--mode source --cadence release --min-tier 5 --max-tier 6 --require-run \
		--out operational-external-results.json

# The release workflow builds once after the source profile, package-smokes
# those exact eight artifacts, records every digest, and never rebuilds before
# upload.
.PHONY: release-artifact
release-artifact:
	@$(MAKE) --no-print-directory build
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/package_smoke.py "$(OPERATIONAL_DIST_DIR)"
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/release_artifact.py record \
		--dist "$(OPERATIONAL_DIST_DIR)" --manifest "$(RELEASE_ARTIFACT_MANIFEST)"

.PHONY: verify-release-artifact
verify-release-artifact:
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/release_artifact.py verify \
		--dist "$(OPERATIONAL_DIST_DIR)" --manifest "$(RELEASE_ARTIFACT_MANIFEST)"

define RUN_RELEASE_SCENARIOS
	@wheel=$$(find "$(OPERATIONAL_DIST_DIR)" -maxdepth 1 -name 'archetype_ecs-*.whl' -print -quit 2>/dev/null); \
		if [ -z "$$wheel" ]; then \
			echo "release evidence requires the archetype-ecs wheel anchor in $(OPERATIONAL_DIST_DIR)"; \
			exit 1; \
		fi; \
		$(2) PYTHONPATH=$(PYTHONPATH):. uv run python scripts/run_operational_scenarios.py \
			--mode wheel --cadence release --require-run --require-clean \
			--wheel "$$wheel" --wheel-dir "$(OPERATIONAL_DIST_DIR)" $(1)
endef

.PHONY: operational-release
operational-release: release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 4 --out "$(OPERATIONAL_RELEASE_RESULTS)")

.PHONY: operational-release-openai
operational-release-openai: verify-release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 6 --scenario example.05_llm_agents --out operational-release-openai-results.json)

.PHONY: operational-release-docker
operational-release-docker: verify-release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 6 --scenario dogfood.sandbox.docker --out operational-release-docker-results.json)

.PHONY: operational-release-r2
operational-release-r2: verify-release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 6 --scenario dogfood.storage.r2 --out operational-release-r2-results.json)

.PHONY: operational-release-apple
operational-release-apple: verify-release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 6 --scenario dogfood.sandbox.apple_container --out operational-release-apple-results.json)

.PHONY: operational-release-modal
operational-release-modal: verify-release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 6 --scenario dogfood.agent_mission.modal_live --out operational-release-modal-results.json,ARCHETYPE_MODAL_AGENT_MISSION_LIVE=1)

.PHONY: operational-release-physical-modal-r2
operational-release-physical-modal-r2: verify-release-artifact
	$(call RUN_RELEASE_SCENARIOS,--min-tier 0 --max-tier 6 --scenario dogfood.physical_ai.modal_r2_live --out operational-release-physical-modal-r2-results.json,ARCHETYPE_MODAL_PHYSICAL_R2_LIVE=1)

.PHONY: verify-pr
verify-pr: static test package-smoke
	@echo "PR verification profile passed"

.PHONY: verify-full-source
verify-full-source: static test-cov eval-conformance eval-capability examples-smoke operational-runtime operational-commands docs test-process eval-reliability
	@echo "Full source verification profile passed"

.PHONY: verify-full
verify-full: verify-full-source package-smoke operational-wheel-existing
	@echo "Full verification profile passed"

.PHONY: verify-release
verify-release: verify-full-source operational-release
	@echo "Release verification profile passed"

.NOTPARALLEL: verify-full verify-release

.PHONY: release-check
release-check: sync-dev verify-release
	@echo ""
	@echo "✅ Release check passed for v$(VERSION)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. git tag v$(VERSION)"
	@echo "  2. git push origin v$(VERSION)"
	@echo "  3. Dispatch the Release workflow for v$(VERSION)"

.PHONY: verify-test-index
verify-test-index:
	@uvx --from pypi-attestations==0.0.30 --with packaging==26.1 \
		python scripts/verify_release_index.py \
		complete --manifest "$(RELEASE_ARTIFACT_MANIFEST)" \
		--expected-commit "$$(git rev-parse HEAD)" \
		--api-template 'https://test.pypi.org/pypi/{distribution}/{version}/json' \
		--integrity-template 'https://test.pypi.org/integrity/{distribution}/{version}/{filename}/provenance' \
		--publisher-repository VangelisTech/archetype \
		--publisher-environment release-testpypi \
		--attestation-repository https://github.com/VangelisTech/archetype \
		--registry-artifact-host test-files.pythonhosted.org \
		--attempts 12 --interval-seconds 5
	@uv run --no-project --with packaging==26.1 python scripts/registry_smoke.py \
		--manifest "$(RELEASE_ARTIFACT_MANIFEST)" --index-url https://test.pypi.org/simple \
		--extra-index-url https://pypi.org/simple

.PHONY: verify-published
verify-published:
	@uvx --from pypi-attestations==0.0.30 --with packaging==26.1 \
		python scripts/verify_release_index.py \
		complete --manifest "$(RELEASE_ARTIFACT_MANIFEST)" \
		--expected-commit "$$(git rev-parse HEAD)" \
		--integrity-template 'https://pypi.org/integrity/{distribution}/{version}/{filename}/provenance' \
		--publisher-repository VangelisTech/archetype \
		--publisher-environment release-pypi \
		--attestation-repository https://github.com/VangelisTech/archetype \
		--registry-artifact-host files.pythonhosted.org \
		--attempts 12 --interval-seconds 5
	@uv run --no-project --with packaging==26.1 python scripts/registry_smoke.py \
		--manifest "$(RELEASE_ARTIFACT_MANIFEST)"

# ------------------------------------------------------------------------------
# Docs (Material for MkDocs)
# ------------------------------------------------------------------------------

.PHONY: docs-gen
docs-gen:
	@echo "Generating API & CLI reference docs..."
	@PYTHONPATH=$(PYTHONPATH):. uv run python scripts/generate_contract_traceability.py
	@PYTHONPATH=$(PYTHONPATH) uv run python scripts/generate_version_inventory.py
	@uv run python scripts/generate_python_api_docs.py
	@uv run python scripts/generate_api_docs.py
	@uv run python scripts/generate_cli_docs.py

.PHONY: docs
docs: docs-gen
	@rm -rf site
	@uv run --group docs mkdocs build
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
	@find packages -type d -name '*.egg-info' -prune -exec rm -rf {} + 2>/dev/null || true
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

# Pre-Merge Audit — Testability Categorization

For each item in the `valencia → main` pre-merge audit checklist, this document
records whether the invariant can be expressed as a mechanical test/lint **today**
or whether it requires human judgment / spec clarification.

The goal is to convert as much of the checklist as possible into pytest tests
under `tests/audit/` so that:

1. The audit runs on every CI cycle. Staleness is impossible.
2. Each red test maps to one atomic PR.
3. Items requiring judgment are isolated in `DEFER` so they don't block automation.

## Categories

| Tag | Meaning |
|-----|---------|
| `LINT` | Pure grep / AST scan, no runtime needed. Cheapest, most reliable. |
| `REFLECT` | Inspect Python objects (model fields, protocol members, `__annotations__`, enum members). Importable, no fixtures. |
| `UNIT` | Function/class behavior in isolation, possibly with light fixtures. |
| `INTEGRATION` | Multiple services + DB. Marked `audit_integration`. |
| `E2E` | Full stack via httpx (API) or CliRunner / subprocess (CLI). Marked `audit_e2e`. |
| `SUBPROCESS` | Run an example script or a docs build. Marked `audit_slow`. |
| `COVERED` | Already tested by an existing module — only needs a manifest pointer. |
| `DEFER` | Requires human judgment, ambiguous spec, or substantial new infra. Tracked here, not in tests, until we figure out how to assert it. |

---

## Section A — Service-layer additions

| Item | Tag | Notes |
|------|-----|-------|
| A.1.1 OnDestroy event | REFLECT | `tests/audit/test_model_shape.py::test_a_1_1_on_destroy_event_shape` |
| A.1.2 WorldInfo | REFLECT | model_fields + frozen check |
| A.1.3 ProcessorInfo | REFLECT | |
| A.1.4 HookInfo | REFLECT | |
| A.1.5 ResourceInfo | REFLECT | qualname-only |
| A.1.6 EpisodeConfig | REFLECT | |
| A.1.7 RolloutConfig | REFLECT | |
| A.1.8 EpisodeResult | REFLECT | |
| A.1.9 RolloutResult | REFLECT | |
| A.1.10 CommandType entries | REFLECT | enum membership |
| A.2.1 fork_world signature | REFLECT | inspect.signature on iWorldService |
| A.2.2-4 fork semantics | COVERED | `tests/integration/test_fork_destroy_contracts.py` |
| A.2.5 spawn-then-fork | COVERED | same |
| A.2.6 hook isolation | COVERED | same |
| A.2.7 destroy idempotent | COVERED | same |
| A.2.8 destroy fires OnDestroy | UNIT | needs a small fixture; not yet in covered tests |
| A.2.9 destroy preserves storage | COVERED | same fork_destroy_contracts |
| A.2.10 destroy preserves audit | COVERED | same |
| A.2.11 list_* delegate to orchestrator | UNIT | |
| A.2.12 add_resource appends | UNIT | |
| A.2.13 iWorldService protocol shape | REFLECT | inspect dir() |
| A.2.14 remove_world removed | REFLECT + LINT | grep + protocol member check |
| A.3.1-13 simulation service | UNIT / INTEGRATION | mostly behavior; protocol shape via REFLECT |
| A.4.1 gate shape (allow → delegate → audit) | UNIT | source-level introspection |
| A.4.2-9 each gate method | COVERED (partial) | `tests/app/test_audit_contracts.py` covers emission |
| A.4.10 get_audit_history delegates | UNIT | |
| A.4.11 iCommandService protocol shape | REFLECT | |
| **A.5.1 iAsyncStore no delete/drop/remove_data** | **LINT (REFLECT)** | inspect protocol members — high priority |
| **A.5.2 iAuditLog no delete/drop** | **LINT (REFLECT)** | same — high priority |
| A.5.3 AsyncLancedbStore no DELETE calls | LINT | grep src/archetype/app/storage/ |
| A.5.4 10-world stress | COVERED | `tests/integration/test_fork_destroy_contracts.py::test_destroy_preserves_storage_rows` |
| A.5.5 audit row monotonicity | COVERED | same |

## Section B — Runtime rewrite

| Item | Tag | Notes |
|------|-----|-------|
| **B.1.1 runtime/ forbidden app imports** | **LINT** | AST — extends `scripts/check_api_import_boundaries.py` pattern |
| B.1.2 no iWorld/AsyncWorld outside TYPE_CHECKING | LINT | AST visitor that distinguishes `if TYPE_CHECKING` |
| B.1.3 RuntimeWorld __annotations__ no iWorld | REFLECT | |
| B.1.4 RuntimeWorld._state.world_id type | REFLECT | |
| B.1.5 mypy --strict on runtime/ | DEFER | already in CI via lazy-audit; wrap as smoke-test |
| **B.1.6 runtime/__init__.py exports** | **REFLECT** | |
| B.1.7 session.py unchanged | DEFER | "unchanged" is a vacuous predicate; drop or reframe as "Iceberg config exports survived" |
| B.2.1 idempotent shutdown | COVERED | `tests/app/test_runtime_contracts.py` |
| B.2.2 error aggregation | UNIT | not yet covered |
| B.2.3 single-flight | COVERED | runtime_contracts |
| B.2.4-5 op_lock / init_lock | COVERED | runtime_contracts |
| B.2.6 default admin ctx | UNIT | |
| B.2.7 viewer override | UNIT | |
| B.2.8 multi-runtime isolation | UNIT | |
| B.3.1-16 ergonomic routing | UNIT | one parametrized test per method; verifies delegation target |
| B.4.1 lazy activation | COVERED (partial) | runtime_contracts has single-flight; add lazy assertion |
| B.4.2 activation order | UNIT | |
| B.4.3 WorldConfig has no proc/res/hook | REFLECT | |
| B.4.4 pre-activation hook raises | COVERED | runtime_contracts |
| B.5.1 sync surface parity | REFLECT | `dir()` comparison |
| B.5.2 sync owns its Runner | UNIT | |
| B.5.3 sync quickstart | E2E | |
| B.5.4-5 sync property accessors | UNIT | |
| B.5.6 run_sync from running loop | UNIT | |

## Section C — Roles / permissions

| Item | Tag | Notes |
|------|-----|-------|
| **C.1.1-7 COMMANDS_BY_ROLE structure** | **REFLECT** | dict shape, frozenset values, four roles, admin == all, monotone tiers |
| **C.1.8-10 specific role assignments** | **REFLECT** | reads in viewer, CREATE_WORLD only admin, FORK/DESTROY in operator+admin |
| C.1.11 guardrail_allow logic | UNIT | |
| C.2.1 parametrized matrix tests | UNIT | parametrize from `COMMANDS_BY_ROLE.items()` |
| C.2.2 cell-by-cell match with command-gate.md | DEFER | requires parsing markdown table — file as future work |
| C.3.1 no "maintainer" | COVERED | `tests/app/test_old_role_cleanup.py` |
| C.3.2 no "coder" as role | COVERED | same |
| C.3.3-4 same | COVERED | same (canonical regex `["'](?:maintainer\|coder)["']`) |

## Section D — API port

| Item | Tag | Notes |
|------|-----|-------|
| D.1.1-3 deps + lifespan | UNIT | TestClient |
| D.2.1-25 route surface | E2E | one test per route via httpx.AsyncClient |
| D.3.1-3 forbidden imports | COVERED | `scripts/check_api_import_boundaries.py` — pytest wrapper added |
| D.4.1-3 removed routes | LINT + E2E | grep route paths + 404 assertions |
| D.5.1-5 error mapping | UNIT | each exception → expected status |
| D.6.1-2 OpenAPI / docstring roles | E2E | inspect generated openapi |
| D.6.3 openapi.json regenerates | SUBPROCESS | |
| D.6.4 rest-api.md regenerated | DEFER | doc-drift judgment |
| D.7.1 happy path per route | E2E | |
| D.7.2 role matrix | E2E | parametrize from COMMANDS_BY_ROLE |
| D.7.3 WorldInfo response shape | E2E | |
| D.7.4 destroy then query | INTEGRATION | |
| D.7.5 audit emission per route | E2E | |
| D.7.6 import-graph linter | COVERED | api-boundary-audit |
| D.7.7 existing test_routes.py green | COVERED | runs in `make test` |

## Section E — CLI port

| Item | Tag | Notes |
|------|-----|-------|
| E.1.1-5 auth flag wiring | UNIT | CliRunner |
| E.2.1-20 command surface | E2E | one --help smoke per command + happy path |
| **E.3.1-3 renamed/removed cmds** | **LINT** | grep cli/ for old names |
| **E.4.1-3 cli/ forbidden imports** | **LINT** | AST |
| E.5.1-4 output formatters | UNIT | |
| E.6.1-5 test files | partly COVERED, partly DEFER | most need new tests; help-smoke is mechanical |

## Section F — End-to-end integration

| Item | Tag | Notes |
|------|-----|-------|
| F.1.1-7 full stack | INTEGRATION | one big golden-path test |
| F.2.1-7 ActorCtx propagation | INTEGRATION + E2E | API + CLI + runtime, parametrized |
| F.3.1-3 append-only across surfaces | INTEGRATION | row counters |
| F.3.4-5 append-only via curl/CLI | E2E | recording mock store |

## Section G — Documentation + examples

| Item | Tag | Notes |
|------|-----|-------|
| G.1.1-2 README quickstart | SUBPROCESS | extract code blocks, exec |
| G.1.3-5 README content drift | DEFER | judgment call; could grep for specific endpoint names as a proxy |
| G.2.1-5 doc files exist | LINT | `pathlib.exists` |
| G.2.6 specification.md unchanged | DEFER | "unchanged" needs a baseline hash to be meaningful |
| G.2.7 make docs builds | SUBPROCESS | |
| G.3.1-3 AGENTS.md / CLAUDE.md content | DEFER | judgment / partial LINT |
| G.4.1-6 examples run | SUBPROCESS | one test per example, `audit_slow` |

## Section H — Final merge readiness

| Item | Tag | Notes |
|------|-----|-------|
| H.1.1-5 CI green | COVERED | `make ci` is the gate |
| **H.2.1-7 forbidden imports** | **LINT** | consolidated in `tests/audit/test_import_boundaries.py` |
| **H.3.1-4 no stale references** | **LINT** | grep |
| H.4.1-2 CHANGELOG entry | DEFER | content judgment; could LINT for required keywords |
| H.5.1 fresh clone + uv sync + make ci | SUBPROCESS | already CI-equivalent; skip locally |
| H.5.2 examples | covered by G.4 |
| H.5.3-5 smoke flows | E2E |

---

## Round 1 — what gets written first

The bolded rows above. Concretely:

1. **A.1.1-10** — model & enum shape (REFLECT) — should all pass today; locks them in.
2. **A.5.1-2** — protocol method-name reflection (REFLECT) — load-bearing append-only invariant.
3. **B.1.1** + **B.1.6** + **E.4.1-3** + **H.2.x** — import boundaries (LINT/AST) — load-bearing single-gate invariant.
4. **C.1.1-11** — COMMANDS_BY_ROLE shape (REFLECT) — load-bearing RBAC invariant.
5. **C.3.1-4** + **H.3.1-4** — stale-string greps (LINT).

Estimated: ~40 distinct assertions, all in one self-contained `tests/audit/` package.
Expected outcome: most pass today, locking in current correctness as regression
guards. Any that fail are the first actionable atomic PRs for the loop.

## Round 2 — once Round 1 lands

UNIT-tier items where the surface already exists:
- C.1.11 guardrail_allow behavior + C.2.1 parametrized matrix.
- B.1.3-4 RuntimeWorld annotations.
- B.4.3 WorldConfig has no proc/res/hook fields.
- A.2.13/A.3.12/A.4.11 protocol shape via inspect.signature.

## Round 3+ — after we've shaken out the cheap wins

INTEGRATION + E2E rounds, route-by-route and CLI-command-by-command.
SUBPROCESS round for examples and docs build.

## Deferred (need design discussion)

- C.2.2 — cell-by-cell match against `command-gate.md`. Requires either parsing
  the markdown table or maintaining the table as machine-readable YAML alongside.
- B.1.7 / G.2.6 — "unchanged" predicates. Either drop them or pin a content hash.
- G.1.3-5, G.3.1-3, H.4.1-2 — doc-content drift. Possible proxies via grepping
  for old endpoint/role names, but not full coverage.
- D.6.4 — `rest-api.md` regenerated from OpenAPI. Make this a `make docs`-time
  check that diffs the generated file vs the committed one.

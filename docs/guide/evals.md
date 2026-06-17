# Evaluation Criteria

Archetype evals verify runtime behavior through a **task → trial → grader** model.
Tasks grade **outcomes**, not the path the system took to produce them.

Source: `evals/` at the repo root. Run with:

```bash
uv run python -m evals.run
uv run python -m evals.run --suite regression
uv run python -m evals.run --suite spec
uv run python -m evals.run --trials 3 --out eval-results.json
```

## Hierarchy

| Level | Definition | Pass condition |
|---|---|---|
| **Grader** | One deterministic check on trial output | `passed=True` (score 0.0–1.0) |
| **Trial** | One execution of a task | **All** graders pass |
| **Task** | Registered eval with optional `--trials k` | **All** trials pass (`all_passed`) |
| **Run** | Full harness execution | See [suite gates](#suite-gates) below |

Vocabulary matches `evals/types.py`: `GraderResult`, `TrialResult`, `TaskResult`.

## Grader types

Graders live in `evals/graders.py`. They are code-based: fast, objective, and reproducible.

### `exact_match`

Binary grader. Passes if `actual == expected`. Score is 1.0 or 0.0.

### `state_check`

Outcome verification over a dict of named boolean checks. Passes only if **every** check is `True`.
Score is the fraction of checks passing (partial credit for reporting), but the trial still fails if any check fails.

### `threshold`

Numeric bounds grader. Passes if the value is within `[min_val, max_val]`.

### `raises`

Passes if the callable raises the expected exception type.

## Suite gates

Evals are grouped into three suites. Registration lives in:

- `evals/suites/regression.py`
- `evals/suites/poison_command.py` (regression suite)
- `evals/suites/spec_contracts.py`
- `evals/suites/capability.py`

### Regression

Must always pass. Any failure is a regression. Includes core invariants (serialization, RBAC, command pipeline) and adversarial poison-command handling.

### Spec

Must always pass. Structural guardrails derived from normative docs under `docs/guide/`. Each task verifies that a spec clause still has an executable check independent of older test modules.

### Capability

Measures what the system can do well under load. Pass rate may vary, but trials must not crash.

### Run exit code

`evals/run.py` returns exit code `0` only when:

1. At least one task ran in the requested scope.
2. **Regression and spec:** every task in each required suite has `all_passed=True`.
3. **Capability:** no capability trial has `error` set (uncaught exception).

When no `--suite` filter is given, **regression** and **spec** are required. Capability tasks may fail graders without failing the run, as long as they do not error out.

## Regression tasks (10)

### Core invariants

Defined in `evals/suites/regression.py`.

| Task | Criteria |
|---|---|
| `component_serde` | `to_row_dict()` values match; prefix is `health__`; schema fields are `{health__hp}` |
| `archetype_signatures` | Signatures order-invariant, sorted by class name, remove-op correct, names stable and unique, schema has base + component fields, row dict values correct |
| `rbac_enforcement` | Admin allowed spawn/add_processor; viewer denied spawn/despawn; player allowed spawn, denied add_processor; all token costs > 0; default role is `viewer` |
| `command_ordering` | Sort order: lower tick → lower priority → earlier seq; 50 commands get unique IDs |
| `command_pipeline` | Submit enqueues (pending=1); step drains (applied=1); pending cleared; history recorded with correct type; viewer blocked at service boundary |

### Adversarial and idempotency-style

Defined in `evals/suites/poison_command.py`. These verify that malformed, invalid, or unhandled commands are swallowed gracefully by `drain_and_apply` without corrupting world state or blocking valid commands in the same tick.

| Task | Criteria |
|---|---|
| `poison_in_batch` | Malformed spawn (no `type` key) swallowed; **2/3** valid spawns applied; world has typed archetype, no base `Component` archetype |
| `missing_payload_keys` | DESPAWN, REMOVE_COMPONENT, and UPDATE with missing keys fail silently; **0 entities**, world unchanged |
| `unknown_component_type` | REMOVE with bogus type name leaves entity signature and existence intact |
| `despawn_nonexistent` | DESPAWN of missing `entity_id=99999` is a no-op; entity count unchanged, real entity intact |
| `unhandled_command_noop` | MESSAGE, QUERY_WORLD, and CUSTOM dequeued; pending=0; no entities created |

**Poison-command design principle:** bad commands may log at debug and are dropped. They must **never** corrupt world state or block valid commands in the same tick.

## Spec tasks (6)

Defined in `evals/suites/spec_contracts.py`.

| Task | Criteria |
|---|---|
| `spec.manifest_traceability` | Spec cases cite normative docs and registered eval tasks |
| `spec.role_permission_matrix` | Role permissions match `command-gate.md` exactly |
| `spec.runtime_gate_only_boundary` | Runtime imports only gate-facing app modules; stores no live world refs |
| `spec.command_service_gate_map` | CommandService public methods use the documented gate and audit shape |
| `spec.append_only_protocols` | Storage and audit protocols expose no destructive delete/drop methods |
| `spec.info_class_downgrades` | Gate lifecycle and introspection methods return frozen info snapshots |

## Capability tasks (2)

Defined in `evals/suites/capability.py`.

| Task | Criteria |
|---|---|
| `storage_roundtrip` | 20 entities written, flushed, read back; all entity IDs match; per-entity `hp`, `name`, `level` field integrity |
| `simulation_correctness` | 50 entities × 3 steps with `ApplyVelocity`: entity count, presence, no duplicates; positions `x += 3`, `y -= 3`; velocity and health unchanged; query returns all expected columns |

## Metrics (multi-trial runs)

Pass `--trials k` to run each task multiple times. Useful for non-deterministic behavior or reliability checks.

| Metric | Meaning |
|---|---|
| `pass@k` | Fraction of trials that passed |
| `pass^k` | 1.0 only if **every** trial passed (strict reliability) |
| `avg_score` | Mean grader score across trials |
| `all_passed` | Every trial passed every grader |

Default is `k=1`.

## What is not evaluated today

These boundaries exist in the codebase but have **no dedicated eval task** yet:

- Command submit idempotency via `idempotency_key`
- Double-despawn of the same entity
- `destroy_world` on an unknown world ID
- Processor failure continuation semantics
- Native/Rust core parity

See [Specification](specification.md) for documented idempotent vs non-idempotent operation boundaries.

## Adding a task

1. Implement a task function returning `list[GraderResult]`.
2. Register it in the appropriate suite's `register(harness)` function.
3. Use `exact_match` for binary checks and `state_check` for multi-condition outcome verification.
4. Regression and spec tasks should be deterministic and fast. Capability tasks may exercise heavier paths (storage flush, multi-step simulation).

## Running a single task

```bash
uv run python -c "
from evals.harness import EvalHarness
from evals.suites import poison_command
h = EvalHarness()
poison_command.register(h)
for t in h.run(suite_filter='regression'):
    if t.task_id == 'despawn_nonexistent':
        print(t.task_id, 'PASS' if t.all_passed else 'FAIL')
        for g in t.trials[0].grader_results:
            print(f'  [{g.grader_name}] {g.details}')
"
```

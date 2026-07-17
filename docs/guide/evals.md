# Evaluation Harness

Archetype's evals are independent executable oracles for repository-level
contracts. They complement unit and integration tests: a task assembles a
realistic boundary, observes the result, and grades the outcome without
depending on the feature test that originally motivated the contract.

The harness lives under `evals/` and uses a **task → trial → grader** model.
It does not ask whether the implementation followed one preferred code path;
it asks whether the resulting behavior satisfies the contract.

These are meta-evaluation terms internal to the repository harness.
`evals.types.TrialResult` is not the physical-AI trial that produces one
dataset episode in the
[Dataset and Evaluation Ontology](dataset-eval-ontology.md), and harness task
ids are not dataset task natural keys.

## Running the suites

```bash
make eval          # every suite; writes eval-results.json
make eval-reg      # regression only
make eval-idem     # idempotency only
make eval-cap      # capability only

# Spec-only and repeated-trial runs use the module directly.
uv run python -m evals.run --suite spec
uv run python -m evals.run --suite regression --trials 3
uv run python -m evals.run --out eval-results.json
```

`--trials k` must be a positive integer. Each task runs `k` times and reports:

| Metric | Meaning |
|---|---|
| `pass@k` | Fraction of trials that passed |
| `pass^k` | `1.0` only when every trial passed |
| `avg_score` | Mean grader score across trials |
| `all_passed` | Every grader passed in every trial |

The default is one trial. Repeated trials are useful when a boundary involves
process scheduling or another source of nondeterminism; they do not turn a
deterministic failure into an acceptable result.

## What passes the gate

The four suites have deliberately different meanings:

| Suite | Contract | CLI exit condition |
|---|---|---|
| Regression | Previously working behavior must remain true | Every task passes every trial |
| Spec | Normative guide claims retain independent executable evidence | Every task passes every trial |
| Idempotency | Retry, replay, and non-idempotent boundaries match the specification matrix | Every task passes every trial |
| Capability | Exercise larger end-to-end behavior without turning a score into a release threshold | No trial crashes; grader misses remain visible in the report |

An empty requested suite is a failure, never a vacuous success. A full run also
requires the regression, spec, and idempotency suites to be present.

`make ci` runs regression and idempotency evals after the static checks and
pytest suite. The spec suite has its own pytest registration and CLI smoke
contracts under `tests/spec_contracts/`, so it is exercised by that same gate.
The GitHub Actions eval job runs regression and capability separately.

The idempotency suite has an additional fast static oracle:
`make idempotency-audit` verifies that every normative matrix row in the
[specification](specification.md) maps to a registered task before the
behavioral scenarios run.

## Outcomes, not log noise

Expected poison commands are part of the regression input. The command drain
may log while converting one of those failures into a dropped command, but the
eval CLI filters the known malformed-command, no-op-command, reserved-spawn
replay, and missing-despawn records from its report. Other `archetype` records
remain visible. A task failure is still explicit: uncaught exceptions populate
`TrialResult.error`, failed graders include their details, and either condition
affects the suite according to the table above.

This quieting belongs to the eval process boundary. It does not change library
log levels, production runtime logging, or `RunConfig(debug=True)`. A host that
has explicitly attached an `archetype` log handler keeps that configuration.

## Graders

Code-based graders live in `evals/graders.py`:

- `exact_match` checks equality and returns a binary score.
- `state_check` requires every named boolean check to pass while retaining a
  fractional score for diagnosis.
- `threshold` checks an inclusive numeric range.
- `raises` checks an expected exception type.
- `crap_score` combines cyclomatic complexity and coverage for code-quality
  evaluation.

Every trial must produce at least one meaningful `GraderResult`; a task that
observes nothing is not an oracle. An empty result list fails the trial with a
`TrialResult.error`, and `state_check` rejects an empty check mapping. A
required-suite run therefore exits nonzero instead of accepting Python's
vacuous `all([])` result or a grader that wraps equally vacuous state evidence.

## Registered task manifest

This inventory is checked against `build_harness()` by
`tests/spec_contracts/test_documentation_contracts.py`. Adding, removing, or
renaming a task requires updating this table in the same change.

<!-- eval-task-manifest:start -->

### Regression

| Suite | Task | Contract exercised |
|---|---|---|
| `regression` | `component_serde` | Component row serialization, prefixes, and schemas |
| `regression` | `archetype_signatures` | Signature ordering, set operations, naming, and schema composition |
| `regression` | `rbac_enforcement` | Role permissions, token costs, and the default role |
| `regression` | `command_ordering` | Deterministic `(tick, priority, seq)` command order |
| `regression` | `command_pipeline` | Submit → broker → step → history, with RBAC at the service boundary |
| `regression` | `query_correctness` | Cold gated reads union component subsets, honor filters/projection, and discover durable signatures |
| `regression` | `tick_quota_resets` | Per-tick command quotas reset at each tick rather than process-wide |
| `regression` | `quota_boundaries` | Exact 499/500/501 limits, atomic bulk accounting, actor isolation, and UTC daily rollover |
| `regression` | `episode_value_termination` | Value-based episode termination stops before the defensive cap |
| `regression` | `poison_in_batch` | A malformed command does not block valid commands in the same drain |
| `regression` | `missing_payload_keys` | Missing required keys do not corrupt world state |
| `regression` | `unknown_component_type` | An unknown removal type preserves the entity signature |
| `regression` | `despawn_nonexistent` | Despawning a missing entity is an observable no-op |
| `regression` | `unhandled_command_noop` | Message, query, and custom commands drain without implicit mutation |

### Spec

| Suite | Task | Contract exercised |
|---|---|---|
| `spec` | `spec.manifest_traceability` | Spec cases cite normative sources and registered task IDs |
| `spec` | `spec.role_permission_matrix` | Code permissions match the command-gate matrix |
| `spec` | `spec.runtime_gate_only_boundary` | Runtime dependencies remain behind the command gate |
| `spec` | `spec.command_service_gate_map` | Public command methods retain gate and audit coverage |
| `spec` | `spec.append_only_protocols` | Storage and audit protocols expose no destructive methods |
| `spec` | `spec.receipt_authority_firewall` | Receipts and facts remain evidence rather than authority |
| `spec` | `spec.dataset_eval_ontology` | Dataset natural keys remain separate from optional runtime provenance |
| `spec` | `spec.info_class_downgrades` | Lifecycle and introspection return frozen information snapshots |

### Idempotency

| Suite | Task | Contract exercised |
|---|---|---|
| `idempotency` | `idempotency.manifest_traceability` | The specification matrix and behavioral manifest agree |
| `idempotency` | `idempotency.storage_pooling_and_shutdown` | Storage pooling and cached-store shutdown converge safely |
| `idempotency` | `idempotency.world_lifecycle` | Explicit-ID create and missing or repeated destroy behavior |
| `idempotency` | `idempotency.broker_and_submit_non_idempotent` | Duplicate logical enqueues remain distinct commands |
| `idempotency` | `idempotency.submit_spawn_distinct_entities` | Repeated submitted spawns reserve distinct entity IDs |
| `idempotency` | `idempotency.async_world_entity_ids_and_missing_remove` | Entity allocation and missing removal preserve world state |
| `idempotency` | `idempotency.runtime_aliases_and_history` | Actor aliases and fixed-filter history reads remain stable |
| `idempotency` | `idempotency.staged_spawn_last_write_wins` | Duplicate raw staged spawn rows resolve last-write-wins at materialization |
| `idempotency` | `idempotency.same_tick_duplicate_mutations` | Duplicate same-tick entity mutations collapse at materialization |
| `idempotency` | `idempotency.component_signature_noops` | Signature-preserving component operations stage no rows or hooks |
| `idempotency` | `idempotency.fixed_reads` | Repeated fixed-state queries, history, and signature reads agree |
| `idempotency` | `idempotency.query_archetype_repeatable` | Repeated reads are stable while an updater replay remains an append |
| `idempotency` | `idempotency.step_and_run_non_idempotent` | Repeated execution advances time and appends rows |
| `idempotency` | `idempotency.atomic_publish_retry` | A failed publication stays invisible and retry publishes once |
| `idempotency` | `idempotency.durable_discovery` | Cold discovery, reads, and catalog registration converge |
| `idempotency` | `idempotency.resume_and_writer_fencing` | Resumed writers fence stale attempts from visibility |
| `idempotency` | `idempotency.durable_fact_replay` | Concurrent fact replay converges; content conflicts fail loudly |
| `idempotency` | `idempotency.durable_fact_crash_recovery` | Lease takeover completes an appended orphan without duplication |
| `idempotency` | `idempotency.evaluation_receipt_replay` | Receipt replay returns prior evidence without grading again |
| `idempotency` | `idempotency.process_crash_cold_resume` | Cold resume retries one tick after a hard process failure |
| `idempotency` | `idempotency.process_writer_fence_race` | Exactly one of two racing writer processes publishes |
| `idempotency` | `idempotency.process_fact_replay` | Independent processes converge on one external fact identity |
| `idempotency` | `idempotency.process_evaluation_replay` | Concurrent grading runs once and changed subjects conflict first |

### Capability

| Suite | Task | Contract exercised |
|---|---|---|
| `capability` | `storage_roundtrip` | Mixed component fields survive persistence and retrieval |
| `capability` | `simulation_correctness` | Multi-step processing preserves entities and updates expected fields |
| `capability` | `fork_divergence` | Fork-of-fork lineage, first-step continuity, shared resources, and isolated mutations compose through the runtime |
| `capability` | `time_travel_and_run_id` | Live and cold historical reads preserve one run identity across durable resume |

<!-- eval-task-manifest:end -->

## Adding or changing a task

1. Put the scenario in the suite that owns its failure semantics.
2. Drive the highest stable boundary that proves the contract. Use lower-level
   seams only when the task is explicitly about that seam or must construct a
   crash state that public calls cannot produce deterministically.
3. Grade externally visible outcomes. Do not duplicate the implementation in
   the assertion.
4. Register the task in the suite's `register(harness)` function and update the
   manifest above.
5. Run the focused suite, its static traceability check when applicable, and
   `make ci` for cross-layer behavior.

Keep task IDs stable once a contract cites them. When semantics genuinely
change, update the normative source, implementation, task, and this manifest as
one reviewable unit.

## Relationship to other checks

- Pytest provides focused unit, integration, race, and contract diagnosis.
- Evals provide independent repository-level outcomes and traceability.
- [Mutation testing](mutation-testing.md) probes whether focused assertions
  detect controlled implementation changes; it is intentionally on demand.
- Benchmarks measure cost and trend, not semantic correctness.

No one layer substitutes for the others. A useful change identifies the
smallest oracle that proves its local contract, then runs the broader gates in
proportion to the boundary it crosses.

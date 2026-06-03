# Graduation

**Document type:** Normative (in development).
**Scope:** Implementation lifecycle from frontier `prompt()` baseline to native Rust extension. Sits above `evals/` and `archetype.experiments`; orthogonal to the world / processor stack.

## 1. Purpose

Graduation is the discipline by which an implementation of a task earns its way from a frontier-LLM baseline toward a native Daft extension through eval-gated promotion.

The thesis is three-line:

1. A DataFrame is the substrate.
2. Extension is the expansion — when the substrate doesn't reach, you extend it instead of escaping it.
3. Falsifiability is the discipline — promotion is empirical (an eval delta), never aesthetic.

This page defines:

- The level vocabulary and what each level is made of.
- Hard requirements on the registry, gates, and audit trail.
- The protocols implementations, evals, and gates MUST satisfy.
- Open questions that MUST be resolved (not guessed) before M0 lands.

## 2. Levels

| Level | Made of | Promotion gate from level below |
|---|---|---|
| **L0** | `daft.functions.prompt(model=…)` against a pinned frontier model | none — L0 is the baseline |
| **L1** | Python `@daft.func` / `@daft.cls` (often LLM-generated) | beats L0 on the eval, or matches at strictly lower cost |
| **L2** | Optimized Python: vectorized, batched, GIL-aware | meets a throughput / cost target the L1 form can't |
| **L3** | Native Rust Daft expression | meets a latency target Python can't |
| **L4** | Upstreamed to Daft proper | the abstraction is general enough that the substrate should own it |

An implementation occupies exactly one level at a time, set by the most recent successful promotion or demotion. Levels are not aspirational; they are an attestation backed by gate evidence.

## 3. Hard requirements

### R1 — Single current level

The Registry MUST hold exactly one current level per `implementation_id`. Promotions and demotions append history; the current level is derived from the latest transition.

### R2 — Promotions cite reproducible gate evidence

Every promotion MUST reference the eval result(s) that authorized it. Re-running the cited eval against the cited fixture MUST yield the same gate verdict. Non-reproducible gates are a spec violation.

### R3 — Eval fixtures are immutable once cited

Once a gate has cited an eval fixture, that fixture MUST NOT be mutated. Fixture changes require a new fixture ID and re-evaluation. This is what makes gate evidence auditable across time.

### R4 — Frontier baselines are first-class implementations

`prompt()` against a pinned model is itself an L0 implementation in the Registry. Same eval shape, same gate path. The baseline is not a special case.

### R5 — Demotion is allowed and audited

If an L_N implementation regresses below the gate that promoted it — cost grows, eval delta narrows, dependency breaks — it MAY be demoted. Demotions follow the same audit shape as promotions.

### R6 — Pinned model identifiers

Every L0 implementation MUST pin the exact frontier model identifier it uses (provider + model + version). Frontier drift is real; an unpinned baseline makes every gate above it flaky.

### R7 — The Registry dogfoods the substrate

The Registry MUST be expressed as Daft tables (in-process or persisted to a Daft-readable format). The graduation framework cannot escape its own substrate.

### R8 — No skipping levels in the audit trail

A promotion from L_N to L_{N+2} MUST record the L_{N+1} eval that justified the leap (i.e., why the intermediate level was unnecessary). Skipping silently is a spec violation; skipping with evidence is a documented choice.

## 4. Contracts

Typed protocols. Signatures are normative; internals are not.

```python
from typing import Protocol
from daft import DataFrame

class Implementation(Protocol):
    id: str
    level: Level                # L0 | L1 | L2 | L3 | L4
    signature: Signature        # input columns -> output columns
    def __call__(self, df: DataFrame) -> DataFrame: ...

class Eval(Protocol):
    id: str
    fixture_id: str
    def __call__(self, impl: Implementation) -> EvalResult: ...

class Gate(Protocol):
    def verdict(
        self, below: EvalResult, candidate: EvalResult
    ) -> Verdict:               # Promote | Hold | Demote
        ...

class Registry(Protocol):
    def register(self, impl: Implementation, level: Level) -> None: ...
    def promote(self, impl_id: str, evidence: list[EvalResult]) -> None: ...
    def demote(self, impl_id: str, evidence: list[EvalResult]) -> None: ...
    def current(self, impl_id: str) -> Implementation: ...
    def history(self, impl_id: str) -> list[Transition]: ...
```

`EvalResult` carries pass@k, cost, latency, and the fixture ID used. The exact shape is **OPEN Q 2** below.

## 5. Open questions

Per the spec-driven mandate, these MUST be resolved before M0 lands. Each question names the trade-off; none is answered here.

- **OPEN Q 1 — Registry storage.** In-process Daft table written through `iCommandService` (R7-aligned, full dogfood) versus a Daft-readable Parquet / Iceberg table on disk (durable across runtimes, queryable from non-Daft tools). The first is consistent with Archetype's append-only ECS substrate; the second is operationally simpler.
- **OPEN Q 2 — Gate evidence shape.** Candidate fields: pass@k delta with confidence interval, cost ratio, latency p50/p99, eval fixture ID, model pin (for L0 evidence). The `Verdict` predicate is a function of which subset is required and which is advisory.
- **OPEN Q 3 — Versioning.** When an L1 implementation is replaced (new code, same `id`), does the new version inherit L1 or re-earn it from L0? Re-earning preserves R2 reproducibility; inheritance is ergonomic but turns the level into a label that can drift from its evidence.
- **OPEN Q 4 — Demotion trigger.** Automatic on regression-eval failure, or manual (a maintainer's call)? Automatic is honest but noisy and risks demoting on transient infra issues; manual lets work compound but means stale levels.
- **OPEN Q 5 — L3 / L4 boundary.** What makes an extension Archetype-specific versus general? Working hypothesis: "depends on Component / processor concepts" → stays in Archetype; "pure DataFrame primitive" → upstreams to Daft. The criterion needs sharpening before M6.
- **OPEN Q 6 — Frontier drift policy.** When a pinned L0 model is deprecated by its provider, what happens to gates that cited it? Options: re-baseline against the successor and re-run dependent gates; freeze affected gates as historical evidence; require a new L0 implementation for the successor and treat it as a separate baseline.
- **OPEN Q 7 — Relationship to `archetype.experiments`.** `Experiment` / `Run` / `Result` / `BranchHead` already model the run lifecycle as ECS state (see `autoresearch.md`). The Registry SHOULD reuse those primitives rather than duplicate them, but the mapping — is an `Implementation` a long-lived `BranchHead` target? is a `Gate` a `Result` predicate? — is not yet defined.

## 6. Build order

| Milestone | Deliverable | Status |
|---|---|---|
| **M0** | Registry as Daft tables; `Implementation`, `EvalResult`, `Promotion`, `Demotion`, `Transition` schemas | not started |
| **M1** | Gate framework with audit-trail emission and reproducibility check | not started |
| **M2** | L0 baseline machinery — pinned-model `prompt()` wrappers as registered implementations | not started |
| **M3** | L0 → L1 autoresearch loop — LLM proposes N candidates, evals select, gate authorizes promotion | not started |
| **M4** | L1 → L2 optimization gate — throughput / cost regression tests built into the registry | not started |
| **M5** | L2 → L3 Rust extension scaffold — first `@daft.func`-Rust port with full gate evidence | not started |
| **M6** | L3 → L4 upstream path — formal criteria for "general enough" and a contribution playbook | not started |

The first move is M0. Everything above it hangs off the audit trail; without a registry, gates have nowhere to write evidence.

## 7. Status

**In development.** This page defines the contract; no implementation exists yet.

- The eval layer (`evals/graders.py`, `tests/eval_framework/`) is the foundation gates will build on. `llm_judge` is the first L0-shaped grader.
- `archetype.experiments` (see [`autoresearch.md`](autoresearch.md)) already models runs as ECS state. **OPEN Q 7** is the first design question: how the Registry composes with those primitives.
- `daft.functions.prompt` + Pydantic `response_format` + injectable `prompt_fn=` is the canonical L0 surface (see `feedback_prompt_is_the_right_shape` in operator memory). Custom `@daft.cls` HTTP wrappers are not.

Cross-references:

- [`autoresearch.md`](autoresearch.md) — run / branch-head lifecycle this Registry SHOULD compose with
- [`specification.md`](specification.md) — umbrella contract page; this page should be added to the Contract Inventory once M0 lands and the contract is no longer "in development"
- `evals/graders.py` — gate predicates compose over grader output

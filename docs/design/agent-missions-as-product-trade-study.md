# Agent Missions as a Product — Trade Study

**Status:** Decision support. Compares how agent missions should be authored
and constructed once the prefab library exists (`docs/design/prefab-library.md`,
`src/archetype/graph/prefab.py`). It does not change any normative contract; it
recommends which of four paths to invest in and in what order. Grounded in the
mission authority spec (`docs/guide/agent-missions.md`) and dogfooded by
`examples/13_mission_library.py`.

---

## 1. Decision question

A coding mission today is one `Mission` row plus one `TaskGate` row carrying a
`plan_json` blob, driven by a state machine whose authority
(`archetype.app.missions`) owns claim fences, RBAC, audit, and the "world tick
is the commit boundary" rule. Missions are *constructed* — someone writes
`Mission(plan_json=...)` at a call site (test, API route, CLI) — not *authored*
from a reusable, inspectable source.

The prefab library makes a second option real: a mission can be *instantiated*
from an explicit ECS asset graph. **Should agent missions remain procedurally
constructed, or become prefab-instantiated — and if the latter, at what layer
and representation, and how far toward a versioned product do we go now?**

This matters because the product thesis ("agent missions — that's a product")
rests on properties construction cannot give: a named, versioned catalog of
mission kinds; variants and overrides without code edits; and a gradeable
population where re-instantiation under a new version is the upgrade path and
both generations stay on the ledger.

---

## 2. Constraints (non-negotiable)

- **Mission authority is inviolable.** Nothing may create or advance a mission
  outside `CommandService`/`MissionService`; claim fences must stay durable
  before provider I/O; the world tick stays the commit boundary
  (`docs/guide/agent-missions.md` §1). Any option that lets `instantiate()`
  write mission state must route through the same authority, not around it.
- **No silent representation drift.** `plan_json` and `TaskGate` must stay
  consistent (the gate mirrors `plan[0]`); a mission row that violates its
  component validators must be impossible to instantiate.
- **Additive first.** graph-system.md forbids core/app changes in the prefab
  stages (0–7); the runtime must keep working untouched while we evaluate.

---

## 3. Alternatives

**A — Procedural status quo.** Missions are constructed inline as
`Mission(plan_json=...)` + `TaskGate(...)` wherever they are spawned. No library,
no asset graph. (Baseline.)

**B — Prefab authoring layer, runtime unchanged.** A mission prefab carries the
real `Mission` + `TaskGate` components plus its role/validator/sandbox topology;
`instantiate()` produces today's mission rows from per-run overrides (repo,
branch, plan). The topology and shared-asset edges are additive ECS facts the
state machine ignores. Runtime, authority, and `plan_json` are untouched. This
is exactly what `examples/13_mission_library.py` already does.

**C — Prefab-native mission representation.** Missions become entity graphs:
tasks, validators, agents, and the sandbox are related entities; `plan_json` is
decomposed into task/validator nodes the runtime reads through relations instead
of a JSON blob. Maximally inspectable and composable, but the state machine,
identity/staleness checks, and claim logic must be re-expressed against the
graph — changes inside normative authority.

**D — Versioned mission registry (the product).** B (or later C) plus the
Stage 8 registry: named, versioned mission libraries; a catalog/manifest;
eval binding; and `instantiate(version=...)` as the supported mission-creation
entry point. This is where "that's a product" actually lives — the library
becomes a shippable, gradeable asset, not a code pattern.

---

## 4. Criteria and weights

| # | Criterion | Weight | Why |
|---|---|---|---|
| C1 | Blast radius / migration cost | 5 | Touching normative authority is the dominant risk. |
| C2 | Preserves mission authority | 5 | Hard constraint; an option that erodes it is disqualified, not just scored low. |
| C3 | Versioning & gradeability | 4 | The product thesis: versioned kinds, re-instantiation upgrade, both generations on the ledger. |
| C4 | Inspectability & queryability | 4 | Missions as ECS facts — query variants, topology, shared assets. |
| C5 | Authoring ergonomics & reuse | 3 | Variants, overrides, shared agents/validators without code edits. |
| C6 | Time to first product value | 3 | How soon a user gets a catalogued, instantiable mission. |
| C7 | Runtime cost | 2 | Instantiation and any query-time resolution overhead. |

Scores are 1 (poor) – 5 (excellent). C2 is a gate: any option scoring below 4
on C2 is rejected regardless of weighted total.

---

## 5. Scoring

| Criterion (weight) | A: Procedural | B: Authoring layer | C: Prefab-native | D: Registry (on B) |
|---|---|---|---|---|
| C1 Blast radius (5) | 5 | 5 | 1 | 4 |
| C2 Authority preserved (5) | 5 | 5 | 3 | 5 |
| C3 Versioning/gradeability (4) | 1 | 3 | 4 | 5 |
| C4 Inspectability (4) | 1 | 4 | 5 | 4 |
| C5 Authoring/reuse (3) | 1 | 4 | 5 | 5 |
| C6 Time to value (3) | 5 | 4 | 1 | 3 |
| C7 Runtime cost (2) | 5 | 5 | 3 | 4 |
| **Weighted total** | **86** | **112** | **80*** | **113** |

Weighted totals out of 130. \*C fails the C2 gate (3 < 4): it is scored for
comparison but not eligible as a near-term choice — it moves mission authority
onto an unproven representation. Note the gate, not the total, is what removes
C: on raw score A (86) already edges C (80) because C's blast radius tanks C1.

Row notes:

- **A** is cheap and safe but scores near zero on every property that makes
  missions a product (C3/C4/C5 all 1); it is the thing we are trying to grow
  past, and its respectable total is entirely do-nothing safety.
- **B** dominates A on inspectability, reuse, and versioning at *identical* blast
  radius and authority scores — because it is purely additive (proven: the
  dogfood produces valid `READY` mission rows and the runtime never sees the
  topology). Its C3 is only 3 because versioning is still ad hoc without D.
- **C** has the best representation (C4/C5 = 5) but fails the authority gate
  today and carries the largest migration (plan_json → graph, state machine
  rewrite, and it forces the OQ1 runtime-resolution decision). High ceiling,
  wrong time.
- **D** edges B (113 vs 112) and is the actual product surface: it adds
  versioning/gradeability at some blast-radius and time-to-value cost, and
  depends on B landing and on the Stage 8 versioning story (#543).

---

## 6. Recommendation

**Adopt B now; build D as the product; defer C until OQ1 is decided.**

1. **B is already dogfooded — bless it as the authoring path.** Missions are
   authored as prefabs and instantiated into the existing rows; no runtime or
   authority change. This immediately buys inspectability (topology and shared
   assets are ECS facts), reuse (variants via `IsA` + overrides), and the
   `Prefab`-exclusion safety that stops the factory running its own template as
   a mission. Keep instantiation feeding `MissionService`/`CommandService` — the
   prefab produces the row; authority still commits it.
2. **Layer D on top of B for the product.** The Stage 8 registry — named,
   versioned mission libraries, catalog/manifest, eval binding — is where the
   gradeable, shippable product lives. It needs its own design and must answer
   `#543` (schema evolution against persisted tables) before it can promise
   re-instantiation as the upgrade path. Nothing in D requires C.
3. **Defer C behind a decision, not a schedule.** Prefab-native missions are the
   strongest end state but move authority onto the graph and force OQ1
   (`INHERIT` copy-at-instantiate vs resolve-at-query). Revisit only after B/D
   prove the shapes in production and OQ1 has an explicit yes/no; treat any
   plan_json → graph migration as a separate, gated design.

The through-line: **B makes missions authorable at zero risk, D makes them a
product, and C is a representation bet to make deliberately later — never as a
side effect.**

---

## 7. Risks and open decisions

- **Instantiation must not become a governance bypass.** The moment
  `instantiate()` is wired into a live mission-creation path, its spawns must go
  through `CommandService` (they already use the governed world API). A future
  "instantiate directly into the store" optimization would breach C2 and is
  out of scope.
- **Consistency of `plan_json` and `TaskGate`.** B relies on the override
  producing a gate that mirrors `plan[0]`; the dogfood centralizes that in one
  `mission_overrides` helper. D should promote that into a validated library
  builder so an authored mission cannot encode an inconsistent gate.
- **Versioning (#543).** D's upgrade-by-re-instantiation only holds if a mission
  prefab version is addressable and its schema evolution against persisted
  tables is defined. This is the D blocker and is inherited wholesale.
- **OQ1 coupling.** C's value (shared, non-duplicated mission policy resolved
  through `IsA`) is exactly the query-time-resolution capability OQ1 defers.
  C and OQ1 are one decision, not two.

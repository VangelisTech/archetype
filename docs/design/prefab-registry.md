# PreFab Registry — Design

**Status:** Proposed, for review. Written after stages 1–7 of the graph track
landed (`docs/design/graph-system.md`), per issue #555's design-doc-first
rule. Nothing here is implemented.

---

## 1. Motivation

Stages 1–7 made prefabs world content: a template is an entity with a
`Prefab` marker, component values, and a `ChildOf` subtree; `instantiate`
copies it and records `IsA` lineage; editing a template never mutates
instances, so both generations sit on the ledger. What is still missing is
the library layer: how agents discover prefabs, how versions are named, how a
prefab binds to the eval suite that grades it, and how templates move between
worlds. That layer is the registry, and it closes the hill-climb loop: agents
author prefabs, the ledger grades them, validated prefabs become the floor
for the next generation.

---

## 2. What the ledger already provides

Most classic registry machinery is unnecessary here, for the same reason most
of Sander's relationship roadmap was: the substrate keeps everything.

- **Versioning is ticks.** A prefab's identity is `(world_id, entity_id)`;
  its version is the tick at which its values were read. Edits are new rows,
  so every version that ever existed is queryable, and "pin to a version"
  means instantiating from a historical `GraphView` slice.
- **Lineage is edges.** `IsA` rows already record which instance came from
  which template, and the edge's own tick records when. Population-level
  provenance queries are joins.
- **Fitness is queries.** Grading a prefab is instantiating it into a lab
  world, running a scenario, and grading from history — the autoresearch
  pattern. Fitness rows key naturally by `(prefab, version tick)`.

The registry therefore adds only three genuinely new things: **names**,
**cross-world provenance**, and **eval binding**.

---

## 3. Proposed decisions

### R1 — A prefab library is a world

No package format, no store. A library is an ordinary world whose population
is templates. It forks, grades, resumes, and is discovered like any world.
Cross-world instantiation already has its seam: `instantiate(world, view,
prefab)` takes the target handle and the source view separately, so
"import" is instantiating from a library world's view into a consumer world.

### R2 — `IsA` gains cross-world provenance payload

Today `IsA.target` is an entity id, which is world-local: an instance
imported from a library world holds a dangling reference. `IsA` grows payload
fields — `world: str = ""` (source world id; empty means same-world) and
`at_tick: int = -1` (the version instantiated) — so lineage is complete and
durable across worlds. This is a schema change to a shipped component and
must land before adoption spreads (#543's lesson: schema evolution against
persisted tables bites).

### R3 — The manifest is an artifact bundle

A registry entry is a published artifact (the existing bundle machinery, per
`docs/guide/artifacts.md`): name, source `(world_id, entity_id, tick)`, the
component set with **prefixed schema hashes**, the subtree inventory, the
eval-suite reference, and evidence receipts from grading runs. Publishing a
prefab version is publishing a manifest; the registry index is the artifact
index. No new storage system.

### R4 — Schema identity is the compatibility contract

Imports match components by name plus prefixed schema — the same
`_same_component` rule stages 4–7 converged on. A consumer whose component
class has drifted from the manifest's schema hash fails loudly at
instantiation (the #543 failure becomes a named, expected error instead of a
`FieldNotFound` deep in a query). Migration is explicit: re-author the
template or adapt the consumer; the registry never coerces silently.

### R5 — Eval binding is part of the name

A registry name without a grading story is advertising. The manifest's
eval-suite reference is mandatory; `FLAG`-style validations (cycle checks,
schema drift, orphaned lineage) run as evals over the library world, and a
version without evidence receipts is visibly ungraded in the index.

---

## 4. What stays out

- No central naming authority beyond the artifact index in this design;
  cross-org trust and signing are future work.
- No automatic migration of drifted schemas (R4 forbids silent coercion).
- No processor-native instantiation; the driver-level seam stands until the
  #604 core discussion resolves.

---

## 5. Open questions for review

1. **Naming ownership** — is a name bound in the control catalog (one
   authority per storage identity) or per library world? The catalog is the
   natural home but couples the registry to app-layer authority; the family
   split suggests manifest models live in a family and the binding service
   lives under `app`.
2. **`IsA` payload rollout** — R2 changes a shipped component's schema.
   Land it immediately (small blast radius now) or version the relation?
3. **Registry family placement** — `archetype.prefabs` as its own family
   (manifest models + frame-pure index readers), with the publishing service
   under `app.artifacts`? This mirrors the missions split.

---

## 6. Implementation sketch (post-review)

1. `IsA` provenance payload + migration note (R2) — small, ships first.
2. Manifest models + schema-hash capture in a family package (R3, R4).
3. Publish/lookup through the artifact bundle service (R3).
4. Eval-binding conventions + library-world validations (R5).
5. Cross-world import example: a library world feeding the RTS toy (#603).

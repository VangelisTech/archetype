# PreFab Registry — Design

**Status:** Ruled. Written after stages 1–7 landed; Everett's rulings
recorded 2026-07-20 (§5, §7). Implementation may proceed per §6; nothing is
implemented yet.

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

The registry therefore adds only four genuinely new things: **names**,
**cross-world provenance**, **eval binding**, and an explicit declaration of
the **behavior modules** required to interpret an asset's schemas.

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
fields — `world: str = ""` (source world id; empty means same-world),
`run_id: str = ""` (the source run that persisted the copied row — one
world's history spans runs, so a tick alone under-identifies a version), and
`at_tick: int = -1` (the captured tick) — so lineage is complete and durable
across worlds: the full version coordinate is `(world, run, tick)`. This is a schema change to a shipped component and
must land before adoption spreads (#543's lesson: schema evolution against
persisted tables bites).

### R3 — The manifest is an artifact bundle

A registry entry is a published artifact (the existing bundle machinery, per
`docs/guide/artifacts.md`): name, source `(world_id, entity_id, tick)`, required
behavior-module identities, the component set with **prefixed schema hashes**,
the subtree inventory, the eval-suite reference, and evidence receipts from
grading runs. Publishing a prefab version is publishing a manifest; the
registry index is the artifact index. No new storage system.

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

### R6 — Code registration precedes asset loading

Biome imports C modules before evaluating asset scripts. The same ordering is
required here: a host installs approved component classes, processors,
resources, and hooks before it loads, imports, or evaluates prefab content.
Those registrations are executable process state; prefab entities and rules
are durable world state. They are related, but they are not one registry.

A prefab manifest declares its required behavior modules and their schema
identities. It never embeds a callable, imports a module by an untrusted string,
or treats code as automatically executable asset data. The host resolves each
declared requirement through an allowlisted composition root, installs a fresh
world-local registration, and then checks the manifest's component schemas
under R4. A cold resume repeats code registration before the first step; it
does not reconstruct processors or resources from ledger rows.

This preserves the useful half of Flecs' two-layer model without turning the
artifact index into a plugin loader. Family examples may provide convenience
factories such as the example-local `register_biome_rts()`, while durable asset
authoring remains a separate operation such as
`author_prefab_library(world)`. That example bundle is not the reusable
registration contract; the `PrefabLibrary` model in §7 is.

### R7 — The relation-copy boundary

`instantiate` copies component values, recursively copies the `ChildOf`
subtree, rebuilds only `ChildOf` edges, and records `IsA` provenance. It
copies no other relation — catalog structure, assignments, sockets, supply
lines — and exposes no source-to-instance id map. Domain wiring belongs in
rule entities interpreted by a driver or service after instantiation. If
enough families need arbitrary graph cloning, the sanctioned broadening is a
separate `InstantiationResult(root_id, id_map)` API with explicit
relation-copy policies; `instantiate` is never silently widened. The Biome
example's `AssetChildOf` remains example-local until a catalog relation is
deliberately generalized under `archetype.prefabs`.

---

## 4. What stays out

- No central naming authority beyond the namespace directory (§7); cross-org
  trust and signing are future work.
- No automatic migration of drifted schemas (R4 forbids silent coercion).
- Processor-native instantiation is IN scope once the mutation-outbox seam
  lands: ruled on #604 (2026-07-20), design in
  `docs/design/mutation-outbox.md` — designed, not implemented.
- No declarative prefab file format yet (Biome's `.flecs` layer); Python
  authoring through the library object comes first.
- No automatic execution of code named by a manifest. Module resolution is an
  explicit, allowlisted host-composition decision (R6).

---

## 5. Rulings (Everett, 2026-07-20)

1. **Naming ownership — hybrid, split by rate of change.** The control
   catalog holds only the namespace directory (library name → library world
   id): small, authoritative, RBAC'd, rarely changing. Prefab names below a
   namespace are world content (`Prefab.name` rows), unique-per-library via
   a FLAG eval, versioned by ticks, forking with the library. Resolution is
   two hops: catalog for the namespace, ledger for the name at a version.
2. **`IsA` payload — land now.** Necessary for cross-world lineage; the
   installed base is a week old, so the schema change is nearly free today
   and expensive later (#543's lesson). First implementation step.
3. **Family placement — `archetype.prefabs` is its own family**, separate
   from `archetype.graph` (mechanics stay in graph, governance in prefabs);
   manifest models + frame-pure index readers in the family, the binding
   service under `app`. Registered via its own
   `quality/architecture.d/prefabs.toml` fragment.

---

## 6. Implementation sketch (rulings applied)

1. `IsA` provenance payload + migration note (R2) — small, ships first.
2. `PrefabLibrary` authoring object in `archetype.prefabs` (§7): emit and
   register components/relations/processors/hooks under a namespace; publish
   prefab entities into a library world; install into consumer worlds.
3. Manifest models + schema-hash and required-library capture in the family
   (R3, R4, R6).
4. Namespace directory in the control catalog + binding service under `app`
   (§5.1); publish/lookup rides the artifact bundle service (R3).
5. #604 MutationOutbox seam in core (rulings on the issue), then
   processor-native instantiate.
6. Eval-binding conventions + library-world validations (R5).
7. Cross-world import example: a library world feeding the Biome RTS reference
   example (#603).

## 7. Registration model (ruled 2026-07-20)

Biome's registration is two layers: C modules (`ECS_IMPORT`) register
component types, systems, observers, and hooks under a module namespace;
`.flecs` scripts then declaratively compose prefabs from those registered
parts, namespaced by blocks (`buildings.Solar`). Archetype adopts the same
split with what already exists:

- **Layer 1 — the library object (code).** `PrefabLibrary(name)` is the
  authoring surface: it emits and registers the primitives you build with —
  component and relation classes, processors, hooks — under its namespace,
  and carries the prefab-authoring API. Installing a library into a world is
  the `ECS_IMPORT` analog and rides the declarative world config (R6:
  processors, resources, hooks in declared order). Registration is explicit,
  not import-magic.
- **Layer 2 — the library world (data).** Prefab entities composed from
  layer-1 parts, exactly what stage 7 built. Publishing writes them and the
  manifest artifact; the catalog's namespace directory points at the world.

Sketch of the authoring surface:

```python
units = PrefabLibrary("vangelis.units")

@units.component
class Chassis(Component):
    armor: int = 10

@units.processor
class Locomotion(AsyncProcessor): ...

harvester = units.prefab("harvester", Chassis(armor=42))

world = runtime.world("sim", **units.install())      # layer 1 into a world
await units.publish(library_world)                    # layer 2 + manifest
```

**Collision rule:** component names are registered under the library's
namespace in the directory at publish/install time; installing two
libraries whose component names collide fails loudly at install, before any
table exists. No column-prefix mangling — the prefix convention stands.

Biome's auto-wiring (`EcsWith` pairs, on-add hooks assigning building bits)
uses existing registration shapes: a library can bundle `OnSpawn`/
`OnComponentAdded` hooks and counter resources. That does not make lifecycle
hooks transactional. Their failures are logged rather than aborting a
mutation, so correctness-critical auto-wiring must use explicitly authored
components, pure processors, or the ruled mutation-outbox/application seam.
Biome's declarative script layer is deferred (§4).

**Security rule:** a manifest may identify the `PrefabLibrary` implementation
required to interpret its component schemas, but it never embeds a callable or
causes a module named by an untrusted string to execute. The host composition
root resolves declared library identities through an allowlist and installs
them explicitly. The artifact index is not a plugin loader.

**Lifecycle rule:** registrations are process-local and world-local. The same
approved library bundle must install atomically for a new world and again
before the first step of a cold-resumed world. New-world configuration already
has the required processor/resource/hook shape; cold-resume parity remains
runtime-contract work and must not be bypassed through internal services.

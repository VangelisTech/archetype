# Prefab Libraries — Design

**Status:** Proposed. Extends `docs/design/graph-system.md` — this document
is the "own design" that its Stage 8 requires, and it reshapes Stage 7 from
"copy a prefab and its `ChildOf` subtree" into a defined graph operation with
merge rules, edge remapping, and per-component instantiation policy. It does
**not** revisit the D2 edges-as-entities representation; it builds on it.

The one place it asks graph-system.md to reconsider a committed position is
the non-goal "no runtime inheritance resolution" — see PD6 and Open Question
OQ1. Everything else is additive.

---

## 1. Motivation

The Stage 7 sketch in graph-system.md treats a prefab as component values plus
a `ChildOf` subtree that `instantiate()` copies onto fresh entities. That is
correct as far as it goes, but it under-specifies the operation to the point
where a naive implementation is unsafe. Three questions have no answer in the
current text:

1. **Internal relationships.** If a prefab's `completion_gate` node
   `Observes` its `test_runner` node, a copy that duplicates the edge verbatim
   leaves the instance's completion gate observing the *prefab's* test runner,
   not its own. Copying entities without remapping the edges *between* them
   produces an instance graph wired back into the asset graph.

2. **Derivation.** If `CodingMission IsA SoftwareMission` and both author a
   node called `validator`, is that an override, a second child, or an error?
   Without stable per-node identity the answer is "whatever the copy order
   happens to produce."

3. **Per-component intent.** A `RetryCounter` must reset on instantiation; a
   `MissionPolicy` should carry over; a `WorkingDirectory` is factory-bound and
   should not be copied at all. "Copy the prefab's component values" collapses
   all three into one behavior and turns the prefab system into an accidental
   `deepcopy()` framework.

The reframing that resolves all three: **a prefab is a non-executing entity
graph, and a prefab library is a queryable, composable hierarchy of those
graphs.** This is the same asset-hierarchy / scene-hierarchy split Flecs
draws — asset hierarchies define reusable *kinds* (defaults, variants,
overrides, nested children); scene hierarchies are the instantiated runtime
structure (ownership, containment, lifetime). Archetype already has the
substrate for both: edges are entities (D2), `ChildOf` is the blessed
containment relation with a `DELETE` cascade (D4), and `reserve_ids` /
`spawn_reserved` let a whole instance graph be allocated before any row is
written. This document specifies how those pieces compose into prefabs.

This is the bridge between the relationships work and the mission runtime: a
factory run stops being assembled procedurally from Python constructors and
becomes an instantiation of an explicit, inspectable, versionable asset graph.

---

## 2. What is kept, extended, and reconsidered

| graph-system.md position | Here |
|---|---|
| D2 edges are entities; `IsA`, `ChildOf` are relations | **Kept unchanged.** Prefabs add no new representation. |
| D5 `instantiate()` copies values + `ChildOf` subtree, records `IsA` lineage | **Extended** into a defined graph operation (PD7): id reservation, edge classification + remap, merge, overrides, one atomic batch. |
| D5 "editing a prefab does not mutate instances; re-instantiation is the upgrade path; both generations stay on the ledger" | **Kept.** This is the property that makes prefab populations gradeable, and every decision below preserves it. |
| Stage 7 `Prefab` marker | **Extended** with default query-exclusion semantics (PD1). |
| Stage 8 registry "requires its own design" | **This is that design** for the semantic core; the manifest/versioning surface stays a follow-on (Stage 8, OQ3). |
| Non-goal: "no runtime inheritance resolution for prefabs" | **Reconsidered, not overturned.** v1 stays copy-on-instantiate. But the per-component *policy* (PD6) is specified now so `INHERIT` has defined meaning, and OQ1 records why a lakehouse could later resolve inheritance as a join rather than a live cache. |

---

## 3. Decisions

### PD1 — Prefabs are tagged, non-executing entities

A prefab is an ordinary entity carrying a `Prefab` marker component. The marker
is the sole thing that distinguishes an asset entity from a live one; asset
entities otherwise hold the same components and relations as their instances.

Processors and queries must not execute asset entities by accident. Rather than
add an implicit filter to every query — which would be a core change and a
footgun of its own — the marker is a normal component and **the exclusion is a
documented query convention**, expressed with the same anti-join the graph
library already uses for cascade:

```python
# live missions only — assets excluded
live = world.query(Mission, MissionAssignment).where(~col("prefab__present"))

# authoring query — assets only
assets = world.query(Prefab, MissionPolicy)
```

A thin `graph.without_prefabs(frame)` / `graph.only_prefabs(frame)` helper pair
makes the convention one call and keeps the column name in one place. Making
exclusion opt-in-by-helper rather than implicit-in-core keeps D2's "assets and
instances share representation" honest: an asset is a live entity you have
chosen not to step.

### PD2 — The library is a `ChildOf` namespace; domain meaning is explicit relations

The library hierarchy (`ArchetypeLibraries → SoftwareFactoryLibrary →
Missions → CodingMission`) is expressed with `ChildOf` and nothing else.
`ChildOf` carries exactly what graph-system.md already gives it: containment,
namespace, and lifecycle (its `DELETE` cascade means dropping a library folder
drops the assets under it, one generation per cascade pass, recorded on the
ledger).

`ChildOf` is **not** overloaded to mean composition. Domain semantics use
explicit relations, each a `Relation` subclass with its own EdgeTable:

```python
class IsA(Relation):        pass          # inheritance / instancing (PD3)
class Uses(Relation):       pass          # CodingMission Uses ImplementerAgent
class Requires(Relation):   pass          # CodingMission Requires CodeSandbox
class Produces(Relation):   pass          # CodingMission Produces PullRequest
class CapableOf(Relation):  pass          # ReviewerAgent CapableOf CodeReview
```

Because these are ordinary relations, the library is queryable the moment it
exists: `graph.edges(world, IsA)` filtered to a source answers "what does
`BugFixMission` inherit," and a bounded `neighborhood()` over `ChildOf`
answers "what is in `SoftwareFactoryLibrary`." No prefab-specific query path.

### PD3 — `IsA` is the inheritance-and-instancing relation

One relation carries prefab variation, instantiation, and lineage, exactly as
graph-system.md's Stage 7 shape declares (`class IsA(Relation)`):

```text
Mission → SoftwareMission → CodingMission          # variation (asset IsA asset)
mission_0192… IsA CodingMission                      # instancing (instance IsA asset)
implementer_0192… IsA CodingMission/implementer      # child correspondence (PD4)
```

`IsA` is **not** exclusive: a prefab may derive from more than one base, and an
instance's child nodes each carry their own `IsA` back to the authored node.
`IsA` edges are lineage records, never resolved at runtime in v1 (D5 kept). A
query that wants "all instances of `CodingMission`" is
`graph.edges(world, IsA).where(col("isa__target") == coding_mission_id)`.

### PD4 — Authored children carry a stable `PrefabNodeKey`

Each authored child of a prefab holds a `PrefabNodeKey(key="validator")`
component. The key is stable identity *within a prefab lineage* — it is what
makes derivation and remapping well-defined, and it is what an instance's child
points back to through `IsA`. Keys are unique among the direct children of one
prefab; nested prefabs namespace their keys by path (`reviewer/policy`).

`PrefabNodeKey` is authoring metadata: it is copied onto instance children (so
`implementer_0192… IsA CodingMission/implementer` is answerable) but it does
**not** turn the node into an asset — only `Prefab` does that, and instance
children are not `Prefab`.

### PD5 — Derivation is a merge algebra keyed by `PrefabNodeKey`

When `CodingMission IsA SoftwareMission` and both author a node with key
`validator`, the derived prefab's intent is stated explicitly, not inferred
from copy order. The merge operates over the *node key set* of the base and the
derived prefab:

| Authoring op on the derived prefab | Effect on a base node of the same key |
|---|---|
| `OverrideNode("validator")` (default when a key collides) | Derived node replaces the base node's components; identity and key preserved. |
| `ExtendNode("validator")` | Derived node's components merge onto the base node's (derived wins per component type). |
| `RemoveNode("validator")` | Tombstone: the base node is absent from instances of the derived prefab. |
| new key | Additional child; base children are inherited by key. |

Resolution is bottom-up along the `IsA` chain: compute the base's effective
node set, then apply the derived prefab's ops. A tombstone is explicit data
(a `RemoveNode` marker on the derived prefab), never a silent omission, so an
instance can always be traced to the exact overlay that produced it. Component
overrides on the *root* of the derived prefab follow the same rule as node
overrides: same component type wins, others inherited.

### PD6 — Per-component `InstantiationPolicy`

Instantiation is not `deepcopy`. Each component declares how it crosses the
asset→instance boundary, defaulting to `COPY` so existing components need no
change:

```python
class InstantiationPolicy(StrEnum):
    INHERIT = "inherit"   # value belongs to the asset; instance shares it
    COPY    = "copy"      # value is duplicated onto the instance (default)
    RESET   = "reset"     # instance gets the component's field defaults
    OMIT    = "omit"      # component is not placed on the instance at all
```

Declared as a class var on the component, next to the serialization metadata it
already carries:

```python
class MissionPolicy(Component):
    on_instantiate = InstantiationPolicy.INHERIT
    max_parallel_agents: int = 1

class RetryCounter(Component):
    on_instantiate = InstantiationPolicy.RESET
    attempts: int = 0

class WorkingDirectory(Component):
    on_instantiate = InstantiationPolicy.OMIT   # factory-bound at runtime
    path: str = ""
```

Reference assignment for the factory: `MissionPolicy`, `AgentRole`,
`ModelConfiguration` → `INHERIT`; `RetryCounter`, `MissionStatus` → `RESET`;
`WorkingDirectory` → `OMIT`; `PromptContext` → `COPY`.

**The v1 subtlety, and why it is safe (see OQ1).** graph-system.md's non-goal
forbids runtime inheritance resolution, and v1 honors it: `INHERIT` is
*materialized as a copy at instantiate time*, identical to `COPY` in what lands
on the instance row. The distinction is recorded, not yet exploited — the
`IsA` lineage edge plus the policy tag are exactly the information a later stage
needs to switch `INHERIT` from copy-at-instantiate to resolve-at-query (a join
through `IsA`, not a live cache — which is why a lakehouse can afford it where a
cache-line ECS cannot). Authoring against the policy now means that switch is a
storage optimization, not an API break. `RESET` and `OMIT` change what lands on
the instance and take effect immediately in v1.

### PD7 — `instantiate()` is a graph operation over reserved ids

`instantiate()` never mutates the prefab and never copies an entity before the
whole instance graph's identity is known. The primitive that makes this atomic
already exists: `reserve_ids(n)` allocates the instance-graph ids up front, so
edges can be remapped to their final targets *before* the first row is written,
and the whole graph lands in one `spawn_many` batch.

```python
async def instantiate(
    world: WorldLike,
    prefab: int,                              # the asset root entity id
    overrides: dict[type[Component], Component] | None = None,
    node_overrides: dict[str, dict[type[Component], Component]] | None = None,
) -> int:                                     # the instance root entity id
    ...
```

Algorithm:

1. **Resolve the effective graph.** Walk the `IsA` chain to compute the merged
   node set (PD5) and each node's effective components. This is a read over the
   asset `ChildOf` subtree and `IsA` edges — bounded-depth joins, no mutation.
2. **Reserve.** `reserve_ids(len(nodes))`, building a map
   `prefab_node_id → instance_id`. The root maps to the first reserved id.
3. **Classify every authored edge** whose source is inside the subtree:
   - **internal** — target is another node in the subtree → remap `target` to
     the reserved instance id.
   - **shared-library** — target is an asset *outside* the subtree (e.g.
     `reviewer Uses CodeReviewPolicy`) → keep the target as-is; the instance
     shares the library asset.
   - **runtime-bound** — target is a placeholder resolved from `overrides` /
     `node_overrides` or an `on_instantiate` hook → bind now, else leave unset
     and let a factory processor fill it.
   The rule is mechanical: internal iff the target id is a key in the
   reservation map.
4. **Apply policy (PD6) and overrides.** Per node, per component: `OMIT` drops
   it, `RESET` substitutes field defaults, `COPY`/`INHERIT` carry the value;
   then root `overrides` and `node_overrides` win last.
5. **Spawn.** One `spawn_many` batch: every instance node (with its resolved
   components, its `PrefabNodeKey`, and no `Prefab` marker) plus every remapped
   internal edge, plus the `IsA` lineage edges (`instance_root IsA prefab`, each
   `instance_child IsA authored_node`). Shared-library edges are spawned with
   original targets.
6. **Run `on_instantiate` hooks** registered for the prefab, for last-mile
   binding that needs the freshly reserved ids.

Steps 1–4 are frame-pure and independently testable; only step 5 touches the
world, and it touches it once. A failure before step 5 has staged nothing.

### PD8 — Edge classification is subtree membership, nothing cleverer

PD7 step 3 leans entirely on one predicate: *is this edge's target a node of
the prefab being instantiated?* That is answered by the reservation map, which
is derived from the effective `ChildOf` subtree. No relation needs to declare
itself "internal" or "shared"; the same `Observes` relation is internal when it
points within the subtree and shared when it points out. This keeps the classi-
fication a property of the *graph shape at instantiate time*, not of relation
type declarations that would drift from how authors actually wire prefabs.

---

## 4. Shapes

```python
# graph/components.py — additions
class IsA(Relation):
    pass                                  # non-exclusive lineage (PD3)

class Prefab(Component):
    pass                                  # asset marker (PD1)

class PrefabNodeKey(Component):
    key: str = ""                         # stable per-lineage identity (PD4)

# derivation overlay markers (PD5)
class OverrideNode(Component): key: str = ""
class ExtendNode(Component):   key: str = ""
class RemoveNode(Component):   key: str = ""
```

```python
# core/component.py — one class var, defaulted so nothing else changes (PD6)
class Component(LanceModel):
    on_instantiate: ClassVar[InstantiationPolicy] = InstantiationPolicy.COPY
```

```python
# graph/prefab.py — frame-pure resolution + handle sugar
def effective_nodes(edges, childof_subtree, isa_chain, prefab) -> NodeSet: ...
def classify_edges(edges, reservation: dict[int, int]) -> EdgeClassification: ...

async def instantiate(world, prefab, overrides=None, node_overrides=None) -> int: ...

def without_prefabs(frame): ...            # ~col("prefab__present")  (PD1)
def only_prefabs(frame): ...
```

---

## 5. Worked example

`CodingMission` authored in `SoftwareFactoryLibrary`:

```text
CodingMission                       [Prefab, MissionPolicy INHERIT, Budget COPY]
  ChildOf SoftwareFactoryLibrary/Missions
  IsA SoftwareMission
  ├─ planner          key=planner
  ├─ implementer      key=implementer   Uses ImplementerAgent        (shared)
  ├─ reviewer         key=reviewer      Uses CodeReviewPolicy         (shared)
  ├─ test_runner      key=test_runner
  └─ completion_gate  key=completion_gate  Observes test_runner       (internal)
```

`instantiate(world, CodingMission, overrides={TargetRepository: repo,
Objective: obj, Budget: Budget(tokens=500_000)})` yields:

```text
mission_0192…  IsA CodingMission   [MissionPolicy (copied), Budget(500k), TargetRepository, Objective]
  ├─ planner_0192…          IsA CodingMission/planner
  ├─ implementer_0192…      IsA CodingMission/implementer   Uses ImplementerAgent   (target UNCHANGED — shared asset)
  ├─ reviewer_0192…         IsA CodingMission/reviewer      Uses CodeReviewPolicy   (target UNCHANGED — shared asset)
  ├─ test_runner_0192…      IsA CodingMission/test_runner
  └─ completion_gate_0192…  IsA CodingMission/completion_gate  Observes test_runner_0192…  (REMAPPED)
```

`CodeReviewPolicy` stays a single shared library asset; `Observes` is rewired to
the instance's own `test_runner`. A `BugFixMission IsA CodingMission` that adds
`ExtendNode("reviewer")` with a `RegressionValidator` and `RemoveNode("planner")`
instantiates the same topology minus the planner, with the reviewer carrying
both the inherited `CodeReviewPolicy` edge and the added validator.

---

## 6. Stage plan

Refines graph-system.md Stage 7 (which depended on stages 2 and 5b, both
landed) into review-gate-sized pieces, and re-scopes Stage 8.

| Stage | Ships | Depends on | Size |
|---|---|---|---|
| 7a | `Prefab` marker, `IsA` relation, `without_prefabs`/`only_prefabs`, query-convention docs | graph 2, 5b | ~150 lines |
| 7b | `InstantiationPolicy` class var + resolution helpers (`COPY`/`RESET`/`OMIT`; `INHERIT`≡`COPY` in v1), unit tests over frame-pure resolution | 7a | ~200 lines |
| 7c | `PrefabNodeKey`, `instantiate()` graph op (reserve → classify → remap → batch), `examples/12_prefabs.py` | 7b | ~300 lines |
| 7d | Derivation algebra: `OverrideNode`/`ExtendNode`/`RemoveNode`, `IsA`-chain merge | 7c | ~250 lines |
| 8 | PreFab registry: library manifest, versioning against #543, eval binding | 7d, OQ3 | own follow-on |

No core change is required except the single defaulted `on_instantiate` class
var in PD6; `instantiate()` composes existing world API (`reserve_ids`,
`spawn_many`). If the class var is deemed a core change per the layering rule,
7b is gated on that discussion; the rest of the family library is not.

---

## 7. Non-goals

- No runtime inheritance resolution in v1 (D5 kept; `INHERIT` materializes as a
  copy — see OQ1).
- No implicit query-time prefab exclusion in core (PD1 is a convention + helper,
  not an engine filter).
- No transitive `IsA` closure engine; merge walks a bounded `IsA` chain.
- No REST/registry surface in stages 7a–7d; that is Stage 8.
- No mutation of a prefab by any instance operation, ever.

---

## 8. Open questions

**OQ1 — Should `INHERIT` eventually resolve at query time?** In a cache-line
ECS, inherited-component lookup needs the reachable-cache machinery Sander
prices at years, which is why graph-system.md declined it. In a lakehouse it is
a *join* through the `IsA` edge against the asset row — uncached, like every
Archetype query, and consistent by construction. The cost is that an instance's
effective state is no longer a single row read, and that editing an asset would
then retroactively change `INHERIT` instances (breaking the "both generations
on the ledger" gradeability D5 prizes). **Recommendation: keep v1 copy-at-
instantiate** (this document), specify the policy now so authoring is stable,
and treat query-time resolution as a measured Stage 9 with its own decision —
gated on a real need (very large shared policies) and on preserving lineage-
pinned reads for grading. This is the one point where the essay pushes past a
committed non-goal, and it deserves an explicit yes/no rather than drift.

**OQ2 — Library world vs same world.** Assets can live in the runtime world
(tagged `Prefab`) or in a dedicated library world instances are drawn from.
Same-world is simpler and PD1's exclusion covers it; a library world gives
cleaner lifecycle and sharing across runs but means `instantiate()` copies
across a world boundary. Recommend same-world for 7a–7d, revisit at Stage 8
when versioning forces the question.

**OQ3 — Versioning (#543).** Re-instantiation as the upgrade path only works if
a prefab version is addressable and its schema evolution against persisted
tables is defined. This is the Stage 8 blocker and inherits #543 wholesale.

**OQ4 — Hook registration surface.** PD7 step 6 assumes per-prefab
`on_instantiate` hooks. Whether those are `core/hooks.py` lifecycle events, a
prefab-local callback registry, or processors keyed on a freshly-spawned marker
is left to 7c, where the first real binding need will decide it.

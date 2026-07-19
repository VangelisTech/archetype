# Graph System, Projections, and Family Libraries — Design

**Status:** Proposed. Direction reviewed with Everett 2026-07-19; implementation
is staged as a dependency-ordered issue chain (linked per stage below). Each
stage is one atomic issue and PR sized for the review gate.

---

## 1. Motivation

The PreFab goal: agents build libraries of components and processors that
incrementally climb the sophistication real domain problems require. In Flecs,
prefabs are implemented through entity relationships (`IsA`, `ChildOf`), so
relationships come first.

Sander Mertens' relationship roadmap prices a fragmenting implementation at
roughly two years for a cache-line archetype ECS. Most of that budget pays for
cache coherence: query caches and their invalidation, the component index,
archetype cleanup under id recycling, reachable caches, empty-table garbage
collection. A lakehouse ECS has none of those constraints. Queries read
persisted state through Daft's optimizer, ids are never recycled, and there
are no live caches to invalidate. The transferable piece is small, and almost
all of it is a library rather than an engine change.

---

## 2. Decisions

### D1 — Family libraries are a layer beside core

A family library (`archetype.htn`, `archetype.graph`, `archetype.projections`)
lives at the top level and imports only `archetype.core` and third-party
packages. Its logic is frame-pure — functions over DataFrames — with thin
handle sugar beside it. Because it sits beside core in the dependency DAG,
everyone may import it: scripts, runtime consumers, and `app` (so `app.api`
can serve a projection over REST without projections moving into app).

Enforced, not conventional: stage 1 adds a `package_rule` to
`quality/architecture.toml` forbidding family libraries from importing
`archetype.app`, `archetype.runtime`, `archetype.api`, and `archetype.cli`.

Compatibility note: `htn` and `datasets` are expected to move under a larger
agent-missions module in a future reorganization. `graph` and `projections`
are simulation-facing and stable under that move.

### D2 — Edges are entities; EdgeTables are archetype tables

A relation is a `Component` subclass with `source` and `target` entity-id
fields. An edge is an ordinary entity carrying one relation component. This is
the non-fragmenting representation: pairs never enter the archetype signature,
so relationship targets cannot explode the table count.

Everything an edge needs is inherited: ticks, `is_active`, persistence, fork
lineage. Temporal edges are a filter (`tick == t`), wildcards are a `where` on
`source` or `target`, and traversal is a join.

### D3 — GraphView rides `PostTick.results`

`PostTick` already delivers `results: dict[ArchetypeSignature, DataFrame]` —
every just-persisted frame — and `core/hooks.py` blesses handlers that close
over state at registration. `GraphView` is a `Resource` whose hook method
stores those frames; processors at tick N read tick N−1 by construction.

Contract: strictly previous-tick visibility, read-only, lazy frames (the Daft
DAG rules hold), safe across concurrent table tasks. No core changes, no
query-service reentrancy, wired through the declarative world config
(`resources=[view], hooks=[(PostTick, view.on_post_tick)]`).

### D4 — Cleanup is policy, propagated by ticks

A relation declares `on_delete_target`: `REMOVE` (default; treat edges to
inactive targets as absent at read time), `DELETE` (cascade despawn), or
`FLAG` (surface to evals; the ledger-world replacement for Flecs' `Panic`).
A late-priority `CascadeDespawn` processor applies the policy using GraphView
liveness. Cascades propagate one generation per tick and land in history —
correct for an append-only world, and every step is auditable.

### D5 — `IsA` copies at instantiation

`instantiate(world, prefab, overrides)` materializes the prefab's component
values (and its `ChildOf` subtree) onto fresh entities and records `IsA`
lineage edges. There is no runtime resolution: editing a prefab does not
mutate instances. Re-instantiation under the new prefab version is the
upgrade path, and both generations stay on the ledger, which is what makes
prefab populations gradeable.

### D6 — Components are not models

App-level `models.py` files hold DTOs: plain Pydantic models or dataclasses
that cross service boundaries. Simulation state is a `Component` and belongs
in a `components.py`. Family libraries follow the `htn` file idiom:
`components.py`, `processors.py`, plus frame-pure modules. Existing app
modules whose `models.py` actually contains components should be renamed as
part of the agent-missions reorganization.

---

## 3. Shapes

```python
# graph/components.py
class Relation(Component):
    source: int = 0   # entity_id, matches BASE_SCHEMA / OnSpawn.entity_id
    target: int = 0

class ChildOf(Relation):
    exclusive = True                      # stage 5a
    on_delete_target = Policy.DELETE      # stage 5b

class IsA(Relation):
    pass                                  # stage 7
```

```python
# graph/view.py
class GraphView:
    def __init__(self) -> None:
        self._frames: dict[ArchetypeSignature, DataFrame] = {}
        self.tick: int = -1

    async def on_post_tick(self, event: PostTick) -> None:
        self._frames = dict(event.results)   # lazy, just persisted
        self.tick = event.tick - 1           # the completed tick

    def frame(self, *components: type[Component]) -> DataFrame | None:
        """Concat archetype frames whose signature contains all components."""
```

```python
# graph/traverse.py — frame-pure, importable by app
def targets(edges: DataFrame, rel: type[Relation], source: int) -> DataFrame:
    p = rel.get_prefix()   # "childof__" — prefix convention names the columns
    return edges.where(col(f"{p}source") == source)

def neighborhood(
    edges: DataFrame, rel: type[Relation], roots: DataFrame, depth: int
) -> DataFrame:
    """Bounded-depth iterated joins; lazy in, lazy out."""

# handle sugar beside it for scripts:
async def edges(world, rel: type[Relation], *, at: int | None = None) -> DataFrame: ...
async def link(world, rel: Relation) -> int:
    return await world.spawn(rel)   # exclusivity replaces in-batch from stage 5a
```

Projections take the same split: frame-pure read models
(`overview(frames)`, `possession(frames, entity, rels, depth)`) with handle
wrappers; REST exposure later via app importing the frame layer.

---

## 4. Sander-roadmap disposition

| Roadmap item | Fate here | Why |
|---|---|---|
| 1. Components as entities | Dropped (v1) | Relations get identity from the component type system |
| 2. Observers | Exists | `core/hooks.py` lifecycle events |
| 3. Pair encoding in archetype ids | Rejected | Fragmenting pairs would mint a table per target; edges are rows instead |
| 4. Relationship components | Free | Payload fields are columns on the relation component |
| 5. Wildcard queries | Free | `where` filters on `source`/`target` |
| 6. Component index | Exists | Control catalog and table listing |
| 7. Cleanup | Reduced | No id recycling; dangling edge is a join-time filter (D4) |
| 8. Cleanup traits | Kept, reshaped | `on_delete_target` policy on the relation (D4) |
| 9. Multi-source queries | Free | Joins |
| 10. Relationship traversal | Kept | `neighborhood()` as bounded iterated joins |
| 11. Query cache revalidation | Not applicable | No query caches |
| 12. Breadth-first traversal | Kept, reshaped | `Depth` component maintained as data (stage 6) |
| 13. Uncached queries | Not applicable | Every query is uncached through Daft |
| 14. Multi-component observers | Not applicable | No cache notification problem |
| 15. Event propagation / reachable cache | Not applicable | Same |
| 16. Empty-table GC | Deferred | Table churn is bounded without fragmenting pairs |
| 17. Rule engine | Already have one | Daft is the relational engine; transitivity = bounded joins |
| 18. Exclusive relationships | Kept | Uniqueness on `(relation, source)` at edge-write (stage 5a) |
| 18b. Inheritance / IsA | Kept, reshaped | Copy-on-instantiate with lineage edges (D5) |
| 19. Query DSL | Not applicable | SQL and dataframes exist |

---

## 5. Stage plan

| Stage | Ships | Depends on | Size |
|---|---|---|---|
| 0 | `projections/`: world-overview read model, frame-pure + handle sugar | — | ~250 lines |
| 1 | `graph/`: `Relation`, `link`/`unlink`, `edges`, wildcard filters, architecture.toml family rule | — | ~300 lines |
| 2 | Traversal: `neighborhood`, join helpers, `examples/11_graph_relationships.py` | 1 | ~300 lines |
| 3 | FPS projection: `possession()` neighborhood read model | 2 | ~200 lines |
| 4 | `GraphView` resource + `frame()` lookup | 1 | ~250 lines |
| 5a | Exclusivity: replace-in-batch on `link` | 1 | ~100 lines |
| 5b | `ChildOf`, `on_delete_target` policies, `CascadeDespawn` processor | 4, 5a | ~300 lines |
| 6 | `Depth` component + maintenance processor, `sort("depth")` ordering | 4, 5b | ~200 lines |
| 7 | `Prefab` marker, `IsA`, `instantiate()`, `examples/12_prefabs.py` | 2, 5b | ~400 lines |
| 8 | PreFab registry: manifest, versioning, eval binding | 7, design doc first | — |

Stage 8 requires its own design (`docs/design/prefab-registry.md`) after
stage 7 lands; its versioning story must answer #543 (schema evolution against
persisted tables).

---

## 6. Non-goals

- No core or app changes anywhere in stages 0–7.
- No runtime inheritance resolution for prefabs.
- No transitive-closure engine; traversal is bounded-depth.
- No REST endpoints in this chain; app may import the frame layer later.
- Core query-API traversal sugar is deliberately deferred until the library
  proves the shapes.

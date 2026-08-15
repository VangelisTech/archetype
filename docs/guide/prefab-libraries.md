# Prefab Libraries

**Document type:** Normative current contract and user guide.

Archetype prefab libraries are entity-backed asset graphs.  A library can be
queried, versioned by tick, forked, graded, and composed with the same
relationship tools as a runtime scene.  The RTS capstone in
`examples/biome_rts/` demonstrates the pattern with harvesters, turrets,
nested tools, command hierarchies, supply lines, visibility, and targeting.
It is a reference package, not a shipped `archetype` family. The live
dogfood in `examples/biome_agent/` crosses the next boundary: it controls
Sander Mertens' actual Biome ECS world instead of recreating another RTS.

## Current relation and temporal-view contract

This section defines the implemented `archetype.graph` contract.  The design
records linked later explain its origin and possible extensions; their
`Proposed` status does not override this current contract.

A concrete relation is a `Relation` Component subclass with `source` and
`target` entity ids.  Each edge is an ordinary entity carrying one relation
Component, so it inherits the ledger's ticks, liveness, persistence, history,
and fork lineage.  Relationship targets do not enter an archetype signature.
`link()` stages an edge for the next commit, `edges()` returns its append-only
table, `edges(..., at=tick)` is an ordinary temporal filter, and `unlink()`
idempotently stages every matching live edge for despawn.  The sync helpers
provide the same contract for sync world handles.

An exclusive relation has at most one persisted live edge per source after a
replacement commit.  Replacement reads the latest persisted state and stages
the new edge plus prior-edge despawns together.  Two exclusive links staged
before an intervening step are therefore outside that guarantee: the tick
boundary is the consistency unit.

`GraphView` is a world-local, read-only resource populated from committed
`PostTick.results`.  Before the first committed tick it is empty.  Afterward,
processors evaluating tick N see lazy frames captured from tick N−1; inactive
rows are filtered from component views.  A `GraphSnapshot` freezes one capture
for multi-read operations such as prefab instantiation.  Neither view performs
query-service reentrancy or creates an additional durability boundary.

Cleanup remains explicit driver policy.  `cascade()` compares a relation's
captured live edges with the captured population and applies
`on_delete_target` one generation per invocation: `REMOVE` despawns the
dangling edge, `DELETE` also despawns its source, and `FLAG` only reports the
dangling edge.  Those mutations are staged and become visible at the following
tick.  A relation-declared non-empty foreign scope is excluded from local
liveness decisions.  `ChildOf` is exclusive and uses `DELETE`.

## Registration has two layers

Biome's [`main.c`](https://github.com/SanderMertens/biome/blob/main/src/main.c)
first imports C modules that register component metadata, systems, observers,
and hooks.  It then evaluates the root
[`biome.flecs`](https://github.com/SanderMertens/biome/blob/main/etc/scenes/biome.flecs)
script, which creates named prefabs and rules from those registered types.
Archetype keeps the same code-versus-data boundary:

| Biome | Archetype | Lifetime |
|---|---|---|
| C module import | `register_biome_rts()` world options | Process-local code wiring |
| Flecs script | `author_prefab_library(world)` | Durable ECS content |
| Reflected C component metadata | Imported Python `Component` schemas | Registered on first archetype use |

```python
from biome_rts import author_prefab_library, register_biome_rts

registration = register_biome_rts()
world = runtime.world(
    "biome-rts",
    **registration.world_options(),
)

assets = await author_prefab_library(world)
await world.step()  # publish the declarative asset layer
```

`registration.world_options()` supplies those three keyword arguments as a
convenience.  A registration is world-local because its `GraphView` contains
previous-tick state.  Create a fresh one for each world.  Processors,
resources, and hooks are executable process state, not ledger rows, so they
must also be reinstalled when resuming a world.  Prefab entities and their
relations persist normally.

`BiomeRTSRegistration` is example-local convenience, not the reusable
registration contract for every prefab family.  The ruled generic surface is
the future `archetype.prefabs.PrefabLibrary`: its stable identity is resolved
by an allowlisted host composition root, its `install()` operation composes
processors/resources/hooks in declared order, and collisions fail before any
table exists.  A manifest may declare required library identities and schemas,
but it never embeds a callable or imports code from an untrusted string.

`register_biome_rts()` currently targets new `runtime.world(...)` activation.
The public cold-resume path does not yet accept a complete
processor/resource/hook bundle.  Runtime activation will own reinstalling a
fresh, approved world-local bundle before the first resumed step; resume-safe
installation remains separate runtime-contract work.  Do not bypass that
boundary by reaching into application services.

This is not a literal port of Flecs registration.  Flecs `EcsWith`, `on_add`,
and `on_set` hooks can synchronously enforce invariants while a component is
attached.  Archetype lifecycle hooks are intentionally non-transactional:
their failures are logged and do not abort a mutation or tick.  Do not use an
`OnSpawn` or `OnComponentAdded` hook to emulate correctness-critical automatic
component insertion.  Author complete prefab component sets explicitly, use
pure processors for same-archetype derived state, and use a driver or
application service when cross-entity mutations must be staged durably.

Biome's `BuildingRule2x1` is the useful bridge between the layers: the rule is
asset data, while imported C behavior interprets it and performs the wiring.
The Archetype analogue should likewise store mission rules, benchmark
constraints, placement rules, or connector recipes as Components and
relations in the library world.  Registered processors can compute pure
matches; a driver or application service owns any resulting entity creation.
The rule stays queryable and versioned instead of disappearing into a Python
callback registry.

## Asset hierarchy and scene hierarchy are different graphs

Sander Mertens distinguishes the hierarchy used to author reusable assets
from the hierarchy of instantiated objects in a scene.  Archetype makes that
distinction explicit:

```text
AssetChildOf                         ChildOf
biome-rts library                    first-army
├── units                            └── alpha squad
│   └── harvester prefab                 ├── commander
│       └── mining-tool prefab           ├── harvester instance
└── structures                           │   └── mining-tool instance
    └── turret prefab                    └── turret instance
```

`AssetChildOf` is catalog structure.  It provides namespacing, discovery, and
asset-library cleanup.  `ChildOf` is copied prefab composition and runtime
lifetime: instantiating the harvester rebuilds its mining-tool subtree with
new ids, and destroying a squad can cascade through its live descendants.
Domain meaning stays on named relations such as `AssignedTo`, `CommandedBy`,
`SupplyLine`, `Targets`, and `VisibleTo`; it is not overloaded onto either
hierarchy.  `AssetChildOf` remains local to the Biome example until a catalog
relation is deliberately generalized in `archetype.prefabs`.

## The Biome pattern being translated

Biome's archived
[`buildings.flecs`](https://github.com/SanderMertens/biome/blob/29b7f0e126c81568ba61946c1eb46d921abe8aa2/etc/scenes/config/buildings.flecs)
shows the composition directly.  `PoweredBuilding` supplies common state;
`Drill` composes it with a drill emitter and adds `Building`, `StorageDesc`,
`Storage`, `Miner`, `Power`, `Recipe`, and rendering components.  The
[`miner` systems](https://github.com/SanderMertens/biome/blob/main/src/modules/miner.c)
then match combinations including miner, storage, storage-description, and
power state.  The prefab is not a class that calls mining behavior.  Its
component set makes an instance eligible for systems that implement mining.

The same file also contains anonymous child objects, prefab references in
building rules, and a drone factory function.  Those are three distinct
asset-graph jobs: nested composition, references between reusable assets, and
runtime instantiation.  Keeping those jobs distinct is more useful than
treating every edge as generic parentage.  This follows the separation between
asset and scene hierarchies in Mertens' article on
[data-oriented hierarchies](https://ajmmertens.medium.com/building-an-ecs-data-oriented-hierarchies-62fb2847d100)
and the relationship taxonomy in his
[entity-relationships roadmap](https://ajmmertens.medium.com/a-roadmap-to-entity-relationships-5b1d11ebb4eb).

## Components are the prefab interface

Biome does not need a separate building-interface type.  A drill behaves as a
miner because its prefab has the mining, storage, placement, and power
components required by those systems.  The Biome reference example follows
the same rule:

| Asset | Static capability components | Dynamic instance state |
|---|---|---|
| Harvester | `UnitSpec`, `Mobility`, `Harvester` | `Position`, `Heading`, `Health`, `Cargo` |
| Turret | `UnitSpec`, `Weapon` | `Position`, `Health` |

`MovementProcessor` requires `Position + Heading + Mobility`.
`HarvestProcessor` requires `Cargo + Harvester`.  The harvester therefore
moves and gathers; the turret does neither.  Adding or removing a capability
component changes which systems match without a central interface registry.

Static descriptions and dynamic state are separated for another reason.  A
prefab entity has descriptions but no runtime state, so runtime processors do
not match its archetype.  The catalog remains live and queryable without the
canonical harvester itself moving or collecting resources.  This is the
library-level counterpart of Flecs queries excluding its `Prefab` tag by
default, expressed without a core query change.

## Instantiation is a ledger operation

This section defines the implemented
`graph.prefabs.copy_on_instantiate` contract.

```python
unit = await instantiate(
    world,
    view,
    assets.harvester,
    overrides=[
        CommandNode(name="harvester-1", kind="unit"),
        Position(x=4, y=8),
        Heading(x=1),
        Health(current=80),
        Cargo(),
        Depth(),
    ],
)
```

Archetype deliberately uses copy-on-instantiate rather than Flecs's live
component inheritance.  The operation copies component values and a bounded
`ChildOf` subtree, applies root overrides, and records `IsA` lineage.  Editing
a prefab does not mutate existing instances; a later instantiation receives
the new values.  Both generations remain in history, which makes a prefab
population gradeable.

The generic operation copies only that component data and `ChildOf` subtree.
It does not clone `AssetChildOf` or arbitrary socket, assignment, supply,
targeting, or other relation entities, and it returns the new root id rather
than an old-to-new id map.  Libraries express those connections as durable
rule or recipe entities referencing stable roles.  A domain driver or
application service traverses the new `ChildOf` subtree, joins `IsA` lineage
back to its source nodes, resolves those roles, and stages the intended edges.
Generic relation cloning would require a separate result carrying an id map
and explicit allowlisted relation-copy policies.

`minimap_overview()` composes the generic `projections.overview()` primitive
into a per-tick strategic population series, while `minimap()` provides the
latest spatial rows.  `fog_of_war()` then joins that spatial view to
`VisibleTo`, and `unit_view()` composes the generic possession projection.

That difference is important when translating Flecs examples:

| Flecs | Archetype |
|---|---|
| Shared inherited component lookup | Values copied at instantiation |
| Asset edit can affect inheriting instances | Existing instances remain unchanged |
| Fragmenting relationship pairs group storage | Relation entities form non-fragmenting EdgeTables |
| Query caches amortize inheritance/traversal | Daft plans express joins and bounded traversal |

Do not describe `IsA` as live inheritance in Archetype.  It is durable
provenance under the accepted D5 contract.

## From an RTS library to missions and physical AI

The pattern transfers by changing the vocabulary, not the substrate:

| RTS asset | Agent-mission analogue | Physical-AI analogue |
|---|---|---|
| `UnitSpec` | agent role/model policy | robot embodiment/policy |
| `Harvester` | tool-use capability | manipulation skill |
| `Weapon` | validator/reviewer capability | task-specific controller |
| `CommandNode` hierarchy | mission/team/agent topology | benchmark/suite/episode topology |
| `SupplyLine` | artifact/context flow | observation/action stream |
| `VisibleTo` | context-access policy | sensor visibility |
| prefab generation | mission configuration candidate | policy/environment candidate |

Keep reusable descriptions on prefab entities, add execution or episode state
at instantiation, and grade populations through history and `IsA` lineage.
Authorization, artifact ingestion, and cross-world registry control
remain application authority; a domain library should not absorb those
responsibilities.

Run the complete credential-free composition with:

```bash
uv run python examples/13_biome_rts.py
```

The generic graph and projection contracts remain the underlying API.  The
current relation and copy-on-instantiate sections above are normative.  See
the proposed [graph-system](../design/graph-system.md) and
[prefab-registry](../design/prefab-registry.md) design records for rationale
and the ruled, not-yet-implemented cross-world registry; those records do not
expand the implemented contract.

## Literal Biome dogfood

`examples/14_biome_agent.py` treats Biome for what it already is: an
executable ECS asset library and environment. It does not translate `Drill`
into a Python class or reproduce the miner, power, storage, placement, or
logistics systems.

```text
live Biome world ── reflected observation ──> policy
       │                                      │
       │                           place Drill + Solar
       │                                      │
       <──────── native purchase + placement ──┘
       │
       ├── native placement selects the deposit
       ├── native power energizes the Drill
       ├── native miner depletes the deposit
       └── native storage receives the resource
                          │
                          └──> Archetype goal/action/outcome history
```

This yields two ECS worlds with different authority:

| World | Owns | Does not own |
|---|---|---|
| Biome/Flecs | Prefabs, scene entities, placement, power, mining, storage, rendering, simulation time | Agent mission history or benchmark grading |
| Archetype | Goal, policy decision, episode phase, terminal result, durable `(world_id, run_id, tick)` evidence | Biome's native transition logic |

The adapter reads `biome.miner.Deposit`,
`flecs.engine.terrain.TerrainPosition`, `biome.power.PowerConsumer`,
`biome.miner.Miner`, and `biome.resources.Storage` through the Flecs Remote
API. Its one high-level action calls an example-local reflected C bridge that
charges Biome's real upstream recipes and invokes `biomePlaceBuilding` for the
upstream `buildings.Solar` and `buildings.Drill` prefabs. The benchmark scene
owns one upstream `buildings.Base` with its finite 500-Iron/500-Copper starting
inventory; the agent does not mint resources. A run succeeds only after the
Drill is powered, targets the selected deposit, and the deposit's native
amount decreases by the requested quantity while its native storage receives
at least that quantity.

That action boundary also demonstrates the relation-copy rule. Archetype's
generic `instantiate()` neither copies arbitrary relations nor returns an
old-to-new ID map. Domain code interprets a durable decision and materializes
the required external entities/relations explicitly. The generic prefab
operation is not silently expanded to understand Biome placement or power.

### Run it

The fully reproducible path is one command:

```bash
uv run python examples/14_biome_agent.py --launch --keep-open
```

On first use, the bootstrap clones the upstream repositories under the
gitignored `.context/upstream/` directory, checks out exact revisions, stages
the Archetype-owned mission scene, builds Biome, launches its graphical app
and REST server, runs the Copper extraction goal, and leaves the game open.
Omit `--keep-open` to terminate only the child process that the example
started. Use `--resource Iron --amount 25` to change the mission goal.

If Biome is already running:

```bash
uv run python examples/14_biome_agent.py --require-live
```

The pinned compatibility set is:

| Project | Revision | Reason |
|---|---|---|
| `SanderMertens/biome` | `d3372c2b3d7491b9260727292c27e554d12c0478` | Upstream game and prefab library used by the dogfood |
| `SanderMertens/flecs` | `fd137d63deccded67aba4a0dd8a8a4231d24e897`, originally from `script_await` | Exact commit containing the async script task/future API used by Biome |

Named upstream branches are provenance, not immutable inputs. The current
`script_await` history has diverged from this compatibility commit, so the
bootstrap fetches the exact SHA explicitly before detached checkout instead of
assuming the pin remains reachable from the branch tip.

The bootstrap passes `FETCHCONTENT_SOURCE_DIR_FLECS` to CMake because the
checked-in Biome CMake configuration currently names Sander's local Flecs
checkout. Reconfiguring after the Flecs checkout is necessary so CMake's
source glob includes `src/addons/script/async.c`. The mission scene omits the
upstream HUD because this compatibility revision eagerly evaluates its
zero-capacity resource gauge and aborts the remainder of the default scene;
all environment and Drill systems remain upstream-native.

### Flecs REST is a trusted local control boundary

The upstream executable starts an unauthenticated, mutating REST server whose
default bind address is `0.0.0.0:27750`. Run this dogfood only on a trusted
local network or behind an appropriate host firewall; do not expose the port
to an untrusted network. `BiomeClient` refuses non-loopback target URLs by
default and validates every identifier it passes to the native bridge, but
that does not change the server's bind address or add server-side
authorization.

At the pinned Biome revision the upstream repository does not declare a
license. Archetype therefore does not vendor, package, or redistribute Biome
source, assets, or the derived executable. The bootstrap patches only its
disposable local checkout to register the Archetype-owned bridge and produce
a private executable for the evidence run. Obtain permission or a declared
upstream license before distributing that derived build or any copied asset.

For a package-owned example that compiles framework prefab graphs into Agent
Missions authoring values, continue with the
[Mission Factory Asset Bible](mission-factory-assets.md).

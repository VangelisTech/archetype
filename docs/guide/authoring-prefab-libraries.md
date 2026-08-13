# Authoring a Prefab Library

A hands-on walkthrough of writing a prefab library with what ships today.
[Prefab Libraries](prefab-libraries.md) covers the two-layer model and the
theory; this page is the craft. Every call below is current public API. The
step snippets are sequential excerpts from one `async` function; the complete
runtime, storage, library, and consumer setup is shown in Step 2.

## The mental model

A prefab library is a world whose population is templates. A template is an
ordinary entity carrying the `Prefab` marker, its component values, and
optionally a `ChildOf` subtree of child templates. Authoring a library is
authoring a world carefully — ticks, edges, and history all apply.

## Step 1 — Declare the vocabulary

Components are capabilities and defaults. Relations are the domain language.
Two disciplines: keep definition state and runtime state in separate
components (a template carries `MissionPolicy`, never a `RetryCounter` —
that is instance business), and give relations real names instead of
overloading `ChildOf`:

```python
from archetype import Component
from archetype.graph import Relation


class MissionPolicy(Component):
    max_parallel_agents: int = 1
    require_tests: bool = True


class Role(Component):
    name: str = ""
    model: str = "claude-fable-5"


class Uses(Relation):          # template -> shared policy: domain meaning
    pass


class AssignedTo(Relation):    # runtime-only; never authored on templates
    exclusive = True
```

## Step 2 — Open the library world with a view

```python
from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.core.hooks import PostTick
from archetype.graph import GraphView


async def author_and_instantiate() -> None:
    storage = StorageConfig(uri="./archetype_data", namespace="mission-prefabs")
    view = GraphView()

    async with ArchetypeRuntime() as runtime:
        library = runtime.world(
            "mission-library",
            storage=storage,
            resources=[view],
            hooks=[(PostTick, view.on_post_tick)],
        )
        consumer = runtime.world("mission-consumer", storage=storage)

        # Run the Step 3 and Step 4 bodies here, inside this context.
```

The view is both your reading instrument and `instantiate`'s source. The
tick law applies: nothing exists until a step persists it — authoring stages
rows, and the step mints the version. `consumer` is a distinct world handle;
passing the library's view to `instantiate` is the explicit cross-world import.

## Step 3 — Author templates as entities

The following body belongs inside `author_and_instantiate()` after the setup
above:

```python
from archetype.graph import ChildOf, Prefab, link

mission = await library.spawn(
    Prefab(name="coding-mission"),
    MissionPolicy(max_parallel_agents=1),
)
reviewer = await library.spawn(Prefab(name="coding-mission/reviewer"), Role(name="reviewer"))
policy = await library.spawn(Prefab(name="shared/review-policy"), MissionPolicy())

await link(library, ChildOf(source=reviewer, target=mission))  # composition: copies
await link(library, Uses(source=mission, target=policy))       # domain edge: does not copy
await library.step()  # the version is born here, at (world, run, tick)
```

Path-style names (`coding-mission/reviewer`) are the naming convention;
uniqueness per library is your discipline until the registry's FLAG eval
exists — a five-line query makes a fine check today. Every edit plus a step
is a new version; the old one stays on the ledger.

## Step 4 — Instantiate into a consumer world

Continue in the same runtime context, where `consumer`, `view`, and `mission`
are already defined:

```python
from archetype.graph import instantiate

attempt = await instantiate(
    consumer, view, mission, overrides=[MissionPolicy(require_tests=True)]
)
await consumer.step()
```

Know exactly what this does, because the boundaries are the lesson:

- Component values copy; the `Prefab` marker never does — the copy is not a
  template.
- The `ChildOf` subtree rebuilds under fresh ids.
- Every new entity gets an `IsA` edge stamped with the full version
  coordinate `(world, run, tick)`, so lineage stays unambiguous even across
  worlds — `instantiate(consumer, library_view, ...)` is the import path.
- Overrides overlay by schema identity; markers and relations are refused
  loudly.
- The relation-copy boundary (registry design R7): the `Uses` edge above did
  not copy. Domain wiring is your driver's job after instantiation —
  traverse the new subtree, join `IsA` back to the source nodes for
  correspondence, then stage the relations you want.

Upgrades are re-instantiation: edit the template, step, instantiate again.
Generations coexist; the ledger keeps both.

## Step 5 — What makes it a library rather than a pile

Three habits, all queries:

- **Name hygiene** — assert `Prefab.name` uniqueness per library.
- **Structural lint** — `cascade(library, rel, view)` with a `FLAG` policy
  reports dangling composition instead of mutating; orphaned lineage is a
  join.
- **Grading** — instantiate into a lab world, run the scenario, grade from
  history keyed by the version coordinate. Per the registry design, a
  version without evidence receipts is advertising, not an asset.

## What is coming

The registry chain shrinks the ceremony without changing the moves:
`PrefabLibrary(name)` absorbs steps 1–3's wiring, the manifest publishes
what step 3 authored, and inheritance-as-projection
(`docs/design/prefab-inheritance.md`) will let instances carry only their
deltas against a pinned version. `examples/12_prefabs.py` is this page in
miniature; `examples/13_biome_rts.py` is it at full scale.

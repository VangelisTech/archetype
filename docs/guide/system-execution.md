# System Execution Model

The execution model is the heart of Archetype's ECS. Two lines of code in `System.execute()` determine which processors run on which entities, provide zero-cost component queries, and guarantee schema correctness by construction.

This page walks through the mechanism from first principles.

## Components to Signatures to Schemas

When you spawn an entity, its component types determine which **archetype** it belongs to.

### Step 1: Signature Construction

`Archetype.sig_from_components()` sorts component types alphabetically by class name to produce a canonical **signature** — a tuple of types:

```python
# Entity spawned with [Outbox(), Inbox()]
sig = Archetype.sig_from_components([Outbox(), Inbox()])
# => (Inbox, Outbox)  — sorted alphabetically

# Entity spawned with [Outbox(), Inbox(), DeliveryReceipt()]
sig = Archetype.sig_from_components([Outbox(), Inbox(), DeliveryReceipt()])
# => (DeliveryReceipt, Inbox, Outbox)  — DIFFERENT signature
```

Sorting ensures that `[Inbox(), Outbox()]` and `[Outbox(), Inbox()]` produce the same signature. Order of construction doesn't matter — only the set of types.

### Step 2: Schema Construction

`Archetype.get_archetype_schema()` builds an Arrow schema by combining the base metadata columns with each component's prefixed fields:

```
BASE_SCHEMA:
  world_id (string), run_id (string), entity_id (int32),
  tick (int32), is_active (bool)

+ Inbox.get_prefixed_schema():
  inbox__messages (list<string>)

+ Outbox.get_prefixed_schema():
  outbox__messages (list<string>)

= Full archetype schema
```

Each component class generates its prefix via `Component.get_prefix()`:
- `Inbox` becomes `inbox__`
- `Outbox` becomes `outbox__`
- `DeliveryReceipt` becomes `deliveryreceipt__`

### Step 3: Archetype Naming

The signature maps to a table name like `a_2c_s<hash>` — `2c` for two components, followed by a SHA-256 hash of the schema. Different signatures always produce different tables.

### The Result

Entities with identical component sets share a table. Entities with different component sets live in separate tables. This structural partitioning is what makes everything else work.

## The Subset Rule

A processor runs on an archetype if and only if the processor's declared `components` are a **subset** of the archetype's signature. Not equality — subset.

This is the critical two-line check in both `SyncSystem.execute()` and `AsyncSystem.execute()`:

```python
for proc_instance in sorted(self.processors, key=lambda x: x.priority):
    if set(proc_instance.components).issubset(set(sig)):
        df = proc_instance.process(df, **kwargs)
```

### What This Means

```
PhysicsProcessor(components=(Position, Velocity))
    runs on (Position, Velocity)                      # exact match
    runs on (Accel, Position, Velocity)               # superset matches
    skipped for (Position,)                           # missing Velocity

MessageDeliveryProcessor(components=(DeliveryReceipt, Inbox, Outbox))
    runs on (DeliveryReceipt, Inbox, Outbox)          # exact match
    runs on (Agent, DeliveryReceipt, Inbox, Outbox)   # superset matches
    skipped for (Inbox, Outbox)                       # missing DeliveryReceipt

ObserverProcessor(components=())
    runs on EVERY archetype                           # empty set is subset of all
```

### Why This Matters

The subset rule gives you three guarantees for free:

**Zero-cost queries.** There is no runtime "does this entity have component X?" check. Partitioning is structural — entities are already grouped by their component types. If the subset check passes, every row in the DataFrame has the required columns.

**Schema guarantees.** If your processor runs, the columns exist. You never need defensive checks like `if "col" in df.columns`. The archetype schema was built from the same component types your processor declared.

**Automatic parallelism.** Each archetype table is independent. Processors operating on one archetype can't affect another. This is why `AsyncWorld.step()` runs all archetypes concurrently via `asyncio.gather`.

### The `components=()` Pattern

An empty components tuple is a valid subset of every set. This makes observer processors that run on all archetypes:

```python
class MetricsProcessor(AsyncProcessor):
    components = ()    # matches every archetype
    priority = 100

    async def process(self, df, **kwargs):
        # Runs on every archetype table, every tick
        return df
```

## Priority Ordering

```python
sorted(self.processors, key=lambda x: x.priority)
```

Lower priority number means the processor runs first. This ordering is deterministic and consistent across ticks.

Typical priority ranges:

| Range | Use |
|-------|-----|
| -100 to -1 | Infrastructure (message delivery, command draining) |
| 1 to 9 | Input gathering, sensor reads |
| 10 to 49 | Core logic (agent thinking, physics) |
| 50 to 99 | Output, side effects |
| 100+ | Cleanup, metrics, bookkeeping |

Example: `MessageDeliveryProcessor` at priority -100 populates inboxes before agent processors at priority 10+ read them. This ensures messages sent in tick N are available in tick N+1.

## SyncSystem vs AsyncSystem

Both systems share the identical core subset check. The differences are in ergonomics:

### SyncSystem (`core/sync/system.py`)

Straightforward loop:

```python
for proc_instance in sorted(self.processors, key=lambda x: x.priority):
    if set(proc_instance.components).issubset(set(sig)):
        df = proc_instance.process(df, **input_kwargs)
```

Passes all kwargs directly. Error recovery logs and skips the failing processor.

### AsyncSystem (`core/aio/async_system.py`)

Same subset check, plus:

- **Resources injection** — the `Resources` container is added to kwargs so processors can access shared state via type-safe DI.
- **Signature filtering** — `inspect.signature()` introspects each processor's `process()` method. Only kwargs that the processor's signature declares are forwarded. Processors opt into `resources`, `tick`, etc. via their function signature — no need for `**kwargs` to accept everything.
- **Error recovery** — on exception, the original DataFrame is preserved (DataFrames are immutable, so the pre-error state is safe) and execution continues with the next processor.

```python
# Processor only accepts what it needs:
async def process(self, df, tick: int = 0, resources: Resources = None):
    ...

# AsyncSystem filters kwargs to match:
sig_params = inspect.signature(proc_instance.process).parameters
filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig_params}
df = await proc_instance.process(df, **filtered_kwargs)
```

## Per-Archetype Parallelism

In `AsyncWorld.step()`, each archetype's full processor chain runs as an independent `asyncio.gather` task:

```python
futures = [self._run_archetype(sig, run_config, **kwargs) for sig in sigs]
results = await asyncio.gather(*futures, return_exceptions=True)
```

The subset rule makes this safe. Archetypes are disjoint partitions — a processor operating on one archetype's DataFrame cannot observe or mutate another's. Each archetype goes through the same sequence independently:

1. Query previous state
2. Materialize mutations (spawns/despawns)
3. Execute matching processors in priority order
4. Persist to storage

Cross-archetype communication happens through deferred mechanisms (spawn/despawn caches, the message delivery pipeline) that take effect on the next tick.

## Common Pitfalls

**Processor doesn't run.** Your entity is missing a component that the processor declares. Check that the entity was spawned with all required component types. The subset rule means *every* declared component must be present.

**Unnecessary defensive checks.** If your processor runs, the columns exist. Don't add `if "col" in df.columns` guards — they're dead code by construction.

**Not understanding tick boundaries.** Messages written to Outbox at tick N are delivered to Inbox at tick N+1. Spawned entities appear next tick. These are features, not bugs — they ensure causal ordering.

**Forgetting that `components=()` matches everything.** An observer processor with an empty components tuple will run on every archetype table. This is useful for metrics but can be surprising if unintentional.

## Key Source Files

| File | What to Look For |
|------|-----------------|
| `core/archetype.py:45-52` | `sig_from_components()` — signature construction |
| `core/archetype.py:91-101` | `get_archetype_schema()` — schema construction |
| `core/component.py:45-47` | `get_prefix()` — column prefix generation |
| `core/sync/system.py:60-62` | `SyncSystem.execute()` — the subset check |
| `core/aio/async_system.py:78-79` | `AsyncSystem.execute()` — same check, async |
| `core/aio/async_system.py:93-96` | `inspect.signature` filtering |
| `core/aio/async_world.py:160-161` | `asyncio.gather` — per-archetype parallelism |

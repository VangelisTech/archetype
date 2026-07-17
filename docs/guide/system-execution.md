# System Execution Model

`AsyncSystem.execute()` and `SyncSystem.execute()` each process one archetype
DataFrame. They run registered processors in priority order when the
processor's declared components are a subset of that archetype's signature.
The subset check eliminates per-entity component lookups and guarantees that
the declared component columns exist in the DataFrame.

> The messaging names used below are application-defined examples. Archetype
> does not export `Inbox`, `Outbox`, `DeliveryReceipt`, or
> `MessageDeliveryProcessor`; applications may implement that processor-driven
> mailbox design on top of the execution model described here.

`AsyncWorld.step()` supplies the per-archetype concurrency: it schedules one
compute task for each active physical table, then commits the successful
results as a separate phase. `AsyncSystem` itself processes the one DataFrame
passed to each `execute()` call.

```python
for processor in sorted(self.processors, key=lambda item: item.priority):
    if set(processor.components).issubset(set(sig)):
        df = await processor.process(df, **accepted_kwargs)
```

The sections below detail each stage of the execution pipeline.

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

```text
BASE_SCHEMA:
  world_id (string), run_id (string), entity_id (int32),
  tick (int32), is_active (bool), commit_token (string),
  writer_epoch (int64)

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

The component count and full storage schema map to a table name such as
`a_2c_s<hash>`: `2c` records two component types and `<hash>` is the first 16
hexadecimal characters of the schema's SHA-256 digest. The schema includes the
base storage metadata, so a schema-generation change also produces new table
IDs.

This mapping is not a one-to-one encoding of Python component types. Distinct
signatures with the same component count and identical storage schemas share a
physical table ID. Empty tag components contribute no fields, for example:

```python
class FirstTag(Component):
    pass


class SecondTag(Component):
    pass


first = Archetype.sig_from_components([FirstTag()])
second = Archetype.sig_from_components([SecondTag()])

assert first != second
assert Archetype.get_name(first) == Archetype.get_name(second)
```

`AsyncWorld` interns one canonical signature per table ID so a schema-identical
table is processed only once. Treat empty tags as schema-neutral; do not assume
that a different tag class creates a distinct storage partition.

### The Result

Entities with identical field-bearing component sets share a table. Different
component sets usually produce different tables because their prefixed fields
change the schema, but the schema-identical exception above is part of the
table-identity contract.

## The Subset Rule

A processor runs on an archetype if and only if the processor's declared `components` are a **subset** of the archetype's signature. Not equality — subset.

This is the critical two-line check in both `SyncSystem.execute()` and `AsyncSystem.execute()`:

```python
for proc_instance in sorted(self.processors, key=lambda x: x.priority):
    if set(proc_instance.components).issubset(set(sig)):
        df = proc_instance.process(df, **kwargs)
```

### What This Means

```text
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

The subset rule provides three structural guarantees:

**No per-entity component lookups.** The world tracks one canonical signature
for each physical table. If the subset check passes, every row in the
DataFrame contains the processor's required component fields — no runtime
`has_component()` test is needed.

**Schema correctness.** If a processor executes, the columns it references are present. The archetype schema is constructed from the same component types the processor declares, so `if "col" in df.columns` guards are dead code.

**Per-table DataFrame scheduling.** Each matching call transforms the DataFrame
for one physical table. `AsyncWorld.step()` can schedule those transformations
concurrently. This separates the DataFrame inputs; it does not isolate shared
Python objects, services, or side effects.

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

Lower priority number means the processor runs first. Within one table's
processor chain, this ordering is deterministic and consistent across ticks.

Typical priority ranges:

| Range | Use |
|-------|-----|
| -100 to -1 | Infrastructure (message delivery, command draining) |
| 1 to 9 | Input gathering, sensor reads |
| 10 to 49 | Core logic (agent thinking, physics) |
| 50 to 99 | Output, side effects |
| 100+ | Cleanup, metrics, bookkeeping |

Application-level example: a `MessageDeliveryProcessor` at priority -100 can
populate inboxes before agent processors at priority 10+ read them. In that
design, messages sent in tick N become available in tick N+1.

## SyncSystem vs AsyncSystem

Both systems share priority ordering, the subset check, resource injection, and
fail-fast processor errors. When a caller supplies `resources`, both pass that
same container to every matching processor. The differences are in keyword
forwarding and in how their worlds schedule table execution.

### SyncSystem (`src/archetype/core/sync/system.py`)

Straightforward loop:

```python
for proc_instance in sorted(self.processors, key=lambda x: x.priority):
    if set(proc_instance.components).issubset(set(sig)):
        df = proc_instance.process(df, **input_kwargs)
```

Passes all input kwargs directly, including `resources` when supplied. A
processor exception is logged and re-raised, so the execution fails rather
than continuing with a partial processor chain.

### AsyncSystem (`src/archetype/core/aio/async_system.py`)

Same subset check, plus:

- **Signature-aware forwarding** — `inspect.signature()` introspects each processor's `process()` method. A processor with `**kwargs` receives every input; otherwise only explicitly declared keywords are forwarded. Processors can opt into `resources`, `tick`, or `debug` without accepting unrelated inputs.
- **Fail-fast errors** — a processor exception is logged and re-raised. The world aggregates compute failures and does not enter the commit phase for that tick.

```python
# Processor only accepts what it needs:
async def process(
    self,
    df: DataFrame,
    tick: int = 0,
    resources: Resources | None = None,
) -> DataFrame:
    ...

# AsyncSystem filters only when the processor does not accept **kwargs:
parameters = inspect.signature(processor.process).parameters
accepts_all = any(
    parameter.kind is inspect.Parameter.VAR_KEYWORD
    for parameter in parameters.values()
)
accepted_kwargs = (
    dict(input_kwargs)
    if accepts_all
    else {key: value for key, value in input_kwargs.items() if key in parameters}
)
df = await processor.process(df, **accepted_kwargs)
```

## Per-Archetype Parallelism

In `AsyncWorld.step()`, each active physical table's compute path runs as an
independent `asyncio.gather` task:

```python
futures = [self._compute_archetype(sig, run_config, **kwargs) for sig in sigs]
results = await asyncio.gather(*futures, return_exceptions=True)
```

The DataFrame input and output of each compute task belong to that physical
table. The tick uses two phases:

1. Query previous state
2. Materialize mutations (spawns/despawns)
3. Execute matching processors in priority order
4. If every compute succeeds, commit all resulting frames

That is DataFrame partitioning, not general task isolation. The tasks share:

- the world's mutable `Resources` container and every object stored in it;
- the registered processor instances; and
- any external services or process-level side effects those objects reach.

A processor can therefore mutate a resource that another archetype observes
during the same tick. Scheduling across archetypes is not a deterministic
ordering mechanism. Stateful resources and processors must provide their own
synchronization (for example, an `asyncio.Lock`) or expose concurrency-safe
operations. Keep deterministic entity state in DataFrame columns and use an
explicit deferred mechanism when communication belongs at a tick boundary.

See [Resources](resources.md) for the resource lifecycle and the additional
sharing boundary created by world forks.

## Common Pitfalls

**Processor doesn't run.** Your entity is missing a component that the processor declares. Check that the entity was spawned with all required component types. The subset rule means *every* declared component must be present.

**Unnecessary defensive checks.** If your processor runs, the columns exist. Don't add `if "col" in df.columns` guards — they're dead code by construction.

**Not understanding tick boundaries.** In the illustrative mailbox design,
messages written to Outbox at tick N are delivered to Inbox at tick N+1.
Spawned entities appear next tick. Both patterns preserve causal ordering.

**Forgetting that `components=()` matches everything.** An observer processor with an empty components tuple will run on every archetype table. This is useful for metrics but can be surprising if unintentional.

**Using empty tags as table discriminators.** Empty components add no storage
fields. Two same-arity signatures made only from different empty tags can map
to the same table ID and are interned to one canonical signature. Add a real
field when the distinction must be represented in storage.

**Treating concurrent tasks as isolated.** Per-table DataFrames are separate,
but resources, processor instances, and external side effects are shared.
Synchronize mutable shared objects and do not depend on cross-archetype task
order.

## Key Source Files

| File | What to Look For |
|------|-----------------|
| `src/archetype/core/archetype.py` | Signature construction, storage schema, and table naming |
| `src/archetype/core/component.py` | Component prefixes and Arrow schemas |
| `src/archetype/core/sync/system.py` | Synchronous subset matching and failure behavior |
| `src/archetype/core/aio/async_system.py` | Async subset matching and keyword filtering |
| `src/archetype/core/aio/async_world.py` | Signature interning and two-phase per-table scheduling |
| `src/archetype/core/resources.py` | Mutable world-shared resource container |

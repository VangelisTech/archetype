# World Lifecycle

**Document type:** Normative.
**Scope:** `iWorldService`, fork semantics, destroy semantics, info-class downgrade. Append-only invariant.

## 1. Append-only is non-negotiable

Archetype's storage and audit layers are append-only. There is no method on `iAsyncStore` or `iAuditLog` that deletes data. Ever.

The verb "drop" does not appear in either protocol. The verb "delete" does not appear in either protocol. Implementation code MUST NOT issue `DELETE` statements against archetype tables or the audit table.

This invariant is load-bearing for time-travel queries, audit integrity, fork persistence, and the `destroy_world` semantics defined below. It is the second-most important invariant in the system, after single-gate enforcement.

(Caveat: `AsyncCachedStore` may evict from its in-memory cache. That is cache eviction, not data deletion. The persisted layer underneath is untouched.)

## 2. World lifecycle operations

Three operations on `iWorldService`, plus their gated proxies on `iCommandService`:

| Operation | What it does |
|---|---|
| `create_world` | Establish new world identity. Storage allocated. |
| `fork_world` | Create a derivative world from a snapshot of source's current state. |
| `destroy_world` | In-memory cleanup. Storage and audit rows are NEVER touched. |

## 3. `create_world`

```python
# iWorldService (returns iWorld; internal use)
async def create_world(
    config: WorldConfig,
    storage_config: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
    system: iAsyncSystem | None = None,
) -> iWorld: ...

# iCommandService (returns WorldInfo; downgrade at gate boundary)
async def create_world(
    ctx: ActorCtx,
    config: WorldConfig,
    storage_config: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
) -> WorldInfo: ...
```

`WorldConfig` is serializable: `world_id`, `run_id`, `name`, `tick`, `next_entity_id`, dictionaries for `entity2sig`, `spawn_cache`, `despawn_cache`, plus `lineage` (fork ancestry read segments, see §4.6). NOTHING ELSE.

Processors, resources, and hooks are arbitrary Python objects; they do NOT go in `WorldConfig`. The runtime wires them at activation through dedicated gated paths:

- `iCommandService.add_processor(ctx, world_id, processor)`
- `iCommandService.add_resource(ctx, world_id, resource)`
- `iCommandService.add_hook(ctx, world_id, event_type, fn)`

Each is one gated call, one audit row.

## 4. `fork_world`

```python
async def fork_world(
    source_world_id: UUID | str,
    name: str | None = None,
    storage_config: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
) -> iWorld: ...
```

Fork creates a new world from a snapshot of the source's current state.

### 4.1 — Generated fresh in the fork

| Field | Value |
|---|---|
| `world_id` | new `uuid7()` |
| `name` | from caller |
| `run_id` | new `uuid7()` — fork starts a new run lineage |

### 4.2 — Copied from source (deep snapshot at fork time)

| Field | Reason |
|---|---|
| `tick` | fork inherits source's current tick — no temporal reset |
| `next_entity_id` | preserves entity-id space alignment for cross-fork comparison |
| `entity2sig` (dict) | the entities exist in the fork; mapping must be independent |
| `spawn_cache` (dict) | pending spawns at fork moment carry over to the fork |
| `despawn_cache` (dict) | same reason |
| hooks registry | hook registrations at fork moment carry over; new registrations on either side don't propagate |

Pending mutation transfer is the critical detail. If a user spawns an entity then immediately forks before the next step materializes the spawn, the fork inherits the pending spawn. Source and fork both see the entity on their next tick; the entity diverges from there.

### 4.3 — Shared between source and fork (same Python instance)

| Field | Reason |
|---|---|
| `resources` | resources are typically clients / connections / handles. Source and fork share the same `Resources` instance. |
| `processors` (in `system`) | the system holds processor instances. Forks share the same processor instances. |

### 4.4 — Implication of resource sharing

A `NATSConnection` in `source.resources` is the SAME `NATSConnection` in `fork.resources`. State mutations on the resource are visible to both. Stateful resources (live connections) need user-side awareness.

For users who want isolated resources per fork, they instantiate new resource instances explicitly. A future API may add `fork_world(..., new_resources=[...])`; not in v1 scope.

### 4.5 — Storage

The fork writes to the same physical store as the source by default, with rows partitioned by `world_id`. The optional `storage_config` argument allows the fork to write to a different store entirely.

Routing a fork to a different store severs read lineage (§ 4.6): ancestor
segments name rows in the source's store, and the fork's reads only see its
own store. A cross-store fork therefore starts from transferred pending
state (un-materialized spawns/despawns), not from the source's persisted
history. Forking with an explicit `storage_config` on a stepped source logs
a warning for this reason. Same-store forks — the default — get full
history continuation.

### 4.6 — Read lineage (copy-on-write history)

A fork does not copy the source's materialized rows. Instead it carries a `lineage` — an ascending list of `(world_id, run_id, up_to_tick)` segments: the source's own lineage plus, if the source has stepped, one segment covering the source's rows for ticks `0..tick-1`.

Reads resolve per tick: a tick at or below a segment's `up_to_tick` reads from that ancestor's run; later ticks read from the fork's own run. Because the store is append-only, ancestor rows are immutable history — a parent that keeps running (or is destroyed) after the fork never affects the fork's view, and rows the parent writes after the fork point are excluded from the fork's history.

Consequences:

- Forking stays O(metadata) regardless of world size.
- A fork of a stepped world is immediately queryable, and its first step processes the parent's last tick as input — state continues across the fork point.
- Lineage flattens across generations: a fork of a fork reads base history, mid-fork history, and its own rows through one segment list.
- Lineage is durable. At fork time the full ancestor chain is appended to the store as `WorldLineage` rows under the fork's `(world_id, run_id)` (negative entity ids — metadata, never live entities). The store is append-only, so provenance is never compromised: gated reads resolve ancestry for destroyed worlds by loading the persisted chain (`QueryService.get_lineage`). Live worlds resolve from memory; the persisted rows are the fallback and the system of record.

Contract tests: `tests/integration/test_fork_destroy_contracts.py` (§8 fork lineage, §9 persisted lineage / dead-world ancestry).

### 4.7 — Audit emission

`iCommandService.fork_world` emits one audit row with:

- `command_type = "fork_world"`
- `payload_json = {"source_world_id": ..., "fork_world_id": ..., "name": ..., "tick_at_fork": ...}`

See [Audit Log](audit-log.md) for the audit row schema.

## 5. `destroy_world`

In-memory cleanup. Drops the live `iWorld` instance from the registry. Persisted storage and audit rows are NEVER touched.

```python
# iWorldService
async def destroy_world(world_id: str | UUID) -> None: ...

# iCommandService (orchestrates cross-service cleanup)
async def destroy_world(ctx: ActorCtx, world_id: str | UUID) -> None: ...
```

### 5.1 — `iCommandService.destroy_world` steps, in order

1. Mark the world closing. No new `step()`, `run()`, episode, or rollout may be
   admitted after this point.
2. Flush any buffered audit rows for this world via `iAuditLog.flush()`. (Flush ≠ delete — rows are written to permanent storage; nothing is dropped.)
3. Cancel pending in-memory broker commands for this world via `iCommandBroker.clear(world_id)`.
4. Take the app-owned per-world operation lock and await any operation admitted
   before step 1 (wait-then-destroy). The same lock guards direct service and API
   traffic; runtime-handle locking is not the serialization boundary.
5. Call `iWorldService.destroy_world(world_id)`. While holding the operation
   lock it fires `OnDestroy` and removes the world from the registry. Hook
   handlers may read final state but MUST NOT submit commands; the world is
   closing.
6. Emit ONE audit row recording the destroy.

### 5.2 — `iWorldService.destroy_world` steps

1. Resolve world from registry; return early if absent (idempotent).
2. Mark the world closing and acquire its app-owned operation lock.
3. Fire `OnDestroy` via the world's hook bus.
4. Remove from `WorldRegistry` and mark its durable catalog row destroyed.

`iWorldService.destroy_world` is the registry-level primitive. The cross-service cleanup (broker.clear, audit.flush) is `iCommandService`'s concern; `iWorldService` doesn't have references to the broker or audit log.

### 5.3 — What destroy_world is NOT

- It does NOT delete the world's persisted rows from `iAsyncStore`. Time-travel queries against the destroyed world remain valid forever.
- It does NOT delete audit rows. `iAuditLog.query(world_id=...)` returns the destroyed world's rows.
- It does NOT delete storage files. The `StorageService` keeps files associated with the destroyed world's data.

### 5.4 — `OnDestroy` event

Defined in `core/hooks.py`:

```python
@dataclass(frozen=True, slots=True)
class OnDestroy(HookEvent):
    """Fires before in-memory world cleanup begins.
    Handlers MAY read final world state but MUST NOT submit commands."""
    world_id: UUID
```

Read-only by convention; no enforcement in v1.

### 5.5 — Idempotency

Destroying an unknown world_id is a no-op, not an error. Repeated calls to `destroy_world(world_id)` after the first succeed silently.

## 6. `resume_world` (fenced mutable cold resume)

Added by issue #273 on top of the atomic-visibility contract
([Atomic Visibility](atomic-visibility.md)).

```python
# iWorldService (returns AsyncWorld; internal use)
async def open_world_mutable(
    storage_config: StorageConfig,
    world_id: str | UUID,
) -> AsyncWorld: ...

# iCommandService (returns WorldInfo; downgrade at gate boundary)
async def resume_world(
    ctx: ActorCtx,
    storage_config: StorageConfig,
    world_id: str | UUID,
) -> WorldInfo: ...

# ArchetypeRuntime (returns a RuntimeWorld handle)
world = await runtime.resume(world_id, storage=...)
```

Resume reconstructs a live, writable world from durable state in a process
that shares nothing with the previous writer but the storage config:

- **Tick** — the last manifest tick + 1, never from fact claims or rows:
  neither a visible fact nor a crashed attempt's unpublished rows may
  advance the simulation head.
- **Entity directory** — the latest visible row per entity across every
  catalog table decides its archetype and liveness; `next_entity_id`
  resumes past the highest id ever seen (ids are never reused). The
  inventory reads through the open-never-create seam on base columns, so
  classes are demanded only for archetypes with LIVE entities.
- **Component classes** — resolved by the stored schema fingerprint, not by
  name alone: same-named classes from different modules cannot be confused,
  and a definition that drifted since the rows were written is refused.
  Identity is the schema.
- **Lineage** — restored from the persisted ancestor chain; a fork record
  whose lineage rows are missing is detectable corruption and refuses.
- **Fence** — acquired last (epoch + 1): the previous writer's next publish
  fails closed with `StaleWriterError`.

Resume preflights reconstruction before fencing, then repeats it against the
authoritative post-fence snapshot. If state changed between those reads and
the second reconstruction fails, the new fence has already made the previous
writer stale; the failure is logged as an operator-visible orphaned-writer
condition and the caller must retry after correcting the reconstruction error.

Resume fails loudly rather than reconstructing unfaithfully: unrecorded
worlds (`KeyError`), destroyed worlds (queryable, never resumable), worlds
already live in this process, missing or drifted component classes, and
corrupt fork records all refuse with the cause named.

**Code is not rows.** Processors, resources, and hooks are never restored
and never claimed to be. The caller reattaches them after resume; until
then the world steps as a pure ledger-advancing simulation. Gated as
`CREATE_WORLD` — resuming creates a live writer, the same authority class
as creating one.

## 7. `WorldInfo` and the info-class downgrade

The gate downgrades live objects to immutable info classes before returning to user code. This is what enforces "the runtime never holds an `iWorld`."

### 6.1 — `WorldInfo`

```python
class WorldInfo(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    world_id: UUID
    name: str | None = None
    tick: int
    run_id: UUID | None = None
```

Returned by `create_world`, `fork_world`, `get_world_info`. Field access is sync; the round-trip to fetch is async (gated).

`WorldInfo` is NOT the same shape as `WorldConfig`. `WorldConfig` carries pending caches and entity mappings (large at scale); `WorldInfo` is identity + current tick only.

### 6.2 — Other info classes

Defined in `app/models.py`:

```python
class ProcessorInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    qualname: str
    priority: int
    components: tuple[str, ...]   # component class qualnames

class HookInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    event_type: str               # HookEvent subclass qualname
    handler_qualname: str
    mode: str                     # "blocking" | "spawn"
    handle_id: int                # internal HookHandle id

class ResourceInfo(BaseModel):
    model_config = ConfigDict(frozen=True)
    qualname: str                 # resource class qualname
    # Resources are type-keyed in the underlying container;
    # qualname is sufficient identity. No second field.
```

### 6.3 — Downgrade points

| Internal return | Gate return |
|---|---|
| `iWorldService.create_world → iWorld` | `iCommandService.create_world → WorldInfo` |
| `iWorldService.fork_world → iWorld` | `iCommandService.fork_world → WorldInfo` |
| `iWorldService.list_processors → list[iAsyncProcessor]` | `iCommandService.list_processors → list[ProcessorInfo]` |
| `iWorldService.list_hooks → list[HookHandle]` | `iCommandService.list_hooks → list[HookInfo]` |
| `iWorldService.list_resources → list[object]` | `iCommandService.list_resources → list[ResourceInfo]` |

The downgrade happens at the gate's return statement. Live objects (iWorld, iAsyncProcessor instances, callables, user resources) never escape past the gate.

## 8. Permissions

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `create_world` | — | — | — | ✓ |
| `fork_world` | — | — | ✓ | ✓ |
| `destroy_world` | — | — | ✓ | ✓ |
| `get_world_info` | ✓ | ✓ | ✓ | ✓ |
| `list_processors` | ✓ | ✓ | ✓ | ✓ |
| `list_hooks` | ✓ | ✓ | ✓ | ✓ |
| `list_resources` | ✓ | ✓ | ✓ | ✓ |

`fork_world` and `destroy_world` are operator-permitted because operators routinely fork (rollouts) and destroy (cleanup), and neither operation deletes persisted data.

## 9. Tests

Critical:

- `destroy_world`: storage row count for the world is unchanged before/after (verify via `QueryService.query_archetype`).
- `destroy_world`: audit row count for the world is unchanged before/after (verify via `AuditLog.query(world_id=...)`).
- `fork_world`: spawn-then-fork → entity exists in BOTH source's next tick AND fork's first tick.
- `fork_world`: source.resources is fork.resources (identity check).
- `fork_world`: hook registered on source post-fork does NOT fire on fork.
- `destroy_world` is idempotent on unknown ids.
- `destroy_world` waits on `op_lock` for in-flight `step()` to complete.
- `iAsyncStore` protocol has no `drop_*` / `delete_*` method. (Reflective check.)
- `iAuditLog` protocol has no `drop_*` / `delete_*` method. (Reflective check.)

## 10. Out of scope

- Per-resource forking semantics (isolated resources per fork). v1: shared.
- Per-world storage migration after creation. v1: storage_config at creation time only.
- Snapshot export / world serialization to disk. v1: forks are the only supported "save" mechanism.

# World Lifecycle

**Document type:** Normative.
**Scope:** `iWorldRegistry`, `iWorldLifecycle`, fork, discovery, resume, close,
and boundary-safe world information.

## 1. Append-only is non-negotiable

Closing a world releases live process ownership. It does not delete persisted
ECS rows, lineage, command records, or audit evidence. Append-only history is
load-bearing for durable reads, time travel, fork lineage, and crash recovery.
Cache eviction is not data deletion.

Physical visibility and scans belong to `archetype.storage`; interpretation of
those rows as ECS liveness, signatures, lineage, ticks, and entity-ID state
belongs to `archetype.world`.

## 2. World lifecycle operations

`WorldRegistry` and `WorldLifecycle` split state ownership from lifecycle
behavior:

| Owner | Responsibility |
|---|---|
| `WorldRegistry` | Strong live ownership, name and storage-coordinate indexes, activation serialization, exact-world locks, close leases, projector bindings, retained receipts |
| `WorldLifecycle` | Create, fork, durable discovery, readonly cold open, fenced mutable resume, retryable close |

The family-owned protocols are `iWorldRegistry` and `iWorldLifecycle`.
Application code depends on those ports; only the composition root constructs
the concrete owners.

Operations on one live world serialize through `registry.operation(world_id)`.
Different world IDs may progress concurrently. Multi-world operations acquire
sorted IDs and release them in reverse order. No ambient reentrancy or
task-inherited cleanup authority exists.

Lifecycle idempotency is exact:

- create with an already-live explicit `world_id` returns that binding;
- a duplicate name or conflicting durable registration fails without leaving a
  hidden live world;
- destroying an absent live world is a no-op; and
- failed close retains the exact world, close lease, aliases, and dependencies
  for a later serialized retry.

## 3. `create_world`

```python
async def create_world(
    config: WorldConfig,
    storage_config: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
    system: iAsyncSystem | None = None,
) -> AsyncWorld: ...
```

Creation:

1. serializes activation for the exact `world_id`;
2. validates the live ID and name indexes;
3. resolves the storage backend through `iStorageService`;
4. registers an active, `resumable` durable world with a fresh immutable UUIDv7
   `run_id`;
5. acquires a writer fence;
6. binds a commit coordinator to world, run, and writer epoch;
7. calls the module-level `build_world(...)` constructor with the scheduler
   materializer and optional system; and
8. inserts the world, storage coordinates, and optional required projector into
   `WorldRegistry`.

If durable activation fails after registration, lifecycle marks the incomplete
record non-active and propagates the failure. A rejected activation is never
reachable through the live registry.

Processors, resources, and hooks are live Python capabilities and are not
durable configuration. Trusted callers attach them after creation; untrusted
callers use actor-aware dispatcher operations.

### 3.1. Atomically private workflow worlds

`iWorldLifecycle.create_closing_world(...)` is an internal family/composition
capability for a workflow whose world must persist durable evidence without
ever becoming public live work. It performs the same durable construction as
`create_world`, but registry insertion atomically installs the exact sticky
`WorldCleanupLease` and returns `(world, lease)`. There is no interval in which
another public operation can acquire the world lock: `registry.operation(...)`
rejects with `WorldClosingError` from the first visible binding.

The catalog records these worlds as `status="active"` with immutable
`writer_mode="cleanup_only"` from their first registration. Active status is
required while the scheduler materializes commands at tick boundaries;
`cleanup_only` is the orthogonal crash rule that prevents a fresh process from
reconstructing the writer as ordinary mutable work. A crashed workflow's
published rows therefore remain discoverable and queryable, but the provider
processors can never be reactivated through mutable resume.

Physical workflow composition reserves a process-owned cleanup slot before it
may create a closing world. `create_closing_world(...)` synchronously binds the
exact control catalog and complete cleanup-only `WorldRecord` into that slot
before calling `register_world(...)`; there is no registration effect without
an already-retained retirement owner. Immediately after registry insertion
installs the sticky `WorldCleanupLease`, lifecycle promotes that same slot to
the canonical exact-world cleanup target without awaiting or replacing its
process owner.

An activation failure invokes the currently bound owner cancellation-resistantly.
Before promotion it performs identity-safe registration retirement, including
an absent-row tombstone for an ambiguous remote write; after promotion it
revalidates and executes through registered `WorldCleanup`. There is no
unregistered lifecycle-destroy fallback. A failed attempt remains in the
`workflow-handles` shutdown inventory for retry, associated provider close
still joins it, and multiple failures are reported as an aggregate rather than
abandoning any cause. See
[Durable Discovery](durable-discovery.md#2-the-control-catalog) for the remote
v8 retirement contract.

Public `ListWorlds` omits closing entries, entries removed after its
point-in-time snapshot, and same-ID replacement bindings created after that
snapshot, so one private or concurrently retiring writer cannot make unrelated
live worlds unlistable. `snapshot_world_bindings()` captures opaque exact-entry
references, not only world-object identity: removing an entry and reinserting
even the same Python world object creates a replacement binding outside that
snapshot. A binding reference is only a comparison witness; public admission
authority remains `registry.operation(...)`. Listing admits each captured ID
independently and synchronously proves the admitted world still belongs to
that exact binding before reconciliation. A `KeyError` before
`registry.operation(...)` yields is a stale snapshot omission; once admission
yields, a `KeyError` from reconciliation propagates and is never reclassified
from a later ambient `contains(...)` observation. After reconciling each
admitted candidate, listing linearizes `is_public_binding(binding, world)`
without another await before capturing `WorldInfo`; a close that became sticky
during reconciliation is therefore omitted, while a later close occurs after
that candidate's valid snapshot point.

The owning workflow executes state changes only inside
`registry.cleanup_operation(lease)` and through lock-held world functions. On
completion or cancellation it reconciles and destroys the writer through the
same exact cleanup authority. Destroyed rows, lineage, and run identity remain
durably queryable. The lease cannot authorize a sibling, a replacement, or an
already-live world, and this construction path is not a registered public
operation.

## 4. `fork_world`

```python
async def fork_world(
    source_world_id: UUID | str,
    name: str | None = None,
    storage_config: StorageConfig | None = None,
    cache_config: CacheConfig | None = None,
) -> AsyncWorld: ...
```

Fork holds the source's exact-world lock while taking its snapshot. The fork
receives fresh UUIDv7 world and run identities and a fresh writer fence,
command-materializer binding, commit coordinator, lock, and required-projector
binding.

Before selecting that snapshot, the source retries any retained required
projection and reconciles any prepared publication under its exact identity.
The resulting receipt is projected before the fork boundary is selected. If
publication remains ambiguous, fork fails without registering a child; it
never branches from stale live tick/caches while a newer manifest may already
be durable.

It snapshots:

- the current tick and next entity ID;
- the entity-to-signature directory;
- pending spawn and despawn caches;
- the flattened durable lineage; and
- the current processor and hook registrations.

Resources are intentionally shared with the source. Processor instances and
hook callables are process-local capabilities; the fork receives the current
registrations but later registration changes do not propagate.

By default source and fork share the physical store and rows remain partitioned
by world/run identity. The fork does not copy materialized rows. Instead,
ascending `(world_id, run_id, up_to_tick)` lineage segments select immutable
ancestor history before the fork point and fork-owned rows after it. Lineage is
persisted at fork time, so durable query and resume do not require the ancestor
to remain live.

An explicit different storage configuration cannot read ancestor rows from the
source store. Such a fork carries only its transferred process-local snapshot
and pending mutations into that storage authority.

## 5. `destroy_world`

Application destroy starts by obtaining a sticky `WorldCleanupLease`. New
public operations reject once close has begun with the family-owned
`WorldClosingError`. The synchronous target-tick snapshot emits the same typed
state so policy can place authorized durable-world calls in the explicit
tick-zero quota bucket without catching unrelated resolver failures. That quota
fallback does not grant live operation authority. Under the exact cleanup
lease, `WorldCleanup`:

1. retries any already-retained required-projector receipt;
2. reconciles any prepared tick publication under its exact commit identity
   and retains/projects the resulting receipt;
3. cancels only the remaining unsettled commands; and
4. delegates to `WorldLifecycle.destroy_world(...)`, which fires advisory
   `OnDestroy`, marks the durable world record destroyed, and releases registry
   ownership only after cleanup succeeds.

If any cleanup step fails, the entry stays strongly reachable and closing. The
same lease authorizes a later retry against that exact entry; it cannot
authorize a sibling or replacement world. Aliases and locks disappear only
after `finish_close`. Composition callers—registered `DestroyWorld` and a
workflow's retained retirement handle—holding that same lease join one complete
reconcile, command-cancel, lifecycle-close transaction. Successful completion
is memoized only on that lease and never authorizes a replacement. Composition
registers the transaction with the process owner before executing cleanup. A
physical provider owner waits for every exact evidence-world
retirement associated with its identity before closing the provider. A failure
therefore retains both sides in the `workflow-handles` shutdown inventory and
is retried before audit or storage teardown rather than being abandoned in the
registry. A successfully completed advisory `OnDestroy` dispatch is
checkpointed on that exact cleanup lease before the durable status write; if a
later status write fails, returns an ambiguous response, or is cancelled, the
retry repeats only the idempotent durable write and does not emit `OnDestroy`
again. Cancellation while the hook dispatch itself is still running does not
checkpoint completion and remains retryable. Required projection or
prepared-commit reconciliation failure produces no command cancellation,
`OnDestroy`, or durable destroyed status. A pending required-projector receipt
also prevents final release until it is acknowledged.

Destroy never removes persisted rows, lineage, command history, audit history,
or storage files. Destroyed worlds remain durably queryable but are not
resumable.

## 6. `resume_world` (fenced mutable cold resume)

The family primitive is
`iWorldLifecycle.open_world_mutable(storage_config, world_id)`. The exact
boundary model is `ResumeWorld`; the runtime returns a lazy `RuntimeWorld`
handle after trusted dispatch.

Mutable resume reconstructs a writer in a process that shares only durable
storage with the previous writer:

- preflight physical visibility before acquiring a fence;
- acquire the next writer epoch;
- repeat an authoritative scan after fencing;
- derive liveness and signature ownership using latest-wins and same-tick
  active-wins rules;
- resolve component classes by durable schema fingerprint and table identity;
- restore persisted lineage;
- derive the next tick from the published manifest, never unpublished rows;
- derive `next_entity_id` from visible rows and durable reservations;
- restore the catalog's immutable UUIDv7 `run_id`; and
- bind a new commit coordinator and command materializer.

The preflight avoids fencing a world that is already known to be
unreconstructable. Once the fence is acquired, any failure is
operator-visible: the prior writer is stale and the caller must correct the
cause before retrying.

Resume requires both `status="active"` and `writer_mode="resumable"`. It
refuses destroyed worlds, `cleanup_only` evidence worlds, unknown future writer
modes, already-live worlds, corrupt lineage, missing runs, and unresolved or
schema-drifted worlds. Status and writer-mode refusal happens before opening a
store or acquiring a writer fence. Resume never guesses. Processors, resources,
and hooks are code rather than rows and must be reattached by the caller.

When a required projector is configured, resume reconstructs the manifest-head
`CommittedTickReceipt` and retains it for idempotent acknowledgment before a
new tick is admitted.

Readonly cold open is separate:
`iWorldLifecycle.open_world_readonly(...)` returns durable `WorldInfo` without
acquiring a writer fence or constructing a live world.

### 6.1. Whole-storage migration

Local whole-storage migration is not `fork_world` and mints no World or run
identity. It preserves World IDs, run IDs, statuses, writer modes, ticks,
lineage, manifests, and durable history. It imports only a writer-epoch floor,
never the source process's active fence holder, so the first destination
mutable resume acquires a strictly higher epoch.

Processors, hooks, Resources, provider clients, and other executable Python
capabilities are not durable state and do not migrate. Callers must reinstall
the code and capabilities required by a resumed World. See
[Storage Migration](storage-migration.md).

## 7. Boundary-safe information

Lifecycle primitives may return an internal `AsyncWorld`. Neither
`ArchetypeRuntime` nor the REST layer returns that capability. Registered
handlers and adapters downgrade it to frozen values such as:

```python
class WorldInfo(BaseModel):
    world_id: UUID
    name: str | None
    tick: int
    run_id: UUID
```

Processor, hook, and resource listings similarly return `ProcessorInfo`,
`HookInfo`, and `ResourceInfo`, never live instances or callables.

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

## 9. Executable contracts

Focused create/fork/resume/fence/close behavior lives under `tests/world/`.
Cross-boundary lifecycle and persistence behavior remains under
`tests/integration/`, `tests/app/`, and `tests/api/`.

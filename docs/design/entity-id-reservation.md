# Entity-ID Reservation — Joint Python/Rust Design

**Status:** Normative. Both the Python async runtime (`AsyncWorld`) and the Rust
kernel (`crates/archetype-core`, branch `everettVT/rust-core-crate`) implement this
contract.

---

## 1. Motivation

External systems — environment bridges, replay harnesses, eval drivers — sometimes
need to key by entity ID *before* the spawn lands on the ledger. The classic
example is `ManipTask.env_key`: a manipulation task must reference its robotic-arm
entity at submission time, before that entity has been registered with the world.

Previously this required an ad-hoc out-of-band counter (the `env_key` workaround).
This design replaces it with a first-class API.

---

## 2. ID-Space Ownership

Both engines maintain a single monotonically increasing counter:
`next_entity_id` (Python) / `next_entity_id: AtomicU64` (Rust).

**Invariant:** Every ID ever exposed to user code — whether from
`create_entity`, `create_entities`, or `reserve_entity_ids` — is drawn from
this counter by advancing it forward. The counter never decrements.

```
                         ┌──────────────────────────────┐
                         │   next_entity_id counter       │
                         │   (monotonically increasing)   │
                         └──────┬───────────────┬─────────┘
                                │               │
                    create_entity /         reserve_entity_ids(n)
                    create_entities          advances by n
                    (advance by 1 each)     returns range [start, start+n)
```

### Collision Rules

1. IDs are globally unique within a world for its lifetime.
2. Reserved IDs and auto-assigned IDs share the same counter, so they are
   always disjoint by construction — no coordination layer needed.
3. If `reserve_entity_ids(n)` and `create_entity` interleave, each call
   advances the counter atomically (Python: single-threaded event loop;
   Rust: `fetch_add` on the atomic counter). No locking required beyond
   what the engine already provides.
4. `reserve_entity_ids(0)` is an error (raises `ValueError` / panics with
   `debug_assert`).

---

## 3. Reservation Lifecycle

```
reserve_entity_ids(n) → [id₁, id₂, …, idₙ]
                               │
                     (external system uses the IDs
                      to set up references / keys)
                               │
         spawn_with_reserved_id(idₖ, components)
                               │
                    entity is now registered:
                      entity2sig[idₖ] = sig
                      spawn_cache[sig].append(row)
                      OnSpawn fires
                               │
                     next step() materializes
                     the raw spawn row (x₀)
```

A reserved ID that is *never* materialised via `spawn_with_reserved_id` (or
the Rust equivalent `queue_spawn_with_entity_id`) is simply a gap in the
entity namespace. This is safe: the world never writes a row for it, so no
phantom state is produced.

---

## 4. Fork Semantics

When a world is forked (Python) / a snapshot is cloned (Rust), the child
inherits `next_entity_id` from the parent at fork time. Both engines
advance their own counters independently after the fork. This means:

- IDs issued by the parent after the fork do not appear in the child.
- IDs issued by the child after the fork do not appear in the parent.
- The lineage (pre-fork rows) is shared via the ancestor's run segment; no
  ID from that segment is ever re-used by either branch.

---

## 5. Cancellation (Despawn Before Materialize)

If `spawn_with_reserved_id(id, ...)` is called and then `remove_entity(id)`
is called in the *same tick*, before `step()` runs:

1. `remove_entity` finds the entity in `entity2sig` (it was registered by
   `spawn_with_reserved_id`).
2. It finds the pending row in `spawn_cache[sig]` and removes it.
3. `OnDespawn` fires. No row reaches the ledger.

This matches the existing same-tick cancel semantics of `create_entity` →
`remove_entity`. See `AsyncWorld.remove_entity` for the authoritative
implementation.

A reserved ID that was *never* materialised has no entry in `entity2sig`, so
`remove_entity` is a no-op (log warning, no crash) — exactly matching the
behaviour for any unknown entity ID.

---

## 6. Rust Counterpart: `queue_spawn_batch_with_entity_ids`

The Rust kernel in `crates/archetype-core` (branch `everettVT/rust-core-crate`)
exposes:

```rust
/// Reserve n entity IDs. Advances next_entity_id by n atomically.
pub fn reserve_entity_ids(&mut self, n: u64) -> RangeInclusive<EntityId>;

/// Enqueue a single spawn for an externally reserved ID.
/// Equivalent to Python's spawn_with_reserved_id.
pub fn queue_spawn_with_entity_id(
    &mut self,
    entity_id: EntityId,
    components: ComponentBundle,
) -> Result<(), SpawnError>;

/// Enqueue a batch of spawns with externally reserved IDs.
/// This is the Rust equivalent of Python create_entities when IDs
/// are pre-reserved by the caller.
pub fn queue_spawn_batch_with_entity_ids(
    &mut self,
    spawns: impl IntoIterator<Item = (EntityId, ComponentBundle)>,
) -> Result<(), SpawnError>;
```

**Mapping to Python:**

| Python (`AsyncWorld`) | Rust (`World`) |
|---|---|
| `reserve_entity_ids(n)` | `reserve_entity_ids(n)` |
| `spawn_with_reserved_id(id, components)` | `queue_spawn_with_entity_id(id, bundle)` |
| `create_entities(entities)` | `queue_spawn_batch(bundles)` (auto-IDs) |
| `create_entities` + pre-reserved IDs | `queue_spawn_batch_with_entity_ids(pairs)` |

The Rust `SpawnError::IdAlreadyRegistered` maps to Python's `ValueError` from
`spawn_with_reserved_id`.

---

## 7. Initial-Conditions Contract (x₀ invariant)

Both engines: the first persisted row for an entity is its *raw* spawn values
at the tick it materializes. Processors first apply on the following tick.

Formally: `x₀` is given, `x_{t+1} = f(x_t)`. The ledger contains
`x₀, f(x₀), f²(x₀), …`.

For batch spawns: all entities in a single `create_entities` / `queue_spawn_batch`
call share the same materialization tick. Each entity's `x₀` is written
independently; no cross-entity dependency is implied.

---

## 8. env_key Retirement Path

`ManipTask.env_key` was an integer workaround that shadowed the entity's
true ledger ID. Now that `reserve_entity_ids` is available, the correct
pattern is:

```python
# Old pattern (workaround)
env_key = some_external_counter()
task = ManipTask(env_key=env_key, ...)

# New pattern (first-class)
(entity_id,) = world.reserve_entity_ids(1)
task = ManipTask(entity_id=entity_id, ...)  # env_key field retired
await world.spawn_reserved(entity_id, Arm(), Gripper())
```

The `env_key` field on `ManipTask` and friends is retained for backward
compatibility in this release. The experiments refactor (a separate lane)
will migrate callers and remove the field.

---

## 9. Audit Checklist for Codex

- [ ] `reserve_entity_ids(n < 1)` raises `ValueError` (Python) / `debug_assert` / `Err` (Rust).
- [ ] No collision under interleaved reserve + create_entity + create_entities (property test in `tests/core/test_batch_spawn_contract.py`).
- [ ] Same-tick cancel removes the spawn_cache row cleanly.
- [ ] `spawn_with_reserved_id` on an already-registered ID raises `ValueError`.
- [ ] `create_entities` fires one `OnSpawn` per entity (not one per batch).
- [ ] Rust `queue_spawn_batch_with_entity_ids` applies `SpawnError::IdAlreadyRegistered` guard.
- [ ] Fork child counter starts at parent's `next_entity_id` at fork time.

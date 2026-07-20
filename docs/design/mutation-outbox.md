# Mutation Outbox — Design

**Status:** Ruled (#604, 2026-07-20), not implemented. This document makes
the seam's design repo-resident; the rulings are recorded on issue #604.

---

## 1. Motivation

Processors are pure ``DataFrame → DataFrame`` and cannot spawn or despawn:
``is_active`` is engine-owned, flipped only by mutations staged through the
world API. Cleanup therefore shipped as a driver-level helper
(``graph.cascade``) called between steps. The outbox lets mutation-producing
logic run *inside* the tick without giving processors effects: they declare
intents; the engine applies them.

## 2. Shape

A per-tick resource the engine injects; processors require it and emit:

```python
class MutationOutbox:
    def despawn(self, entity_id: int) -> None: ...
    def spawn(self, *components: Component) -> None: ...
```

The engine drains the outbox through the same staging path
``world.spawn``/``world.despawn`` use, so intents emitted at tick N land at
tick N+1, and ``OnSpawn``/``OnDespawn`` hooks and audit fire identically to
driver calls.

## 3. Rulings (issue #604)

1. The outbox-as-Resource pattern is accepted, and it stays internal.
2. Built-in processors raise concrete error types per failure mode and
   double as the demonstrations of the mature processor pattern.
3. Determinism rides existing machinery: entity ids are a world concern,
   assigned at drain; priority orders emission; the drain applies a stable
   sort so id assignment is reproducible.
4. No actor context on the outbox — authority is which processors are
   installed (consistent with runtime R3); the command gate remains the
   boundary for external mutation.
5. The outbox is tick-scoped: intents die with an aborted tick, which is
   stricter than driver-level staging and is the contract.
6. Processor-native ``instantiate`` is in scope once the seam lands —
   programmatic entity creation from inside the sim is a requirement.

## 4. Invariants

- Intents at tick N take effect at N+1; one generation per tick is preserved
  by construction.
- Frames returned by processors are unchanged by intents; the outbox is not
  a data channel.
- Draining reuses the staged-mutation path; no second mutation mechanism.

## 5. Non-goals

- No update/overlay intents in the first cut; spawn and despawn only.
- No cross-world intents.
- No relaxation of processor purity: emitting an intent is declaring data,
  not performing an effect.

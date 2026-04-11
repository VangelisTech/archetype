# Broker `__global__` queue leaks every world lifecycle command

**Date:** 2026-04-11
**Severity:** Medium (memory + observability bug, no data corruption)
**Area:** `app/broker.py`, `app/command_service.py`, `api/routes/worlds.py`
**Discovered on branch:** `claude/find-bug-document-issue-uLZEa`

## TL;DR

`POST /worlds`, `DELETE /worlds/{id}`, and `POST /worlds/{id}/fork` enqueue a
command into the broker under the synthetic key `"__global__"` and then apply
it directly via `apply_world_lifecycle()`. The command is **never dequeued or
ack'd**, so every world lifecycle call permanently leaks one entry into the
broker's `_queues["__global__"]` heap and the `_pending` dict. Over the
lifetime of an `archetype serve` process this grows without bound.

The leak also corrupts the broker's global pending counter and breaks the only
escape hatch — calling `drain_and_apply("__global__", tick)` raises
`ValueError: badly formed hexadecimal UUID string` because
`CommandService.drain_and_apply` unconditionally parses `world_id` as a UUID.

## Reproduction

```python
import asyncio
from archetype.app.broker import CommandBroker
from archetype.app.command_service import CommandService
from archetype.app.world_service import WorldService
from archetype.app.storage_service import StorageService
from archetype.app.models import Command, CommandType
from archetype.app.auth.models import ActorCtx
from uuid_utils import uuid7

async def main():
    storage = StorageService()
    broker = CommandBroker()
    ws = WorldService(storage_service=storage, broker=broker)
    cs = CommandService(broker=broker, world_service=ws)
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    # Mirror the FastAPI route flow: enqueue under "__global__", apply directly.
    for i in range(5):
        cmd = Command(
            type=CommandType.CREATE_WORLD,
            tick=0,
            payload={
                "config": {"name": f"w{i}"},
                "storage_uri": "./_tmp_archetype_test",
                "namespace": "archetypes",
            },
        )
        await cs.submit("__global__", cmd, ctx)
        await cs.apply_world_lifecycle(cmd)

    print("global queue pending:", await broker.get_pending_count("__global__"))
    print("total broker pending:", await broker.get_pending_count())
    print("_queues[__global__] size:", len(broker._queues["__global__"]))
    print("_pending dict size:    ", len(broker._pending))

asyncio.run(main())
```

Output:

```
global queue pending: 5
total broker pending: 5
_queues[__global__] size: 5
_pending dict size:     5
```

Five "active" commands remain in the broker after every world has already been
created. There is no API surface that drains them.

Trying to drain manually fails:

```python
await cs.drain_and_apply("__global__", tick=0)
# ValueError: badly formed hexadecimal UUID string
```

…because `CommandService.drain_and_apply` does
`world = self._world_service.get_world(UUID(str(world_id)))` at
`src/archetype/app/command_service.py:81`.

## Root cause

Three FastAPI routes (`src/archetype/api/routes/worlds.py:39,91,111`) each use
the pattern:

```python
await cs.submit("__global__", cmd, ctx)             # ① RBAC + enqueue
world = await cs.apply_world_lifecycle(cmd)         # ② immediate apply
```

Step ① is intentional — it routes the command through `guardrail_allow` and
records it in `_history["__global__"]` for audit. Step ② intentionally
sidesteps the tick-scheduled drain because lifecycle commands must apply
synchronously to return the new `WorldResponse` to the client. The bug is that
nothing closes the loop: the command is never popped from `_queues["__global__"]`
nor removed from `_pending`. Cleanup only happens inside
`CommandBroker.dequeue` / `dequeue_due` / `ack` / `clear`, none of which are
called for the synthetic global key.

`CommandBroker.enqueue` (`src/archetype/app/broker.py:74-87`) appends to all
three structures:

```python
heapq.heappush(self._queues[key], cmd)
self._pending[cmd.id] = cmd
self._history.setdefault(key, []).append(cmd)
```

`_history` growth is by-design (audit). `_queues[key]` and `_pending` growth is
the leak.

## Impact

1. **Unbounded memory growth.** Every `create_world`, `destroy_world`, and
   `fork_world` call adds two persistent references (one in `_pending`, one in
   the `__global__` heap). For long-running `archetype serve` processes that
   churn worlds — exactly the benchmarking / MCTS workflow Archetype is built
   for — this accumulates indefinitely. Each leaked `Command` retains its
   `payload` dict, including any forked-world config the caller passed in.

2. **Broken global pending counter.** `CommandBroker.get_pending_count()`
   without a `world_id` returns `len(self._pending)`
   (`src/archetype/app/broker.py:185-190`). Every leaked lifecycle command
   inflates the count, so any future operator dashboard, health check, or
   debug log that uses the global counter will report monotonically increasing
   "pending" work that does not exist.

3. **`peek` / history pollution under the synthetic key.** `peek("__global__")`
   returns the leaked commands as if they were pending work — confusing for
   anyone debugging via the broker API. `_history["__global__"]` is the only
   place these are *meant* to live, but right now both `_queues["__global__"]`
   and `_history["__global__"]` are growing in lockstep.

4. **No escape hatch.** Calling `clear("__global__")` works but is destructive
   to audit history. Calling `drain_and_apply("__global__", …)` crashes on
   UUID parsing before it can pop anything (see above). There is currently no
   non-destructive way to reclaim the leaked memory.

The bug does not corrupt persistent state — `WorldRegistry`, LanceDB stores,
and the worlds themselves are unaffected. It is purely an in-process leak.

## Suggested fix (non-prescriptive)

The minimum-touch fix is to ack the command immediately after a successful
synchronous apply in the route layer. Either of these works:

**Option A — ack at the route boundary** (smallest blast radius):

```python
# src/archetype/api/routes/worlds.py
await cs.submit("__global__", cmd, ctx)
world = await cs.apply_world_lifecycle(cmd)
await cs._broker.ack([cmd.id])      # but this still leaves the heap entry
```

This fixes `_pending` but not `_queues["__global__"]`, since `ack()` only
clears `_pending`. So this option is insufficient on its own.

**Option B — give `CommandService` a public "apply now and drain" helper**
(preferred):

```python
# command_service.py
async def submit_and_apply_lifecycle(self, cmd: Command, ctx: ActorCtx) -> iWorld | None:
    await self._broker.enqueue("__global__", cmd, ctx)   # RBAC + history
    try:
        return await self.apply_world_lifecycle(cmd)
    finally:
        await self._broker.discard("__global__", cmd.id)  # new method
```

…and add a `CommandBroker.discard(world_id, cmd_id)` that pops a specific id
from both `_queues[key]` (rebuilding the heap with `heapq.heapify`) and
`_pending`, leaving `_history` intact. The three world routes then call this
single helper and the synthetic-queue invariant ("__global__ is for audit
only") is enforced in one place.

**Option C — bypass the heap entirely.** Add an `audit_only=True` flag to
`CommandBroker.enqueue` that records into `_history` but skips `_queues` and
`_pending`. Cleanest semantically; slightly larger diff.

Whichever path is chosen, the fix must also either:

- Fix `CommandService.drain_and_apply` so passing `"__global__"` doesn't crash
  on UUID parsing (defensive — even with the leak fixed, an operator might try
  this). A `try/except ValueError` around the UUID parse, or a guard on the
  string `"__global__"`, is enough.
- **Or** delete the synthetic-key path entirely and require lifecycle commands
  to flow through a dedicated `LifecycleBroker` / first-class control plane.

## Test plan

A regression test is straightforward and belongs in
`tests/api/test_routes.py` or `tests/integration/test_command_flow.py`:

```python
async def test_world_lifecycle_does_not_leak_broker_state(client, container):
    for i in range(10):
        client.post("/worlds", json={"name": f"leak_{i}", ...})

    # The synthetic global queue should not retain applied lifecycle commands.
    assert len(container.broker._queues.get("__global__", [])) == 0
    assert await container.broker.get_pending_count() == 0

    # History is still recorded for audit.
    history = await container.broker.get_history("__global__", limit=100)
    assert len(history) == 10
```

The same shape applies to `DELETE /worlds/{id}` and `POST /worlds/{id}/fork`.

## Why this slipped through

- All existing broker tests (`tests/app/test_broker_extended.py`,
  `tests/integration/test_command_flow.py`) call
  `get_pending_count(world_id)` with a real UUID, so the per-world view is
  always 0 and the leak is invisible.
- `tests/api/test_routes.py::test_get_pending` only checks the per-world
  endpoint (`/worlds/{id}/commands/pending`), which reads
  `_queues[real_uuid]` and is unaffected.
- The leaked entries are tiny (`~1 KB` each), so a short test run never trips
  any memory ceiling. The bug surfaces only under sustained operation — which
  is exactly the meta-goal workload (`spawn_world()` benchmarking, MCTS).

## Acceptance criteria

- [ ] After N world create / destroy / fork operations, `len(broker._pending)`
      and `len(broker._queues["__global__"])` are both `0`.
- [ ] `broker.get_history("__global__")` still returns all N audit entries.
- [ ] `CommandBroker.get_pending_count()` (no `world_id`) reflects only
      genuinely pending tick-scheduled commands.
- [ ] `drain_and_apply` either handles `"__global__"` gracefully or the
      synthetic key is removed entirely.
- [ ] New regression test covers create / destroy / fork.

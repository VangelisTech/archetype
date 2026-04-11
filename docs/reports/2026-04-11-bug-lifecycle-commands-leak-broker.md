# Bug Report: World-lifecycle commands leak forever into the broker's `__global__` queue and `_pending` dict

**Date:** 2026-04-11
**Severity:** High (unbounded memory growth on every world create/destroy/fork; broker history bloats indefinitely)
**Affects:** `archetype.api.routes.worlds` (REST handlers for `CREATE_WORLD`, `DESTROY_WORLD`, `FORK_WORLD`) + `archetype.app.command_service.CommandService.apply_world_lifecycle` — every server process that exposes the public REST/CLI surface
**Discovered by:** Overnight bug hunt

## Summary

The REST handlers for `POST /worlds`, `DELETE /worlds/{id}`, and `POST /worlds/{id}/fork` follow the pattern:

```python
await cs.submit("__global__", cmd, ctx)
await cs.apply_world_lifecycle(cmd)
```

`submit` enqueues the command into `broker._queues["__global__"]` and `broker._pending`. `apply_world_lifecycle` runs the side effect (creating, destroying, or forking the world) **directly** — it never goes through `drain_and_apply`, so the broker's `dequeue_due` and `ack` are never called. Nothing in the codebase ever drains the `"__global__"` pseudo-world queue. Every lifecycle operation leaves one zombie command in `_pending` and one in `_queues["__global__"]` forever, plus a permanent entry in `_history["__global__"]`.

In a long-running `archetype serve` process — which the project documents as the *only* deployment model (`LEARNINGS.md` "Single Process, Single Event Loop", Apr 2026) — every world create/destroy/fork is a slow leak. A server that handles 10k world creations over its lifetime ends up with 10k zombie `Command` objects in memory, indexed by both their UUID and their (forever-growing) priority queue. The audit history at `broker.get_history("__global__", limit=100)` shows exclusively zombies; no real lifecycle audit trail can survive past 100 ops.

## Impact

1. **Unbounded memory leak on `archetype serve`.** Every CREATE_WORLD, DESTROY_WORLD, and FORK_WORLD call adds one `Command` to `broker._pending`, one to `broker._queues["__global__"]`, and one to `broker._history["__global__"]`. None are ever removed. The leak is permanent for the lifetime of the server process. For a workload that creates/destroys many forks (MCTS rollouts, scenario sweeps, ensemble runs — all explicitly motivated in `AGENTS.md` and `LEARNINGS.md`), this grows to a serious memory footprint quickly.
2. **`broker.get_pending_count()` (no world_id) over-reports by the leak count.** The broker exposes `get_pending_count(world_id=None)` which returns `len(self._pending)` (`broker.py:185-190`). After N lifecycle ops, this returns N + true-pending — making the metric useless for monitoring real backpressure.
3. **Audit history is poisoned.** `broker.get_history("__global__", limit=100)` is supposed to give operators a record of world lifecycle events. But every entry in it is a leaked, never-acked zombie. There's no way to distinguish "lifecycle command that ran successfully" from "lifecycle command that's still in flight" — they both look identical because nothing ever calls `ack`.
4. **`broker._max_dequeue` cap (`50_000`) becomes a soft DoS.** `broker.dequeue` and `dequeue_due` both cap at `_max_dequeue`. After 50k+ lifecycle ops, the `__global__` queue exceeds the cap. Any future code that tried to drain it would still leak (because the cap silently truncates).
5. **Lifecycle audit trail is broken end-to-end.** The pattern advertised in `AGENTS.md` ("All mutations are RBAC-gated through the CommandBroker; … audit history") implies that lifecycle commands are tracked through the broker. They are submitted to the broker, but the broker's view of them is "permanently pending, never applied" — the opposite of an audit trail.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 1872b4f, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: lifecycle commands leak into broker._pending and the __global__
queue and are never acked.

Mirrors the api/routes/worlds.py CREATE_WORLD flow:
    await cs.submit("__global__", cmd, ctx)
    await cs.apply_world_lifecycle(cmd)
"""
import asyncio
import tempfile

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        container = ServiceContainer()
        try:
            ctx = ActorCtx(id=uuid7(), roles={"admin"})
            broker = container.broker
            print(f"initial _pending = {len(broker._pending)}")
            print(f"initial _queues  = {dict((k, len(v)) for k, v in broker._queues.items())}")

            for i in range(5):
                cmd = Command(
                    type=CommandType.CREATE_WORLD,
                    tick=0,
                    payload={
                        "config": {"name": f"world-{i}"},
                        "storage_uri": tmp,
                        "namespace": "archetypes",
                    },
                )
                await container.command_service.submit("__global__", cmd, ctx)
                await container.command_service.apply_world_lifecycle(cmd)

            print(f"\nafter 5 CREATE_WORLD:")
            print(f"  _pending     = {len(broker._pending)}")
            print(f"  _queues      = {dict((k, len(v)) for k, v in broker._queues.items())}")
            history = await broker.get_history("__global__", limit=100)
            print(f"  history len  = {len(history)}")

            assert len(broker._pending) == 0, (
                f"BUG: lifecycle commands leaked into _pending "
                f"({len(broker._pending)} zombies)"
            )
        finally:
            await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
initial _pending = 0
initial _queues  = {}

after 5 CREATE_WORLD:
  _pending     = 5
  _queues      = {'__global__': 5}
  history len  = 5
AssertionError: BUG: lifecycle commands leaked into _pending (5 zombies)
```

`_pending` and `_queues["__global__"]` both grow by exactly one per CREATE_WORLD call. None of the side-effect-completing `apply_world_lifecycle` calls reach back into the broker to dequeue or ack.

### Baseline (proves the leak is specific to the lifecycle dispatch path)

Entity-level commands routed through `SimulationService.step` drain and ack correctly via `drain_and_apply`:

```python
info = await container.world_service.create_world(
    WorldConfig(name="baseline"), StorageConfig(uri=tmp)
)
ctx = ActorCtx(id=uuid7(), roles={"admin"})

# Submit 5 SPAWN commands to the real world.
for i in range(5):
    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(info.world_id, cmd, ctx)

print(f"after submit: _pending={len(broker._pending)}")  # 5
await container.simulation_service.step(info.world_id)
print(f"after step:   _pending={len(broker._pending)}")  # 0
# OK (baseline): entity-level commands drain and ack correctly.
```

The contrast is sharp: entity-level commands trigger `drain_and_apply` → `dequeue_due` → `apply` → `ack`. Lifecycle commands trigger `submit` → `apply_world_lifecycle` directly, with no equivalent of `dequeue_due` and no `ack`.

## Root cause

Two cooperating files. The REST handlers in `src/archetype/api/routes/worlds.py:24-47` (`POST /worlds`):

```python
@router.post("", response_model=WorldResponse)
async def create_world(
    req: CreateWorldRequest,
    cs: CommandService = Depends(get_command_service),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    cmd = Command(
        type=CommandType.CREATE_WORLD,
        tick=0,
        payload={
            "config": {"name": req.name},
            "storage_uri": req.storage_uri,
            "namespace": req.namespace,
        },
    )
    await cs.submit("__global__", cmd, ctx)
    # Apply immediately — world lifecycle commands are not tick-scheduled.
    world = await cs.apply_world_lifecycle(cmd)
    return WorldResponse(...)
```

And the same shape in `worlds.py:80-93` (`DELETE /worlds/{id}`) and `worlds.py:96-122` (`POST /worlds/{id}/fork`).

The dispatcher `src/archetype/app/command_service.py:109-145`:

```python
async def apply_world_lifecycle(self, cmd: Command) -> iWorld | None:
    """Dispatch a world-level lifecycle command (create/destroy/fork).

    These commands operate on ``WorldService`` directly and don't require
    a pre-existing world instance, so they are separated from the
    per-world ``apply()`` path.

    Returns the created/forked world for CREATE and FORK, None for DESTROY.
    """
    payload = cmd.payload
    match cmd.type:
        case CommandType.CREATE_WORLD:
            ...
            return await self._world_service.create_world(world_config, storage_config)
        case CommandType.DESTROY_WORLD:
            ...
            self._world_service.remove_world(target_id)
            return None
        case CommandType.FORK_WORLD:
            ...
            return await self._world_service.fork_world(source_id, fork_name, StorageConfig())
        case _:
            raise ValueError(f"apply_world_lifecycle does not handle {cmd.type.value}")
```

`apply_world_lifecycle` runs the side effect against `WorldService` directly. It does **not**:

- Call `self._broker.dequeue_due("__global__", ...)` to remove the command from the queue.
- Call `self._broker.ack([cmd.id])` to remove the command from `_pending`.
- Call any equivalent of `drain_and_apply`.

`drain_and_apply` (`command_service.py:67-94`) is the only place in the codebase that calls `broker.ack`. It is called exclusively from `SimulationService.step` and `SimulationService.run` for *real* world ids. There is no driver loop for `"__global__"`.

Trace for the MRE (one CREATE_WORLD):

1. REST handler builds `cmd = Command(type=CREATE_WORLD, payload={...})`.
2. `cs.submit("__global__", cmd, ctx)` → `broker.enqueue("__global__", cmd, ctx)`:
   - `guardrail_allow(cmd, ctx)` (RBAC) — passes for admin.
   - `_queues["__global__"].append(cmd)` (heappush).
   - `_pending[cmd.id] = cmd`.
   - `_history["__global__"].append(cmd)`.
3. `cs.apply_world_lifecycle(cmd)`:
   - matches `CREATE_WORLD`, calls `world_service.create_world(...)` → returns the new world.
4. REST handler returns `WorldResponse(...)`.
5. **No code touches `broker._pending` or `broker._queues["__global__"]` again.** The command sits there forever.

Repeat for every CREATE/DESTROY/FORK request. The zombies accumulate one per call.

## Why existing tests miss this

`grep -rn "apply_world_lifecycle" tests/` returns **zero matches**. There is no test that exercises the `submit + apply_world_lifecycle` pattern, neither at the unit-test level nor at the REST-route level.

The closest tests are:

- `tests/api/test_routes.py::test_create_world` (line 51) — calls `POST /worlds`, asserts `200` and the response shape. Does not check `broker._pending` or any pending-count endpoint after the create.
- `tests/api/test_routes.py::test_fork_world` (line 102) — same shape, only checks the response.
- `tests/api/test_routes.py::test_get_pending` (line 217) — creates a world, then checks `GET /worlds/{world_id}/commands/pending` and asserts `pending_count == 0`. **This test actually leaks but doesn't notice**, because `get_pending_count(world_id)` is the *per-world* count — the leaked CREATE_WORLD command is queued under `"__global__"`, not under the new world_id, so the per-world count is correctly 0. The global `_pending` dict (which is what `get_pending_count(None)` returns) has 1 zombie that no test checks.
- `tests/api/test_routes.py::test_get_pending_after_submit` (line 228) — same setup, then submits a SPAWN command and asserts the per-world count goes to 1. Again, the `__global__` zombie from the create is invisible.
- `tests/app/test_broker_extended.py` has full coverage of `enqueue`, `dequeue`, `ack`, and `get_pending_count`, but **only** for entity-level commands against real world ids. There is no test that submits to `"__global__"` and watches what happens.

The pattern is: every test that creates a world via the REST flow leaks at least one zombie command, but no test ever asserts on the broker's global `_pending` dict, so the leak is invisible.

## Suggested fixes

**Fix A — make `apply_world_lifecycle` ack the command after applying.** Minimal change, lands entirely in `app/`:

```diff
 async def apply_world_lifecycle(self, cmd: Command) -> iWorld | None:
     """Dispatch a world-level lifecycle command (create/destroy/fork)."""
     payload = cmd.payload
+    result: iWorld | None = None
     match cmd.type:
         case CommandType.CREATE_WORLD:
             ...
-            return await self._world_service.create_world(world_config, storage_config)
+            result = await self._world_service.create_world(world_config, storage_config)
         case CommandType.DESTROY_WORLD:
             ...
             self._world_service.remove_world(target_id)
-            return None
+            result = None
         case CommandType.FORK_WORLD:
             ...
-            return await self._world_service.fork_world(source_id, fork_name, StorageConfig())
+            result = await self._world_service.fork_world(source_id, fork_name, StorageConfig())
         case _:
             raise ValueError(f"apply_world_lifecycle does not handle {cmd.type.value}")
+
+    # Drain the zombie from the broker so it doesn't leak forever.
+    await self._broker.dequeue_due("__global__", cmd.tick)
+    await self._broker.ack([cmd.id])
+    return result
```

This is the smallest correct fix. The only subtlety: `dequeue_due` removes commands by `tick <= cmd.tick`, so calling it with `cmd.tick` will drain *every* lifecycle command at or below that tick. Since lifecycle commands are always `tick=0` and they're always processed in submission order (the heappop), this is safe. `ack` is then a no-op for the just-dequeued command (it was already removed from `_pending` by `dequeue_due`), but it's idempotent and harmless.

**Fix B — collapse `submit + apply_world_lifecycle` into a single `submit_lifecycle` method that does the right thing internally.** Cleaner API but slightly larger surface change:

```python
# command_service.py
async def submit_lifecycle(self, cmd: Command, ctx: ActorCtx) -> iWorld | None:
    """Submit and immediately apply a world-lifecycle command. Unlike
    entity-level commands, lifecycle commands run synchronously (not
    tick-scheduled) so we don't need a separate drain phase.
    """
    await self._broker.enqueue("__global__", cmd, ctx)
    try:
        return await self.apply_world_lifecycle(cmd)
    finally:
        # Always drain — even on failure — so the queue doesn't fill with zombies.
        await self._broker.dequeue_due("__global__", cmd.tick)
        await self._broker.ack([cmd.id])
```

The REST handlers then become:

```diff
-await cs.submit("__global__", cmd, ctx)
-world = await cs.apply_world_lifecycle(cmd)
+world = await cs.submit_lifecycle(cmd, ctx)
```

Fix B is the right long-term shape (one call instead of two, harder to misuse). Fix A is the smallest patch that closes the leak today. I'd land Fix A as the urgent fix and migrate to Fix B as a follow-up.

**Fix C — bound `_pending` and `_history` with TTLs / max-size eviction.** Defence-in-depth: even after Fix A, any future code path that submits without acking will leak. A bounded LRU on `_pending` and `_history` would catch these regressions before they become production memory leaks. This is a follow-up; the right urgent fix is A.

## Suggested regression tests

Add to `tests/app/test_services.py` (or a new `tests/app/test_command_service_lifecycle.py`):

```python
@pytest.mark.asyncio
async def test_create_world_does_not_leak_into_broker_pending(tmp_path):
    """Regression: CREATE_WORLD via the lifecycle dispatch path must not
    leave a zombie command in broker._pending or broker._queues."""
    container = ServiceContainer()
    try:
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        broker = container.broker
        baseline_pending = len(broker._pending)

        cmd = Command(
            type=CommandType.CREATE_WORLD,
            tick=0,
            payload={
                "config": {"name": "regression"},
                "storage_uri": str(tmp_path),
                "namespace": "archetypes",
            },
        )
        await container.command_service.submit("__global__", cmd, ctx)
        await container.command_service.apply_world_lifecycle(cmd)

        assert len(broker._pending) == baseline_pending, (
            f"CREATE_WORLD leaked into _pending: "
            f"baseline={baseline_pending}, after={len(broker._pending)}"
        )
        assert len(broker._queues.get("__global__", [])) == 0, (
            f"CREATE_WORLD leaked into __global__ queue: "
            f"{len(broker._queues['__global__'])} zombie(s)"
        )
    finally:
        await container.shutdown()


@pytest.mark.asyncio
async def test_lifecycle_round_trip_does_not_leak(tmp_path):
    """Regression: 100 CREATE/DESTROY pairs must not leave any zombies."""
    container = ServiceContainer()
    try:
        ctx = ActorCtx(id=uuid7(), roles={"admin"})
        broker = container.broker

        for i in range(100):
            create = Command(
                type=CommandType.CREATE_WORLD,
                tick=0,
                payload={
                    "config": {"name": f"w{i}"},
                    "storage_uri": str(tmp_path),
                    "namespace": "archetypes",
                },
            )
            await container.command_service.submit("__global__", create, ctx)
            world = await container.command_service.apply_world_lifecycle(create)

            destroy = Command(
                type=CommandType.DESTROY_WORLD,
                tick=0,
                payload={"world_id": str(world.world_id)},
            )
            await container.command_service.submit("__global__", destroy, ctx)
            await container.command_service.apply_world_lifecycle(destroy)

        assert len(broker._pending) == 0, (
            f"100 CREATE/DESTROY pairs leaked {len(broker._pending)} zombies"
        )
        assert len(broker._queues.get("__global__", [])) == 0
    finally:
        await container.shutdown()
```

The first test fails on `main` with `_pending: baseline=0, after=1`. The second fails with `100 CREATE/DESTROY pairs leaked 200 zombies`.

A REST-level test belongs in `tests/api/test_routes.py`:

```python
def test_create_world_endpoint_does_not_leak_pending(self, client, tmp_path):
    """The /worlds POST endpoint must not leave zombies in the broker."""
    from archetype.api.deps import get_command_service
    cs = client.app.dependency_overrides.get(get_command_service, get_command_service)()

    baseline = len(cs._broker._pending)
    resp = client.post(
        "/worlds",
        json={"name": "leak_test", "storage_uri": str(tmp_path / "store")},
    )
    assert resp.status_code == 200
    assert len(cs._broker._pending) == baseline, "REST CREATE_WORLD leaked"
```

## Notes / scope

- Affects `src/archetype/app/command_service.py:109-145` and the three REST handlers in `src/archetype/api/routes/worlds.py:24-47, :80-93, :96-122`. Both files are in `app/`/`api/`, not `core/`, so the fix can land directly without `core/` approval.
- Distinct from the seven other already-filed bugs:
  - The five `*-spawn-despawn-*` / `add-components-pending-spawn` / `sync-active-signatures-drops-despawn` reports are all in `core/`.
  - `simulation-service-run-discards-runconfig` is `SimulationService.run` discarding the user's `RunConfig`.
  - `update-command-silently-noops` and `remove-component-strings-noop` are about `command_service.apply` mis-routing entity-level commands.
  - This bug is about `command_service.apply_world_lifecycle` running side effects without acking the broker. The earlier two are silent no-ops; this one is a memory leak.
- The leak is *also* present for the CLI flow (`POST /worlds` is what `archetype world create` calls under the hood), and for any direct in-process caller of `submit + apply_world_lifecycle`. Anything that goes through the documented "broker mediates lifecycle" pattern from `AGENTS.md` triggers the leak.
- One follow-up worth a separate hunt: the broker's `_history` dict has no eviction policy whatsoever — `_history.setdefault(key, []).append(cmd)` (`broker.py:81`) grows monotonically. Even after Fix A closes the lifecycle leak, any high-throughput command stream eventually fills `_history` to the moon. Out of scope for this report.
- The `__global__` magic string is itself a smell — it's a "use the command broker as if there were a 0th world" pattern that doesn't fit the rest of the broker's per-world model. After Fix B, the REST handlers shouldn't need to know about `__global__` at all; the lifecycle path is just `submit_lifecycle(cmd, ctx)` and the broker manages the queue key internally.

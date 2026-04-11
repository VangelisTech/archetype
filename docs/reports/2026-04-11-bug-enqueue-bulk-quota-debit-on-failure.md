# Bug Report: `CommandBroker.enqueue_bulk` debits quota for commands it never enqueues when RBAC fails partway through

**Date:** 2026-04-11
**Severity:** Medium-High (silent quota loss + RBAC bypass via bulk submission, on a documented "all-or-nothing" public API)
**Affects:** `archetype.app.broker.CommandBroker.enqueue_bulk` + `archetype.app.auth.guard.guardrail_allow` — any caller using bulk command submission, including `CommandService.submit_batch`
**Discovered by:** Overnight bug hunt

## Summary

`CommandBroker.enqueue_bulk` (`broker.py:89-111`) advertises an **all-or-nothing** guarantee: "validates all commands before enqueueing any". The validation phase loops over the bulk and calls `guardrail_allow(cmd, ctx)` for each command. But `guardrail_allow` (`auth/guard.py:77-113`) does **not** just check — it *mutates* the global per-tick counter and the daily token budget at lines 104 and 113. If a later command in the bulk fails RBAC, the earlier commands have already debited those counters, the bulk raises `PermissionError`, **zero** commands are enqueued, and the actor's quota has been silently consumed for nothing.

The all-or-nothing guarantee is honoured for the *queue side* (no partial enqueue), but it's silently violated for the *quota side* (partial debit). The two views diverge: `broker._queues` and `broker._pending` say "0 commands submitted", `_tick_counters[actor_id]` says "N commands consumed".

## Impact

1. **Silent quota DoS via bulk submission.** A misbehaving or malicious caller can submit a bulk of `[allowed, allowed, ..., allowed, forbidden]` to burn through the actor's per-tick quota and daily token budget without ever enqueueing a single command. With the default `MAX_CMDS_PER_TICK = 500` (`auth/guard.py:44`), one bulk of 499 SPAWNs followed by one ADD_PROCESSOR — submitted by a `player` role that lacks `add_processor` — exhausts the actor's per-tick quota in one rejected call. Subsequent legitimate commands (single SPAWNs, smaller bulks) raise "exceeded per-tick quota" until `reset_tick_counters` runs.
2. **Silent token budget DoS.** Same shape, larger blast radius. `MAX_TOKENS_PER_DAY = 200_000` (`auth/guard.py:45`); the daily token pool is *not* reset by `reset_tick_counters` — only by `reset_daily_tokens()` which has no scheduled caller in production. A bulk of 24,999 ADD_COMPONENTs (~ 200,000 tokens at 8/cmd) followed by one forbidden cmd would burn the actor's entire daily budget on a single rejected bulk. The actor cannot recover until midnight (or whatever the daily-reset scheduler does — there isn't one yet).
3. **Audit history shows zero applied commands but the actor's "fair share" of cluster compute is gone.** Operators looking at `broker.get_history(...)` see no entries (the failed bulk wasn't enqueued), but `_tick_counters[actor_id]` and `_daily_tokens[actor_id]` say the actor has been consuming. Operator's mental model: "actor did nothing"; reality: "actor consumed N quota slots before being denied".
4. **Race window for the priority-queue invariant.** During the validation phase, between commands `i` and `i+1`, the world's tick counter for that actor *moves forward* without the queue moving forward. If a tick boundary happens to land in that window (`reset_tick_counters` is called from `simulation_service.step` line 55), the actor's counter resets to 0 — but the bulk's prior debits are *gone forever*, never reflected in the queue, never refundable. Quotas become non-deterministic relative to tick boundaries.
5. **Discovery is ironic and easy.** The MRE for this report submits four commands as a `player`. The third is `ADD_PROCESSOR` (forbidden). The bulk raises after debiting quota for the first two. Two of the *previous* bug reports filed today (`update-command-silently-noops`, `remove-component-strings-noop`, `add-processor-missing-await`) all flagged "quota-exhaustion attack vector" as a side note. This bug is the *general form* of that vector — anyone who can submit bulk with a forbidden trailing command can burn quota at will.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit 5c9edff, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: enqueue_bulk debits quota for commands that are never enqueued."""
import asyncio

from uuid_utils import uuid7

from archetype.app.auth.guard import _daily_tokens, _tick_counters, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType


async def main() -> None:
    reset_tick_counters()
    _daily_tokens.clear()

    broker = CommandBroker()
    actor_id = uuid7()
    # player role: can spawn/despawn/update/message/custom; CANNOT add_processor
    ctx = ActorCtx(id=actor_id, roles={"player"})

    bulk = [
        Command(type=CommandType.SPAWN, payload={"components": []}),
        Command(type=CommandType.SPAWN, payload={"components": []}),
        Command(type=CommandType.ADD_PROCESSOR, payload={}),  # forbidden
        Command(type=CommandType.SPAWN, payload={"components": []}),
    ]

    print(f"before: tick_counter={_tick_counters.get(actor_id, 0)} daily_tokens={_daily_tokens.get(actor_id, 0)}")
    try:
        await broker.enqueue_bulk("w1", bulk, ctx)
    except PermissionError as e:
        print(f"raised PermissionError: {e}")

    print(f"after:  tick_counter={_tick_counters.get(actor_id, 0)} daily_tokens={_daily_tokens.get(actor_id, 0)}")
    print(f"queue:  {len(broker._queues.get('w1', []))}")
    print(f"pending:{len(broker._pending)}")

    assert len(broker._queues.get("w1", [])) == 0
    assert _tick_counters.get(actor_id, 0) == 0, (
        f"BUG: _tick_counters debited {_tick_counters[actor_id]} for a bulk that enqueued 0 commands"
    )


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
before: tick_counter=0 daily_tokens=0
raised PermissionError: Actor 019d7eb5-d13c-7860-8e0c-4e11a9373b50 with roles {'player'} cannot execute 'add_processor'
after:  tick_counter=2 daily_tokens=20
queue:  0
pending: 0
AssertionError: BUG: _tick_counters debited 2 for a bulk that enqueued 0 commands
```

The bulk enqueued **zero** commands, but `_tick_counters[actor_id] == 2` and `_daily_tokens[actor_id] == 20`. The actor lost two quota slots and 20 tokens for a request that returned an error.

### Baseline (proves the bug is scoped to "validation fails partway through")

A successful bulk debits quota for exactly the commands enqueued:

```python
bulk = [
    Command(type=CommandType.SPAWN, payload={"components": []}),
    Command(type=CommandType.SPAWN, payload={"components": []}),
    Command(type=CommandType.SPAWN, payload={"components": []}),
]
await broker.enqueue_bulk("w1", bulk, ctx)
# after success: tick_counter=3 queue=3
# OK (baseline): successful bulk debits quota for exactly the commands enqueued.
```

The bug only fires when validation aborts mid-loop. Successful bulks behave correctly.

## Root cause

`src/archetype/app/broker.py:89-111`:

```python
async def enqueue_bulk(
    self,
    world_id: str | UUID,
    cmds: list[Command],
    ctx: ActorCtx | None = None,
) -> None:
    """
    Enqueue multiple commands for a specific world.
    All-or-nothing: validates all commands before enqueueing any.
    """
    if ctx is not None:
        for cmd in cmds:
            guardrail_allow(cmd, ctx)              # <-- mutates global counters

    async with self._lock:
        key = str(world_id)
        if key not in self._queues:
            self._queues[key] = []

        for cmd in cmds:
            heapq.heappush(self._queues[key], cmd)
            self._pending[cmd.id] = cmd
            self._history.setdefault(key, []).append(cmd)
```

`src/archetype/app/auth/guard.py:77-113`:

```python
def guardrail_allow(cmd: Command, ctx: ActorCtx) -> None:
    """
    Check RBAC permissions and quotas. Raises PermissionError if denied.
    """
    # 1. Permission check
    cmd_type = cmd.type.value
    allowed = False
    for role in ctx.roles:
        perms = ROLE_PERMS.get(role, set())
        if "*" in perms or cmd_type in perms:
            allowed = True
            break

    if not allowed:
        raise PermissionError(...)

    # 2. Per-tick quota
    current_count = _tick_counters.get(ctx.id, 0)
    if current_count >= MAX_CMDS_PER_TICK:
        raise PermissionError(...)
    _tick_counters[ctx.id] = current_count + 1   # <-- mutation, not pure check

    # 3. Daily token budget
    cost = estimate_token_cost(cmd)
    current_tokens = _daily_tokens.get(ctx.id, 0)
    if current_tokens + cost > MAX_TOKENS_PER_DAY:
        raise PermissionError(...)
    _daily_tokens[ctx.id] = current_tokens + cost  # <-- mutation, not pure check
```

Trace for the MRE:

1. Caller submits `enqueue_bulk("w1", [SPAWN, SPAWN, ADD_PROCESSOR, SPAWN], player_ctx)`.
2. `for cmd in cmds:` (line 100) iterates:
   - `cmd[0] = SPAWN`. RBAC: `player ⊇ {spawn}` → allowed. `_tick_counters[ctx.id] = 1`. `_daily_tokens[ctx.id] = 10`.
   - `cmd[1] = SPAWN`. Same path. `_tick_counters[ctx.id] = 2`. `_daily_tokens[ctx.id] = 20`.
   - `cmd[2] = ADD_PROCESSOR`. RBAC: `player ⊉ {add_processor}` → `raise PermissionError`.
3. The exception propagates out of the for loop, out of `enqueue_bulk`, out of the `async with self._lock:` (which was never entered). The `for cmd in cmds:` enqueue loop at line 108 never runs. **Zero commands are enqueued.**
4. The mutations made by guardrail_allow on `cmd[0]` and `cmd[1]` are not rolled back. `_tick_counters[ctx.id] == 2` and `_daily_tokens[ctx.id] == 20` after the function returns.

The fundamental defect: `guardrail_allow` is named like a pure predicate ("allow?"), but it has side effects (debit). The all-or-nothing semantics of `enqueue_bulk` apply only to the queue mutations under the lock, not to the side effects of the validation step. The two phases are inconsistent under failure.

## Why existing tests miss this

`grep -rn "enqueue_bulk" tests/` returns **zero matches**. The bulk submission path is entirely uncovered by the test suite. The closest relevant tests are:

- `tests/app/test_auth.py::TestGuardrails` — exercises `guardrail_allow` directly with one command at a time. Never tests the bulk failure path.
- `tests/app/test_auth.py::test_reset_tick_counters_clears_quota` (line 95) — verifies that `reset_tick_counters` clears `_tick_counters`. Doesn't check the bulk path.
- `tests/app/test_broker_extended.py` — covers `enqueue`, `dequeue`, `dequeue_due`, `ack`, `peek`, `get_history`, `get_pending_count`. **No tests for `enqueue_bulk`.**
- `tests/app/test_services.py::test_submit_batch` would be the natural place to cover this, but no such test exists either.

There is no test in the suite that:

1. Submits a bulk with a partial RBAC failure.
2. Asserts that `_tick_counters[actor_id]` is unchanged after the failed bulk.
3. Asserts that the queue is empty after the failed bulk.

Both 2 and 3 would have to pass for the all-or-nothing guarantee to hold. Today, only 3 passes.

## Suggested fixes

**Fix A — separate validation from accounting; debit only on successful enqueue.** The cleanest fix: add a pure-check function that does RBAC + quota budget *without* mutating, and a separate `commit` step that debits after the queue mutations succeed:

```diff
 # auth/guard.py
+def guardrail_check(cmd: Command, ctx: ActorCtx, projected_count: int = 0, projected_tokens: int = 0) -> int:
+    """Pure check: returns the token cost if allowed, raises PermissionError if not.
+    `projected_count` and `projected_tokens` let bulk callers stack pre-check
+    quota usage across the bulk without committing it to the global state."""
+    cmd_type = cmd.type.value
+    if not any(("*" in ROLE_PERMS.get(r, set()) or cmd_type in ROLE_PERMS.get(r, set())) for r in ctx.roles):
+        raise PermissionError(...)
+
+    if _tick_counters.get(ctx.id, 0) + projected_count >= MAX_CMDS_PER_TICK:
+        raise PermissionError(...)
+
+    cost = estimate_token_cost(cmd)
+    if _daily_tokens.get(ctx.id, 0) + projected_tokens + cost > MAX_TOKENS_PER_DAY:
+        raise PermissionError(...)
+    return cost
+
+
+def guardrail_commit(ctx: ActorCtx, count: int, tokens: int) -> None:
+    """Apply the deltas computed by previous guardrail_check calls."""
+    _tick_counters[ctx.id] = _tick_counters.get(ctx.id, 0) + count
+    _daily_tokens[ctx.id] = _daily_tokens.get(ctx.id, 0) + tokens
```

```diff
 # broker.py
 async def enqueue_bulk(self, world_id, cmds, ctx=None):
     """All-or-nothing: validates all commands before enqueueing any."""
     if ctx is not None:
-        for cmd in cmds:
-            guardrail_allow(cmd, ctx)
+        # Pure check: stacks projected debits across the bulk without
+        # touching global counters until everything is allowed.
+        projected_tokens = 0
+        for i, cmd in enumerate(cmds):
+            projected_tokens += guardrail_check(
+                cmd, ctx, projected_count=i, projected_tokens=projected_tokens
+            )
+        # All commands allowed — commit the debit *now*, before enqueue.
+        guardrail_commit(ctx, len(cmds), projected_tokens)

     async with self._lock:
         ...
```

`enqueue` (single-command) should be migrated to the same pattern for consistency. The change is mechanical.

**Fix B — try/except + rollback in `enqueue_bulk`.** Smaller patch but more fragile:

```diff
 async def enqueue_bulk(self, world_id, cmds, ctx=None):
     if ctx is not None:
-        for cmd in cmds:
-            guardrail_allow(cmd, ctx)
+        debited = 0
+        debited_tokens = 0
+        try:
+            for cmd in cmds:
+                guardrail_allow(cmd, ctx)
+                debited += 1
+                debited_tokens += estimate_token_cost(cmd)
+        except PermissionError:
+            # Roll back partial debits.
+            from archetype.app.auth.guard import _tick_counters, _daily_tokens
+            _tick_counters[ctx.id] = _tick_counters.get(ctx.id, 0) - debited
+            _daily_tokens[ctx.id] = _daily_tokens.get(ctx.id, 0) - debited_tokens
+            raise
     ...
```

Fix B works but reaches into `auth.guard`'s private globals. Fix A is the clean separation of concerns.

I'd recommend **Fix A**.

**Fix C (defence in depth) — add a periodic auditor that warns when `_tick_counters` drifts from `_pending` size for a given actor.** This is monitoring, not a fix. Belongs in a separate observability PR. Doesn't help by itself; useful alongside Fix A.

## Suggested regression tests

Add to `tests/app/test_broker_extended.py`:

```python
@pytest.mark.asyncio
async def test_enqueue_bulk_does_not_debit_quota_on_partial_failure(self):
    """Regression: bulk RBAC failure must not consume quota for the
    earlier commands that passed validation."""
    from archetype.app.auth.guard import _tick_counters, _daily_tokens, reset_tick_counters
    from archetype.app.auth.models import ActorCtx
    from uuid_utils import uuid7

    reset_tick_counters()
    _daily_tokens.clear()

    broker = CommandBroker()
    actor_id = uuid7()
    ctx = ActorCtx(id=actor_id, roles={"player"})  # cannot add_processor

    bulk = [
        Command(type=CommandType.SPAWN, payload={"components": []}),
        Command(type=CommandType.SPAWN, payload={"components": []}),
        Command(type=CommandType.ADD_PROCESSOR, payload={}),
    ]

    with pytest.raises(PermissionError):
        await broker.enqueue_bulk("w1", bulk, ctx)

    # Quota MUST be unchanged on a failed bulk (all-or-nothing).
    assert _tick_counters.get(actor_id, 0) == 0
    assert _daily_tokens.get(actor_id, 0) == 0
    # Queue MUST be empty.
    assert len(broker._queues.get("w1", [])) == 0


@pytest.mark.asyncio
async def test_enqueue_bulk_success_debits_for_all_enqueued(self):
    """Sanity: successful bulk debits exactly len(cmds) and enqueues exactly len(cmds)."""
    from archetype.app.auth.guard import _tick_counters, reset_tick_counters
    from archetype.app.auth.models import ActorCtx
    from uuid_utils import uuid7

    reset_tick_counters()
    broker = CommandBroker()
    actor_id = uuid7()
    ctx = ActorCtx(id=actor_id, roles={"player"})

    bulk = [
        Command(type=CommandType.SPAWN, payload={"components": []}),
        Command(type=CommandType.SPAWN, payload={"components": []}),
        Command(type=CommandType.SPAWN, payload={"components": []}),
    ]
    await broker.enqueue_bulk("w1", bulk, ctx)
    assert _tick_counters[actor_id] == 3
    assert len(broker._queues["w1"]) == 3
```

The first test fails on `main` at `assert _tick_counters.get(actor_id, 0) == 0` because the counter is `2`. The second passes on both `main` and a fixed branch.

## Notes / scope

- Affects `src/archetype/app/broker.py:89-111` and `src/archetype/app/auth/guard.py:77-113`. Both files are in `app/`, not `core/`, so the fix can land directly without `core/` approval.
- Distinct from the ten other already-filed bugs:
  - The five `core/` mutation cache bugs are about world internals.
  - `simulation-service-run-discards-runconfig` is about `RunConfig` substitution.
  - `update-command-silently-noops`, `remove-component-strings-noop`, and `add-processor-missing-await` are about `command_service.apply` arms doing nothing.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is about hook plumbing.
  - This bug is about the *quota accounting* layer being inconsistent with the *queue* layer in the failure path. It's the broker's other half of the reliability story.
- `submit_batch` (`command_service.py:57-65`) calls `broker.enqueue_bulk` directly, so REST callers using `POST /worlds/{id}/commands/bulk` (if/when that endpoint exists) inherit this bug 1:1. There is no such endpoint today, but `tests/app/test_broker_extended.py` and `tests/integration/test_command_flow.py` should still cover the bulk path.
- The same shape exists in the **single-command** `enqueue` path: if `guardrail_allow` succeeds and then `heappush` raises (e.g., a corrupt comparator on a custom Command subclass), the counter has been bumped but the queue is unchanged. Less likely in practice, but the same fix applies.
- The six "quota-exhaustion attack vector" notes scattered across the previous bug reports collapse into this one root cause: any caller that can submit bulk + a forbidden trailing command can burn quota at will. Fix A closes all of them at once.

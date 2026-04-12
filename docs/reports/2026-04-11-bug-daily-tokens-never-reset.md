# Bug Report: `reset_daily_tokens` is defined but never called by any production code path — actors permanently blocked once they exceed `MAX_TOKENS_PER_DAY`

**Date:** 2026-04-11
**Severity:** High (silent permanent denial of service to any actor that submits enough commands; the documented "daily" token budget is actually a per-process-lifetime budget)
**Affects:** `archetype.app.auth.guard` — `_daily_tokens` accumulator + `MAX_TOKENS_PER_DAY` enforcement; impacts every actor on every long-running `archetype serve` process
**Discovered by:** Overnight bug hunt

## Summary

`auth/guard.py` defines a per-actor daily token budget (`MAX_TOKENS_PER_DAY = 200_000`, `auth/guard.py:45`) and tracks usage in a module-global `_daily_tokens` dict (`auth/guard.py:69`). Every call to `guardrail_allow` (the broker's RBAC entry point) credits the actor with `estimate_token_cost(cmd)` against this budget (`auth/guard.py:107-113`). The function `reset_daily_tokens()` (`auth/guard.py:121-123`) clears the dict, with the docstring "Called at day boundary."

But **no production code path ever calls `reset_daily_tokens()`**. `grep -rn "reset_daily_tokens" src/archetype/` returns one match — the function definition itself. There is no scheduler, no cron job, no startup hook, no SimulationService timer, no day-boundary detector. The function exists only as something tests call in `setup`/`teardown` to clear state between cases. In production, `_daily_tokens` accumulates monotonically from process start until process exit. Once an actor crosses 200,000 tokens, every subsequent command they submit raises `PermissionError: Actor X exceeded daily token budget (200000 tokens)` until the server restarts. There is no recovery, no backoff, no warning.

The "daily" in `MAX_TOKENS_PER_DAY` is fictional. The real budget is `MAX_TOKENS_PER_PROCESS_LIFETIME`.

## Impact

1. **Permanent DoS for any active actor on a long-running server.** With `MAX_TOKENS_PER_DAY = 200_000` and `_TOKEN_COSTS["spawn"] = 10`, an actor can submit ~20,000 SPAWN commands across the *entire process lifetime* before being permanently locked out. For a busy multi-agent simulation that submits 100 commands/tick at 1 tick/sec, that's 200 seconds — about 3 minutes — before the actor is silently denied. Restarting the server is the only recovery.
2. **The advertised quota is wrong by a factor of (process uptime in days).** A `archetype serve` process running for 24 hours allows the same 200,000 tokens as one running for 24 weeks. Users seeing "200,000 tokens per day" in `auth/guard.py:45` will reasonably expect the quota to refresh every 24 hours. It doesn't.
3. **Compounds with the filed `enqueue-bulk-quota-debit-on-failure` report.** That bug debits the actor's `_daily_tokens` even for commands that never get enqueued. Combined with this bug — no reset — every failed bulk submission *permanently* burns part of the actor's process-lifetime budget. A misbehaving caller can lock themselves out in seconds via failed bulks, with no path to recovery.
4. **Multi-agent simulations are the worst hit.** `AGENTS.md` and `LEARNINGS.md` advertise the system as designed for multi-agent debate, MCTS rollouts, and ensemble runs — workloads that submit thousands of commands per minute. Each agent has its own actor_id and its own `_daily_tokens` slot. Long sims will eventually trip the budget for at least one agent, silently locking that agent out for the rest of the run.
5. **Discovery is invisible from logging.** When the budget is exceeded, `guardrail_allow` raises a `PermissionError` with a misleading message ("daily token budget"). The user sees the message, restarts the server (because "daily" suggests time-based recovery), the budget resets, the bug appears to "fix itself" — and then the same lockout happens again hours or days later. Each diagnosis cycle takes a server restart.
6. **The function `reset_daily_tokens` is real evidence the design was supposed to be daily.** It's not a stub — it does the right thing (`_daily_tokens.clear()`). Someone wrote it with the explicit intent of being called periodically. The wiring to call it was just never built. This is a half-implemented feature that the codebase claims as functional.

## Reproduction

### Environment

- Branch: `claude/bug-mre-issue-sMWgS` (reproduced on commit e53a335, no diff)
- Python 3.12, `daft==0.7.5`
- Verified on macOS (darwin 25.2.0)

### Minimal Reproducible Example

```python
"""MRE: reset_daily_tokens is never called by any production code path.
Once an actor exceeds MAX_TOKENS_PER_DAY they are permanently blocked
until the process restarts."""
import asyncio
import tempfile

from uuid_utils import uuid7

from archetype.app.auth.guard import (
    MAX_TOKENS_PER_DAY,
    _daily_tokens,
    reset_daily_tokens,
    reset_tick_counters,
)
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import StorageConfig, WorldConfig


async def main() -> None:
    reset_tick_counters()
    reset_daily_tokens()

    container = ServiceContainer()
    try:
        actor_id = uuid7()
        ctx = ActorCtx(id=actor_id, roles={"admin"})

        # Prime _daily_tokens close to the limit (simulate long-running session).
        _daily_tokens[actor_id] = MAX_TOKENS_PER_DAY - 5
        print(f"primed _daily_tokens[actor] = {_daily_tokens[actor_id]} / {MAX_TOKENS_PER_DAY}")

        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        try:
            await container.command_service.submit("w1", cmd, ctx)
            print("first SPAWN accepted")
        except PermissionError as e:
            print(f"first SPAWN raised: {e}")

        # Run a few simulation steps — does ANY production code reset _daily_tokens?
        before = _daily_tokens.get(actor_id, 0)
        with tempfile.TemporaryDirectory() as tmp:
            info = await container.world_service.create_world(
                WorldConfig(name="reset_check"), StorageConfig(uri=tmp)
            )
            for _ in range(5):
                await container.simulation_service.step(info.world_id)
        after = _daily_tokens.get(actor_id, 0)
        print(f"_daily_tokens before/after 5 simulation steps: {before} -> {after}")

        # Re-attempt the same SPAWN — still raises because nothing reset the budget.
        try:
            await container.command_service.submit("w1", cmd, ctx)
            print("second SPAWN accepted (budget reset)")
        except PermissionError as e:
            print(f"second SPAWN STILL raises: {e}")

        assert after == before, "BUG: _daily_tokens drifted (unexpected)"
        assert after >= MAX_TOKENS_PER_DAY - 5, "BUG: nothing should have reset"
    finally:
        await container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Observed output

```
primed _daily_tokens[actor] = 199995 / 200000
first SPAWN raised: Actor 019d7f11-... exceeded daily token budget (200000 tokens)
_daily_tokens before/after 5 simulation steps: 199995 -> 199995
second SPAWN STILL raises: Actor 019d7f11-... exceeded daily token budget (200000 tokens)
```

After the actor exceeds the budget by 5 tokens (`SPAWN cost 10` over the remaining 5 budget), no number of simulation steps, world creates, or other server-side actions ever lower `_daily_tokens[actor_id]`. The actor is permanently blocked.

### Baseline (proves the per-tick budget DOES get reset)

`_tick_counters` is reset every step by `simulation_service.step` (`simulation_service.py:55`):

```python
_tick_counters[actor_id] = MAX_CMDS_PER_TICK - 1  # primed near limit

await container.simulation_service.step(info.world_id)

# after simulation_service.step: _tick_counters[actor] = 0
# OK (baseline): _tick_counters IS reset by simulation_service.step.
```

The auth quota mechanism *does* work for the per-tick counter. It just doesn't work for the daily token counter, because no equivalent reset call exists in `simulation_service.step` or anywhere else.

## Root cause

`src/archetype/app/auth/guard.py:43-69`:

```python
# ── Quotas ──

MAX_CMDS_PER_TICK: int = 500
MAX_TOKENS_PER_DAY: int = 200_000

# Token cost estimates per command type
_TOKEN_COSTS: dict[str, int] = {
    "spawn": 10,
    "despawn": 5,
    ...
}

# Per-actor tick counters: actor_id → count this tick
_tick_counters: dict[UUID, int] = {}
# Per-actor daily token usage: actor_id → tokens used today
_daily_tokens: dict[UUID, int] = {}
```

`src/archetype/app/auth/guard.py:106-113`:

```python
# 3. Daily token budget
cost = estimate_token_cost(cmd)
current_tokens = _daily_tokens.get(ctx.id, 0)
if current_tokens + cost > MAX_TOKENS_PER_DAY:
    raise PermissionError(
        f"Actor {ctx.id} exceeded daily token budget ({MAX_TOKENS_PER_DAY} tokens)"
    )
_daily_tokens[ctx.id] = current_tokens + cost
```

`src/archetype/app/auth/guard.py:121-123`:

```python
def reset_daily_tokens() -> None:
    """Reset daily token budgets. Called at day boundary."""
    _daily_tokens.clear()
```

`src/archetype/app/simulation_service.py:54-55`:

```python
applied = await self._command_service.drain_and_apply(world_id, tick)
reset_tick_counters()  # only resets _tick_counters, not _daily_tokens
```

`grep -rn "reset_daily_tokens" src/archetype/`:

```
src/archetype/app/auth/guard.py:121:def reset_daily_tokens() -> None:
```

**One match. The function definition. Nothing else.**

`grep -rn "reset_daily_tokens" tests/`:

```
tests/app/test_auth.py:14:    reset_daily_tokens,
tests/app/test_auth.py:25:    reset_daily_tokens()
tests/app/test_auth.py:28:    reset_daily_tokens()
tests/app/test_services.py:9:from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
tests/app/test_services.py:25:    reset_daily_tokens()
tests/app/test_services.py:28:    reset_daily_tokens()
tests/integration/test_command_flow.py:9:from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
tests/integration/test_command_flow.py:19:    reset_daily_tokens()
tests/integration/test_command_flow.py:22:    reset_daily_tokens()
tests/integration/test_trajectory_pipeline.py:19:from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
tests/integration/test_trajectory_pipeline.py:37:    reset_daily_tokens()
tests/integration/test_trajectory_pipeline.py:40:    reset_daily_tokens()
tests/cli/test_cli.py:21:from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
tests/cli/test_cli.py:31:    reset_daily_tokens()
tests/cli/test_cli.py:34:    reset_daily_tokens()
tests/api/test_routes.py:10:from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
tests/api/test_routes.py:21:    reset_daily_tokens()
tests/api/test_routes.py:24:    reset_daily_tokens()
```

**Every call site is a test fixture (setup/teardown).** Tests rely on the function to clean state between cases — exactly the role tests should play. But the production runtime never calls it.

Trace for the MRE:

1. Server starts. `_daily_tokens == {}`.
2. Actor X submits a command. `guardrail_allow(cmd, ctx)` runs:
   - Permission check: passes.
   - Per-tick quota: `_tick_counters[X] = 1`.
   - Daily token budget: `_daily_tokens[X] = 10`.
3. SimulationService.step runs after some interval. It calls `reset_tick_counters()` (`simulation_service.py:55`). `_tick_counters` is now empty. **But `_daily_tokens` is NOT touched.**
4. Steps 2-3 repeat for the lifetime of the server. `_daily_tokens[X]` grows monotonically.
5. After ~20,000 SPAWN commands (or fewer for higher-cost types), `_daily_tokens[X] >= 200_000`. `guardrail_allow` raises `PermissionError`.
6. Actor X retries an hour later. Still raises. A day later. Still raises. A week later. Still raises. The "day boundary" never arrives because nothing in the runtime triggers `reset_daily_tokens()`.
7. The only recovery is to restart the server process (which clears the module-global dict on import). Then the cycle starts over.

The defect is one missing scheduler call. The fix is straightforward (Fix A below). The reason it stayed in main: tests use fixtures to reset between cases, hiding the missing-scheduler problem.

## Why existing tests miss this

Every test that exercises `_daily_tokens` calls `reset_daily_tokens()` in `setup_method` or `teardown_method`:

```python
# tests/app/test_auth.py:23-28 (typical pattern)
def setup_method(self):
    reset_tick_counters()
    reset_daily_tokens()

def teardown_method(self):
    reset_tick_counters()
    reset_daily_tokens()
```

So *every test* starts with `_daily_tokens == {}`. The test never observes accumulation across runs. The "permanent lockout after process lifetime" symptom is structurally impossible to trigger from a test that resets between cases.

There is no test that:

1. Submits commands that cumulatively exceed `MAX_TOKENS_PER_DAY`.
2. Verifies the actor is still able to submit commands after a "day boundary" event.
3. Checks that `simulation_service.step` (or any other production trigger) periodically resets `_daily_tokens`.

`grep -rn "MAX_TOKENS_PER_DAY\|exceeded daily" tests/` returns no test that exercises the daily-token failure mode at all. The daily quota is functionally untested.

## Suggested fixes

**Fix A — schedule a daily reset task in the server.** The minimal correct fix: add a background asyncio task in `ServiceContainer.__init__` (or `archetype.api.main` startup) that calls `reset_daily_tokens()` every 24 hours. Lands in `app/`:

```diff
 # src/archetype/app/container.py (or wherever ServiceContainer.__init__ lives)
+import asyncio
+from datetime import datetime, timedelta, timezone
+
+from archetype.app.auth.guard import reset_daily_tokens
+
 class ServiceContainer:
     def __init__(self, ...):
         ...
+        self._daily_reset_task: asyncio.Task | None = None
+        try:
+            loop = asyncio.get_running_loop()
+            self._daily_reset_task = loop.create_task(self._daily_reset_loop())
+        except RuntimeError:
+            # No running loop yet (sync context); the task will be started
+            # by the first async call that needs it.
+            pass
+
+    async def _daily_reset_loop(self):
+        """Reset _daily_tokens at midnight UTC, then every 24 hours."""
+        while True:
+            now = datetime.now(timezone.utc)
+            tomorrow_utc = (now + timedelta(days=1)).replace(
+                hour=0, minute=0, second=0, microsecond=0
+            )
+            await asyncio.sleep((tomorrow_utc - now).total_seconds())
+            reset_daily_tokens()
+
+    async def shutdown(self):
+        if self._daily_reset_task is not None:
+            self._daily_reset_task.cancel()
+            try:
+                await self._daily_reset_task
+            except asyncio.CancelledError:
+                pass
+        ...
```

This is the simplest implementation of "called at day boundary". For test environments, the loop never fires (tests don't run for 24 hours), so existing test fixtures continue to work as the manual reset path.

**Fix B — debit-and-decay with a sliding window.** Rather than a hard reset, track per-actor token usage with a per-tick decay so the budget recovers smoothly. More complex, more fair, but also a bigger change:

```python
# auth/guard.py
_TOKEN_DECAY_PER_RESET = MAX_TOKENS_PER_DAY // (24 * 60 * 60)  # tokens per second

def decay_daily_tokens(elapsed_seconds: float) -> None:
    """Decay token usage by elapsed time — for use as a per-tick or per-second hook."""
    decay = int(_TOKEN_DECAY_PER_RESET * elapsed_seconds)
    for actor_id in list(_daily_tokens.keys()):
        _daily_tokens[actor_id] = max(0, _daily_tokens[actor_id] - decay)
        if _daily_tokens[actor_id] == 0:
            del _daily_tokens[actor_id]
```

Then call `decay_daily_tokens(dt)` from `simulation_service.step` with the elapsed wall-clock time since the previous step. This avoids the "midnight cliff" reset and is more in spirit with what an actual rate limiter does. Out of scope for a quick fix; file as a separate enhancement.

**Fix C — remove the daily quota entirely until it's actually scheduled.** If no one wants to maintain a scheduler, remove `_daily_tokens` and `MAX_TOKENS_PER_DAY` from `guardrail_allow` until a real implementation lands. Misleading dead code is worse than no code:

```diff
 # auth/guard.py
-MAX_TOKENS_PER_DAY: int = 200_000
-_daily_tokens: dict[UUID, int] = {}
-
 def guardrail_allow(cmd: Command, ctx: ActorCtx) -> None:
     ...
-    # 3. Daily token budget
-    cost = estimate_token_cost(cmd)
-    current_tokens = _daily_tokens.get(ctx.id, 0)
-    if current_tokens + cost > MAX_TOKENS_PER_DAY:
-        raise PermissionError(
-            f"Actor {ctx.id} exceeded daily token budget ({MAX_TOKENS_PER_DAY} tokens)"
-        )
-    _daily_tokens[ctx.id] = current_tokens + cost
```

Fix C is the cleanest architectural choice if the daily-quota *concept* isn't actually wanted. The function exists, but the wiring suggests no one actually committed to enforcing it.

I'd recommend **Fix A as the urgent fix** (it makes the documented behaviour real) and **Fix B as a follow-up** (sliding window is the right shape long-term). Fix C only if the team decides the feature isn't worth keeping.

## Suggested regression tests

Add to `tests/app/test_auth.py`:

```python
@pytest.mark.asyncio
async def test_daily_tokens_reset_called_periodically_in_long_running_server():
    """Regression: a long-running ServiceContainer must periodically reset
    _daily_tokens, otherwise actors get permanently locked out after
    enough commands."""
    from archetype.app.auth.guard import (
        MAX_TOKENS_PER_DAY,
        _daily_tokens,
        reset_tick_counters,
    )
    from archetype.app.container import ServiceContainer

    reset_tick_counters()
    _daily_tokens.clear()

    container = ServiceContainer()
    try:
        actor_id = uuid7()
        # Mark the actor as over-budget.
        _daily_tokens[actor_id] = MAX_TOKENS_PER_DAY + 1

        # The container must have *some* mechanism to reset this — either an
        # asyncio task, a hook on simulation_service.step, or an explicit
        # cron-like primitive. We don't care which, just that it gets cleared.
        await container.maybe_reset_daily_quotas()  # Hypothetical post-fix entry point

        assert _daily_tokens.get(actor_id, 0) == 0, (
            "_daily_tokens accumulates monotonically; reset_daily_tokens "
            "must be wired into a production code path"
        )
    finally:
        await container.shutdown()


def test_reset_daily_tokens_function_has_at_least_one_caller_in_src():
    """Defensive: catch the regression of removing the only production
    caller of reset_daily_tokens. This test grep-checks the source tree
    rather than running anything, so it's resilient to the exact wiring."""
    import subprocess

    result = subprocess.run(
        ["grep", "-rn", "reset_daily_tokens", "src/archetype/"],
        capture_output=True,
        text=True,
        check=False,
    )
    callers = [
        line
        for line in result.stdout.splitlines()
        if "guard.py:" not in line  # exclude the definition itself
        and "def reset_daily_tokens" not in line
    ]
    assert callers, (
        "reset_daily_tokens has no production callers — the daily quota is "
        "permanently locked once exceeded. Wire it into ServiceContainer or remove it."
    )
```

Both tests fail on `main`. The first requires Fix A to add the wiring. The second is a meta-test that catches future regressions of the same shape (a defined-but-uncalled reset function).

## Notes / scope

- Affects `src/archetype/app/auth/guard.py:69-123` and `src/archetype/app/container.py` (where the daily reset scheduler should live). Both are in `app/`, not `core/`, so the fix can land directly.
- Distinct from the fifteen other already-filed bugs:
  - Five `core/` mutation cache bugs are about world internals.
  - Four `command_service.apply` bugs are about dispatcher routing/typing/awaiting.
  - `simulation-service-run-discards-runconfig` is `RunConfig` substitution.
  - `lifecycle-commands-leak-broker` is the broker queue leak.
  - `on-spawn-on-despawn-hooks-never-fire` is hook plumbing.
  - `enqueue-bulk-quota-debit-on-failure` is quota debit on partial failure (related!).
  - `component-prefix-collision` and `component-get-type-by-name-no-recurse` are Component registration.
  - `cached-store-read-shadows-disk` is the cache hiding flushed rows.
  - `create-world-name-collision-orphan` is the orphan world leak.
  - This bug is the *third* `auth/guard.py` issue today (after `enqueue-bulk-quota-debit-on-failure` and the security implications of `update-command-silently-noops`/`remove-component-strings-noop`/`add-processor-missing-await` quota notes). The whole quota subsystem deserves an audit pass.
- Compounds badly with `enqueue-bulk-quota-debit-on-failure`: that bug debits quota for commands that never enqueue; this bug means the wasted debits never get refunded by a daily reset. Together they make the daily quota *fragile* (failed bulks burn it) AND *non-recoverable* (no scheduler clears it).
- `reset_tick_counters` is correctly wired (called in `simulation_service.step:55` after every drain). The asymmetry between the two reset functions strongly suggests the daily reset was either forgotten in implementation or deferred and never picked up.
- Worth flagging in `2026-03-28-security-program-review.md` as a quota DoS vector: any actor that spams commands can lock themselves out for the lifetime of the server. Combined with the bulk quota leak, the lockout becomes trivial.
- A small follow-up worth a separate hunt: the `_TOKEN_COSTS` table at `auth/guard.py:48-64` is independent of the actual cost of executing the command (no token estimation for LLM calls, no measurement of compute time, no I/O cost). The "tokens" are arbitrary integers, not real model tokens. The naming is misleading.

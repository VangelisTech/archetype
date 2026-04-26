# Execution Hierarchy

**Document type:** Normative.
**Scope:** `iSimulationService` and the gated proxies on `iCommandService`. Step / Run / Episode / Rollout semantics.

## 1. The four execution levels

```
Rollout      = N episodes, each in a fork of a base world.
               Lives at iSimulationService. Forks via iWorldService directly.
               Audit unit: the rollout call (one row at the gate).

Episode      = step-until-termination on the world you give it.
               Does NOT implicitly fork. Operates on the supplied world_id directly.
               Lives at iSimulationService.

Run          = N steps with one RunConfig. No termination check, no fork.
               Vanilla path: repeated `run` calls accumulate state on the same world.

Step         = 1 tick. Primitive.
```

The key call: **episodes do not fork. Rollouts do.** Forking is orthogonal to termination. A user can run an episode on the live base world (state mutates in place) or on a fork (state isolated) — the episode itself doesn't impose either choice.

## 2. The vanilla pattern

The most common use of the simulation layer is repeated `run` calls on a single world. State accumulates across runs; no termination, no fork.

```python
await world.run(steps=100)   # tick 0..100
await world.run(steps=100)   # tick 100..200, same world, accumulated state
await world.run(steps=100)   # tick 200..300, same world
```

Step + Run cover this baseline. Episode adds termination on top; Rollout adds fork-and-aggregate on top of that.

## 3. Episode

An episode runs a world until termination or a step cap.

### 3.1 — Termination

Two flavors, both supported, evaluated after each step:

1. **`terminal_component: type[Component] | None`** — declarative default. Episode terminates iff any *active* entity has this component type. ECS-native, easy to audit. Recommended.
2. **`termination: Callable[[iWorld], bool] | None`** — escape hatch. Arbitrary predicate over world state. Used when termination doesn't fit the component pattern (e.g., "tick > T", "average score crosses threshold").

If both are `None`, only `max_steps` caps the episode (defensive bound).

### 3.2 — `EpisodeConfig`

```python
class EpisodeConfig(BaseModel):
    episode_id: UUID = Field(default_factory=uuid7)
    run_config: RunConfig                                   # how each step inside runs
    max_steps: int = 1000                                   # defensive cap
    terminal_component: type[Component] | None = None
    termination: Callable[[iWorld], bool] | None = None
```

### 3.3 — `EpisodeResult`

```python
class EpisodeResult(BaseModel):
    episode_id: UUID
    world_id: UUID                  # the world the episode ran on (fork or live)
    final_tick: int
    terminated: bool                # True if predicate fired; False if max_steps hit
    duration_steps: int
```

### 3.4 — Method shape

```python
# iSimulationService
async def run_episode(world_id, config: EpisodeConfig, **kw) -> EpisodeResult: ...
```

The implementation:

```
- resolve world via iWorldService.get_world
- start_tick = world.tick
- step_count = 0
- while step_count < config.max_steps:
    if config.terminal_component:
        if any active entity has the component: break
    if config.termination:
        if config.termination(world): break
    await self.step(world_id, config.run_config)
    step_count += 1
- return EpisodeResult(
      episode_id=config.episode_id,
      world_id=world_id,
      final_tick=world.tick,
      terminated=(predicate fired),
      duration_steps=(world.tick - start_tick),
  )
```

## 4. Rollout

A rollout = "run the same episode N times in isolation and aggregate." Isolation is achieved via forking; each episode runs in its own fork of the base world.

### 4.1 — `RolloutConfig`

```python
class RolloutConfig(BaseModel):
    rollout_id: UUID = Field(default_factory=uuid7)
    episode_config: EpisodeConfig                           # template per episode
    num_episodes: int                                       # how many forks
    parallel: bool = False                                  # asyncio.gather across forks
    name_prefix: str = "ep"                                 # forks named "<base>:<prefix>:<i>"
    destroy_forks_on_complete: bool = False                 # in-memory cleanup post-rollout
```

`destroy_forks_on_complete` does NOT delete persisted data. It calls `destroy_world` per fork after the episode completes. Each fork's storage rows and audit rows remain queryable forever.

### 4.2 — `RolloutResult`

```python
class RolloutResult(BaseModel):
    rollout_id: UUID
    base_world_id: UUID
    episodes: list[EpisodeResult]
    total_duration_steps: int
```

### 4.3 — Method shape

```python
# iSimulationService
async def run_rollout(world_id, config: RolloutConfig, **kw) -> RolloutResult: ...
```

The implementation:

```
- base = await iWorldService.get_world(world_id)
- async def _one(i: int) -> EpisodeResult:
    fork = await self._world_service.fork_world(
        world_id, name=f"{base.name}:{config.name_prefix}:{i}"
    )
    result = await self.run_episode(fork.world_id, config.episode_config)
    if config.destroy_forks_on_complete:
        await self._world_service.destroy_world(fork.world_id)
    return result
- if config.parallel:
    results = await asyncio.gather(*(_one(i) for i in range(config.num_episodes)))
- else:
    results = [await _one(i) for i in range(config.num_episodes)]
- return RolloutResult(...)
```

Forks happen via `iWorldService.fork_world` directly — NOT through the gate. Forks inside a rollout are not individually gated or audited. The rollout call IS the audit unit.

## 5. Gate proxies

`iCommandService` exposes thin gated proxies for both:

```python
async def run_episode(ctx, world_id, config) -> EpisodeResult: ...
async def run_rollout(ctx, world_id, config) -> RolloutResult: ...
```

Each follows the standard gate shape: `guardrail_allow → delegate → audit.record`. Each emits exactly ONE audit row. The rollout's audit row payload captures `num_episodes`, fork ids, and aggregate stats.

`iSimulationService.run_rollout` is NOT gated internally. The gate runs once at `iCommandService.run_rollout`; per-fork operations within `iSimulationService` are not gate-visible.

## 6. Permissions

Per `command-gate.md`:

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `step` / `run` | — | — | ✓ | ✓ |
| `run_episode` | — | — | ✓ | ✓ |
| `run_rollout` | — | — | ✓ | ✓ |

A `player` does not advance the world; an `operator` does.

## 7. Tests

- `run_episode` terminates on `terminal_component` (any active entity has it).
- `run_episode` terminates on `termination` callable.
- `run_episode` caps at `max_steps` when no predicate fires.
- `run_episode` does NOT fork: world_id pre/post is identical.
- `run_episode` returns `terminated=True` iff a predicate fired.
- `run_rollout` produces `num_episodes` `EpisodeResult` entries.
- `run_rollout` with `destroy_forks_on_complete=True`: forks no longer in registry; storage AND audit rows for each fork PRESERVED.
- `run_rollout(parallel=True)` executes episode forks concurrently.
- Rollout audit emission: ONE rollout-level audit row, not N. Payload captures fork ids.

## 8. Out of scope

- Heterogeneous episodes within one rollout (different configs per episode). v1: one template + count. Heterogeneous rollouts are N rollouts of size 1 with different configs.
- Concurrency cap on `parallel=True`. v1: unbounded `asyncio.gather`. Storage backend is the practical bottleneck.
- Mid-rollout cancellation. v1: rollouts run to completion or raise.

# Execution Hierarchy

**Document type:** Normative.
**Scope:** `archetype.world.simulation` and the gated proxies on
`iCommandGateway`.

## 1. The four execution levels

```text
Rollout = N episodes, each in a fresh lifecycle-owned fork.
Episode = step until termination or a cap on the supplied world.
Run     = N steps with one RunConfig; no termination check or fork.
Step    = one committed managed tick.
```

Episodes do not fork. Rollouts do. A caller may run an episode on a base world
or on a caller-created fork; the episode preserves that choice.

The supported runtime usually exposes these operations through a
`RuntimeWorld` handle. Internally, `RuntimeApplication` calls the module
functions in `archetype.world.simulation`; untrusted calls first pass
`iCommandGateway`.

## 2. Step and run

`step(registry, world_id, run_config, **inputs)` acquires the registry's exact
world lease and advances one managed tick. Before admitting a new tick it
retries any retained required-projector receipt. The core tick then
materializes due commands, computes and publishes state, and returns a stable
`CommittedTickReceipt`. If a required projector is bound, managed simulation
retains, projects, and acknowledges that receipt before returning.

`run(...)` acquires the lease once and invokes the lock-held step path
`run_config.num_steps` times. Repeated runs preserve the immutable run identity
owned by the world:

```python
await world.run(steps=100)  # tick 0..100
await world.run(steps=100)  # tick 100..200, same world and run_id
```

A pre-publication failure leaves the tick retryable. A required-projector
failure is post-commit: it raises with the exact receipt, retains that receipt,
and never replays the tick.

## 3. Episode

An episode runs one supplied world until a termination condition fires or
`max_steps` is reached.

```python
class EpisodeConfig(BaseModel):
    episode_id: UUID = Field(default_factory=uuid7)
    run_config: RunConfig = Field(default_factory=RunConfig)
    max_steps: int = 1000
    terminal_component: type[Component] | None = None
    terminal_field: str | None = None
    terminal_all: bool = True
    termination: Callable[[AsyncWorld], bool] | None = None
```

Termination has three forms:

1. With only `terminal_component`, the episode stops when that component
   appears in an active signature.
2. With `terminal_component` plus `terminal_field`, persisted values are
   reduced through `iStorageService`. `terminal_all` selects all-versus-any.
3. `termination` is a trusted in-process predicate over the live world.

Every condition is bounded by `max_steps`. The result identifies the exact
episode, world, immutable run, starting tick, final tick, termination outcome,
and duration.

```python
class EpisodeResult(BaseModel):
    episode_id: UUID
    world_id: UUID
    run_id: UUID
    start_tick: int
    final_tick: int
    terminated: bool
    duration_steps: int
```

`run_episode(...)` acquires one exact-world lease for the whole bounded
attempt. It does not implicitly fork.

## 4. Rollout

A rollout repeats one episode template on independent forks:

```python
class RolloutConfig(BaseModel):
    rollout_id: UUID = Field(default_factory=uuid7)
    episode_config: EpisodeConfig = Field(default_factory=EpisodeConfig)
    num_episodes: int = 1
    parallel: bool = False
    name_prefix: str = "ep"
    destroy_forks_on_complete: bool = False
```

The world lifecycle creates each fork. Sequential rollouts await episodes in
index order. Parallel rollouts keep each fork under its own exact-world lock
and preserve result order, but the parent structurally drains every started
episode before it returns or raises. One episode failure does not cancel
siblings: bounded siblings finish naturally so cancellation cannot interrupt
the transfer of a newly registered fork into rollout-owned teardown. The first
observed failure remains the primary exception after the drain. Additional
failures are recorded as deterministic child/phase/type/fork notes so an
existing causal chain on the primary exception is never replaced.

For either execution mode, caller cancellation follows the same boundary. The
rollout cancels each started child once so episode work stops promptly, while
shielding fork acquisition until the child owns the returned ID and shielding
that fork's teardown to completion. Repeated cancellation cannot interrupt
cleanup. Cancellation propagates only after every started child reaches its
`finally` teardown, and a cancelled sequential rollout does not start its next
episode. A substantive child failure observed before caller cancellation
remains chained beneath the caller's original cancellation. A teardown failure
caused by cancellation instead becomes primary after cleanup completes, with
the initiating cancellation retained as its cause or note.

If `destroy_forks_on_complete` is true, application teardown runs in `finally`
for each fork. It reconciles committed work, cancels unsettled durable commands,
then delegates lifecycle close. Closing removes live ownership but never
deletes persisted world, command, or audit history.

```python
class RolloutResult(BaseModel):
    rollout_id: UUID
    base_world_id: UUID
    episodes: tuple[EpisodeResult, ...]
    num_episodes: int
    total_duration_steps: int
```

Forks inside a rollout are family implementation details. The external audit
unit is the one gated rollout call, not one authorization event per internal
fork.

## 5. Gate proxies and permissions

`iCommandGateway` exposes authorized `step`, `run`, `run_episode`, and
`run_rollout` proxies. Each authorizes once, delegates to
`iRuntimeApplication`, and records one access decision.

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `step` / `run` | — | — | ✓ | ✓ |
| `run_episode` | — | — | ✓ | ✓ |
| `run_rollout` | — | — | ✓ | ✓ |

## 6. Executable contracts

Focused world execution behavior lives under `tests/world/`. Gateway
authorization and safe-result behavior remain under `tests/app/` and
`tests/api/`.

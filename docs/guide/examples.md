# Examples

Every example on this page runs end-to-end with a single command. The
recommended pattern is `ArchetypeRuntime` for scripts. A small number of
examples intentionally exercise internal services as focused implementation
fixtures; application examples stay on the runtime.

## 0. Quickstart

The smallest complete simulation defines one component and one processor,
then runs through the public runtime surface. It stays below 30 non-comment
source lines and is part of the credential-free example smoke suite.

```bash
uv run python examples/00_quickstart.py
```

Source: [`examples/00_quickstart.py`](https://github.com/VangelisTech/archetype/blob/main/examples/00_quickstart.py)

The script prints `3`: the initial state is persisted first, then the
processor increments the counter on three subsequent ticks.

---

## 1. World Mutations

Demonstrates the trusted mutation surface: spawn entities with components,
inject processors at runtime, fork a world, and query state/history.

```bash
uv run python examples/01_world_mutations.py
```

Source: [`examples/01_world_mutations.py`](https://github.com/VangelisTech/archetype/blob/main/examples/01_world_mutations.py)

This example uses actor-free `ArchetypeRuntime`. RBAC is intentionally absent
from trusted scripting; the command-gateway and API tests cover role policy.

**What it demonstrates:**

- **SPAWN / DESPAWN / UPDATE** through the runtime application surface
- **ADD_COMPONENT / REMOVE_COMPONENT** with archetype migration at tick boundaries
- **ADD_PROCESSOR** to inject a `MovementProcessor` at runtime
- **FORK** while preserving runtime ownership and independent world identity
- **History reads** through `world.history()` without fabricating access events

Output:

```text
1. SPAWN
   runtime spawned: scout=1, dummy=2

2. UPDATE + COMPONENT MUTATIONS
   scout after update/add_components: pos=(2.0, 1.0), vel=(1.5, 0.5), hp=80

3. PROCESSOR MUTATIONS
   MovementProcessor installed and removed
```

**Runtime operations in this example (curated, not exhaustive):**

| Runtime call | Owning application family |
|---|---|
| `world.step()` / `world.run()` | simulation |
| entity/component mutations | mutation |
| processor mutations | mutation |
| `world.fork()` / `world.info()` | world lifecycle |
| `world.query()` | query |
| `world.history()` | audit projection |

The runtime delegates directly to `iRuntimeApplication`; it does not construct
an `ActorCtx` or pass through `CommandGateway`. See the normative
[Command Gate](command-gate.md#3-the-permissions-matrix) for the separate
untrusted-adapter permission matrix.

---

## 2. Fork for Counterfactuals

Fork a world three times, run each branch, compare results.

```bash
uv run python examples/02_fork_counterfactual.py
```

Source: [`examples/02_fork_counterfactual.py`](https://github.com/VangelisTech/archetype/blob/main/examples/02_fork_counterfactual.py)

```python
import asyncio
from archetype import ArchetypeRuntime, Component, StorageConfig


class Probe(Component):
    label: str = ""


async def main():
    storage = StorageConfig(uri="./archetype_data", namespace="counterfactuals")

    async with ArchetypeRuntime() as runtime:
        base = runtime.world("base", storage=storage)
        await base.spawn(Probe(label="seed"))
        await base.run(steps=1)

        for branch in ["low", "baseline", "high"]:
            fork = await base.fork(f"branch-{branch}", storage=storage)
            result = await fork.run(steps=10)
            rows = (await fork.query(Probe)).collect().to_pylist()
            print(f"{branch}: tick={result.final_tick}, entities={len(rows)}")

asyncio.run(main())
```

Output:

```text
low: tick=11
baseline: tick=11
high: tick=11
```

All three branches start from the same base state and diverge independently.
Forks share resource instances by default; trusted scripts attach replacement
resources through the runtime when per-branch resource isolation is required.

---

## 3. Time-Travel Queries

Run ticks, rewind to any past tick by filtering the `tick` column, then fork
a counterfactual branch and diff it against the source at the same tick.
Every tick is preserved.

```bash
uv run python examples/03_time_travel.py
```

Source: [`examples/03_time_travel.py`](https://github.com/VangelisTech/archetype/blob/main/examples/03_time_travel.py)

`world.query(...)` returns the full append-only history, so a point-in-time
view is a Daft filter:

```python
df = await world.query(Position, Velocity)
at_tick_2 = df.where(col("tick") == 2)
```

The fork half of the example stages a divergent component value on the fork
(`fork.update(entity, Velocity(vx=10.0))`), steps both worlds the same number
of ticks, and prints the source-vs-fork diff at the same tick — plus the
fork's view of its pre-fork history, read through lineage.

Initial conditions are part of the ledger: an entity's first persisted row
is its raw spawn values at the tick it materializes, and processors first
apply on the following tick — the table contains `x_0, f(x_0), f^2(x_0), ...`.

---

## 4. Agent Messaging

Three agents exchange greetings through an example-local shared `Mailbox`.
Priority-ordered processors realize pending messages on the following tick,
then update mood and energy from each inbox.

```bash
uv run python examples/04_messaging.py
```

Source: [`examples/04_messaging.py`](https://github.com/VangelisTech/archetype/blob/main/examples/04_messaging.py)

**What it demonstrates:**

- **Components**: `AgentState` (name, mood, energy), `Inbox`, `Outbox`
- **Resources**: `SimConfig` for shared parameters, `Mailbox` for pending messages
- **Processors**: `GreetingProcessor` (deposits messages), `MessageRealizationProcessor` (drains the mailbox into inboxes), `MoodProcessor` (updates mood based on inbox)
- **Hooks**: `PreTick` and `PostTick` lifecycle callbacks

Selected output (earlier history rows omitted):

```text
Archetype Messaging Demo: Resources + Shared Mailbox + Hooks

-> Pre-tick 1: Starting processing...
<- Post-tick 2: Completed!
   Messages delivered so far: 0
   Messages pending in mailbox: 6

-> Pre-tick 2: Starting processing...
<- Post-tick 3: Completed!
   Messages delivered so far: 6
   Messages pending in mailbox: 6

Message counts:
  Alice: 2 messages received
  Bob: 2 messages received
  Charlie: 2 messages received

Total messages delivered: 6
```

---

## 5. LLM-Powered Agents

Three agents with different personalities, each calling an LLM every tick via `daft.functions.prompt`. The ECS handles batching automatically — all entities get LLM calls in parallel because world state is a DataFrame.

```bash
export OPENAI_API_KEY=sk-...
uv run python examples/05_llm_agents.py
```

Source: [`examples/05_llm_agents.py`](https://github.com/VangelisTech/archetype/blob/main/examples/05_llm_agents.py)

**What it demonstrates:**

- **Component**: `Agent` with name, role, and a JSON journal of thoughts
- **Processor**: `ThinkProcessor` uses `daft.functions.prompt` to call an LLM for every agent entity in a single DataFrame operation
- **Pattern**: `ArchetypeRuntime` keeps the script surface to `world.spawn(...)`, `world.run(...)`, and `world.query(...)`

Requires an OpenAI API key (or any provider via `daft.set_provider()`).

---

## 6. Trajectory Analysis

Ingest agent trajectories, label them with LLM-based evaluation, and compare techniques via world forking. Demonstrates the full ECS pattern: components, processors, resources, and forking in a single script.

```bash
uv run python examples/06_trajectory_analysis.py
```

Source: [`examples/06_trajectory_analysis.py`](https://github.com/VangelisTech/archetype/blob/main/examples/06_trajectory_analysis.py)

**What it demonstrates:**

- **Components**: `Trajectory` (JSON-encoded turns), `Label` (evaluation result)
- **Processors**: `SamplingProcessor` (filter), `LabelingProcessor` (LLM eval), `ScoringProcessor` (normalize)
- **Resources**: `SamplingConfig`, `LabelingConfig` staged on `runtime.world(..., resources=[...])`
- **Fork-based comparison**: Clone a world with `world.fork(...)` and run an independent branch

---

## 7. Lifecycle Hooks

Record lifecycle audit events, measure tick duration, and publish per-tick metrics without putting side effects inside processors.

```bash
uv run python examples/07_hooks.py
```

Source: [`examples/07_hooks.py`](https://github.com/VangelisTech/archetype/blob/main/examples/07_hooks.py)

**What it demonstrates:**

- **Mutation audit**: `OnSpawn`, `OnDespawn`, `OnComponentAdded`, and `OnComponentRemoved`
- **Tick telemetry**: `PreTick` starts a timer and `PostTick` computes metrics from `event.results`
- **Hook handles**: unregister a temporary debug hook with `world.remove_hook(handle)`
- **Boundary discipline**: hooks emit side effects; processors keep the simulation state deterministic

# Examples

Every example on this page runs end-to-end with a single command. The
recommended pattern is `ArchetypeRuntime` for scripts. A small number of
examples intentionally call the service layer when they need lower-level
storage or queue control.

## 1. World Mutations

Demonstrates every mutation type: spawn entities with components, inject processors at runtime, RBAC permission checks, fork a world, and query the full command audit trail.

```bash
uv run python examples/01_world_mutations.py
```

Source: [`examples/01_world_mutations.py`](https://github.com/VangelisTech/archetype/blob/main/examples/01_world_mutations.py)

This example uses `ArchetypeRuntime` plus `world.as_actor(...)` to show
multiple `ActorCtx` roles on one logical world without dropping to the
service layer.

**What it demonstrates:**

- **SPAWN / DESPAWN / UPDATE** through the gated runtime surface
- **ADD_COMPONENT / REMOVE_COMPONENT** with archetype migration at tick boundaries
- **ADD_PROCESSOR** to inject a `MovementProcessor` at runtime
- **RBAC** checks through actor-bound handles: viewer denied spawn, player denied add_processor
- **FORK** from an actor-bound handle while keeping the same actor binding on the branch
- **Audit history** through `world.history()`

Output:

```text
1. SPAWN + RBAC
   viewer: SPAWN denied (correct)
   player: spawned scout=1, dummy=2

2. UPDATE + COMPONENT MUTATIONS
   scout after update/add_components: pos=(2.0, 1.0), vel=(1.5, 0.5), hp=80

3. PROCESSOR MUTATIONS
   player: ADD_PROCESSOR denied (correct)
```

**Command types** (15 total, including `run_rollout` and `run_episode` for MCTS):

| Command | Payload | Who Can Run It |
|---------|---------|----------------|
| `spawn` | `{"components": [...]}` | player, operator, admin |
| `despawn` | `{"entity_id": int}` | player, operator, admin |
| `update` | `{"entity_id": int, "components": [...]}` | player, operator, admin |
| `add_component` | `{"entity_id": int, "components": [...]}` | operator, admin |
| `remove_component` | `{"entity_id": int, "component_types": [...]}` | operator, admin |
| `add_processor` | `{"processor": ...}` | operator, admin |
| `remove_processor` | `{"processor_type": str}` | operator, admin |
| `create_world` | `{"config": {"name": str}}` | admin |
| `destroy_world` | `{"world_id": str}` | operator, admin |
| `fork_world` | `{"source_world_id": str, "name": str}` | operator, admin |
| `message` | `{"sender_id", "receiver_id", "content"}` | player, operator, admin |
| `custom` | `{...}` | player, operator, admin |
| `query_world` | `{}` | viewer, player, operator, admin |

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
Forks share resource instances by default; attach replacement resources through the gated resource-management path when per-branch resource isolation is required.

---

## 3. Time-Travel Queries

Run 10 ticks, then query any point in history. Every tick is preserved.

```bash
uv run python examples/03_time_travel.py
```

Source: [`examples/03_time_travel.py`](https://github.com/VangelisTech/archetype/blob/main/examples/03_time_travel.py)

This example uses lower-level service calls for historical storage queries, but
external reads still enter through `CommandService` so viewer permissions and
audit behavior apply.

```python
import asyncio

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    info = await container.command_service.create_world(
        ctx, WorldConfig(name="time-travel-demo"), StorageConfig(),
    )

    # Spawn 3 entities and run 5 ticks
    for _ in range(3):
        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(ctx, info.world_id, cmd)
    await container.command_service.run(ctx, info.world_id, RunConfig(num_steps=5))

    # Spawn 2 more and run 5 more ticks
    for _ in range(2):
        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(ctx, info.world_id, cmd)
    await container.command_service.run(ctx, info.world_id, RunConfig(num_steps=5))

    # Query state at different ticks
    for tick in [1, 5, 10]:
        state = await container.command_service.query_archetype(
            ctx, (), str(info.world_id), str(info.run_id or ""), ticks=[tick]
        )
        print(f"tick {tick:>2}: {len(state.collect().to_pylist())} rows")

    # Full command audit trail
    history = await container.command_service.get_audit_history(ctx, info.world_id)
    print(history)

    await container.shutdown()

asyncio.run(main())
```

---

## 4. Agent Messaging

Three agents send greetings to each other via tick-deferred `MESSAGE` commands. Mood and energy update based on messages received.

```bash
uv run python examples/04_messaging.py
```

Source: [`examples/04_messaging.py`](https://github.com/VangelisTech/archetype/blob/main/examples/04_messaging.py)

**What it demonstrates:**

- **Components**: `AgentState` (name, mood, energy), `Inbox`, `Outbox`
- **Resources**: `SimConfig` for shared parameters, `CommandBroker` for message routing
- **Processors**: `GreetingProcessor` (sends messages), `MessageRealizationProcessor` (drains broker into inboxes), `MoodProcessor` (updates mood based on inbox)
- **Hooks**: `PreTick` and `PostTick` lifecycle callbacks

Output:

```text
Archetype Messaging Demo: Resources + MESSAGE + Hooks

-> Pre-tick 0: Starting processing...
<- Post-tick 1: Completed!
   Messages pending in broker: 6

Final State:
  Alice:   mood=happy, energy=130.0, 2 messages received
  Bob:     mood=happy, energy=130.0, 2 messages received
  Charlie: mood=happy, energy=130.0, 2 messages received
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

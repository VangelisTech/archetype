# Examples

Every example on this page runs end-to-end with a single command. Copy, paste, run.

## World Mutations

Demonstrates every mutation type: spawn entities with components, inject processors at runtime, RBAC permission checks, fork a world, and query the full command audit trail.

```bash
uv run python examples/world_mutations.py
```

Source: [`examples/world_mutations.py`](https://github.com/VangelisTech/archetype/blob/main/examples/world_mutations.py)

**What it demonstrates:**

- **SPAWN** with typed components (`Position`, `Velocity`, `Health`)
- **ADD_PROCESSOR** to inject a `MovementProcessor` at runtime
- **RBAC** checks: viewer denied spawn, player allowed spawn, player denied add_processor
- **FORK** a world and run source and fork independently
- **Command history** as a full audit trail

Output:

```text
1. SPAWN — create entities with components
   Spawned 2 entities (tick=1)

2. ADD_PROCESSOR — inject behavior at runtime
   Added MovementProcessor (priority=10)
   Ran 3 ticks (tick=4)

4. RBAC — permission checks
   viewer: SPAWN denied (correct)
   player: SPAWN allowed (correct)
   player: ADD_PROCESSOR denied (correct)

5. FORK — branch the world
   Fork tick=5 (matches source tick=5)
   Fork after 5 more ticks: tick=10
   Source unchanged: tick=5

6. COMMAND HISTORY — full audit trail
   tick=0: spawn
   tick=0: spawn
   tick=0: spawn
```

**Command types** (15 total, including `run_rollout` and `run_episode` for MCTS):

| Command | Payload | Who Can Run It |
|---------|---------|----------------|
| `spawn` | `{"components": [...]}` | player, maintainer, admin |
| `despawn` | `{"entity_id": int}` | player, maintainer, admin |
| `update` | `{"entity_id": int, "components": [...]}` | player, coder, maintainer, admin |
| `add_component` | `{"entity_id": int, "components": [...]}` | coder, maintainer, admin |
| `remove_component` | `{"entity_id": int, "component_types": [...]}` | coder, maintainer, admin |
| `add_processor` | `{"processor": ...}` | maintainer, admin |
| `remove_processor` | `{"processor_type": str}` | maintainer, admin |
| `create_world` | `{"config": {"name": str}}` | admin |
| `destroy_world` | `{"world_id": str}` | admin |
| `fork_world` | `{"source_world_id": str, "name": str}` | admin |
| `message` | `{"sender_id", "receiver_id", "content"}` | player, admin |
| `custom` | `{...}` | player, admin |
| `query_world` | `{}` | viewer, operator, admin |

---

## Fork for Counterfactuals

Fork a world three times with different parameters, run each branch, compare results.

```bash
uv run python examples/fork_counterfactual.py
```

Source: [`examples/fork_counterfactual.py`](https://github.com/VangelisTech/archetype/blob/main/examples/fork_counterfactual.py)

```python
import asyncio
from dataclasses import dataclass

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@dataclass
class PhysicsConfig:
    gravity: float = 9.8
    drag: float = 0.1


async def main():
    container = ServiceContainer()
    ctx = ActorCtx(id=uuid7(), roles={"admin"})

    # Create the base world and spawn an entity
    base = await container.world_service.create_world(
        WorldConfig(name="base"), StorageConfig(),
    )
    cmd = Command(type=CommandType.SPAWN, payload={"components": []})
    await container.command_service.submit(base.world_id, cmd, ctx)
    await container.simulation_service.run(base.world_id, RunConfig(num_steps=1))

    # Fork with different gravity values and run each
    for gravity in [1.0, 9.8, 25.0]:
        fork = await container.world_service.fork_world(
            source_world_id=base.world_id,
            name=f"gravity-{gravity}",
            storage_config=StorageConfig(),
        )
        fork.resources.insert(PhysicsConfig(gravity=gravity))
        await container.simulation_service.run(fork.world_id, RunConfig(num_steps=10))

        state = await container.query_service.get_world_state(fork.world_id)
        print(f"gravity={gravity:>5.1f}: tick={state.tick}")

    await container.shutdown()

asyncio.run(main())
```

Output:

```text
gravity=  1.0: tick=11
gravity=  9.8: tick=11
gravity= 25.0: tick=11
```

All three branches start from the same base state and diverge independently.

---

## Time-Travel Queries

Run 10 ticks, then query any point in history. Every tick is preserved.

```bash
uv run python examples/time_travel.py
```

Source: [`examples/time_travel.py`](https://github.com/VangelisTech/archetype/blob/main/examples/time_travel.py)

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

    world = await container.world_service.create_world(
        WorldConfig(name="time-travel-demo"), StorageConfig(),
    )

    # Spawn 3 entities and run 5 ticks
    for _ in range(3):
        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(world.world_id, cmd, ctx)
    await container.simulation_service.run(world.world_id, RunConfig(num_steps=5))

    # Spawn 2 more and run 5 more ticks
    for _ in range(2):
        cmd = Command(type=CommandType.SPAWN, payload={"components": []})
        await container.command_service.submit(world.world_id, cmd, ctx)
    await container.simulation_service.run(world.world_id, RunConfig(num_steps=5))

    # Query state at different ticks
    for tick in [1, 5, 10]:
        state = await container.query_service.get_world_state(world.world_id, tick=tick)
        print(f"tick {tick:>2}: {len(state.entities)} entities")

    # Full command audit trail
    history = await container.query_service.get_command_history(world.world_id)
    for cmd in history:
        print(f"  tick={cmd.tick}: {cmd.type.value}")

    await container.shutdown()

asyncio.run(main())
```

---

## Agent Messaging

Three agents send greetings to each other via the CommandBroker. Messages are enqueued as `MESSAGE` commands and delivered at tick boundaries. Mood and energy update based on messages received.

```bash
uv run python examples/messaging_example.py
```

Source: [`examples/messaging_example.py`](https://github.com/VangelisTech/archetype/blob/main/examples/messaging_example.py)

**What it demonstrates:**

- **Components**: `AgentState` (name, mood, energy), `Inbox`, `Outbox`
- **Resources**: `SimConfig` for shared parameters, `CommandBroker` for message routing
- **Processors**: `GreetingProcessor` (sends messages), `MessageRealizationProcessor` (drains broker into inboxes), `MoodProcessor` (updates mood based on inbox)
- **Hooks**: `pre_tick` and `post_tick` lifecycle callbacks

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

## LLM-Powered Agents

Three agents with different personalities, each calling an LLM every tick via `daft.functions.prompt`. The ECS handles batching automatically — all entities get LLM calls in parallel because world state is a DataFrame.

```bash
export OPENAI_API_KEY=sk-...
uv run python examples/llm_agents.py
```

Source: [`examples/llm_agents.py`](https://github.com/VangelisTech/archetype/blob/main/examples/llm_agents.py)

**What it demonstrates:**

- **Component**: `Agent` with name, role, and a JSON journal of thoughts
- **Processor**: `ThinkProcessor` uses `daft.functions.prompt` to call an LLM for every agent entity in a single DataFrame operation
- **Pattern**: Thoughts accumulate in a journal column across ticks

Requires an OpenAI API key (or any provider via `daft.set_provider()`).

---

## Trajectory Analysis

Ingest agent trajectories, label them with natural language descriptions, and compare techniques via world forking.

```bash
uv run python examples/trajectories/run.py
```

Source: [`examples/trajectories/run.py`](https://github.com/VangelisTech/archetype/blob/main/examples/trajectories/run.py)

**What it demonstrates:**

- **TrajectoryPipeline**: High-level API for ingesting conversation turns
- **Components**: `Trajectory`, `Turn`, `Label`
- **Processors**: `SamplingProcessor`, `LabelingProcessor`, `ScoringProcessor`
- **Fork-based comparison**: Run different labeling strategies on forked worlds and compare results

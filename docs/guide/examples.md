# Examples

## LLM-Powered Agents

Three agents with different personalities, each calling an LLM every tick via `daft.functions.prompt`. The ECS handles batching automatically.

```bash
export OPENAI_API_KEY=sk-...
uv run python examples/llm_agents.py
```

Source: [`examples/llm_agents.py`](../../examples/llm_agents.py)

Key pattern: `daft.functions.prompt` inside an `AsyncProcessor.process()` gives you parallel LLM calls across all entities in a single DataFrame operation.

```python
class ThinkProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 10

    async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
        return df.with_columns({
            "agent__last_thought": prompt(
                col("agent__role") + "\nTick " + str(tick) + ". What next?",
                model="gpt-5-mini",
                max_output_tokens=60,
            ),
        })
```

## Agent Messaging

Agents send messages to each other via the CommandBroker. Messages are enqueued as `MESSAGE` commands and delivered at tick boundaries.

```bash
uv run python examples/messaging_example.py
```

Source: [`examples/messaging_example.py`](../../examples/messaging_example.py)

Key pattern: Processors access the broker via `Resources` and enqueue commands that other processors drain next tick.

## Patterns

### Fork for Counterfactuals

```python
# Run the same scenario with different parameters
for gravity in [5.0, 9.8, 15.0]:
    fork = await container.world_service.fork_world(
        source_world_id=world.world_id,
        config=WorldConfig(name=f"gravity-{gravity}"),
    )
    fork.resources.insert(PhysicsConfig(gravity=gravity))
    await container.simulation_service.run(fork.world_id, RunConfig(num_steps=100))

    state = await container.query_service.get_world_state(fork.world_id)
    print(f"gravity={gravity}: tick={state.tick}")
```

### Multi-Model Debate

```python
class DebateProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 10

    async def process(self, df, tick=0, **kwargs):
        # Each agent uses a different model based on their role
        return df.with_columns({
            "agent__last_thought": prompt(
                col("agent__role") + ": Respond to the debate. Tick " + str(tick),
                model="gpt-5-mini",
            ),
        })
```

### Processor Pipeline

```python
# Perception → Thinking → Action → Cleanup
class PerceiveProcessor(AsyncProcessor):
    components = (Agent, Sensor)
    priority = 1

class ThinkProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 10

class ActProcessor(AsyncProcessor):
    components = (Agent, Position)
    priority = 20

class RecordProcessor(AsyncProcessor):
    components = (Agent,)
    priority = 100
```

### Time-Travel Query

```python
# Query world state at tick 42
snapshot = await container.query_service.get_world_state(world.world_id, tick=42)

# Get a specific entity at tick 42
entity = await container.query_service.get_entity(world.world_id, entity_id=7, tick=42)
```

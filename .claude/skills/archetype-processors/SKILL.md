---
name: archetype-processors
description: Enforces correct Processor implementation. Use when creating or editing AsyncProcessor subclasses, process methods, or pipeline stages — anywhere under src/, tests/, or examples/.
paths: "src/**/*.py,tests/**/*.py,examples/**/*.py"
user_invocable: true
---

## Rules

### 1. Extend `AsyncProcessor`, not raw classes

```python
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component

class MyProcessor(AsyncProcessor):
    components = (ComponentA, ComponentB)  # tuple of Component TYPES
    priority = 10                          # lower = runs first

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Transform and return. Never mutate in place.
        return df.with_columns({...})
```

### 2. `components` declares dependencies

The tuple contains Component **types** (not instances). The processor only runs on entities that have ALL listed components.

```python
components = (Agent, Position)  # runs on entities with both Agent AND Position
```

### 3. `process()` is a pure function: DataFrame in, DataFrame out

- Never mutate `df` in place.
- Never `.collect()` mid-pipeline (breaks the Daft DAG — see daft-patterns skill).
- Always return the transformed DataFrame.

### 4. Access shared state via Resources

```python
async def process(
    self,
    df: DataFrame,
    *,
    resources: Resources,
    **kwargs,
) -> DataFrame:
    config = resources.require(MyConfig)    # raises if missing
    cache = resources.get(MyCache)          # None if missing
```

Resources are injected into `world.resources` before running:
```python
world.resources.insert(MyConfig(...))
world.resources.insert(MyCache(...))
```

### 5. Use `daft.functions.prompt()` for LLM calls

```python
from daft.functions import prompt

async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
    return df.with_columns({
        "agent__response": prompt(
            col("agent__role") + "\nTick: " + str(tick),
            model="gpt-5-mini",
            max_output_tokens=200,
        ),
    })
```

Daft batches all LLM calls across all entities automatically.

### 6. Tick-gate expensive operations

```python
async def process(self, df: DataFrame, tick: int = 0, **kwargs) -> DataFrame:
    if tick != 2:  # only run on tick 2
        return df
    # expensive work here
```

Use for: inner simulations, checkpoints, aggregation at sim end.

### 7. Priority ordering

Processors execute in priority order within a tick (lower first):

| Priority | Use case |
|----------|----------|
| 0-9 | Setup, initialization |
| 10-19 | Sampling, filtering |
| 20-29 | Core logic, LLM calls, labeling |
| 30-39 | Post-processing, scoring |
| 40+ | Cleanup, persistence |

### 8. Message delivery is application composition

Archetype has no framework message envelope or delivery policy. Define
application-owned Components and resources, then make processor priority
express the causal boundary. For example, a realization processor can drain an
application mailbox before a later producer deposits new work; deposits from
tick N are then realized at tick N+1.

Do not treat that delay as a framework guarantee. It belongs to the
application's processors, priorities, and synchronization policy. See
`examples/04_messaging.py`.

### 9. Column references use the component prefix

```python
# RIGHT
col("agent__name")
col("session__total_turns")

# WRONG
col("name")
col("total_turns")
```

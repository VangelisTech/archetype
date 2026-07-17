# Processors

A processor transforms all matching entities in one Daft DataFrame operation.
Declare the component types it needs, then return a new DataFrame from
`process()`. Archetype runs the processor once per matching archetype.

```python
from daft import DataFrame, col

from archetype import AsyncProcessor


class Move(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_columns(
            {
                "position__x": col("position__x") + col("velocity__dx"),
                "position__y": col("position__y") + col("velocity__dy"),
            }
        )
```

`Move` receives only archetypes whose entities have both `Position` and
`Velocity`. It does not loop over Python objects; the DataFrame expression
updates the matching population together.

## Run order

Lower `priority` values run first. Use priorities when one processor consumes
the rows produced by another.

```python
class Integrate(AsyncProcessor):
    components = (Position, Velocity)
    priority = 10


class ResolveCollisions(AsyncProcessor):
    components = (Position, Collider)
    priority = 20
```

Keep one processor responsible for one transformation. It makes order and
tests straightforward.

## Failure and archetype boundaries

A processor error fails the whole world tick, not only the table whose
processor raised. Archetype computes every table before it appends any of
them. On failure, `step()` raises, the tick does not advance, no table appends,
and staged mutations remain available for retry.

A processor's `components` tuple is a matching predicate, not a request to
change an entity's component set. Return a DataFrame compatible with the
current archetype. Widen or narrow an entity explicitly between steps:

```python
await world.add_components(entity_id, Targetable())
await world.step()  # carries the row into the wider signature
await world.step()  # processors newly matched by Targetable now transform it

await world.remove_components(entity_id, Targetable)
```

The migration step persists the carried row under its target signature after
that tick's processor pass. Processors newly matched by the target signature
first see the row on the following step.

Hooks have a deliberately different failure policy. They are advisory
callbacks: exceptions are logged and suppressed so later hooks and the tick
can continue. See [Lifecycle Hooks](hooks.md).

## Add processors to a world

Pass processors when you create the handle for the usual script path:

```python
world = runtime.world("demo", processors=[Move(), ResolveCollisions()])
```

You can change a live world through its gated methods:

```python
await world.add_processor(Move())
await world.remove_processor(Move)
```

`remove_processor()` takes the processor type, not an instance.

## Use shared resources

Resources hold shared configuration or services that do not belong to one
entity. Processors receive them as keyword arguments when they declare them.

```python
from dataclasses import dataclass


@dataclass
class Rules:
    max_speed: float = 5.0


class LimitSpeed(AsyncProcessor):
    components = (Velocity,)

    async def process(self, df: DataFrame, rules: Rules, **_) -> DataFrame:
        return df.with_columns(
            {"velocity__dx": col("velocity__dx").clip(-rules.max_speed, rules.max_speed)}
        )


world = runtime.world("demo", processors=[LimitSpeed()], resources=[Rules()])
```

See [Resources](resources.md) for lifecycle and fork behavior.

## Call an LLM for each row

Use Daft's `prompt()` function when a processor needs an LLM call. Daft runs
the row work in parallel and returns a column you can persist like any other
state.

```python
from daft.functions import prompt


class Think(AsyncProcessor):
    components = (Agent,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_column(
            "agent__last_thought",
            prompt(
                "You are " + col("agent__name") + ". What should you do next?",
                model="gpt-5-mini",
            ),
        )
```

The component field is part of your history, so keep prompts and outputs small
enough for the storage and cost profile you want.

## Test a processor

Test the transformation with a representative DataFrame, then use a small
world-level test to verify component matching and priority. Do not mutate a
DataFrame in place; always return the DataFrame that should become the next
state.

For engine-level details, see [system execution](system-execution.md).

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
state. Set transport bounds explicitly rather than inheriting a provider's
long defaults.

```python
from daft.ai.openai.provider import OpenAIProvider
from daft.functions import prompt


class Think(AsyncProcessor):
    components = (Agent,)

    def __init__(self, provider: OpenAIProvider) -> None:
        self.provider = provider

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_column(
            "agent__last_thought",
            prompt(
                "You are " + col("agent__name") + ". What should you do next?",
                provider=self.provider,
                model="gpt-5-mini",
            ),
        )


provider = OpenAIProvider(timeout=15.0, max_retries=2)
world = runtime.world("demo", processors=[Think(provider)])
```

The component field is part of your history, so keep prompts and outputs small
enough for the storage and cost profile you want.

### Choose the failure policy

`prompt()` is lazy. A processor builds the expression, but the provider call
happens later when Archetype materializes the tick. The default policy is
fail-closed: once the provider's bounded retries are exhausted, a timeout,
HTTP 429, or other terminal error fails the whole tick. No archetype appends
and the tick remains available for retry.

That atomicity covers Archetype state, not the provider. Requests already sent,
tokens already billed, and any other external effects cannot be rolled back.
Retrying a failed tick can repeat them. Keep model calls side-effect-free from
the simulation's perspective, use provider idempotency controls when available,
and never treat a tick retry as exactly-once delivery to an external service.

For a deterministic whole-tick fallback, replace the failing processor and
retry the unchanged tick explicitly:

```python
from daft import lit
from openai import APITimeoutError, RateLimitError


class FallbackThought(AsyncProcessor):
    components = (Agent,)

    async def process(self, df: DataFrame, **_) -> DataFrame:
        return df.with_column("agent__last_thought", lit("provider_unavailable"))


try:
    await world.step()
except (APITimeoutError, RateLimitError):
    await world.remove_processor(Think)
    await world.add_processor(FallbackThought())
    await world.step()  # retries the same tick without another model request
```

Persist a source/status field when downstream logic must distinguish model
output from fallback state. Keep the fallback deterministic: do not introduce
a second unbounded network path while handling the first one.

The repository currently admits Daft 0.7.19. With that version, keep the
built-in OpenAI `prompt()` path fail-closed and do not pass its UDF
`on_error` option: the adapter incorrectly forwards that option to the OpenAI
request. The upstream separation fix landed in
[Daft #7277](https://github.com/Eventual-Inc/Daft/pull/7277) after the admitted
release. Per-row null/fallback policy should be adopted only after a release
containing that fix passes this repository's dependency gate; the coordinated
upgrade is tracked in [Archetype #442](https://github.com/VangelisTech/archetype/issues/442).

Finally, Archetype's command-gate "token" budget is an admission estimate for
commands such as `step` and `run`; it does not meter prompt tokens or provider
spend. Enforce real model budgets at the provider or deployment boundary. The
credential-free `llm_facing` capability eval exercises success, transport
timeout, HTTP 429, atomic failure, and explicit fallback against a loopback
OpenAI-compatible fixture.

## Test a processor

Test the transformation with a representative DataFrame, then use a small
world-level test to verify component matching and priority. Do not mutate a
DataFrame in place; always return the DataFrame that should become the next
state.

For engine-level details, see [system execution](system-execution.md).

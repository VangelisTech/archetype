---
title: Custom operations
description: How domain-specific behavior enters Archetype's exact operation registry
---

Archetype does not treat an arbitrary JSON payload as executable behavior.
The HTTP compatibility spelling `type="custom"` is rejected before durable
admission. There is no built-in custom no-op that can be authorized, queued,
and reported as applied without a domain effect.

Domain behavior enters through an exact, family-owned Pydantic operation and
one reviewed commands registration.

## Define meaning in the owning family

The operation model and behavior belong to the family whose state they change.
That family does not import `archetype.commands`.

```python
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict
from uuid_utils import UUID


class TriggerEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    direct_only: ClassVar[bool] = True

    operation: Literal["trigger_event"] = "trigger_event"
    world_id: UUID
    event_type: str


async def trigger_event(operation: TriggerEvent) -> None:
    ...
```

Use `direct_only = True` whenever the request includes callbacks, clients,
resources, credentials, or another live Python capability.

## Register governed entry at composition

The composition root imports the family model and behavior, then adds one
exact `OperationSpec`:

```python
OperationSpec(
    name="trigger_event",
    model=TriggerEvent,
    handler=trigger_event,
    permission="trigger_event",
    summarize=lambda op: {
        "operation": op.operation,
        "world_id": str(op.world_id),
    },
    quota_scope="live_world",
    world_key=lambda op: op.world_id,
    durable=None,
)
```

Registration validates the literal discriminator, exact model identity,
availability, quota coordinates, summarizer, and optional durability. It does
not guess from base classes or caller-provided names.

Adding a built-in operation also requires an explicit permission decision in
`archetype.commands.policy.PERMISSIONS_BY_ROLE`. Unknown permissions are denied
for every built-in role; admin does not automatically acquire them.

## Direct versus durable

Direct operations provide no `DurableOperation`. They enter through
`CommandDispatcher.apply` or `apply_as` and fail if passed to `defer`.

A durable operation must be portable and tick-local. Its registration adds:

```python
DurableOperation(
    decode=TriggerEvent.model_validate_json,
    materialize=materialize_trigger_event,
)
```

The materializer receives the actual already-locked `AsyncWorld`. It must call
family lock-held behavior directly, stage no separate commit, and never resolve
the world again. The model must set `direct_only = False`.

Durability metadata belongs in `DurableOptions`, not the family model:

```python
await dispatcher.defer_as(
    actor,
    operation,
    DurableOptions(target_tick=12, priority=0, max_attempts=3),
)
```

The scheduler canonicalizes the exact model, persists its identity, leases it
in ledger order, calls the registered materializer, and settles only with the
tick manifest.

## Safety checklist

Before adding an operation:

1. Name the owning family and normative behavior.
2. Use a frozen, extra-forbidden exact model with a literal discriminator.
3. Decide trusted and untrusted availability.
4. Add an explicit role permission and quota scope.
5. Keep access metadata bounded and free of payloads, credentials, diffs, and
   arbitrary results.
6. Mark it direct-only unless portability and retry behavior are proven.
7. For durable behavior, add canonical round-trip, actual-world
   materialization, retry, and manifest-settlement contracts.
8. Register it once at composition and update the registry coverage oracle.

See [Command gate](command-gate.md) and
[Durable commands](durable-commands.md).

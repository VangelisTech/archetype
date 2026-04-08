---
title: Custom Command Handlers
description: How to define and register custom command types in Archetype
---

The `CommandType.CUSTOM` type lets you extend the command system with domain-specific mutations. Custom commands flow through the same RBAC, priority queue, and audit trail as built-in commands.

## Submitting a Custom Command

Custom commands carry an arbitrary JSON payload:

```python
from archetype.app.models import Command, CommandType

cmd = Command(
    type=CommandType.CUSTOM,
    payload={
        "action": "trigger_event",
        "event_type": "explosion",
        "position": {"x": 10, "y": 20},
    },
)

await container.command_service.submit(world_id, cmd, ctx)
```

Via the REST API:

```bash
curl -X POST localhost:8000/worlds/{world_id}/commands \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "custom",
    "tick": 0,
    "payload": {
      "action": "trigger_event",
      "event_type": "explosion",
      "position": {"x": 10, "y": 20}
    }
  }'
```

## Default Behavior

By default, `CommandService.apply()` is a no-op for custom commands:

```python
# src/archetype/app/command_service.py
case CommandType.CUSTOM:
    pass  # No default handler
```

The command is still enqueued, RBAC-checked, drained at the correct tick, and logged to audit history — it just doesn't mutate world state unless you add a handler.

## Registering a Handler

To handle custom commands, subclass `CommandService` and override `apply()`:

```python
from archetype.app.command_service import CommandService
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_world import AsyncWorld


class GameCommandService(CommandService):
    """CommandService with domain-specific custom handlers."""

    async def apply(self, world: AsyncWorld, cmd: Command) -> None:
        if cmd.type == CommandType.CUSTOM:
            await self._dispatch_custom(world, cmd)
        else:
            await super().apply(world, cmd)

    async def _dispatch_custom(self, world: AsyncWorld, cmd: Command) -> None:
        action = cmd.payload.get("action")
        match action:
            case "trigger_event":
                await self._handle_trigger_event(world, cmd.payload)
            case "apply_buff":
                await self._handle_apply_buff(world, cmd.payload)
            case _:
                logger.warning(f"Unknown custom action: {action}")

    async def _handle_trigger_event(self, world, payload):
        # Your domain logic here
        ...

    async def _handle_apply_buff(self, world, payload):
        ...
```

Wire it into your `ServiceContainer` by replacing the default `CommandService`:

```python
container = ServiceContainer()
container.command_service = GameCommandService(container.broker)
```

## RBAC and Quotas

Custom commands are governed by the same rules as built-in commands:

| Aspect | Value |
|--------|-------|
| **Required role** | `player` or `admin` |
| **Token cost** | 10 tokens per command |
| **Per-tick limit** | 500 commands (shared across all types) |
| **Daily budget** | 200,000 tokens (shared across all types) |

See the [Token Costs and Quotas](token-quotas.md) guide for details.

## Command Flow

Custom commands follow the standard lifecycle:

```text
Client submits Command(type=CUSTOM, payload={...})
    |
    v
CommandService.submit()
    |
    v
CommandBroker.enqueue()
  - RBAC check (guardrail_allow)
  - Push to priority queue keyed by (tick, priority, seq)
  - Record in audit history
    |
    v
[Queued until target tick]
    |
    v
SimulationService.step()
  -> CommandService.drain_and_apply()
    -> broker.dequeue_due(world_id, tick)
    -> apply(world, cmd)  <-- your handler runs here
    -> broker.ack(cmd)
```

## Example: Domain-Specific Mutations

A game where agents can cast spells:

```python
# Submit a spell command
spell_cmd = Command(
    type=CommandType.CUSTOM,
    payload={
        "action": "cast_spell",
        "caster_id": 42,
        "spell": "fireball",
        "target_ids": [7, 13],
        "damage": 50,
    },
    tick=0,       # Execute immediately
    priority=-10, # High priority (lower = sooner)
)

await container.command_service.submit(world_id, spell_cmd, ctx)
```

Handle it in your custom service:

```python
async def _handle_cast_spell(self, world, payload):
    for target_id in payload["target_ids"]:
        # Read current HP, apply damage, write back
        await world.add_components(
            target_id,
            [DamageEvent(amount=payload["damage"], source=payload["spell"])],
        )
```

## Tips

- **Use `payload["action"]` as a sub-type discriminator.** This keeps a single `CUSTOM` type while supporting many domain actions.
- **Custom commands appear in broker history.** Use `container.broker._history[world_id]` for audit trails.
- **Tick scheduling works normally.** Set `tick=5` to defer execution until tick 5.
- **Priority ordering works normally.** Lower priority values execute first within a tick.

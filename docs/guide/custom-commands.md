# Custom Command Handlers

`CommandType.CUSTOM` is an extension point that lets you define domain-specific mutations without modifying the core command system. Use it when your simulation needs operations that don't map to built-in types (spawn, update, etc.).

## Defining a Custom Command

Custom commands use the same `Command` model as all other commands. Set `type=CommandType.CUSTOM` and put your domain data in `payload`:

```python
from archetype.app.models import Command, CommandType

# A custom "teleport" command
cmd = Command(
    type=CommandType.CUSTOM,
    payload={
        "action": "teleport",
        "entity_id": 42,
        "destination": {"x": 100.0, "y": 200.0},
    },
)
```

The `payload` is a free-form dict — define whatever schema fits your use case.

## Registering a Handler

`CommandService.apply()` has a `CUSTOM` case that does nothing by default. Override it by subclassing `CommandService`:

```python
from archetype.app.command_service import CommandService
from archetype.app.models import Command, CommandType
from archetype.core.aio import AsyncWorld


class GameCommandService(CommandService):
    async def apply(self, world: AsyncWorld, cmd: Command) -> None:
        if cmd.type == CommandType.CUSTOM:
            await self._handle_custom(world, cmd)
        else:
            await super().apply(world, cmd)

    async def _handle_custom(self, world: AsyncWorld, cmd: Command) -> None:
        action = cmd.payload.get("action")

        if action == "teleport":
            entity_id = cmd.payload["entity_id"]
            dest = cmd.payload["destination"]
            from archetype.core.component import Component

            class Position(Component):
                x: float = 0.0
                y: float = 0.0

            await world.add_components(entity_id, [Position(**dest)])

        elif action == "heal":
            entity_id = cmd.payload["entity_id"]
            amount = cmd.payload.get("amount", 10)
            # ... apply heal logic
            pass

        else:
            import logging
            logging.getLogger(__name__).warning(f"Unknown custom action: {action}")
```

## Wiring the Custom Service into a Container

Replace the default `CommandService` when building your `ServiceContainer`:

```python
from archetype.app.container import ServiceContainer
from archetype.app.broker import CommandBroker
from archetype.app.world_service import WorldService

class GameContainer(ServiceContainer):
    def _build_command_service(self) -> GameCommandService:
        return GameCommandService(self.broker, self.world_service)
```

Or construct it manually:

```python
from archetype.app.broker import CommandBroker
from archetype.app.world_service import WorldService
from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig, WorldConfig

broker = CommandBroker()
storage_service = StorageService()
world_service = WorldService(storage_service, broker)
command_service = GameCommandService(broker, world_service)
```

## Submitting Custom Commands

Custom commands require an actor with the `player` or `admin` role (the `player` role explicitly permits `custom`):

```python
from archetype.app.auth.models import ActorCtx
from uuid_utils import uuid7

ctx = ActorCtx(id=uuid7(), roles={"player"})

cmd = Command(
    type=CommandType.CUSTOM,
    payload={"action": "teleport", "entity_id": 42, "destination": {"x": 10.0, "y": 5.0}},
)
await command_service.submit(world.world_id, cmd, ctx)
```

If you submit with a role that doesn't include `custom` (e.g., `viewer` or `coder`), `guardrail_allow()` raises `PermissionError`.

## Dispatching from a Processor

Processors can also emit custom commands by enqueuing them via the broker in `Resources`:

```python
from archetype.app.broker import CommandBroker
from archetype.app.models import Command, CommandType
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from daft import DataFrame


class Health(Component):
    current: int = 100


class RespawnProcessor(AsyncProcessor):
    """Emits a custom 'respawn' command for any entity that has died."""

    components = (Health,)
    priority = 50

    async def process(self, df: DataFrame, resources=None, tick: int = 0, **kwargs) -> DataFrame:
        broker = resources.get(CommandBroker) if resources else None
        world_id = kwargs.get("world_id")
        if broker is None or world_id is None:
            return df

        dead = df.where(df["health__current"] <= 0).collect().to_pylist()
        for row in dead:
            cmd = Command(
                type=CommandType.CUSTOM,
                tick=tick + 1,
                payload={"action": "respawn", "entity_id": row["entity_id"]},
            )
            await broker.enqueue(world_id, cmd)

        return df
```

## Token Cost

Custom commands have a token cost of **10** (same as `spawn`). This counts against the actor's daily budget of 200,000 tokens. See [Token Costs and Quotas](./token-quotas.md) for details.

## Example Use Cases

| Action | Payload fields | Notes |
|--------|---------------|-------|
| `teleport` | `entity_id`, `destination` | Bypasses normal movement physics |
| `respawn` | `entity_id`, `spawn_point` | Reset entity state at a checkpoint |
| `grant_power_up` | `entity_id`, `power`, `duration` | Domain-specific buff system |
| `trigger_event` | `event_type`, `data` | Game-event bus integration |
| `sync_external` | `external_id`, `state` | Sync from an external system |

Custom commands shine when you need mutations that are:
- Too domain-specific to belong in the core command set
- Driven by external systems (game servers, IoT, external APIs)
- Part of a plugin or extension that should not modify core code

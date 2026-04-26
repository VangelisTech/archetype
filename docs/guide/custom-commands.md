---
title: Custom Commands
description: How to route domain-specific commands through Archetype
---

`CommandType.CUSTOM` is the escape hatch for domain-specific command payloads. Custom commands still pass through the command gate, so role permissions, quotas, queue ordering, and audit semantics are the same as built-in commands.

## Submitting a Custom Command

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

await container.command_service.submit(ctx, world_id, cmd)
```

Via REST:

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

The default deferred dispatcher does not mutate world state for unknown custom payloads. The command can still be authorized, queued, drained, and audited; domain behavior requires an application-specific dispatcher.

## Extension Pattern

The stable extension point is the app service layer:

1. Add or reuse a `CommandType`.
2. Decide role permissions in `COMMANDS_BY_ROLE`.
3. Add a gated method to `iCommandService` when the operation is user-visible and should be direct.
4. For tick-deferred custom payloads, add dispatch logic in the command application path used by `drain_and_apply`.
5. Emit one audit row per gated call.

Avoid bypassing the gate from runtime/API code. If the operation is external, route it through `iCommandService`.

## RBAC and Quotas

`CUSTOM` is permitted for `player`, `operator`, and `admin` by default. Custom command cost defaults to 10 tokens unless the guard cost table says otherwise.

See [Command Gate](command-gate.md) and [Token Costs and Quotas](token-quotas.md).

## Command Flow

```text
Client submits Command(type=CUSTOM, payload={...})
    |
iCommandService.submit(ctx, world_id, cmd)
    |
guardrail_allow
    |
iCommandBroker.enqueue
    |
SimulationService.step
    |
iCommandService.drain_and_apply
    |
custom dispatch path
    |
iAuditLog.record
```

## Example: Domain Action Payload

```python
spell_cmd = Command(
    type=CommandType.CUSTOM,
    payload={
        "action": "cast_spell",
        "caster_id": 42,
        "spell": "fireball",
        "target_ids": [7, 13],
        "damage": 50,
    },
    tick=0,
    priority=-10,
)

await container.command_service.submit(ctx, world_id, spell_cmd)
```

Use `payload["action"]` as a sub-type discriminator when one `CUSTOM` command type supports many domain actions.

User-facing history comes from `iCommandService.get_audit_history(...)`, not from broker internals.

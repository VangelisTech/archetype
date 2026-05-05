# Command Gate

**Document type:** Normative.
**Scope:** `iCommandService`, `app/auth/`, role-based authorization.

## 1. The gate model

`iCommandService` is the **policy enforcement point** (PEP). Every external mutation, lifecycle operation, and read flows through it. The gate is a thin reverse proxy: it does not orchestrate, it does not implement; it gates.

Each method on `iCommandService` follows the same three-step shape:

```python
async def <method>(self, ctx: ActorCtx, *args, **kwargs):
    guardrail_allow(<Command(type=..., payload=...)>, ctx)
    result = await self._<underlying_service>.<method>(*args, **kwargs)
    await self._audit.record(audit_row_from(ctx, ..., result))
    return result
```

The gate's contract:

- `ctx` is checked against `COMMANDS_BY_ROLE` before any work happens. Empty intersection raises `GuardrailError`.
- The work is delegated to a single underlying service (`iWorldService`, `iSimulationService`, `iQueryService`, `iMutationService`, or `iAuditLog`).
- One audit row is emitted per gated call. Multi-step gate methods (e.g. `destroy_world`, which orchestrates broker.clear + audit.flush + world_service.destroy_world) still emit ONE audit row at the end.

Nothing below the gate knows about `ActorCtx`. `iWorldService.create_world(config, ...)` takes no ctx. Authorization is the gate's job alone.

## 2. The four-role model

Roles, intent, and use cases:

| Role | Intent | Use case |
|---|---|---|
| `viewer` | Read-only | Dashboards, monitoring, audit |
| `player` | Participate as an actor in a simulation | Multi-agent worlds, game-style frontends |
| `operator` | Run and manage simulations | Researcher, autoresearch agent, ops |
| `admin` | Unrestricted | Platform owner, runtime default |

Roles are flat. A user with `{operator}` is NOT also `viewer` — they get whatever is in the set. To grant viewer + operator, set `{viewer, operator}`.

## 3. The permissions matrix

✓ = allowed; — = denied.

### Reads (information only; no state change)

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `query_archetype` | ✓ | ✓ | ✓ | ✓ |
| `list_signatures` | ✓ | ✓ | ✓ | ✓ |
| `get_world_info` | ✓ | ✓ | ✓ | ✓ |
| `get_audit_history` | ✓ | ✓ | ✓ | ✓ |
| `list_worlds` | ✓ | ✓ | ✓ | ✓ |
| `list_processors` | ✓ | ✓ | ✓ | ✓ |
| `list_hooks` | ✓ | ✓ | ✓ | ✓ |
| `list_resources` | ✓ | ✓ | ✓ | ✓ |

### Entity mutations

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `create_entity` (spawn) | — | ✓ | ✓ | ✓ |
| `remove_entity` (despawn) | — | ✓ | ✓ | ✓ |
| `update` (overlay values) | — | ✓ | ✓ | ✓ |
| `add_components` (extend schema) | — | — | ✓ | ✓ |
| `remove_components` | — | — | ✓ | ✓ |

`player` mutates entity values and creates / destroys entities, but cannot change the *schema* (component types). Schema changes affect processor matching; that's an operator concern.

### Processor / hook / resource management

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `add_processor` | — | — | ✓ | ✓ |
| `remove_processor` | — | — | ✓ | ✓ |
| `add_hook` | — | — | ✓ | ✓ |
| `remove_hook` | — | — | ✓ | ✓ |
| `add_resource` | — | — | ✓ | ✓ |

Processors define behavior. Hooks observe lifecycle. Resources hold shared state. All three are operator-territory.

### Simulation control

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `step` | — | — | ✓ | ✓ |
| `run` | — | — | ✓ | ✓ |
| `run_episode` | — | — | ✓ | ✓ |
| `run_rollout` | — | — | ✓ | ✓ |

A `player` does not advance the world — they participate in the world that something else is advancing.

### World lifecycle

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `create_world` | — | — | — | ✓ |
| `fork_world` | — | — | ✓ | ✓ |
| `destroy_world` | — | — | ✓ | ✓ |

The asymmetry is intentional. `create_world` establishes new platform-level identity → admin-only. `fork_world` and `destroy_world` are operator-territory because operators routinely fork (rollouts) and destroy (cleanup). Forks and destroys never delete persistent data (append-only invariant), so this is safe.

### Generic submission

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `submit` | — | ✓ | ✓ | ✓ |
| `submit_batch` | — | ✓ | ✓ | ✓ |
| `submit_spawn` | — | ✓ | ✓ | ✓ |
| `message` (CommandType.MESSAGE) | — | ✓ | ✓ | ✓ |
| `custom` (CommandType.CUSTOM) | — | ✓ | ✓ | ✓ |

Generic submission is allowed for `player` and up; the underlying command type re-enforces its own role check at apply time.

## 4. Implementation

### 4.1 — `COMMANDS_BY_ROLE` constant

`app/auth/permissions.py` exports the authoritative role-permission constant. Authored role-by-role with set union for inheritance:

```python
# app/auth/permissions.py
from archetype.app.models import CommandType

_READS = frozenset({
    CommandType.QUERY_WORLD,
    CommandType.GET_WORLD_INFO,
    CommandType.GET_AUDIT_HISTORY,
    CommandType.LIST_SIGNATURES,
    CommandType.LIST_WORLDS,
    CommandType.LIST_PROCESSORS,
    CommandType.LIST_HOOKS,
    CommandType.LIST_RESOURCES,
})

_PLAYER_ADDS = frozenset({
    CommandType.SPAWN,
    CommandType.DESPAWN,
    CommandType.UPDATE,
    CommandType.MESSAGE,
    CommandType.CUSTOM,
})

_OPERATOR_ADDS = frozenset({
    CommandType.ADD_COMPONENT,
    CommandType.REMOVE_COMPONENT,
    CommandType.ADD_PROCESSOR,
    CommandType.REMOVE_PROCESSOR,
    CommandType.ADD_HOOK,
    CommandType.REMOVE_HOOK,
    CommandType.ADD_RESOURCE,
    CommandType.STEP,
    CommandType.RUN,
    CommandType.RUN_EPISODE,
    CommandType.RUN_ROLLOUT,
    CommandType.FORK_WORLD,
    CommandType.DESTROY_WORLD,
})

COMMANDS_BY_ROLE: dict[str, frozenset[CommandType]] = {
    "viewer":   _READS,
    "player":   _READS | _PLAYER_ADDS,
    "operator": _READS | _PLAYER_ADDS | _OPERATOR_ADDS,
    "admin":    frozenset(CommandType),
}
```

`admin` is `frozenset(CommandType)` — it auto-includes new CommandTypes as the enum grows. Other roles are explicit; new commands require an explicit choice about whether `viewer`, `player`, or `operator` get them.

### 4.2 — `guardrail_allow`

```python
def guardrail_allow(cmd: Command, ctx: ActorCtx) -> None:
    if not any(cmd.type in COMMANDS_BY_ROLE[r] for r in ctx.roles):
        raise GuardrailError(
            f"role(s) {sorted(ctx.roles)} cannot execute {cmd.type.value}"
        )
```

Set membership over up to four roles. Negligible cost.

### 4.3 — Optional inverse index

For auditing or lookup-keyed-by-command, derive the inverse once at module load:

```python
ROLES_BY_COMMAND: dict[CommandType, frozenset[str]] = {
    cmd: frozenset(role for role, cmds in COMMANDS_BY_ROLE.items() if cmd in cmds)
    for cmd in CommandType
}
```

`COMMANDS_BY_ROLE` is the source of truth. `ROLES_BY_COMMAND` is computed.

### 4.4 — Test pattern

Tests parametrize from `COMMANDS_BY_ROLE` itself; adding a new CommandType produces test cases automatically:

```python
def _matrix_cases():
    return [
        (role, cmd, cmd in COMMANDS_BY_ROLE[role])
        for role in COMMANDS_BY_ROLE
        for cmd in CommandType
    ]

@pytest.mark.parametrize("role,cmd_type,allowed", _matrix_cases())
def test_role_command_matrix(role, cmd_type, allowed):
    ctx = ActorCtx(id=uuid7(), roles={role})
    cmd = Command(type=cmd_type, payload={})
    if allowed:
        guardrail_allow(cmd, ctx)
    else:
        with pytest.raises(GuardrailError):
            guardrail_allow(cmd, ctx)
```

## 5. Default `ActorCtx` for the runtime

The runtime's default `ActorCtx` is `ActorCtx(id=uuid7(), roles={"admin"})`.

Rationale: the script boundary IS the platform admin in single-tenant context. `runtime.world(...)` calls `create_world`, which is admin-only — without admin in the default, the very first ergonomic call fails.

Users testing under constrained roles do so explicitly:

```python
async with ArchetypeRuntime() as runtime:
    world = runtime.world("demo")              # default {admin}

    viewer_ctx = ActorCtx(id=uuid7(), roles={"viewer"})
    viewer_world = world.as_actor(viewer_ctx)

    await viewer_world.query(Position)         # OK
    await viewer_world.spawn(Position())       # raises GuardrailError
```

This is also how multi-tenant API servers map authenticated principals to ActorCtx.

## 6. Audit emission

Every gated call emits one audit row. The audit row schema is defined in [Audit Log](audit-log.md); fields include:

- `command_id`, `world_id`, `tick`, `actor_id`, `actor_roles`
- `command_type`, `payload_json`, `idempotency_key`
- `accepted_at`, `applied_at`, `status`
- Payment-bearing columns (nullable): `signer_address`, `tx_hash`, `price_paid_atomic`, `asset`, `chain_id`

Multi-step gate methods (e.g. `destroy_world` orchestrating broker.clear + audit.flush + world_service.destroy_world) emit ONE audit row, not one per step. The row's `payload_json` captures sub-operation outcomes.

`run_rollout` emits ONE row, not one per fork. The row's payload captures the fork world_ids and aggregate stats.

## 7. Migration from earlier role sets

| Old role | New role |
|---|---|
| `viewer` | `viewer` (unchanged) |
| `player` | `player` (unchanged) |
| `coder` | `operator` (folded in) |
| `maintainer` | `operator` (folded in; was redundant) |
| `operator` | `operator` (unchanged) |
| `admin` | `admin` (unchanged) |

Existing code that constructs `ActorCtx(roles={"maintainer"})` should be updated to `{"operator"}`. Same for `{"coder"}`.

## 8. Future extensions (out of scope for v1)

- **Per-resource ACLs.** Per-world view scoping. Today: roles are global per-runtime.
- **Role hierarchy.** Today: flat sets, explicit composition.
- **Custom roles.** Products may extend `COMMANDS_BY_ROLE` with new keys.
- **Quota differentiation.** Per-tick command quotas key off `ctx.id`, not role. Quotas-by-role is a v2 addition.
- **Payment-aware role.** A `paid` role indicates the request was paid for via x402. Composes with other roles. See platform spec § 3.3.

# Command Gate

**Document type:** Normative.
**Scope:** `iCommandGateway`, `app/gateway/auth/`, role-based authorization.

The concrete class and protocol are `CommandGateway` and `iCommandGateway`.

## 1. The gate model

`iCommandGateway` is the **policy enforcement point** (PEP) for untrusted
ingress. External mutations, lifecycle operations, reads, and deferred-command
admission flow through it. The gateway owns authorization, safe result
downgrade, and access-audit notification. It delegates actor-free execution to
`iRuntimeApplication`.

Each direct method on `iCommandGateway` follows the same three-step shape:

```python
async def <method>(self, ctx: ActorCtx, *args, **kwargs):
    guardrail_allow(<Command(type=..., payload=...)>, ctx)
    admission = principal_snapshot(ctx)
    result = await self._application.<method>(*args, admission=admission, **kwargs)
    await self._audit.record_access(access_event_from(ctx, ..., result))
    return result
```

The gate's contract:

- `ctx` is checked against `COMMANDS_BY_ROLE` before any work happens. Empty intersection raises `GuardrailError`.
- World-scoped quota coordinates are resolved only after authorization. Durable
  world operations use the live target tick when available and the explicit
  `(world_id, 0)` bucket when the live binding is missing or raises the
  family-owned `WorldClosingError`. Other resolver failures propagate without
  quota, application, or audit effects.
- Work is delegated to the actor-free application port; the gateway does not
  compose domain services or own command/audit storage.
- One access event is emitted per admitted or rejected external call. Domain
  receipts and transactional outbox events remain the authority for operation
  outcomes.

Nothing below the gateway knows about `ActorCtx`.
`iWorldLifecycle.create_world(config, ...)` takes no ctx. Authorization is the
gateway's job alone.

The durable-world quota rule applies uniformly to `destroy_world`,
`open_world_readonly`, `resume_world`, `query_components`, `query_archetype`,
world-scoped `list_signatures`, world-scoped `get_audit_history`,
`query_artifacts`, and `evaluate`. The fallback selects only a quota bucket; it
does not grant a live operation lease or suppress lifecycle errors raised later
by the actor-free application workflow.

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
| `query_artifacts` | ✓ | ✓ | ✓ | ✓ |
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
| `autoresearch` | — | — | ✓ | ✓ |

A `player` does not advance the world — they participate in the world that something else is advancing.

### World lifecycle

| Method | viewer | player | operator | admin |
|---|---|---|---|---|
| `create_world` | — | — | — | ✓ |
| `fork_world` | — | — | ✓ | ✓ |
| `destroy_world` | — | — | ✓ | ✓ |
| `publish` | — | — | ✓ | ✓ |
| `ingest_files` | — | — | ✓ | ✓ |
| `write_artifacts` | — | — | ✓ | ✓ |
| `evaluate` | — | — | ✓ | ✓ |

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
    CommandType.AUTORESEARCH,
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

## 5. Trust boundary and `ActorCtx`

`ActorCtx` is constructed only by an authenticated ingress adapter or a focused
gateway/security test. The trusted scripting runtime is actor-free and does not
call the gateway.

The CLI sends credentials; it does not construct a trusted role locally.
FastAPI or another host authenticates the credential into a stable principal,
constructs `ActorCtx`, and invokes the gateway. Production hosts must fail
closed when authentication is absent. A development-only anonymous-admin mode,
if retained, is an explicit host configuration and uses a stable process
principal rather than a new identity per request.

An embedded host that exposes runtime capabilities to sandboxed or untrusted
agents must also use the gateway even when no HTTP transport is involved.

## 6. Access audit

Every gated call emits one access event. The event schema is defined in [Audit
Log](audit-log.md); fields include:

- `command_id`, `world_id`, `actor_id`
- `command_type`, `payload_json`, `idempotency_key`
- `accepted_at`, `applied_at`, `status`

Access evidence records the authorization decision and request identity. It is
not proof that asynchronous or multi-step domain work committed. Command
outcomes, tick manifests, artifact indexes, evaluation results, and research
ledgers are authoritative for their own workflows. Their transactional outbox
events are projected into the analytical audit history independently.

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

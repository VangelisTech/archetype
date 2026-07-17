# Command Broker

`CommandBroker` is the priority queue for tick-deferred commands. It is not the policy enforcement point and it is not the durable audit log.

External callers reach the broker only through `iCommandService.submit(...)`, `submit_batch(...)`, or `submit_spawn(...)`. The gate checks authorization before commands are enqueued.

```text
External caller
    |
iCommandService.submit(ctx, world_id, cmd)
    |  guardrail_allow(cmd, ctx)
    |  audit accepted/queued state as required
    v
iCommandBroker.enqueue(world_id, cmd)
    |
SimulationService.step()
    |
iCommandService.drain_and_apply(world_id, tick)
    |
MutationService / WorldService
```

For RBAC, roles, and audit emission, see [Command Gate](command-gate.md) and [Audit Log](audit-log.md).

## Priority Queue

The broker maintains one priority queue per world, keyed by `str(world_id)`.
Diagnostic history is also partitioned per world and retained in a bounded
ring buffer. `max_history` configures the number of recent commands retained
for each world; it defaults to 50,000.

Commands are ordered by `(tick, priority, seq)`:

- `tick`: future commands remain queued until `dequeue_due(world_id, tick)`.
- `priority`: lower values execute first.
- `seq`: creation-order tie breaker.

The `Command.__lt__` method defines this ordering.

## Queue Methods

| Method | Behavior |
|---|---|
| `enqueue(world_id, cmd)` | Store one command |
| `enqueue_bulk(world_id, cmds)` | Store multiple commands atomically |
| `dequeue_due(world_id, tick)` | Pop commands where `cmd.tick <= tick` |
| `dequeue(world_id)` | Pop pending commands regardless of tick |
| `peek(world_id)` | Return pending commands without removing them |
| `ack(cmd_ids)` | Mark applied commands complete |
| `remove(world_id, cmd_id)` | Remove a pending command; preserve history/introspection |
| `get_pending_count(world_id=None)` | Queue depth |
| `get_history(world_id, limit=100)` | Queue history, not audit history |
| `clear(world_id=None)` | Clear pending queue state |

`get_history` is useful for queue diagnostics. User-facing history uses `iAuditLog` through `iCommandService.get_audit_history(...)`.

## Concurrency

Broker implementations must be safe for concurrent enqueue/dequeue from multiple coroutines. Bulk enqueue is all-or-nothing at the queue layer; the gate has already authorized the commands before enqueue.

## Processor Access

Processors are trusted internal code once registered. If a processor needs delayed command scheduling, it may enqueue through a broker resource or another sanctioned internal path. Those internally scheduled commands are not a substitute for external gate enforcement.

When user-facing accountability matters, route the operation through `iCommandService` so it receives authorization and audit semantics.

## Command Model

`Command` is a frozen Pydantic model with fields used by queuing and dispatch:

| Field | Purpose |
|---|---|
| `id` | command identity |
| `tick` | target tick |
| `actor_id` | actor identity for audit/accountability |
| `type` | command type used by gate permissions and dispatch |
| `payload` | command-specific data |
| `priority` | ordering within a tick |
| `seq` | FIFO tie breaker |

See [Service Protocols](service-protocols.md) for the broker protocol.

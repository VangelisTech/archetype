# Audit Log

**Document type:** Normative.
**Scope:** `iAuditLog`, `AuditRow`, and user-facing history reads through `iCommandService`.

## 1. Purpose

The audit log is the append-only record of gated operations. It is distinct from broker queue history:

- `iCommandBroker` stores, orders, and yields pending commands for the tick-deferred path.
- `iAuditLog` records accepted-and-applied gated operations for user-facing history and accountability.

Runtime history reads call `iCommandService.get_audit_history(...)`, which delegates to `iAuditLog.query(...)`. The runtime does not keep a separate history.

## 2. Append-only invariant

`iAuditLog` has no `drop_*` or `delete_*` methods. Implementations MUST NOT delete audit rows.

Destroying a world does not delete audit rows. Fork cleanup after a rollout does not delete audit rows. Audit rows remain queryable after the live world object has been removed from the registry.

## 3. Audit unit

Every gated call emits one audit row.

Multi-step gate methods still emit exactly one audit row:

- `destroy_world` records one row for the lifecycle cleanup.
- `run_rollout` records one row for the rollout call, not one row per fork.
- Runtime activation emits one row per gated activation step: create world, each staged processor, each staged resource, and each staged hook.

The row payload captures sub-operation outcomes when one gated method performs multiple internal actions.

## 4. Row Schema

`AuditRow` is defined in `app/models.py`.

Required identity and operation fields:

- `audit_id`
- `command_id`
- `world_id`
- `actor_id`
- `command_type`
- `status`
- `payload_json`
- `accepted_at`
- `applied_at`

Payment-bearing fields are nullable:

- `signer_address`
- `tx_hash`
- `price_paid_atomic`
- `asset`
- `chain_id`
- `idempotency_key`

## 5. Query Contract

`iAuditLog.query(...)` is read-only and supports filters for:

- `world_id`
- `tick_range`
- `actor_id`
- `signer_address`
- `idempotency_key`
- `limit`

External callers do not call `iAuditLog` directly. They use `iCommandService.get_audit_history(ctx, ...)`, which applies the read permission check before delegating.


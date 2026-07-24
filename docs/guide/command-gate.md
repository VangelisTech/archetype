# Command gate

**Document type:** Normative.
**Scope:** commands-owned `OperationRegistry`, `CommandDispatcher`, `Policy`,
`ActorCtx`, role authorization, quotas, and bounded access evidence.

The top-level `archetype.commands` family is the policy and admission
authority. Trusted runtime and authenticated API adapters construct the same
family operation models and enter one `CommandDispatcher`; neither owns
policy, counters, queue state, or audit storage.

## 1. The gate model

The dispatcher exposes four primary modes:

```text
apply(operation)                         trusted direct
apply_as(actor, operation)               actor-aware direct
defer(operation, options)                trusted durable
defer_as(actor, operation, options)      actor-aware durable
```

Batch and reserved-spawn variants preserve the same split. Each path begins
under one admission barrier and resolves the exact operation model through the
registry.

For an actor-aware direct call, the order is:

1. exact model registration;
2. pure role preauthorization;
3. trusted/untrusted availability;
4. bounded world/tick and token-cost coordinates;
5. instance-owned quota policy;
6. the registered family handler; and
7. bounded advisory evidence.

Pure role denial happens before world lookup, clock access, quota debit,
scheduler admission, family effects, or access evidence. This prevents an
unauthorized caller from using error or evidence differences to enumerate
worlds. Availability rejection and full-policy denial may emit bounded
rejection evidence, but never copy the operation payload.

For actor-aware durable entry, preauthorization and exact durable eligibility
precede catalog persistence. Deferred coordinates come from
`DurableOptions.target_tick`; the path never consults the live target-tick
resolver. A direct-only or trusted-only operation fails before persistence.

Trusted `apply` and `defer` use the same registrations and family behavior but
do not fabricate `ActorCtx`, authorization, principal, or access-decision
evidence.

## 2. Exact registration

Every governed operation has one `OperationSpec` containing:

- exact Pydantic model type and discriminator name;
- exact family handler;
- permission string;
- quota scope;
- optional bounded world-key extractor;
- trusted/untrusted availability;
- bounded metadata summarizer and token cost; and
- optional durable decoder/materializer.

There is no MRO guessing or generic command-type fallback. Duplicate names and
models fail construction. The registered permission—not a caller-selected
envelope value—is the policy input.

The registry contains the exact world lifecycle, mutation, simulation,
composition, and read models plus registered workflow-family models.
`GetAuditHistory` is the commands-owned boundary read. Actor-aware
availability is explicit per registration; trusted-only workflow operations
cannot enter through untrusted transport.

## 3. Four roles and permissions

Roles are stable flat grants. When an actor carries multiple roles, a
permission is allowed if any grant contains it. The built-in role sets are
explicit and versioned in `PERMISSIONS_BY_ROLE`.

| Role | Added permissions |
|---|---|
| `viewer` | `get_world_info`, `list_worlds`, `discover_worlds`, `open_world_readonly`, `query_components`, `query_archetype`, `list_signatures`, `get_audit_history`, `list_processors`, `list_hooks`, `list_resources`, `query_artifacts` |
| `player` | all viewer permissions plus `spawn`, `create_entities`, `despawn`, `update` |
| `operator` | all player permissions plus `add_components`, `remove_components`, `add_processor`, `remove_processor`, `fork_world`, `destroy_world`, `step`, `run`, `run_episode`, `run_rollout`, `add_resource`, `add_hook`, `remove_hook`, `autoresearch`, `ingest_artifacts`, `evaluate` |
| `admin` | all operator permissions plus `create_world`, `resume_world` |

The asymmetries are intentional:

- players may mutate entity values but not schemas or runtime behavior;
- operators may run, fork, and clean up worlds but cannot create or resume
  platform identities;
- admins own world creation and mutable resume;
- internal `reserve_entity_ids` and `spawn_reserved` registrations reuse the
  `spawn` permission but are not exposed to actor-aware generic dispatch.

Adding an operation requires an explicit registration and an explicit role
decision. An admin role does not gain an unknown permission automatically.

## 4. Instance-owned quotas

`Policy` owns all mutable authorization state. There are no module-global
counters and no reset hook.

World/tick command counters key on:

```text
(actor_id, world_id, target_tick)
```

This isolates actors, worlds, ticks, and policy instances. The default maximum
is 500 commands per coordinate. Batch authorization validates every request
and projected debit before mutating any counter.

Daily token budgets key separately by actor and roll at the UTC date boundary.
The default maximum is 200,000 tokens. Application-scoped operations debit
only that daily budget and do not invent a pseudo world/tick coordinate.

Registry quota scopes are:

- `application`: no world key or tick bucket;
- `live_world`: resolve the current live target tick, propagating lifecycle
  failures; and
- `durable_world`: use the live tick when available, or the reviewed tick-zero
  bucket when the live binding is absent, closing, or durably missing.

The durable-world fallback selects only a quota coordinate. It does not grant
a world lease or suppress the later family error.

`Policy.preauthorize` is pure. Only `authorize`,
`authorize_application`, or `authorize_batch` may debit quotas.

## 5. Atomic actor-aware batches

An actor-aware batch follows ordered phases:

1. resolve every exact registration;
2. preauthorize every permission;
3. verify every untrusted/durable disposition;
4. derive every bounded coordinate and policy request;
5. apply one atomic policy batch;
6. make one same-world scheduler admission; and
7. emit one bounded result row per item.

A denial, invalid member, mixed-world batch, identity conflict, or persistence
failure leaves no partial policy or catalog admission.

## 6. Reserved spawn authority

Callers submit a family `Spawn` model. `defer_spawn` and `defer_spawn_as`
reserve exactly one entity ID through the scheduler, transform it into the
internal `SpawnReserved` model, and bind the reservation to the command
identity.

Role authorization and durable eligibility happen before reservation. An
identical retry reuses the retained reservation, including after caller
cancellation or a failed first catalog write. A conflicting retry fails rather
than allocating another ID.

## 7. Access evidence

Actor-aware allowed, quota-denied, availability-rejected, queued, and failed
calls may produce an `AccessSummary`. It contains only operation, actor,
optional world, decision, outcome, and allowlisted bounded scalar metadata.
The canonical encoded row is limited to 4096 bytes.

Evidence never includes:

- component values or arbitrary results;
- credentials, callbacks, or storage configuration;
- repository diffs or task-base revisions;
- validator output, critic findings, or cleanup state; or
- exception messages.

Evidence construction and storage are advisory. Their failure cannot replace
the primary operation result. Durable command state and family receipts remain
authoritative. See [Audit log](audit-log.md).

## 8. Admission shutdown

`CommandDispatcher.stop_admission` atomically rejects new top-level work.
Operations admitted before that point retain their active count.
`wait_drained` completes only after all of them exit.

There is no context-variable or inherited-task bypass. Compound family
workflows call private sibling behavior under one admitted operation rather
than recursively entering public dispatcher admission.

Destroy adds a world-local closing barrier around catalog admission. A submit
racing destroy is either admitted before terminal cancellation or rejected
after closing begins.

## 9. Trust boundary and `ActorCtx`

`ActorCtx` contains a stable principal identity and its role grants. Only an
authenticated ingress adapter or focused security test constructs it. The
trusted Python runtime is actor-free and never calls an actor-aware entry point.

The CLI sends credentials; it does not mint local roles. FastAPI or another
host authenticates those credentials, constructs `ActorCtx`, and invokes the
actor-aware dispatcher. An embedded host exposing capabilities to sandboxed or
untrusted code must use the same actor-aware dispatcher boundary even without
HTTP.

## Executable contracts

- `tests/commands/test_dispatch_policy_contracts.py`
- `tests/commands/test_integration_contracts.py`
- `tests/app/test_auth.py`
- `tests/app/test_permissions.py`
- `tests/app/test_tick_quota_reset.py`
- `tests/integration/test_command_flow.py`

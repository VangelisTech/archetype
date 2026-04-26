# Specifications Alignment Review

This is a review checklist for wiring the new normative guide documents into the docs index and bringing the rest of the docs back into agreement with them.

New specification documents:

- `command-gate.md`
- `execution-hierarchy.md`
- `runtime.md`
- `service-protocols.md`
- `world-lifecycle.md`

## Proposed Index Change

Add a dedicated `Specifications` group in `mkdocs.yml` and move the existing umbrella specification out of `Getting Started`.

Proposed nav shape:

```yaml
nav:
  - Getting Started:
      - archetype-ecs: index.md
      - Quickstart: guide/quickstart.md
      - Examples: guide/examples.md
      - Contributing Guide: guide/contributing.md
  - Specifications:
      - Overview: guide/specification.md
      - Runtime: guide/runtime.md
      - Service Protocols: guide/service-protocols.md
      - Command Gate: guide/command-gate.md
      - Execution Hierarchy: guide/execution-hierarchy.md
      - World Lifecycle: guide/world-lifecycle.md
  - Guides:
      ...
```

Also update `docs/index.md` so the landing page points readers to the new `Specifications` group when they need normative behavior, not only to the older guide pages.

## Source Specification Fixes First

Before updating dependent docs, clean up a few inconsistencies in the new spec set itself:

- `command-gate.md` permission-code example should include the command types shown in its own tables: `ADD_HOOK`, `REMOVE_HOOK`, `STEP`, and `RUN`.
- `command-gate.md` and `runtime.md` mention `add_hook` / `remove_hook`, but `service-protocols.md` does not list `iCommandService.add_hook`, `iCommandService.remove_hook`, `iWorldService.add_hook`, or `iWorldService.remove_hook`.
- `runtime.md` says pre-activation `add_hook` calls raise, but the listed ergonomic surface includes post-activation hook methods. The final desired runtime hook API needs to be stated once and then reflected everywhere.
- `service-protocols.md` says `CommandService` construction receives `audit`, while current surrounding docs often describe a broker-history model. Decide whether the docs should describe implemented state or the new target contract, then use that consistently.
- `command-gate.md`, `service-protocols.md`, and `world-lifecycle.md` refer to a separate audit-log specification. Add that spec or replace the reference with the current `AuditRow` location.
- Confirm whether `update` should gate as `CommandType.UPDATE` or reuse `ADD_COMPONENT`; the new command-gate matrix treats `update` and `add_components` as distinct user intents.

## Major Consistency Themes

### 1. Gate Is `iCommandService`, Not `CommandBroker`

The new contract says `iCommandService` is the policy enforcement point for every external mutation, lifecycle operation, and read. Older docs repeatedly call `CommandBroker` the choke point and place RBAC, quotas, and audit history there.

Change older docs to say:

- External callers use `iCommandService`.
- `iCommandService` does `guardrail_allow -> delegate -> audit.record`.
- `iCommandBroker` is a pure priority queue for the tick-deferred path.
- Direct runtime/API operations can be gated and applied immediately without entering the broker.
- Broker history is not the audit log.

Affected docs:

- `architecture.md`
- `app-overview.md`
- `services.md`
- `broker.md`
- `data-flow.md`
- `api-layer.md`
- `custom-commands.md`
- `examples.md`
- `token-quotas.md`
- `docs/index.md`
- README and agent guidance outside `docs/guide` after guide docs are settled

### 2. Reads Are Gated at the Boundary

Older docs say `QueryService` reads require no `ActorCtx`, no RBAC, and no broker. That is still true only below the gate. User-visible reads now go through `iCommandService` and require a read permission.

Change older docs to distinguish:

- Internal service: `iQueryService` has no `ActorCtx`.
- External/runtime/API surface: reads call `iCommandService.query_archetype`, `list_signatures`, `get_world_info`, `get_audit_history`, etc.
- `viewer` is a real role for gated read methods.

Affected docs:

- `data-flow.md`
- `app-overview.md`
- `services.md`
- `architecture.md`
- `api-layer.md`
- `docs/index.md`

### 3. Roles Collapse to Four Flat Roles

The new model is `viewer`, `player`, `operator`, `admin`. `coder` and `maintainer` are folded into `operator`.

Change older docs to:

- Remove `coder` and `maintainer` from current role tables.
- State that roles are flat, not hierarchical.
- Use `operator` for schema, processor, hook, resource, run, fork, and destroy permissions.
- Keep `create_world` admin-only.
- Update examples that expect `maintainer` or `coder`.

Affected docs:

- `architecture.md`
- `broker.md`
- `token-quotas.md`
- `examples.md`
- `custom-commands.md`
- `README.md`
- `AGENTS.md`
- `CLAUDE.md`

### 4. Runtime Is the Script Boundary

The new runtime spec is stricter than the older "sugar" framing.

Change older docs to:

- Prefer `ArchetypeRuntime` for scripts and beginner workflows.
- Describe `src/archetype/runtime/` as the runtime implementation; keep `src/archetype/sugar.py` only if it remains a compatibility/export layer.
- State that `RuntimeWorld` holds `world_id` and `ActorCtx`, not an `AsyncWorld`.
- Remove or qualify any runtime examples that reach into `world.resources`, raw `AsyncWorld`, `container.world_service`, or `container.query_service`.
- Keep `ServiceContainer` examples explicitly labeled as lower-level / non-script host examples.

Affected docs:

- `quickstart.md`
- `architecture.md`
- `worlds.md`
- `building-simulations.md`
- `examples.md`
- `resources.md`
- `trajectories.md`
- `docs/index.md`
- README

### 5. World Lifecycle: Fork and Destroy Semantics Changed

The new lifecycle spec makes append-only persistence and in-memory destroy central.

Change older docs to:

- Use `destroy_world`, not "remove world" or "delete world", for the normative lifecycle operation.
- State that destroy removes the live registry entry only; storage and audit rows remain queryable.
- State that destroying an unknown world is a no-op.
- Update REST/CLI wording from "remove" to "destroy" where appropriate.
- Update fork docs: pending spawn/despawn caches transfer to the fork; a spawn-then-fork before the next tick must materialize in both worlds.
- Update fork docs: resources and processor instances are shared Python instances by default, not independent copies.
- Update hook docs: hook registrations existing at fork time are copied; later hook registrations do not propagate.

Affected docs:

- `worlds.md`
- `resources.md`
- `api-layer.md`
- `app-overview.md`
- `services.md`
- `building-simulations.md`
- `examples.md`
- `trajectories.md`
- `docs/index.md`
- README

### 6. Execution Hierarchy Needs to Replace Ad Hoc Run Language

Older docs mention `run`, `run_all`, rollouts, and episodes without the new hierarchy.

Change older docs to:

- Define `step` as the primitive.
- Define `run` as N steps, no termination, no fork.
- Define `episode` as step-until-termination on the supplied world, no implicit fork.
- Define `rollout` as N forked episodes.
- Remove or reframe descriptions where `run_episode` is "sampled initial conditions" or `run_rollout` is "run N steps".
- Update permission tables: `step`, `run`, `run_episode`, and `run_rollout` are `operator` / `admin`.

Affected docs:

- `token-quotas.md`
- `building-simulations.md`
- `examples.md`
- `architecture.md`
- `services.md`
- `app-overview.md`
- `docs/index.md`

### 7. Audit Log Replaces Broker History for User-Facing History

The new specs make `iAuditLog` append-only and make `world.history(...)` call `iCommandService.get_audit_history`.

Change older docs to:

- Stop presenting `container.broker._history` as the audit trail.
- Stop presenting command history endpoints as broker history unless they are explicitly pending/queue introspection.
- Use `get_audit_history` / `world.history()` for accepted-and-applied audit rows.
- Add the one-audit-row rule for multi-step gated calls, especially `destroy_world` and `run_rollout`.

Affected docs:

- `broker.md`
- `custom-commands.md`
- `examples.md`
- `api-layer.md`
- `services.md`
- `data-flow.md`
- `docs/index.md`

### 8. Info-Class Downgrade Must Be Reflected in Service Docs

The gate returns immutable info snapshots rather than live objects.

Change older docs to:

- `iCommandService.create_world`, `fork_world`, and `get_world_info` return `WorldInfo`.
- `list_processors`, `list_hooks`, and `list_resources` return `ProcessorInfo`, `HookInfo`, and `ResourceInfo`.
- Runtime docs and examples should not assume live `AsyncWorld`, processor instances, hook callables, or resource objects escape past the gate.
- Lower-level `iWorldService` may still return live `iWorld` for internal callers; that distinction should be explicit.

Affected docs:

- `services.md`
- `app-overview.md`
- `api-layer.md`
- `worlds.md`
- `resources.md`
- `examples.md`

## File-by-File Checklist

### `mkdocs.yml`

- Add `Specifications` nav group.
- Move `guide/specification.md` into that group as "Overview".
- Add the five new spec docs.
- Decide whether the spec group should sit after `Getting Started` or after `Core Architecture`; recommendation: after `Getting Started`.

### `docs/index.md`

- Add a "Specifications" section near "Where to Start".
- Update "Commands and RBAC" flow from `CommandService -> CommandBroker -> AsyncWorld` to `iCommandService gate -> direct delegate or tick-deferred broker -> services/core`.
- Replace broker-centric RBAC wording with gate-centric wording.
- Update world forking bullets to include shared processors/resources and pending mutation transfer.
- Replace "Remove a world" wording with "Destroy a world" / "in-memory cleanup".
- Link to `runtime.md`, `command-gate.md`, `execution-hierarchy.md`, `service-protocols.md`, and `world-lifecycle.md`.

### `architecture.md`

- Replace old six-role RBAC table with the four-role model.
- Replace `CommandBroker.enqueue()` as the RBAC validation point with `iCommandService`.
- Update command flow so direct gate methods and tick-deferred `submit` are both represented.
- Link "Runtime Layer" to `runtime.md`.
- Link RBAC to `command-gate.md`.
- Link service dependency discussion to `service-protocols.md`.
- Update tick lifecycle to use `iCommandService.drain_and_apply` and avoid saying broker owns authorization.

### `app-overview.md`

- Update "What Services Add" so `CommandService` is the gate, `CommandBroker` is a pure queue, and `AuditLog` is the audit source.
- Remove "broker enforces RBAC" language.
- Add `MutationService` and `AuditLog` to the service summary.
- Update "Creating a World" trace: API route should call `CommandService.create_world(ctx, ...)`, not submit a global lifecycle command and then `apply_world_lifecycle`.
- Update QueryService paragraph to distinguish internal ungated reads from external gated reads.
- Replace `run_all()` if it is not part of the new execution hierarchy.

### `services.md`

- Rewrite service graph to match `service-protocols.md`.
- Add `iMutationService` and `iAuditLog`.
- Move RBAC/quota/audit responsibility from broker to command service.
- Update `CommandService` section for direct gate methods plus tick-deferred `submit`.
- Add info-class return types.
- Replace old query methods such as `get_world_state`, `get_entity`, and `get_command_history` if they are no longer the normative service surface.
- Update shutdown order to include broker clear, audit flush/shutdown, world shutdown, and store shutdown.

### `broker.md`

- Reframe as pure priority queue.
- Remove "choke point" and "enforces access control" wording.
- Remove role table or replace with a pointer to `command-gate.md`.
- Treat quotas carefully: if quotas still live in `guardrail_allow`, describe them as gate checks, not broker checks.
- Replace broker history as audit trail with "queue/history introspection only"; point user-facing history to audit log.
- Clarify whether processor-originated broker enqueue remains supported as internal scheduling, and whether it is audited.

### `data-flow.md`

- Split into "gated external path", "tick-deferred path", "internal service path", and "processor/internal path".
- Remove claim that external mutations must go through `CommandService.submit()` specifically; they must go through `iCommandService`, direct or deferred.
- Remove claim that reads are unconditionally allowed externally.
- Add audit emission after gated calls.
- Update lifecycle operations to direct gate proxies.

### `api-layer.md`

- Update dependency examples to inject/use `CommandService` for both reads and writes where externally visible.
- Replace lifecycle command submission via `__global__` with direct gated lifecycle methods.
- Update route table wording from remove/delete to destroy where this is lifecycle cleanup.
- Add routes or notes for `run_episode`, `run_rollout`, audit history, and info/list introspection if they are part of the public API target.
- Keep default admin `ActorCtx` note, but point to `command-gate.md` for the role model.

### `quickstart.md`

- Ensure it stays runtime-first.
- Link to `runtime.md` for lifecycle and handle semantics.
- Avoid teaching `ServiceContainer` unless clearly labeled as lower-level.
- Confirm examples do not require direct resource mutation after fork.

### `worlds.md`

- Keep it as the `AsyncWorld` engine page, but add a prominent pointer that script users should use `RuntimeWorld`.
- Update fork section to match `world-lifecycle.md`: no pending-mutation rejection; pending caches transfer.
- Update resource and processor sharing semantics.
- Add destroy semantics or point to `world-lifecycle.md`.
- Remove any claim that `RuntimeWorld` operations appear in broker history; use audit history.

### `resources.md`

- Update fork section: resources are shared by default across forks, not copied into isolated sets.
- Remove examples that call `fork.resources.insert(...)` on a runtime handle unless runtime exposes that API.
- Prefer `runtime.world(..., resources=[...])` for staged resources and a gated `add_resource` method for post-activation resources if that remains the contract.
- Clarify processor access to broker/command submission under the new gate model.

### `building-simulations.md`

- Prefer runtime examples.
- Rework lower-level examples that call `container.command_service.submit` plus `container.simulation_service.step`; label them as tick-deferred service-layer examples.
- Replace direct `container.world_service.fork_world` user workflow with runtime `world.fork(...)` or gated `command_service.fork_world`.
- Update rollout/episode language to the new hierarchy.

### `examples.md`

- Update command type permission table to four roles.
- Change `destroy_world` and `fork_world` permissions from admin-only to operator/admin.
- Update runtime fork/resource examples for shared resource semantics.
- Replace `container.broker._history` and query-service command history examples with audit history.
- Ensure lower-level examples use the current gated service protocol.

### `custom-commands.md`

- Rework extension guidance around `iCommandService` and `CommandType`.
- Do not suggest replacing `ServiceContainer.command_service` with a constructor that only receives `broker`; the new service has more dependencies.
- Replace `CommandService.apply()` override guidance if `_apply`/dispatch is no longer the extension point.
- Update required role language from "player or admin" to the explicit `COMMANDS_BY_ROLE` decision.
- Replace broker history audit references.

### `token-quotas.md`

- Move quota enforcement to the command gate if that is the target contract.
- Replace role table with four-role model or link to `command-gate.md`.
- Update command cost descriptions for `run`, `step`, `run_episode`, and `run_rollout`.
- Clarify whether direct gated calls and queued `submit` both consume quota.
- Ensure `operator` owns simulation control and schema/processor/resource changes.

### `trajectories.md`

- Check fork examples for direct resource mutation after `world.fork`.
- If per-fork parameter overrides are still required, show the sanctioned runtime/API path.
- Link to `execution-hierarchy.md` if trajectory evaluation uses episodes or rollouts.

### `processors.md`

- Update processor-originated command examples that import/use `CommandBroker` from resources.
- Clarify whether processors are trusted internal code that can mutate through updater/world APIs or enqueue internal commands for next tick.
- Avoid implying processor submissions are external gated/audited calls unless they pass through `iCommandService`.

### `hooks.md`

- Add `OnDestroy` if the lifecycle spec makes it normative.
- Clarify hook copy behavior at fork time.
- Clarify runtime hook registration: staged hooks at `runtime.world(..., hooks=[...])`, post-activation `add_hook`/`remove_hook` if supported, and no pre-activation `add_hook` if that remains the spec.

### `run-config.md`

- Add or link `EpisodeConfig`, `EpisodeResult`, `RolloutConfig`, and `RolloutResult`.
- Make clear that `RunConfig` is for step/run, while episode/rollout wrap it.

### `contributing.md`

- Update review checklist to include the new spec docs.
- Add "does not bypass `iCommandService` for external operations" as a docs/code review item.
- Add role-model migration note: no new docs should introduce `coder` or `maintainer`.

### `specification.md`

- Decide whether this remains the umbrella normative contract or becomes an overview/index for the newer focused specs.
- Add a "Companion specifications" section that links the five new docs.
- Remove or soften sections that now conflict with the focused specs.
- Keep historical implementation notes only if clearly marked as non-normative status.

### Low-Risk Pages

These likely need only link checks or small terminology updates:

- `archetype.md`
- `components.md`
- `system-execution.md`
- `stores.md`
- `querier.md`
- `updater.md`
- `autoresearch.md`

## Outside `docs/guide`

After the guide docs are aligned, update public/repo-facing docs:

- `README.md`: runtime path, four-role model, gate-centric command flow, destroy/fork semantics, Specifications links.
- `AGENTS.md`: four-role model; `src/archetype/runtime/` instead of only `sugar.py`; point normative behavior to the new spec group.
- `CLAUDE.md`: same role/runtime updates as `AGENTS.md`.
- `docs/reference/rest-api.md`: update generated or hand-written route language for destroy, audit history, episode/rollout if applicable.
- `docs/reference/cli.md`: update generated or hand-written CLI wording for destroy/fork/history if applicable.

## Suggested Implementation Order

1. Fix internal inconsistencies across the five new spec docs.
2. Update `mkdocs.yml` with the `Specifications` group.
3. Update `docs/index.md` and `specification.md` so readers can find the new contract pages.
4. Update architecture/app overview/service/data-flow/broker docs as the core conceptual set.
5. Update runtime-facing guides and examples.
6. Update roles/quotas/custom-command docs.
7. Update README, agent guidance, and generated references.
8. Run `uv run --extra docs mkdocs build`.


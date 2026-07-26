# Current process wiring snapshot

**Document type:** Non-normative implementation note.

The normative dependency and boundary rules live in
[`docs/guide/application-architecture.md`](../../../docs/guide/application-architecture.md).
Arrows below mean consumer to dependency.

## Explicit composition

`archetype.wiring.build_runtime_resources` is the sole concrete cross-family
composition transaction. It constructs:

1. storage and the canonical stateless redactor;
2. the world registry, operation registry, scheduler, lifecycle, and audit;
3. the policy and command dispatcher;
4. one `RuntimeResources` process owner;
5. remaining application-family workflow services and family-owned free handlers; and
6. all 47 exact operation registrations before returning the owner.

Nothing escapes while the registry is incomplete. Runtime consumes the
trusted dispatcher entry points; FastAPI authenticates an `ActorCtx` and
consumes actor-aware dispatcher entry points. Neither host contains family
workflow behavior.

| Consumer | Injected dependencies |
|---|---|
| `WorldRegistry` | none; owns live world identity, exact-world locks, and close leases |
| `CommandScheduler` | exact registry, control-catalog resolution, and ID reservation callables |
| `WorldLifecycle` | `StorageService`, `WorldRegistry`, and `CommandScheduler.materialize` |
| `AuditLog` | storage plus scheduler outbox read/acknowledge callables |
| `RuntimeResources` | dispatcher, audit, storage, and explicit storage ownership |
| artifact handlers and views | storage plus explicit durable operation coordinates |
| `TranscriptIngestionService` | artifact handler, canonical redaction, and storage |
| evaluation handlers | storage; pinned reads additionally use `archetype.world.query` |
| `TrajectoryService` | storage and the pure evaluation grader runner |
| physical-AI free handlers | `WorldRegistry`, `WorldLifecycle`, storage, and the runtime-owned provider-lifetime registrar |
| research handler plus process-shared admissions | `WorldRegistry`, `WorldLifecycle`, storage, and exact owned-world cleanup |
| `MissionService` | a runtime-world factory, sandbox service, narrow redaction capability, generic owner reservation, and exact cleanup factory |

The operation registry contains the 32 world operations, commands-owned audit
history, and 14 direct family operations exactly once. The sole research
operation closes over one process-shared `AutoResearchAdmissions`; its handler
is awaited inside the dispatcher's existing admission and never re-enters the
dispatcher. Registered handlers close over concrete dependencies but never
create a second process owner.

The two physical-AI operations are trusted-only, direct, and non-durable.
Their handlers synchronously transfer each unique live provider to a
`RuntimeResources` owner before any workflow or provider effect, then hold an
identity-ordered exclusive lease for the complete operation. Shared providers
serialize while disjoint provider sets may progress concurrently. Runtime
shutdown invokes each provider's async `aclose()` and retains failed owners
for retry. A handler retires its live world writer before releasing the lease,
so the returned coordinates are durable read evidence and not a later
provider-execution bypass.

## Core world composition

```text
AsyncWorld
  -> AsyncQueryManager -> shared iAsyncStore
  -> AsyncUpdateManager -> shared iAsyncStore
  -> AsyncSystem
  -> Resources
  -> HookRegistry
  -> construction-bound CommandMaterializer
```

World mutation and simulation functions acquire exact leases from
`WorldRegistry`. Durable world queries bypass live locking and read persisted
state through `StorageService`.

## Lifetime and enforcement

`RuntimeResources` owns sticky admission stop, admitted-work drain, supervised
tasks, workflow handles, world handles, audit, and owned storage in dependency
order. Failed cleanup retains the exact owner and cause for retry; injected
storage is never closed.

Every cross-family concrete construction occurs in `archetype.wiring`.
Constructors consume reviewed narrow capabilities, concrete services do not
inherit concrete services, and the merged architecture policy has zero
migration exceptions. `scripts/check_architecture.py` enforces those claims.

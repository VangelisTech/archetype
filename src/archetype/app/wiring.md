# Current app wiring snapshot

**Document type:** Non-normative implementation note.

The normative dependency and boundary rules live in
[`docs/guide/application-architecture.md`](../../../docs/guide/application-architecture.md).
Arrows below mean consumer to dependency.

## Concrete construction

| Consumer | Injected dependencies |
|---|---|
| `WorldRegistry` | none; owns live world identity, exact-world locks, and close leases |
| `CommandScheduler` | exact registry, control-catalog resolution, and ID reservation callables |
| `WorldLifecycle` | `StorageService`, `WorldRegistry`, and `CommandScheduler.materialize` |
| `AuditLog` | `iStorageService` and scheduler outbox read/acknowledge callables |
| `IngestionService` | `iStorageService`, `WorldRegistry` |
| `ArtifactService` | `iStorageService`, `WorldRegistry`, `iIngestionService` |
| `TranscriptIngestionService` | `iArtifactService`, `iIngestionService`, `iRedactionService`, `iStorageService`, `WorldRegistry` |
| `EvaluationService` | `iIngestionService`, `StorageService`, `WorldRegistry`; durable reads call `archetype.world.query` |
| `TrajectoryService` | `StorageService`, `iEvaluationService`; durable reads call `archetype.world.query` |
| `PhysicalAIService` | `WorldRegistry`, `WorldLifecycle`, `iEvaluationService`, `StorageService` |
| `AutoResearchService` | `WorldRegistry`, `WorldLifecycle`, `StorageService` |
| `RuntimeApplication` | `WorldRegistry`, `WorldLifecycle`, `StorageService`, `CommandDispatcher`, a world-command cancellation callable, and optional app workflow ports |
| `CommandGateway` | `iRuntimeApplication`, `CommandDispatcher`, `Policy`, bounded access-evidence callable, and a target-tick resolver |

`ServiceContainer` constructs the concrete graph and exposes
`application` and `command_gateway`. It binds
`CommandScheduler.materialize(AsyncWorld, tick)` into `WorldLifecycle` at
construction, so due commands run inside the already-held world operation
lease. It also wires the scheduler outbox reader/acknowledger into `AuditLog`.
There is no command-drain or quota-reset setter.

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

## Enforcement

Every cross-family concrete construction occurs in `app/container.py`.
Constructors use family protocols, concrete services do not inherit concrete
services, and the merged `quality/architecture.toml` policy plus its family
fragments currently has zero migration exceptions.
`scripts/check_architecture.py` enforces those claims.

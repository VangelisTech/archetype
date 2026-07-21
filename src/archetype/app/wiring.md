# Current app wiring snapshot

**Document type:** Non-normative implementation note.

The normative dependency and boundary rules live in
[`docs/guide/application-architecture.md`](../../../docs/guide/application-architecture.md).
Arrows below mean consumer to dependency.

## Concrete construction

| Consumer | Injected dependencies |
|---|---|
| `WorldService` | `iStorageService` |
| `AuditLog` | `iStorageService` |
| `QueryService` | `iStorageService`, `iAuditLog` (history compatibility) |
| `IngestionService` | `iStorageService`, `iWorldService` |
| `ArtifactService` | `iStorageService`, `iWorldService`, `iIngestionService` |
| `TranscriptIngestionService` | `iArtifactService`, `iIngestionService`, `iRedactionService`, `iStorageService`, `iWorldService` |
| `EvaluationService` | `iQueryService`, `iIngestionService`, `iStorageService`, `iWorldService` |
| `MutationService` | `iWorldService` |
| `SimulationService` | `iWorldService`, `iStorageService`, injected callbacks |
| `PhysicalAIService` | `iWorldService`, `iMutationService`, `iSimulationService`, `iEvaluationService`, `iStorageService` |
| `CommandScheduler` | `iWorldService`, `iMutationService` |
| `AutoResearchService` | `iWorldService`, `iSimulationService`, `iStorageService` |
| `RuntimeApplication` | family ports above |
| `CommandGateway` | `iRuntimeApplication`, `iAuditLog` |

`ServiceContainer` constructs the concrete graph and exposes
`application` and `command_gateway`. It injects:

- `RuntimeApplication.drain_and_apply` into `SimulationService`; and
- the quota reset callback into `SimulationService`.

It also wires the scheduler outbox reader/acknowledger into `AuditLog`.

## Core world composition

```text
AsyncWorld
  -> AsyncQueryManager -> shared iAsyncStore
  -> AsyncUpdateManager -> shared iAsyncStore
  -> AsyncSystem
  -> Resources
  -> HookRegistry
```

Mutation and simulation are siblings over `WorldService`. `QueryService`
bypasses the live registry and reads persisted state through storage.

## Enforcement

Every cross-family concrete construction occurs in `app/container.py`.
Constructors use family protocols, concrete services do not inherit concrete
services, and the merged `quality/architecture.toml` policy plus its family
fragments currently has zero migration exceptions.
`scripts/check_architecture.py` enforces those claims.

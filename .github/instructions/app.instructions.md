---
applyTo: "src/archetype/app/**"
---

# App Service Layer Review Guidelines

All mutations flow through the CommandBroker with RBAC enforcement. The service layer wires everything together via `ServiceContainer`.

## Command Flow

```
External -> CommandService -> CommandBroker (RBAC check + priority queue) -> SimulationService (drain + step) -> World
```

## Review Checklist

- Every command submission must include an `ActorCtx` with appropriate roles.
- Roles are flat (not hierarchical): viewer, player, coder, maintainer, admin. Verify the correct role is required for new command types.
- `CommandBroker.enqueue()` validates RBAC before queuing. Never bypass this.
- Messages enqueued at tick N are realized at tick N+1 (tick-boundary consistency).
- `ServiceContainer` wiring order matters — check for circular dependencies when adding new services.
- `StorageBackendManager` is infrastructure plumbing (pools storage backends). `Resources` is runtime DI for processors. Don't confuse them.

## What to Flag

- Commands that skip the broker or bypass RBAC
- Missing `ActorCtx` on command submissions
- New services not wired through `ServiceContainer`
- Changes to command priority ordering without justification

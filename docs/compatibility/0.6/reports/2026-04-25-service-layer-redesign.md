# Service Layer Redesign

Status: Draft. Class-diagram-level proposal. Iterate before implementation.

## Inventory: what exists today

| File | LoC | Does | Misplaced concerns |
|---|---|---|---|
| `world_service.py` | 384 | World CRUD, fork, list | Broker injection, storage URI validation, registry persistence, registry tick-sync hook, deep `_entity2sig`/`_next_entity_id`/`updater` access during fork |
| `factory.py` | 74 | Wires `AsyncWorld` from configs | Clean. |
| `storage_service.py` | 133 | Multiton of `(Store, Querier, Updater)` per pool key | Clean. |
| `command_service.py` | 361 | Command submit + drain + apply | Auth check inline (`guardrail_allow`); component hydration logic; spawn/despawn dispatch — sized for what it does. |
| `broker.py` | 245 | Queue + history per world | Clean. |
| `simulation_service.py` | 119 | Step/run orchestration | Clean. |
| `query_service.py` | 83 | Read facade — stub | N/A — being designed. |
| `registry.py` | 81 | File-backed JSON catalog | Clean. |
| `container.py` | 56 | Composition root | Wires services in dependency order — clean but bypassed by services that reach for each other directly. |

The actual problem: `WorldService` accumulated cross-cutting concerns (broker, registry, storage validation) because it was already in the wiring path. Other services are mostly OK.

## Diagnosis: where the boundaries are wrong

1. **`WorldService` knows about `CommandBroker`** — injects it into `world.resources` on every create. Decision is composition-root, not lifecycle.

2. **`WorldService._ensure_storage_uri_writable`** — local-path validation lives in a free function called from `create_world`. Belongs in `StorageService` (it owns backends).

3. **`WorldService._persist_entry` + `_attach_registry_sync`** — registry writes and a post-tick hook for tick sync. Mixes a metadata-catalog concern into the world manager.

4. **`WorldService.fork_world`** — reads `_entity2sig`, `_next_entity_id`, `_spawn_cache`, `_despawn_cache`, `system.processors`, `resources.items()`, then calls `updater.update` with re-stamped frames. The world's internals leak into the service. Fork should be `world.fork(...)`.

5. **No interfaces for app services.** Every service is referenced as a concrete class in the container and in callers. Tests mock by patching attributes rather than substituting implementations. New services have no contract to satisfy.

## Proposed structure

### Service interfaces (new file: `packages/archetype-ecs/src/archetype/app/interfaces.py`)

```python
class iStorageService(Protocol):
    async def get_backend(
        self, storage_config: StorageConfig, cache_config: CacheConfig | None
    ) -> tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager]: ...
    async def shutdown(self) -> None: ...


class iWorldFactory(Protocol):
    async def create_world(
        self,
        world_config: WorldConfig,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld: ...


# Hook signature for cross-cutting concerns at creation time.
# Container wires zero or more of these; factory invokes them.
WorldCreationHook = Callable[[iWorld], Awaitable[None]]


class iWorldService(Protocol):
    """Pure lifecycle: register, get, list, remove, fork. No cross-cutting concerns."""
    async def create_world(
        self,
        config: WorldConfig,
        storage_config: StorageConfig | None = None,
        cache_config: CacheConfig | None = None,
        system: iAsyncSystem | None = None,
    ) -> iWorld: ...
    def get_world(self, world_id: UUID) -> iWorld: ...
    def get_world_by_name(self, name: str) -> iWorld: ...
    def list_worlds(self) -> list[WorldInfo]: ...
    async def remove_world(self, world_id: UUID) -> None: ...
    async def fork_world(
        self,
        source_id: UUID,
        name: str | None,
        storage_config: StorageConfig,
        cache_config: CacheConfig | None = None,
    ) -> iWorld: ...
    async def shutdown(self) -> None: ...


class iWorldRegistry(Protocol):
    """File-backed catalog. Already a clean unit; just lift to interface."""
    def get(self, world_id: UUID | str) -> dict[str, Any] | None: ...
    def upsert(self, world_id: UUID | str, entry: dict[str, Any]) -> None: ...
    def delete(self, world_id: UUID | str) -> None: ...
    def list_entries(self) -> list[dict[str, Any]]: ...


class iWorldRegistryService(Protocol):
    """Subscribes to world lifecycle, writes durable metadata, restores on discovery."""
    async def on_world_created(self, world: iWorld, storage_config: StorageConfig) -> None: ...
    async def on_world_removed(self, world_id: UUID) -> None: ...
    async def discover_worlds(self) -> list[iWorld]: ...


class iCommandBroker(Protocol):
    async def submit(self, world_id: str, cmd: Command, ctx: ActorCtx) -> Command: ...
    async def drain_and_apply(self, world_id: str, tick: int) -> list[Command]: ...
    async def get_history(self, world_id: str, limit: int) -> list[Command]: ...
    async def clear(self, world_id: UUID | None = None) -> None: ...


class iCommandService(Protocol):
    async def submit(self, world_id: UUID, cmd: Command, ctx: ActorCtx) -> Command: ...
    async def drain_and_apply(self, world_id: UUID, tick: int) -> list[Command]: ...


class iSimulationService(Protocol):
    async def step(self, world_id: UUID, run_config: RunConfig, **kwargs) -> int: ...
    async def run(self, world_id: UUID, run_config: RunConfig, **kwargs) -> RunResult: ...


class iQueryService(Protocol):
    async def get_world_state(
        self, world_id: UUID, tick: int | None = None, consistency: str = "committed"
    ) -> WorldSnapshot: ...
    async def get_entity(
        self, world_id: UUID, entity_id: int, tick: int | None = None
    ) -> dict: ...
    async def get_components(
        self,
        world_id: UUID,
        component_types: list[type[Component]],
        entity_ids: list[int] | None = None,
        tick: int | None = None,
    ) -> DataFrame: ...
    async def get_command_history(self, world_id: UUID, limit: int = 100) -> list[Command]: ...


class iAuthGuard(Protocol):
    async def allow_command(self, ctx: ActorCtx, cmd: Command) -> bool: ...
    async def allow_read(self, ctx: ActorCtx, world_id: UUID, mode: str) -> bool: ...
```

### iWorld additions (move fork onto the engine)

```python
class iAsyncWorld(Protocol):
    # ... existing ...
    async def fork(
        self,
        new_world_id: UUID,
        new_world_name: str | None,
        target_storage: tuple[iAsyncStore, iAsyncQueryManager, iAsyncUpdateManager],
        new_system: iAsyncSystem,
    ) -> "iAsyncWorld":
        """Produce a new world that snapshots this one's state at the current tick."""
```

The world knows its `_entity2sig`, `_next_entity_id`, run_id, etc. It owns the deep-copy + re-stamp logic. `WorldService.fork_world` becomes a thin wrapper that resolves storage and registers the result.

### Class diagram

```text
                           ┌──────────────────┐
                           │ ServiceContainer │  composition root — wires all
                           └────────┬─────────┘
              ┌─────────────────────┼──────────────────────┐
              │                     │                      │
        ┌─────▼─────┐         ┌─────▼─────┐          ┌─────▼─────┐
        │  Storage  │         │  Command  │          │ WorldRegi-│
        │  Service  │         │  Broker   │          │   stry    │
        └─────┬─────┘         └─────┬─────┘          └─────┬─────┘
              │                     │                      │
              │            ┌────────┴───────┐              │
              │            │   AuthGuard    │              │
              │            └────────┬───────┘              │
              │                     │                      │
        ┌─────▼─────┐                                      │
        │  World    │◄─────────── injected ─────────────── │
        │  Factory  │                                      │
        └─────┬─────┘                                      │
              │                                            │
              │   ┌──────── creation hooks ────────┐       │
              │   │ • broker-injection (CommandBroker)     │
              │   │ • registry-persist (WorldRegistry)─────┘
              │   │ • registry-tick-sync (post_tick hook)
              │   └────────────────────────────────┘
              │
        ┌─────▼─────┐         ┌────────────┐         ┌────────────┐
        │   World   │◄────────│  Command   │         │   Query    │
        │  Service  │         │  Service   │         │  Service   │
        └─────┬─────┘         └─────┬──────┘         └─────┬──────┘
              │                     │                      │
              └──────────┬──────────┘                      │
                         │                                 │
                   ┌─────▼─────┐                           │
                   │Simulation │                           │
                   │  Service  │                           │
                   └───────────┘                           │
                                                           │
                              all read/write via storage ──┘
                                            │
                                      ┌─────▼─────┐
                                      │  iWorld   │
                                      │ engine    │
                                      └───────────┘
```

Dependency arrows go from caller to callee. No cycles.

### What each service depends on

| Service | Dependencies | What it does NOT depend on |
|---|---|---|
| `StorageService` | (none) | broker, registry, world |
| `CommandBroker` | (none) | world, storage |
| `WorldRegistry` | filesystem | services |
| `AuthGuard` | (none) | services |
| `WorldFactory` | `iStorageService` | broker, registry, world_service |
| `WorldService` | `iWorldFactory`, `list[WorldCreationHook]` | broker, registry directly |
| `WorldRegistryService` | `iWorldRegistry`, `iWorldFactory` | broker, world_service |
| `CommandService` | `iCommandBroker`, `iWorldService`, `iAuthGuard` | factory, registry, storage |
| `SimulationService` | `iWorldService`, `iCommandService` | broker, registry |
| `QueryService` | `iWorldService`, `iCommandBroker` (history only), `iAuthGuard` | factory, registry |

Cross-cutting concerns (broker injection, registry persistence, post-tick sync) flow through **`WorldCreationHook`**s wired by the container. `WorldService` never sees them.

## Composition root after redesign

```python
class ServiceContainer:
    def __init__(self, registry_path: str | Path | None = None):
        # Leaves
        self.storage_service: iStorageService = StorageService()
        self.broker: iCommandBroker = CommandBroker()
        self.guard: iAuthGuard = AuthGuard()
        self.registry: iWorldRegistry | None = (
            WorldRegistry(registry_path) if registry_path else None
        )

        # Mid-level
        self.factory: iWorldFactory = WorldFactory(self.storage_service)

        creation_hooks: list[WorldCreationHook] = [
            inject_broker_resource(self.broker),
        ]

        self.registry_service: iWorldRegistryService | None = None
        if self.registry is not None:
            self.registry_service = WorldRegistryService(self.registry, self.factory)
            creation_hooks.append(self.registry_service.on_world_created_hook)
            creation_hooks.append(self.registry_service.attach_tick_sync_hook)

        self.world_service: iWorldService = WorldService(
            factory=self.factory,
            creation_hooks=creation_hooks,
        )

        # Top-level
        self.command_service: iCommandService = CommandService(
            broker=self.broker, world_service=self.world_service, guard=self.guard
        )
        self.simulation_service: iSimulationService = SimulationService(
            world_service=self.world_service, command_service=self.command_service
        )
        self.query_service: iQueryService = QueryService(
            world_service=self.world_service, broker=self.broker, guard=self.guard
        )

    async def shutdown(self) -> None:
        await self.broker.clear()
        await self.world_service.shutdown()
        await self.storage_service.shutdown()
```

`world.fork()` lives on the engine; `WorldService.fork_world` becomes:

```python
async def fork_world(self, source_id, name, storage_config, cache_config=None) -> iWorld:
    source = self.get_world(source_id)
    new_id = uuid7()
    target_storage = await self._factory._storage.get_backend(storage_config, cache_config)
    new_system = AsyncSystem(); new_system.processors = list(source.system.processors)
    new_world = await source.fork(new_id, name, target_storage, new_system)
    self._register(new_world)
    for hook in self._creation_hooks:
        await hook(new_world)
    return new_world
```

## Migration plan

Each step ships green CI. No big-bang.

1. **Add `interfaces.py`** with all Protocols. No behavior changes. Existing concrete classes implement them implicitly.
2. **Move storage URI validation to `StorageService`.** Delete `_ensure_storage_uri_writable` from world_service.
3. **Add `fork()` to `iAsyncWorld` and implement on `AsyncWorld`.** WorldService.fork_world delegates. Same behavior, cleaner ownership.
4. **Add `WorldCreationHook` plumbing to `WorldFactory`.** Factory invokes hooks after construction. WorldService creates with the hook list it received.
5. **Extract broker injection** as a `creation_hook`. Remove from `WorldService.create_world`.
6. **Extract `WorldRegistryService`.** Move `_persist_entry` and `_attach_registry_sync` into it as hooks. Container wires them.
7. **Type all container slots to interfaces, not concretes.** Update sugar, CLI, API, tests to depend on `iWorldService` etc.
8. **Move `discover_worlds`** to `WorldRegistryService`.
9. **Add `entity_count` property to `iWorld`**, delete `_world_entity_count` shim.

After step 9, `world_service.py` is ~80 lines, every service has a Protocol, container is the only place that knows about concretes.

## App folder layout

### What ships with this PR

- Add `packages/archetype-ecs/src/archetype/app/interfaces.py` containing the Protocols above.
  Capture the *current* contracts, not aspirational ones. Existing concretes
  satisfy them structurally.
- Type the container slots to interfaces (cosmetic in this PR; foundational
  for the next).
- No file moves, no behavior changes.

### Eventual layout (deferred to redesign PRs)

Two reasonable shapes — pick one when the redesign lands.

**Option A: flat (~13 files).** Easiest to navigate; matches today.

```text
app/
├── interfaces.py            # all Protocols
├── container.py             # composition root
├── factory.py               # WorldFactory
├── world_service.py         # lifecycle (slim)
├── storage_service.py
├── broker.py                # CommandBroker
├── command_service.py
├── auth_guard.py            # AuthGuard (extracted from guardrail_allow)
├── simulation_service.py
├── query_service.py
├── registry.py              # WorldRegistry repo
├── registry_service.py      # WorldRegistryService (lifecycle subscriber)
├── creation_hooks.py        # WorldCreationHook builtins
└── models.py
```

**Option B: grouped by concept.** Better when it grows past ~15 files.

```text
app/
├── interfaces.py
├── container.py
├── lifecycle/
│   ├── factory.py
│   ├── service.py
│   └── hooks.py
├── storage/service.py
├── command/{broker,service,auth}.py
├── simulation/service.py
├── query/service.py
├── registry/{repo,service}.py
└── models.py
```

Recommendation: stick with flat (A) until grouped justifies itself.

## Core / app conflations to address later

Items where the boundary between `core/` (engine) and `app/` (operational
layer) is currently fuzzy. Document them now; address one at a time.

1. **`AsyncCachedStore` lives in `core/aio/`** but its background flush task
   and threshold tuning (`CacheConfig`: `flush_rows`, `flush_mb`,
   `global_mb`, `idle_sec`) are operational concerns. Either move the cache
   wrapper to `app/storage/` or accept it as a runtime adapter that's
   deliberately co-located with the engine.

2. **Hook system on `AsyncWorld`** (`add_hook`, `remove_hook`, `_fire_hooks`).
   The mechanism is generic, but every consumer is app-layer (registry sync,
   evaluation, observability). Worth lifting to a small event-bus abstraction
   in `app/` with the engine emitting events rather than carrying the hook
   plumbing.

3. **`Resources` injection** (`core/resources.py`). The DI container is
   generic; in practice every injected type is app-layer (`CommandBroker`,
   `Config`, etc.). Keep the mechanism in core but document that "resources
   are an app-layer integration point."

4. **`RunConfig` carries app fields** (`suite`, `trial`, `metadata`,
   `enable_validation`). These are experiment-tracking concerns, not engine
   semantics. Engine cares about `num_steps`, `run_id`, `debug`. Split into
   `EngineRunConfig` (core) + `ExperimentRunConfig` (app), or move the
   metadata fields out.

5. **`StorageContextFactory`** (`core/runtime/storage.py`) builds Iceberg
   catalogs and resolves cloud credentials. That's infrastructure plumbing —
   should live in `app/storage/` or a new `infra/` layer. The engine
   shouldn't know about SQLite Iceberg catalog files.

6. **Sync world has `_live`-equivalent removed**, but `core/sync/` still
   carries the same lifecycle abstractions as `core/aio/`. Long-term, only
   one variant should live in `core/`; the other becomes an `app/` adapter
   (e.g., `app/sync_runtime.py` wraps async with `asyncio.run`). The current
   duplication is technical debt.

None of these block the service-layer redesign. They're future cleanups
that become easier once the app-layer Protocols are in place — each can be
sliced off with a typed boundary on either side.

## Open questions

1. **Should hooks be sync or async?** Lean async — broker injection is sync but registry write hits disk. Async covers both at the cost of one `await`.

2. **Should `WorldCreationHook` get the storage_config too?** Registry needs it. Choice: (a) closure capture, (b) `Callable[[iWorld, StorageConfig], Awaitable[None]]`. Pick (b) — explicit beats closure magic.

3. **Where does `AuthGuard` live?** New file `packages/archetype-ecs/src/archetype/app/auth/guard.py` already exists with `guardrail_allow`. Wrap it in a class implementing `iAuthGuard`.

4. **`run_id` persistence in registry** (the bug we identified): falls naturally out of step 6 — `WorldRegistryService.attach_tick_sync_hook` writes both `tick` and `run_id`.

5. **Lazy fork vs. eager re-stamp.** Out of scope for this refactor. Eager (current behavior) is correct; lazy is an optimization.

6. **Test strategy.** Each migration step adds a Protocol-typed test fixture. By the end, services can be tested with hand-rolled fakes that satisfy the interface, no mocking required.

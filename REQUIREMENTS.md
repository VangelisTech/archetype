# Requirements for Top-Level Sugar Runtime

## Purpose

This document defines the minimum contracts for any top-level "sugar" API that
wraps Archetype's service layer. These requirements exist to prevent a
convenience API from weakening the engine's concurrency guarantees, world
lifecycle isolation, or broker-based command semantics.

The sugar API may improve ergonomics. It may not change the underlying
behavioral contracts unless that change is explicitly designed, versioned, and
tested.

## Scope

These requirements apply to:

- Any proposed top-level `World`, `Processor`, `Archetype`, `Runtime`, or
  `run_sync` sugar API
- Any wrapper that hides `ServiceContainer`, `WorldService`,
  `SimulationService`, or `CommandService`
- Any re-export change that alters the default public API surface

These requirements do not authorize changes to `src/archetype/core/`, which
remains read-only unless separately approved.

## Core Principle

Sugar wraps the service layer. Sugar does not bypass the service layer, weaken
its guarantees, or silently change the semantics of commands, world identity,
or execution.

## Concurrency Contract

### C1. Pure construction

Constructing a sugar wrapper such as `World(...)` must be pure and side-effect
free.

Required behavior:

- No I/O during object construction
- No implicit world creation during object construction
- No mutation of process-global runtime state during object construction
- No background task startup during object construction

### C2. Single-flight activation

The first activation of a lazily initialized wrapper must be serialized.

Required behavior:

- If multiple coroutines concurrently activate the same wrapper, exactly one
  backing world may be created
- Every caller must observe the same backing world identity after activation
- Activation must be idempotent after the first successful initialization

Minimum implementation expectation:

- Activation must be guarded by an async lock or equivalent single-flight
  mechanism

### C3. No partially initialized observable state

The sugar layer must not expose half-initialized runtime state.

Required behavior:

- Properties that depend on an activated world must either:
  - wait for activation to complete, or
  - raise a clear error indicating the world is not yet active
- Callers must never observe an object whose processors, resources, or backing
  world registration are only partially applied

### C4. Serialized lifecycle transitions

Activation, shutdown, and fork are mutually sensitive lifecycle operations and
must not race.

Required behavior:

- `fork()` may not race with first activation
- `shutdown()` may not race with first activation
- `shutdown()` may not invalidate in-flight `run()`, `step()`, or `query()`
  calls without a defined error contract

### C5. Honest command return values

Sugar methods must not claim stronger return semantics than the service layer
can provide.

Required behavior:

- `spawn()` must not claim to return an entity ID unless the architecture can
  reserve that entity ID before broker enqueue
- If entity identity is only known after broker drain and apply, `spawn()` must
  return a command ID, a handle with explicit semantics, or no value
- Return types and docstrings must match actual runtime behavior

### C6. Broker semantics remain intact

Command ordering and tick-boundary application must remain true under sugar.

Required behavior:

- Enqueued commands must still be subject to broker ordering
- Enqueued commands must still be applied at the documented tick boundary
- Sugar must not directly mutate worlds in ways that contradict the public
  brokered mutation contract unless that method is explicitly documented as a
  lower-level escape hatch

## Multi-World Lifetime Contract

### L1. Separate runtime lifetime from world lifetime

The runtime/container lifetime and individual world lifetimes must be modeled as
different scopes.

Required behavior:

- A process-scoped runtime must not be implicitly treated as world-scoped
- A world wrapper must not own the entire container by default
- Destroying or shutting down a world must not automatically tear down the
  runtime that may serve sibling worlds

### L2. World shutdown is world-local

`World.shutdown()` must have world-local semantics.

Required behavior:

- It must detach, destroy, or close only that world's handle and registrations
- It must not tear down shared storage pools, the broker, or sibling worlds
- If full runtime teardown is needed, it must occur through an explicit
  runtime-level API

### L3. Explicit runtime teardown

Container teardown must be explicit and process-scoped.

Required behavior:

- Runtime teardown must be performed through a dedicated runtime object or
  runtime-level function
- The API surface must clearly distinguish:
  - world shutdown
  - runtime shutdown

Recommended shape:

- `async with ArchetypeRuntime() as runtime: ...`
- `await runtime.shutdown()`

### L4. Forks share runtime, not world identity

Forked worlds must share runtime infrastructure while remaining distinct world
lifecycles.

Required behavior:

- A fork must receive its own world identity
- A fork may share storage pools and broker infrastructure through the runtime
- Shutting down a source world must not invalidate the fork
- Shutting down a fork must not invalidate the source world

### L5. Test isolation

The sugar runtime must not make deterministic testing harder.

Required behavior:

- Tests must be able to create isolated runtime instances without inheriting
  process-global state from previous tests
- Global singletons, if used at all, must have an explicit reset or opt-out
  path for tests
- Test suites must be able to exercise multiple runtimes in one process

## Script Ceremony Contract

### S1. Minimal ceremony, explicit boundary

The sugar API should reduce ceremony for scripts, but execution boundaries must
remain explicit.

Required behavior:

- Users may define `World(...)` wrappers declaratively
- The start of runtime ownership must be explicit somewhere in the script
- The API must make it clear where startup and teardown occur

Acceptable shapes include:

- `async with ArchetypeRuntime() as app:`
- `async with Archetype() as app:`
- `with Archetype.sync() as app:`

### S2. Context management belongs at runtime scope

If a context manager is used to manage process resources, it should exist at the
runtime level, not implicitly at each world wrapper.

Required behavior:

- Entering a runtime context may create or attach the container
- Exiting a runtime context may shut down the container
- Exiting a world context must not tear down process-shared infrastructure
  unless the world context is explicitly defined as owning a dedicated runtime

### S3. Sync helpers must not hide process lifetime

Sync conveniences are allowed, but they must not obscure resource ownership.

Required behavior:

- `run_sync()` must document whether it creates a temporary runtime or uses an
  existing one
- Repeated sync calls must not silently create and destroy incompatible runtime
  state around objects that outlive a single call
- Sync entry points must not leave shared global state in an ambiguous state

### S4. Preserve public API compatibility unless versioned

Top-level sugar must not silently redefine long-standing public imports.

Required behavior:

- Existing default exports such as `World` and `Processor` must remain stable
  unless changed as part of an explicit breaking release
- If new sugar types are introduced, prefer additive names first
- Any future alias swap requires migration notes and compatibility tests

### S5. Ergonomics must not bypass governance

Script ergonomics must not come from removing safety mechanisms.

Required behavior:

- If sugar claims to preserve RBAC, audit history, or broker semantics, those
  paths must actually flow through the governing services
- If a method intentionally bypasses governance, that bypass must be explicit in
  naming and documentation
- Direct resource mutation must not be described as governed by the broker

## Acceptance Criteria

No sugar API may be considered ready for implementation until the design can
show how it satisfies all of the following:

- Concurrent first-use of the same wrapper creates exactly one world
- `spawn()` return semantics are correct and tested
- One world's shutdown does not break a sibling world in the same runtime
- Runtime teardown is explicit and distinct from world teardown
- Forked worlds remain valid after the source world is shut down
- Async and sync script entry points have a clear resource ownership model
- Existing public imports remain compatible, or the change is explicitly marked
  as breaking and tested accordingly

## Non-Goals

This document does not choose the final user-facing API names. It establishes
the constraints that any acceptable design must satisfy.

## Suggested Next Step

Design the sugar API around an explicit runtime object that owns the
`ServiceContainer`, while `World` remains a lightweight handle scoped to that
runtime.

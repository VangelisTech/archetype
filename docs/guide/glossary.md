# Glossary

## Core Concepts

### Component
A typed data record (Pydantic model) that defines a schema for entity state. Components are stored with prefixed column names (e.g., `position__x`).

### Entity
A unique identifier (`entity_id: UUID`) that groups components together. Entities don't store data directly—they reference rows in archetype tables.

### Archetype
The unique set of component types attached to an entity. Entities with identical archetypes share a physical table. Example: `(Position, Velocity)` is one archetype, `(Position, Velocity, Health)` is another.

### Processor
A pure function that transforms a DataFrame of entities. Processors declare required components and execution priority.

### System
An ordered collection of processors. Executes processors in priority order each tick.

### World
The container for a simulation. Owns entity namespace, tick counter, and archetype tables.

### Tick
One step of the simulation. Each tick: query state → run processors → persist output.

### Run
A sequence of ticks grouped by `run_id`. Useful for organizing experiments.

## DSL Concepts

### @behavior
Decorator that defines agent behavior. Compiles to a Processor at registration time.

### AgentProxy
Wrapper providing natural attribute access to an entity's components. Tracks mutations for batching.

### ComponentProxy
Handles attribute access for a single component within an AgentProxy.

### spawn_world()
Async context manager that creates a child world, optionally forking state from parent. Used for MCTS and counterfactual reasoning.

### broadcast()
Send a message to all entities with an Inbox component.

## Infrastructure

### CommandBroker
Priority queue for commands (spawn, despawn, message). Commands are processed at tick boundaries.

### WorldOrchestrator
Manages lifecycle of multiple worlds. Handles creation, execution, and shutdown.

### WorldFactory
Creates worlds with proper storage configuration.

### StorageBackendManager
Manages storage backend instances (LanceDB, Iceberg).

### Resources
Type-safe dependency injection container for world-level services.

### Hooks
Lifecycle callbacks: `pre_tick`, `post_tick`. Allow external code to observe or modify behavior.

## Storage

### LanceDB
Columnar database optimized for vector similarity search and append-heavy workloads.

### Iceberg
Open table format for data lakes. Provides ACID transactions and schema evolution.

### AsyncCachedStore
Caching layer that keeps recent ticks in memory.

### StorageContext
Configuration for storage backends (local path, S3 URI, catalog settings).

## Patterns

### Time Travel
Query historical state at any tick. Enabled by append-only storage model.

### Fork
Create a branch of simulation from current or historical state.

### MCTS (Monte Carlo Tree Search)
Search algorithm using `spawn_world()` to simulate possible futures and select best action.

### Counterfactual Reasoning
"What if" analysis by forking simulation and exploring alternative scenarios.

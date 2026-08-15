# Core Architecture

## Purpose and Scope

This page is the hub for Archetype's **engine**. Start with the
[Runtime guide](runtime.md) for the supported application handles, then use
this page to drill into each box.

Product features — HTTP hosting, command gate, agent missions, physical AI,
prefabs — live in the [Application layer](app-overview.md). They compose on
top of this core; they are not a second storage model.

Normative application ownership rules live in
[Application Architecture](application-architecture.md). Visual layout here is
explanatory, not law.

## System component overview

```mermaid
graph TB
    App["ArchetypeRuntime / app code"]
    World["AsyncWorld"]
    System["AsyncSystem"]
    QM["AsyncQueryManager"]
    UM["AsyncUpdateManager"]
    Store["AsyncStore"]

    subgraph "Types"
        Component["Component"]
        Processor["AsyncProcessor"]
        Archetype["Archetype signature"]
    end

    App --> World
    World --> System
    World --> QM
    World --> UM
    System --> Processor
    Processor --> Component
    Component --> Archetype
    QM --> Store
    UM --> Store
    Store --> Archetype
```

| Box | Owns | Does not own |
|---|---|---|
| **AsyncWorld** | Tick orchestration, entity identity, live run | Auth, HTTP, multi-tenant policy |
| **AsyncSystem** | Processor ordering and eligibility | Persistence |
| **AsyncProcessor** | DataFrame transforms for a component set | Spawning other entities as a side channel |
| **AsyncQueryManager** | Reads from the store | Writes |
| **AsyncUpdateManager** | Appends to the store | Reads |
| **AsyncStore** | Physical archetype tables | Simulation policy |
| **Archetype** | Component-set → schema grouping | Runtime product workflows |

## Simulation execution pipeline

```mermaid
sequenceDiagram
    participant App as Application
    participant World as AsyncWorld
    participant System as AsyncSystem
    participant QM as AsyncQueryManager
    participant UM as AsyncUpdateManager
    participant Store as AsyncStore
    participant Commit as CommitCoordinator

    App->>World: step() / run(steps=N)
    World->>QM: query prior frame per active archetype
    QM->>Store: load archetype DataFrame
    Store-->>World: prior DataFrame
    World->>System: execute(prior DataFrame)
    loop Eligible processors by priority
        System->>System: process(df, resources, tick, ...)
    end
    System-->>World: transformed archetype data
    World->>UM: update(...)
    UM->>Store: append rows under tick commit token
    World->>Store: flush all staged appends
    World->>Commit: publish tick manifest last
    Commit-->>World: durable visibility receipt
    World->>World: consume mutation caches; advance tick
```

Teaching model: **query → transform → append → flush → publish → advance**.
The async world fans work out per active archetype table; the read/write split
stays fixed. For runtime-managed worlds, appended rows remain invisible until
the manifest publishes, and mutation caches are consumed only after that
durable visibility boundary.

## World and simulation management

`AsyncWorld` is the engine orchestrator. Facades keep concerns apart:

```mermaid
graph TB
    World["AsyncWorld"]
    World --> Sys["AsyncSystem facade<br/>processors + priority"]
    World --> Q["Read facade<br/>AsyncQueryManager"]
    World --> U["Write facade<br/>AsyncUpdateManager"]
    World --> Meta["Run metadata<br/>world_id, run_id, tick"]
    Q --> Store["AsyncStore"]
    U --> Store
```

- **Spawn / despawn / update** change which entities exist and which components
  they carry; structural changes land on tick boundaries.
- **Step / run** advance time and persist history.
- **Fork** keeps shared past rows and opens a new future run.

Deeper pages: [Working with worlds](working-with-worlds.md) ·
[World internals](worlds.md) · [World lifecycle](world-lifecycle.md) ·
[History and forks](history-and-forks.md)

## Entity-component-system

```mermaid
graph LR
    subgraph "Authoring"
        C["Component classes"]
        P["Processor classes"]
    end

    subgraph "Grouping"
        Sig["Archetype signature<br/>sorted component types"]
        Table["One table per signature"]
    end

    subgraph "Tick"
        DF["DataFrame of matching entities"]
        Out["Transformed DataFrame"]
    end

    C --> Sig
    Sig --> Table
    Table --> DF
    P --> Out
    DF --> Out
```

Entities that share a component set share an archetype table. A processor runs
only when its declared components are a **subset** of that signature — so the
columns it needs are guaranteed to exist.

Deeper pages: [Components](components.md) · [Processors](processors.md) ·
[Systems](system-execution.md) · [Archetypes](archetype.md)

## Data storage and persistence

```mermaid
graph TB
    QM["AsyncQueryManager"]
    UM["AsyncUpdateManager"]
    Store["AsyncStore"]
    Lance["LanceDB tables"]
    Daft["Daft DataFrames"]

    QM --> Store
    UM --> Store
    Store --> Daft
    Daft --> Lance
```

Every tick **appends**. Past state is a filter on `tick` (and `world_id` /
`run_id`), not a destructive overwrite. That is why forks and audits are the
same storage model as the live run.

Deeper pages: [Storage](stores.md) · [Queries](querier.md) ·
[Updates](updater.md) · [Data flow](data-flow.md)

## Processing framework

```mermaid
flowchart TD
    Start["AsyncWorld.step"] --> Arch["For each active archetype"]
    Arch --> Eligible["Processors whose components ⊆ signature"]
    Eligible --> Priority["Sort by priority"]
    Priority --> Process["processor.process(df, ...)"]
    Process --> Collect["Collect transformed frames"]
    Collect --> Commit["AsyncUpdateManager append + tick advance"]
```

Processors are trusted once registered. They transform populations. They are
not the authorization boundary — that lives in the application layer's command
gate.

Deeper pages: [Processors](processors.md) · [System execution](system-execution.md) ·
[Resources](resources.md) · [Lifecycle hooks](hooks.md)

## Where the application layer starts

```mermaid
graph TB
    subgraph "Application layer"
        RT["ArchetypeRuntime"]
        Disp["CommandDispatcher"]
        Families["world · commands · activities · domain families"]
    end

    subgraph "Core"
        Engine["AsyncWorld · AsyncSystem · async query/update/store"]
    end

    RT --> Disp
    Disp --> Families
    Families --> Engine
```

When you are ready for hosting, roles, missions, or eval workflows, leave this
hub and read the [Application layer](app-overview.md).

## Next steps

- [Quickstart](quickstart.md) — hands-on first world
- [Building simulations](building-simulations.md) — end-to-end authoring pattern
- [Application layer](app-overview.md) — families above the engine
- [Architecture Overview](architecture.md) — app contracts and tick lifecycle
- [Agent Missions](agent-missions.md) · [Physical AI](physical-ai.md) · [AutoResearch](autoresearch.md)

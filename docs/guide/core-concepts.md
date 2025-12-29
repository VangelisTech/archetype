# Core concepts (ECS as data)

Archetype is an ECS runtime where “entity state” lives in **columnar tables**.

## Components

A **component** is a typed record (Pydantic + LanceModel) that defines a schema.

Archetype stores component fields with a **prefix**: `position__x`, `velocity__vx`, etc. This avoids collisions when multiple components are unified into a single table.

## Archetypes

An **archetype** is the *set of components* attached to an entity. Physically, an archetype corresponds to a table with a unified Arrow schema:

- base columns: `world_id`, `run_id`, `entity_id`, `tick`, `is_active`
- plus prefixed component fields

Entities move between archetypes when you add/remove components.

## Processors

A **processor** is a pure transformation:

- input: a Daft `DataFrame` containing all entities matching an archetype
- output: a transformed `DataFrame` (same schema, updated fields)

Processors declare:

- `components`: which component types they require
- `priority`: execution order (low runs first)

## System

A **system** is an ordered list of processors. Each tick:

1. select processors that match the current archetype signature
2. execute them in priority order

## World, tick, and run_id

A **world** owns:

- an entity ID namespace
- an entity → archetype mapping
- the tick counter
- spawn/despawn caches (mutations materialized each tick)
- a “live” snapshot of the most recent DataFrame per archetype (for fast reads)

A **run_id** groups ticks into a run. Storage is keyed by `(world_id, run_id, tick)`.

## The “time travel” idea

Because each tick is persisted, you can query:

- “what was the state of archetype X at tick 73?”
- “how did entity 42’s components evolve across ticks?”
- “replay a run deterministically” (given deterministic processors + inputs)


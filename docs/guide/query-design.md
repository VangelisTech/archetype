# Query Design

## How Queries Work

`QueryService` is the read path. It sits between the API routes and `AsyncWorld`, translating query parameters into DataFrame operations.

For **current-tick** queries (no `tick` parameter, or `tick` matching the latest state), it reads directly from `AsyncWorld._live` — the in-memory snapshot of each archetype's most recent processed DataFrame. This is fast: no store access, no deserialization.

For **historical** queries (a specific `tick` value), it calls `AsyncWorld.query_archetype(sig, ticks=[tick])`, which reads from the append-only store. Every tick is persisted, so any historical state is recoverable.

This mirrors the `prefer_live_reads` pattern used in `AsyncWorld.step()`.

## The Three Read Methods

| Method | What it returns | How it works |
|--------|----------------|--------------|
| `get_world_state(world_id, tick?)` | Entity-to-component mappings, archetype row counts | Iterates `_entity2sig` for entity list, counts active rows per archetype |
| `get_entity(world_id, entity_id, tick?)` | Component field values for one entity | Looks up the entity's archetype signature, filters to the entity, serializes the row |
| `get_components(world_id, type_names, tick?)` | All entities matching the requested component types | Resolves type names to classes, calls `get_components()` which unions across matching archetypes |

All three accept an optional `tick` for time-travel. All three materialize DataFrames to Python dicts at the boundary — this is the serialization layer for REST consumers.

## The Underlying Primitive

The core query operation is `AsyncWorld.get_components(component_types)`. It finds every archetype signature that is a superset of the requested types, projects each to the shared schema, and unions the results. The component tuple is the query predicate.

`QueryService` wraps this with entity lookup, type name resolution, and time-travel routing. The REST API routes wrap `QueryService` with HTTP semantics (404 for missing entities, 400 for unknown component types).

## Limitations

- **Signature resolution is current-only.** `_entity2sig` reflects the entity's current archetype. If an entity changed components between ticks, historical queries use the current signature, which may not match the historical one. This is tracked in #103.
- **Materialization at the boundary.** Every query collects the DataFrame to Python dicts for JSON serialization. Internal consumers that want to stay in Daft's lazy execution model should use `AsyncWorld.get_components()` directly.

# API Reference

The REST API runs on FastAPI. Start with `archetype serve` (default: `http://localhost:8000`).

## Worlds

### Create World

```
POST /worlds
```

```json
{
  "name": "my-simulation",
  "storage_uri": "./archetype_data",
  "namespace": "archetypes"
}
```

Response:
```json
{
  "world_id": "01965a3b-...",
  "name": "my-simulation",
  "tick": 0,
  "entity_count": 0
}
```

### List Worlds

```
GET /worlds
```

Response: Array of `WorldResponse`.

### Get World

```
GET /worlds/{world_id}
```

Response: `WorldResponse` or `404`.

### Remove World

```
DELETE /worlds/{world_id}
```

Response:
```json
{"status": "removed", "world_id": "01965a3b-..."}
```

### Fork World

```
POST /worlds/{world_id}/fork
```

```json
{"name": "branch-A"}
```

Creates a new world from the source world's state.

---

## Commands

All mutations flow through commands. Each command has a type, payload, tick, and priority.

### Command Types

| Type | Payload | Description |
|------|---------|-------------|
| `spawn` | `{"components": [...]}` | Create entity |
| `despawn` | `{"entity_id": int}` | Remove entity |
| `update` | `{"entity_id": int, "components": [...]}` | Update components |
| `add_component` | `{"entity_id": int, "components": [...]}` | Add components |
| `remove_component` | `{"entity_id": int, "component_types": [...]}` | Remove components |
| `add_processor` | `{"processor": ...}` | Add processor |
| `remove_processor` | `{"processor_type": ...}` | Remove processor |
| `create_world` | `{"config": {...}}` | Create child world |
| `destroy_world` | `{"world_id": str}` | Destroy child world |
| `fork_world` | `{"source_world_id": str, "name": str \| null}` | Fork world |
| `message` | `{"sender_id", "receiver_id", "content"}` | Agent message |
| `custom` | `{...}` | User-defined |

### Submit Command

```
POST /worlds/{world_id}/commands
```

```json
{
  "type": "spawn",
  "tick": 0,
  "payload": {"components": []},
  "priority": 0
}
```

Response:
```json
{
  "id": "01965a3b-...",
  "type": "spawn",
  "tick": 0,
  "priority": 0
}
```

Returns `403` if RBAC denies the command.

### Submit Batch

```
POST /worlds/{world_id}/commands/batch
```

```json
{
  "commands": [
    {"type": "spawn", "payload": {"components": []}},
    {"type": "spawn", "payload": {"components": []}}
  ]
}
```

All-or-nothing RBAC validation.

### Command History

```
GET /worlds/{world_id}/commands?limit=100
```

Returns array of `CommandResponse` (most recent last).

### Pending Count

```
GET /worlds/{world_id}/commands/pending
```

```json
{"world_id": "...", "pending_count": 5}
```

---

## Simulation

### Step (Single Tick)

```
POST /worlds/{world_id}/step
```

```json
{"num_steps": 1, "debug": false}
```

Response:
```json
{"world_id": "...", "commands_applied": 3}
```

### Run (Multiple Ticks)

```
POST /worlds/{world_id}/run
```

```json
{"num_steps": 100, "debug": false}
```

Response:
```json
{
  "run_id": "01965a3b-...",
  "world_id": "01965a3b-...",
  "ticks_completed": 100,
  "commands_applied": 42,
  "final_tick": 100
}
```

### List Processors

```
GET /worlds/{world_id}/processors
```

Returns array of processor info (name, priority, components).

---

## Query

### World State

```
GET /worlds/{world_id}/state?tick=5
```

Returns world snapshot at the given tick (or current if omitted).

```json
{
  "world_id": "...",
  "tick": 5,
  "entities": {},
  "archetype_counts": {}
}
```

### Entity State

```
GET /worlds/{world_id}/entities/{entity_id}?tick=5
```

Returns entity state at the given tick.

### Components

```
GET /worlds/{world_id}/components?types=Position,Velocity
```

Query specific component types across all entities.

### Command History (Query Route)

```
GET /worlds/{world_id}/history?limit=100
```

Returns serialized command history.

---

## Error Responses

All errors follow:
```json
{"detail": "World 01965a3b-... not found"}
```

| Code | Meaning |
|------|---------|
| 400 | Bad request (unknown command type, invalid payload) |
| 403 | RBAC denied (insufficient role for command type) |
| 404 | World not found |

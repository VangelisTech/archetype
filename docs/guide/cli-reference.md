# CLI Reference

The `archetype` CLI is built with Typer. Install via `uv sync` or `pip install -e .`.

## Server

### `archetype serve`

Start the FastAPI server.

```bash
archetype serve                        # default: 0.0.0.0:8000
archetype serve --host 127.0.0.1       # localhost only
archetype serve --port 3000            # custom port
archetype serve --reload               # auto-reload on code changes
```

## Status

### `archetype status`

Show all worlds and their current state.

```bash
archetype status
```

Output:
```
  01965a3b-...  name=my-sim  tick=42  entities=100
  01965a3c-...  name=branch-A  tick=10  entities=50
```

## World Management

### `archetype world create`

```bash
archetype world create my-simulation
archetype world create my-sim --uri ./data --namespace experiment_1
```

Options:
- `NAME` (required) — world name
- `--uri` — storage path (default: `./archetype_data`)
- `--namespace` — storage namespace (default: `archetypes`)

### `archetype world list`

```bash
archetype world list
```

### `archetype world inspect`

```bash
archetype world inspect 01965a3b-...
```

Shows world ID, name, and current tick.

### `archetype world remove`

```bash
archetype world remove 01965a3b-...
```

## Simulation

### `archetype run`

Run simulation for N ticks.

```bash
archetype run 01965a3b-... --steps 100
archetype run 01965a3b-... -n 1000
```

Options:
- `WORLD_ID` (required)
- `--steps` / `-n` — number of ticks (default: 1)

### `archetype step`

Execute a single tick.

```bash
archetype step 01965a3b-...
```

## Query

### `archetype query`

Query world state, optionally at a specific tick.

```bash
archetype query 01965a3b-...
archetype query 01965a3b-... --tick 5
archetype query 01965a3b-... -t 42
```

Outputs JSON snapshot.

### `archetype history`

Show command history for a world.

```bash
archetype history 01965a3b-...
archetype history 01965a3b-... --limit 10
archetype history 01965a3b-... -n 20
```

Output:
```
  [0] spawn (priority=0)
  [0] spawn (priority=0)
  [1] update (priority=5)
```

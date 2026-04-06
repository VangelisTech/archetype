---
name: archetype-components
description: Enforces correct Component definitions. Auto-triggers when creating or editing Component subclasses, ECS entities, or archetype schema code.
paths: "src/**/*.py,tests/**/*.py,examples/**/*.py"
---

## Rules

### 1. Components extend `Component`, which extends `LanceModel` (Pydantic)

**Components are NOT dataclasses.** They are Pydantic models with Arrow serialization.

```python
from archetype.core.component import Component

# RIGHT
class Agent(Component):
    name: str = ""
    memory: str = "[]"

# WRONG — dataclass, will not serialize to Arrow
@dataclass
class Agent:
    name: str = ""
```

### 2. All fields must be Arrow-serializable

| Type | Arrow? | What to do |
|------|--------|------------|
| `str`, `int`, `float`, `bool` | Yes | Use directly |
| `list[str]`, `list[int]`, `list[float]` | Yes | Use directly |
| `dict`, `list[dict]` | **No** | JSON-encode to `str` |
| Custom objects | **No** | Serialize to JSON `str` |
| `torch.Tensor` | **No** | Save to file, store path as `str` |

### 3. JSON-encode complex types with `_json` suffix convention

```python
# WRONG — list[dict] is not Arrow-serializable
class DebateState(Component):
    history: list[dict] = []

# RIGHT — JSON string
class DebateState(Component):
    history_json: str = "[]"
```

When working with JSON fields:
```python
# Writing
history.append({"agent": name, "text": text})
df = df.with_column("debatestate__history_json", daft.lit(json.dumps(history)))

# Reading
history = json.loads(row["debatestate__history_json"])
```

### 4. Prefix convention is automatic

`Component.get_prefix()` returns `classname.lower() + "__"`. Fields become `{prefix}{field}` in the DataFrame.

- `Agent.name` → `agent__name`
- `EnvironmentComponent.gravity` → `environmentcomponent__gravity`

Always use the prefixed name in processor column references:
```python
col("agent__name")  # not col("name")
```

### 5. Keep components small and focused

One component = one concern. Prefer composition:

```python
# RIGHT — separate concerns
class Position(Component):
    x: float = 0.0
    y: float = 0.0

class Velocity(Component):
    vx: float = 0.0
    vy: float = 0.0

# WRONG — kitchen sink
class Entity(Component):
    x: float = 0.0
    y: float = 0.0
    vx: float = 0.0
    vy: float = 0.0
    name: str = ""
    health: int = 100
```

### 6. Helper types that are NOT Components

Use plain dataclasses or Pydantic models for:
- Turn-level data within a JSON field (e.g., `Turn` dataclass for building `Session.turns`)
- Resource configs injected into `world.resources` (e.g., `SamplingConfig`)
- API request/response models

These never touch LanceDB and don't need Arrow serialization.

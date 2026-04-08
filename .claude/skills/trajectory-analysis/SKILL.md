---
name: trajectory-analysis
description: Enforces correct Trajectory pipeline patterns. Auto-triggers when creating or editing trajectory components, processors, pipelines, or labeling techniques.
paths: "src/archetype/trajectories/**/*.py,tests/**/test_trajectory*.py,examples/trajectories/**/*.py"
---

## Learning Cards

Concrete insights extracted from the trajectory analysis codebase. Each card is a rule — violating it produces broken pipelines or incorrect evaluations.

---

### Card 1: Entity = (Trajectory, Technique) pair

Each entity is a **(trajectory, technique)** pair, not just a trajectory. If you have 3 trajectories and 2 labeling techniques, you get **6 entities**. The pipeline fans out at ingest time.

```python
# pipeline.ingest() creates N_trajectories * N_techniques entities
for trajectory in trajectories:
    for technique, description in self._labels:
        label = Label(technique=technique, description=description)
        cmd = Command(
            type=CommandType.SPAWN,
            payload={"components": [
                {"type": "Trajectory", **trajectory.model_dump()},
                {"type": "Label", **label.model_dump()},
            ]},
        )
```

**Consequence:** Query results have one row per (trajectory, technique). Group by `label__technique` to compare across techniques. Group by `trajectory__trajectory_id` to see all evaluations of one trajectory.

---

### Card 2: Sampling never drops rows

`SamplingProcessor` sets `label__sampled = True/False` but **never removes entities**. All rows are preserved for comparison and audit. Downstream processors gate on `label__sampled`.

```python
# RIGHT — mark unsampled, preserve all rows
df = df.with_columns({"label__sampled": sampled})

# WRONG — never filter/drop entities in sampling
df = df.where(col("label__sampled"))  # only in LabelingProcessor's split logic
```

**Why:** Dropped rows can't be re-evaluated if sampling config changes. The SamplingProcessor recomputes from scratch each tick — rows marked unsampled last tick can become sampled next tick.

---

### Card 3: Fork to compare, not branch

To compare evaluation techniques, **fork the world** and swap the Label description. Don't try A/B testing within a single world.

```python
# RIGHT — fork creates independent world with cloned entities
fork = await pipeline.fork("strict-eval")
fork.label("correctness", "Binary: 1.0 if exactly correct, 0.0 otherwise")
await fork.run()

# Compare: pipeline.results() vs fork.results()
```

**Why:** Forked worlds share storage but have independent Resources, processors, and state. Each fork can evolve independently without cross-contamination.

---

### Card 4: One tick = full pipeline (sample -> label -> score)

Processors execute in priority order within a single tick:

| Priority | Processor | Action |
|----------|-----------|--------|
| 10 | SamplingProcessor | Marks `label__sampled` |
| 20 | LabelingProcessor | LLM evaluation (sampled only) |
| 30 | ScoringProcessor | Clamps scores to [0, 1] |

One `pipeline.run()` call = one tick = one full evaluation pass. Multiple steps re-evaluate with potentially updated configs or resources.

---

### Card 5: Label description IS the prompt

The `label__description` field is the natural language evaluation instruction. The `LabelingProcessor` wraps it into a structured prompt with trajectory context and extracts `VALUE/SCORE/RATIONALE`.

```python
# The description IS what the LLM evaluates against
pipeline.label(
    "efficiency",
    "Rate how directly the agent reached the solution without "
    "unnecessary backtracking or wasted steps."
)
```

**Prompt engineering happens at the pipeline API level**, not inside the processor. To change evaluation criteria, change the description string — don't modify `LabelingProcessor`.

---

### Card 6: prefer_live_reads for forked worlds

Forked worlds **MUST** step with `prefer_live_reads=True`. Without it, the fork tries to read from storage which may not have the forked data materialized yet.

```python
# RIGHT — always use prefer_live_reads for forks
await container.simulation_service.step(
    fork.world_id,
    RunConfig(num_steps=1, prefer_live_reads=True),
)

# WRONG — fork may see empty state
await container.simulation_service.step(
    fork.world_id,
    RunConfig(num_steps=1),  # reads from storage, fork data missing
)
```

---

### Card 7: Sampling config is a live resource

Inserting a new `SamplingConfig` replaces the previous one immediately. The next step recomputes sampling from scratch — no stale state.

```python
# Initial config: sample everything
world.resources.insert(SamplingConfig())
await pipeline.run()  # all sampled

# Update live: now require >= 4 turns
world.resources.insert(SamplingConfig(min_turns=4))
await pipeline.run()  # recomputed, short trajectories now unsampled
```

**The pipeline also handles this:** calling `pipeline.sample(min_turns=4)` after initialization updates the live resource if the world exists.

---

### Card 8: LLM-free testing pattern

Exclude `LabelingProcessor` in tests. `SamplingProcessor` + `ScoringProcessor` cover the full pipeline flow without API keys.

```python
# Test setup — no LabelingProcessor, no API key needed
await world.system.add_processor(SamplingProcessor())
await world.system.add_processor(ScoringProcessor())
# LabelingProcessor intentionally excluded

# Pre-set scores on Label components to test ScoringProcessor
label = Label(technique="test", description="test", score=1.5)
# After step: score clamped to 1.0
```

---

### Card 9: Turn is a dataclass, not a Component

`Turn` is a plain `@dataclass` for building `Trajectory.turns_json`. It is **NOT** a Component and **NOT** stored as an entity. Complex nested data (turns, tags, metadata) are JSON-encoded strings for Arrow/LanceDB compatibility.

```python
# Turn → dict → JSON string (stored in Trajectory.turns_json)
turns = [Turn(role="user", content="..."), Turn(role="assistant", content="...")]
trajectory = Trajectory.from_turns("id", turns)
# trajectory.turns_json = '[{"role": "user", ...}, {"role": "assistant", ...}]'

# To read back:
turns = trajectory.get_turns()  # deserializes JSON → list[Turn]
```

**Convention:** `_json` suffix signals "this field is a JSON-encoded string, not a native type."

---

### Card 10: Pipeline owns its container lifecycle

`TrajectoryPipeline` creates and owns its `ServiceContainer`. The `_owns_container` flag ensures `shutdown()` only cleans up containers the pipeline created. Forks get their own containers.

```python
# Pipeline creates its own container on first use
await pipeline._ensure_init()  # creates ServiceContainer, world, processors

# Fork gets independent container
fork = await pipeline.fork("fork-name")  # fork._owns_container = True

# Shutdown is safe — only cleans up what you own
await pipeline.shutdown()  # shuts down pipeline's container
await fork.shutdown()      # shuts down fork's container independently
```

**Never share a container across pipelines** unless you explicitly manage the lifecycle.

---

## Hookified Prompt Rules

These rules auto-fire when editing trajectory analysis code.

### Rule 1: Column references use component prefix

```python
# RIGHT
col("trajectory__total_turns")
col("label__sampled")
col("label__technique")

# WRONG — unprefixed columns don't exist
col("total_turns")
col("sampled")
col("technique")
```

### Rule 2: Outcome filtering is substring match

`SamplingConfig.outcome_filter` does **substring matching**, not exact match. `"success"` matches both `"success"` and `"partial success"`.

```python
# This matches "success", "partial success", "success: cleaned up"
SamplingConfig(outcome_filter="success")

# For exact match, use the full outcome string
SamplingConfig(outcome_filter="success: implemented feature correctly")
```

### Rule 3: Tag matching is exact membership

Tag filtering checks exact membership in the JSON-encoded tag list, not substring:

```python
@daft.func
def _tags_contain(tags_json: str, tag: str) -> bool:
    return tag in json.loads(tags_json)  # exact membership, not substring
```

### Rule 4: LabelingProcessor splits sampled/unsampled

The LabelingProcessor splits the DataFrame, runs LLM only on sampled rows, then `concat`s back. Unsampled rows keep their existing label values.

```python
sampled_df = df.where(col("label__sampled"))      # LLM calls here
unsampled_df = df.where(~col("label__sampled"))   # untouched
return sampled_df.concat(unsampled_df)             # rejoin
```

**Never skip the concat** — it would silently drop unsampled entities.

### Rule 5: Extractors are row-wise @daft.func

LLM response parsing uses `@daft.func` (row-wise, auto type inference), not `@daft.func.batch`. This follows the UDF decision tree: simple string parsing = row-wise.

```python
# RIGHT — row-wise, auto type inference
@daft.func
def extract_score(response: str) -> float:
    for line in response.split("\n"):
        if line.startswith("SCORE:"):
            return float(line[6:].strip())
    return 0.0

# WRONG — unnecessary batch overhead
@daft.func.batch(return_dtype=DataType.float64())
def extract_score(responses: Series) -> Series:
    return Series.from_pylist([parse(r) for r in responses.to_pylist()])
```

### Rule 6: max_trajectories uses monotonic ID, not row drop

To cap sampled trajectories without dropping rows, `SamplingProcessor` adds a monotonic ID column, marks excess as unsampled, then drops the temp column.

```python
df = df._add_monotonically_increasing_id("_sample_idx")
df = df.with_columns({
    "label__sampled": col("label__sampled") & (col("_sample_idx") < max_trajectories),
}).exclude("_sample_idx")
```

### Rule 7: Pipeline.ingest() requires labels first

Calling `pipeline.ingest()` before `pipeline.label()` raises `ValueError`. The fan-out depends on knowing all techniques upfront.

```python
# WRONG — no techniques defined
pipeline = TrajectoryPipeline(name="eval")
await pipeline.ingest(trajectories)  # ValueError!

# RIGHT — define techniques, then ingest
pipeline.label("efficiency", "Rate directness of solution")
await pipeline.ingest(trajectories)
```

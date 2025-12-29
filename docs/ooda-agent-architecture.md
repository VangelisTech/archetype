# OODA Agent Architecture

**Date:** 2025-12-27  
**Authors:** Everett, lake-claude-opus-4.5

---

## Overview

Archetype models agents canonically using the **OODA Loop** (Observe → Orient → Decide → Act). The purpose is to experiment with **cognitive architectures** for LLM agents in different environments.

**Key Insight:** Rubrics are templated evals implemented as `daft.functions.prompt()` with Pydantic schemas as `output_type`. This unifies:
- Agent cognition (each OODA phase)
- Evaluation (structured outputs score themselves)
- Training data generation (Daft-native, scalable)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           OODA AGENT                                     │
│                                                                          │
│   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐            │
│   │   OBSERVE     │   │    ORIENT     │   │    DECIDE     │            │
│   │               │   │               │   │               │            │
│   │ prompt(       │ → │ prompt(       │ → │ prompt(       │ → ACT      │
│   │   env_state,  │   │   observation,│   │   orientation,│            │
│   │   output_type=│   │   output_type=│   │   output_type=│            │
│   │   Observation │   │   Orientation │   │   Decision    │            │
│   │ )             │   │ )             │   │ )             │            │
│   └───────────────┘   └───────────────┘   └───────────────┘            │
│          ↓                   ↓                   ↓                       │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                    Pydantic Schemas (Rubrics)                    │  │
│   │   Each schema includes self-evaluation fields:                   │  │
│   │   - confidence: float                                            │  │
│   │   - reasoning: str                                               │  │
│   │   - quality_score: float                                         │  │
│   └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Rubrics as Pydantic Schemas

Each OODA phase has a Pydantic schema that serves as both:
1. **Structured output format** for `daft.functions.prompt()`
2. **Self-evaluation rubric** with scoring fields

### Observe Phase

```python
class Observation(BaseModel):
    """What the agent perceives from the environment.
    
    Rubric: Completeness, Relevance, Accuracy
    """
    # Core perception
    entities: list[str] = Field(description="Entities observed in the environment")
    state: dict[str, Any] = Field(description="Current state observations")
    events: list[str] = Field(description="Events/changes detected")
    
    # Self-evaluation (the rubric)
    completeness: float = Field(
        ge=0, le=1,
        description="How complete is this observation? 0=missing critical info, 1=comprehensive"
    )
    relevance: float = Field(
        ge=0, le=1,
        description="How relevant are the observations to the task? 0=irrelevant, 1=highly relevant"
    )
    confidence: float = Field(
        ge=0, le=1,
        description="Confidence in observation accuracy"
    )
```

### Orient Phase

```python
class Orientation(BaseModel):
    """Agent's situational awareness and world model.
    
    Rubric: Coherence, Integration, Timeliness
    """
    # Core orientation
    situation_assessment: str = Field(description="Current situation summary")
    threats: list[str] = Field(description="Identified threats/risks")
    opportunities: list[str] = Field(description="Identified opportunities")
    mental_model: str = Field(description="Updated understanding of the world")
    
    # Self-evaluation (the rubric)
    coherence: float = Field(
        ge=0, le=1,
        description="How coherent is the mental model? 0=contradictory, 1=fully consistent"
    )
    integration: float = Field(
        ge=0, le=1,
        description="How well integrated with prior knowledge? 0=isolated, 1=fully integrated"
    )
    novelty_handled: float = Field(
        ge=0, le=1,
        description="How well does it handle new/unexpected info? 0=ignored, 1=well incorporated"
    )
```

### Decide Phase

```python
class Decision(BaseModel):
    """Selected action with reasoning.
    
    Rubric: Alignment, Feasibility, Optimality
    """
    # Core decision
    action: str = Field(description="The chosen action")
    reasoning: str = Field(description="Why this action was chosen")
    alternatives: list[str] = Field(description="Other actions considered")
    expected_outcome: str = Field(description="What we expect to happen")
    
    # Self-evaluation (the rubric)
    alignment: float = Field(
        ge=0, le=1,
        description="How well aligned with goals? 0=contradicts goals, 1=perfectly aligned"
    )
    feasibility: float = Field(
        ge=0, le=1,
        description="How feasible is execution? 0=impossible, 1=trivially achievable"
    )
    confidence: float = Field(
        ge=0, le=1,
        description="Confidence this is the optimal action"
    )
    risk_assessment: float = Field(
        ge=0, le=1,
        description="Risk level. 0=very risky, 1=safe"
    )
```

### Act Phase

```python
class Action(BaseModel):
    """Executed action and result.
    
    Rubric: Success, Side Effects, Efficiency
    """
    # Core action
    action_taken: str = Field(description="The action that was executed")
    success: bool = Field(description="Whether the action succeeded")
    outcome: str = Field(description="What actually happened")
    side_effects: list[str] = Field(description="Unintended consequences")
    
    # Self-evaluation (the rubric)
    execution_quality: float = Field(
        ge=0, le=1,
        description="How well was it executed? 0=botched, 1=perfect"
    )
    outcome_match: float = Field(
        ge=0, le=1,
        description="How well did outcome match expectation? 0=opposite, 1=exactly"
    )
    efficiency: float = Field(
        ge=0, le=1,
        description="Resource efficiency. 0=wasteful, 1=optimal"
    )
```

---

## Daft Pipeline

```python
import daft
from daft import col
from daft.functions import prompt

def run_ooda_loop(
    df: daft.DataFrame,
    model: str = "gpt-4o",
    environment_col: str = "environment",
    goal_col: str = "goal",
) -> daft.DataFrame:
    """
    Run a full OODA loop on a DataFrame of agent states.
    
    Each row represents an agent in an environment with a goal.
    """
    
    # OBSERVE
    df = df.with_column(
        "observation",
        prompt(
            col(environment_col),
            model=model,
            system=OBSERVE_PROMPT,
            output_type=Observation,
        )
    )
    
    # ORIENT
    df = df.with_column(
        "orientation",
        prompt(
            format(
                "Goal: {goal}\n\nObservation: {observation}",
                goal=col(goal_col),
                observation=col("observation"),
            ),
            model=model,
            system=ORIENT_PROMPT,
            output_type=Orientation,
        )
    )
    
    # DECIDE
    df = df.with_column(
        "decision",
        prompt(
            format(
                "Goal: {goal}\n\nOrientation: {orientation}",
                goal=col(goal_col),
                orientation=col("orientation"),
            ),
            model=model,
            system=DECIDE_PROMPT,
            output_type=Decision,
        )
    )
    
    return df


# System prompts define the cognitive style
OBSERVE_PROMPT = """You are an observation system. Analyze the environment and report what you perceive.
Be thorough but focused on task-relevant information.
Rate your observation quality honestly."""

ORIENT_PROMPT = """You are an orientation system. Build a mental model from observations.
Identify threats, opportunities, and update your understanding.
Rate your situational awareness honestly."""

DECIDE_PROMPT = """You are a decision system. Choose the best action given your orientation.
Consider alternatives and assess risks.
Rate your decision quality honestly."""
```

---

## Reward from Rubrics

The self-evaluation fields in each schema become multi-dimensional rewards:

```python
def compute_ooda_reward(df: daft.DataFrame) -> daft.DataFrame:
    """Compute reward from OODA rubric scores."""
    
    # Extract rubric scores from nested structs
    df = df.with_column(
        "observe_score",
        (
            col("observation.completeness") * 0.3 +
            col("observation.relevance") * 0.4 +
            col("observation.confidence") * 0.3
        )
    )
    
    df = df.with_column(
        "orient_score",
        (
            col("orientation.coherence") * 0.4 +
            col("orientation.integration") * 0.3 +
            col("orientation.novelty_handled") * 0.3
        )
    )
    
    df = df.with_column(
        "decide_score",
        (
            col("decision.alignment") * 0.4 +
            col("decision.feasibility") * 0.3 +
            col("decision.confidence") * 0.2 +
            col("decision.risk_assessment") * 0.1
        )
    )
    
    # Combined reward
    df = df.with_column(
        "reward",
        col("observe_score") * 0.2 +
        col("orient_score") * 0.3 +
        col("decide_score") * 0.5  # Decision matters most
    )
    
    return df
```

---

## Integration with v2 Training

The OODA loop integrates with weights-as-data for training:

```python
async def train_ooda_agent(
    environments: list[dict],
    goals: list[str],
    initial_weights: str,
    num_epochs: int,
    config: TrainingConfig,
) -> str:
    """Train an OODA agent using the v2 weights-as-data pattern."""
    
    current_weights = initial_weights
    
    for epoch in range(num_epochs):
        # 1. Create Daft DataFrame with environments
        df = daft.from_pylist([
            {"environment": env, "goal": goal, "weights_path": current_weights}
            for env, goal in zip(environments, goals)
        ])
        df = df.with_column("weights_file", file(col("weights_path")))
        
        # 2. Run OODA loop (with custom model using LoRA weights)
        df = run_ooda_loop(df, model=f"local:{current_weights}")
        
        # 3. Compute rubric-based rewards
        df = compute_ooda_reward(df)
        
        # 4. Compute GRPO advantages
        df = compute_advantages(df, group_col="goal", reward_col="reward")
        
        # 5. Train via PyTorch bridge
        metrics = train_epoch_pytorch(df, model, optimizer, config)
        
        # 6. Save new weights (weights as data!)
        current_weights = f"checkpoints/epoch_{epoch+1}.safetensors"
        save_file(model.state_dict(), current_weights)
    
    return current_weights
```

---

## Environments

Environments are separate from agents - they provide:
1. **State** - What agents can observe
2. **Dynamics** - How state changes based on actions
3. **Constraints** - What actions are valid

```python
class Environment(BaseModel):
    """Base environment that agents interact with."""
    
    name: str
    state: dict[str, Any]
    valid_actions: list[str]
    
    def step(self, action: Action) -> "Environment":
        """Apply action and return new environment state."""
        raise NotImplementedError
    
    def render(self) -> str:
        """Render environment state as string for LLM observation."""
        raise NotImplementedError
```

---

## Sampling Strategies

Different sampling strategies for OODA phases:

| Phase | Strategy | Rationale |
|-------|----------|-----------|
| Observe | Low temp (0.1-0.3) | Observations should be accurate |
| Orient | Medium temp (0.5-0.7) | Some creativity in interpretation |
| Decide | Configurable | Exploration vs exploitation trade-off |
| Act | Deterministic | Execute the chosen action faithfully |

```python
def run_ooda_with_sampling(
    df: daft.DataFrame,
    observe_temp: float = 0.2,
    orient_temp: float = 0.5,
    decide_temp: float = 0.7,  # Higher for exploration
) -> daft.DataFrame:
    """Run OODA with phase-specific sampling temperatures."""
    ...
```

---

## File Structure (Proposed)

```
archetype/src/archetype/rl/v2/
├── agents/
│   ├── __init__.py
│   ├── ooda.py           # OODA loop implementation
│   ├── components.py     # Observation, Orientation, Decision, Action
│   └── rubrics.py        # Rubric schemas and reward computation
├── environments/
│   ├── __init__.py
│   ├── base.py           # Environment base class
│   └── examples/         # Example environments
└── training.py           # Existing v2 training (unchanged)
```

---

## Next Steps

1. **Implement OODA Components** - Pydantic schemas in `v2/agents/components.py`
2. **Create Rubric System** - Configurable rubric weights in `v2/agents/rubrics.py`
3. **Build OODA Pipeline** - `run_ooda_loop()` in `v2/agents/ooda.py`
4. **Example Environment** - Simple env for testing
5. **Training Integration** - Connect to v2 weights-as-data pattern

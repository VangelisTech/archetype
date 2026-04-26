# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Analysis
===================

Ingest agent trajectories, label them with natural language descriptions,
and compare techniques via world forking using the runtime-level script API.

Components define the schema. Processors define the pipeline stages.
The runtime owns the process boundary. Worlds stay lazy until first use.

Usage:
    uv run python examples/06_trajectory_analysis.py

Set OPENAI_API_KEY for real LLM labeling, or run without it to see
the pipeline structure (labeling will fail gracefully).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any

import daft
from daft import DataFrame, col

from archetype import ArchetypeRuntime
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.core.resources import Resources

# ── Data Types ──────────────────────────────────────────────────────


@dataclass
class Turn:
    """A single turn in an agent trajectory."""

    role: str  # "user", "assistant", "tool_call", "tool_result", "system"
    content: str = ""
    tool_name: str | None = None
    tool_input: str | None = None
    tool_output: str | None = None
    tokens: int = 0
    duration_ms: float = 0.0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = {
            "role": self.role,
            "content": self.content,
            "tokens": self.tokens,
            "duration_ms": self.duration_ms,
        }
        if self.tool_name is not None:
            d["tool_name"] = self.tool_name
        if self.tool_input is not None:
            d["tool_input"] = self.tool_input
        if self.tool_output is not None:
            d["tool_output"] = self.tool_output
        if self.error is not None:
            d["error"] = self.error
        if self.metadata:
            d["metadata"] = self.metadata
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Turn:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ── Components ──────────────────────────────────────────────────────


class Trajectory(Component):
    """A complete agent trajectory stored as JSON-encoded turns."""

    trajectory_id: str = ""
    source: str = ""
    turns_json: str = "[]"
    total_turns: int = 0
    total_tokens: int = 0
    duration_seconds: float = 0.0
    outcome: str = ""
    tags_json: str = "[]"
    metadata_json: str = "{}"

    @classmethod
    def from_turns(
        cls,
        trajectory_id: str,
        turns: list[Turn],
        *,
        source: str = "",
        outcome: str = "",
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Trajectory:
        total_tokens = sum(t.tokens for t in turns)
        duration = sum(t.duration_ms for t in turns) / 1000.0
        return cls(
            trajectory_id=trajectory_id,
            source=source,
            turns_json=json.dumps([t.to_dict() for t in turns]),
            total_turns=len(turns),
            total_tokens=total_tokens,
            duration_seconds=duration,
            outcome=outcome,
            tags_json=json.dumps(tags or []),
            metadata_json=json.dumps(metadata or {}),
        )

    def get_turns(self) -> list[Turn]:
        return [Turn.from_dict(d) for d in json.loads(self.turns_json)]


class Label(Component):
    """An evaluation label attached to a trajectory."""

    technique: str = ""
    description: str = ""
    value: str = ""
    score: float = 0.0
    rationale: str = ""
    sampled: bool = True


# ── Processor Resources ─────────────────────────────────────────────


@dataclass
class SamplingConfig:
    max_trajectories: int = 0
    min_turns: int = 0
    max_turns: int = 0
    require_tags: list[str] | None = None
    exclude_tags: list[str] | None = None
    outcome_filter: str | None = None


@dataclass
class LabelingConfig:
    model: str = "gpt-5-mini"
    max_output_tokens: int = 512


# ── UDFs ────────────────────────────────────────────────────────────


@daft.func
def _tags_contain(tags_json: str, tag: str) -> bool:
    try:
        return tag in json.loads(tags_json)
    except (json.JSONDecodeError, TypeError):
        return False


def _make_outcome_matcher(substring: str):
    @daft.func
    def _outcome_matches(outcome: str) -> bool:
        if not outcome:
            return False
        return substring in outcome

    return _outcome_matches


@daft.func
def extract_value(response: str) -> str:
    if not response:
        return ""
    for line in response.split("\n"):
        if line.startswith("VALUE:"):
            return line[6:].strip()
    return response[:100]


@daft.func
def extract_score(response: str) -> float:
    if not response:
        return 0.0
    for line in response.split("\n"):
        if line.startswith("SCORE:"):
            try:
                return float(line[6:].strip())
            except ValueError:
                return 0.0
    return 0.0


@daft.func
def extract_rationale(response: str) -> str:
    if not response:
        return ""
    for line in response.split("\n"):
        if line.startswith("RATIONALE:"):
            return line[10:].strip()
    return ""


# ── Processors ──────────────────────────────────────────────────────


class SamplingProcessor(AsyncProcessor):
    """Selects which trajectories to evaluate based on SamplingConfig."""

    components = (Trajectory, Label)
    priority = 10

    async def process(
        self, df: DataFrame, resources: Resources | None = None, **kwargs: Any
    ) -> DataFrame:
        resources = resources or kwargs.get("resources") or Resources()
        config = resources.get(SamplingConfig) or SamplingConfig()

        sampled = daft.lit(True)

        if config.min_turns > 0:
            sampled = sampled & (col("trajectory__total_turns") >= config.min_turns)
        if config.max_turns > 0:
            sampled = sampled & (col("trajectory__total_turns") <= config.max_turns)
        if config.outcome_filter:
            matcher = _make_outcome_matcher(config.outcome_filter)
            sampled = sampled & matcher(col("trajectory__outcome"))
        if config.require_tags:
            for tag in config.require_tags:
                sampled = sampled & _tags_contain(col("trajectory__tags_json"), daft.lit(tag))
        if config.exclude_tags:
            for tag in config.exclude_tags:
                sampled = sampled & ~_tags_contain(col("trajectory__tags_json"), daft.lit(tag))

        df = df.with_columns({"label__sampled": sampled})

        if config.max_trajectories > 0:
            df = df._add_monotonically_increasing_id("_sample_idx")
            df = df.with_columns(
                {
                    "label__sampled": col("label__sampled")
                    & (col("_sample_idx") < config.max_trajectories),
                }
            ).exclude("_sample_idx")

        return df


class LabelingProcessor(AsyncProcessor):
    """Applies a labeling technique to sampled trajectories via LLM."""

    components = (Trajectory, Label)
    priority = 20

    async def process(
        self, df: DataFrame, resources: Resources | None = None, **kwargs: Any
    ) -> DataFrame:
        resources = resources or kwargs.get("resources") or Resources()
        config = resources.get(LabelingConfig) or LabelingConfig()

        from daft.functions import prompt

        sampled_df = df.where(col("label__sampled"))
        unsampled_df = df.where(~col("label__sampled"))

        eval_prompt = (
            "You are an expert evaluator of AI agent trajectories.\n\n"
            "## Evaluation Technique\n"
            + col("label__technique")
            + ": "
            + col("label__description")
            + "\n\n## Trajectory\n"
            "Source: "
            + col("trajectory__source")
            + "\nOutcome: "
            + col("trajectory__outcome")
            + "\nTotal turns: "
            + col("trajectory__total_turns").cast(daft.DataType.string())
            + "\nDuration: "
            + col("trajectory__duration_seconds").cast(daft.DataType.string())
            + "s\n\nTurns:\n"
            + col("trajectory__turns_json")
            + "\n\n## Instructions\n"
            "Evaluate this trajectory according to the technique above.\n"
            "Respond in EXACTLY this format (no other text):\n"
            "VALUE: <a short categorical label>\n"
            "SCORE: <float 0.0 to 1.0>\n"
            "RATIONALE: <1-2 sentence explanation>"
        )

        llm_col = prompt(
            eval_prompt,
            model=config.model,
            max_output_tokens=config.max_output_tokens,
        )

        sampled_df = sampled_df.with_columns(
            {
                "label__value": extract_value(llm_col),
                "label__score": extract_score(llm_col),
                "label__rationale": extract_rationale(llm_col),
            }
        )

        return sampled_df.concat(unsampled_df)


class ScoringProcessor(AsyncProcessor):
    """Clamps scores to [0, 1]."""

    components = (Trajectory, Label)
    priority = 30

    async def process(self, df: DataFrame, **kwargs: Any) -> DataFrame:
        @daft.func
        def clamp_score(score: float) -> float:
            return max(0.0, min(1.0, score))

        return df.with_columns({"label__score": clamp_score(col("label__score"))})


# ── Synthetic Data ──────────────────────────────────────────────────


def make_trajectories() -> list[Trajectory]:
    """Build synthetic agent trajectories for demonstration."""

    efficient = Trajectory.from_turns(
        trajectory_id="traj-001",
        source="claude-code",
        outcome="success: implemented feature correctly on first attempt",
        tags=["feature", "python", "clean"],
        metadata={"repo": "acme/api", "model": "claude-sonnet-4-6"},
        turns=[
            Turn(role="user", content="Add a /health endpoint to the API", tokens=12),
            Turn(
                role="assistant", content="I'll add a health endpoint to the FastAPI app.", tokens=8
            ),
            Turn(
                role="tool_call",
                content="Reading app/main.py",
                tool_name="Read",
                tool_input='{"path": "app/main.py"}',
                tokens=5,
                duration_ms=120,
            ),
            Turn(
                role="tool_result",
                content="from fastapi import FastAPI\napp = FastAPI()\n...",
                tokens=50,
            ),
            Turn(
                role="tool_call",
                content="Editing app/main.py",
                tool_name="Edit",
                tool_input='{"path": "app/main.py", "new": "@app.get(\\"/health\\")..."}',
                tokens=30,
                duration_ms=85,
            ),
            Turn(
                role="assistant",
                content='Added the /health endpoint. It returns {"status": "ok"}.',
                tokens=15,
            ),
        ],
    )

    backtracking = Trajectory.from_turns(
        trajectory_id="traj-002",
        source="claude-code",
        outcome="success: fixed bug after two wrong approaches",
        tags=["bugfix", "python", "backtracking"],
        metadata={"repo": "acme/api", "model": "claude-sonnet-4-6"},
        turns=[
            Turn(role="user", content="Fix the 500 error on POST /users", tokens=10),
            Turn(
                role="assistant",
                content="Let me investigate the error in the users endpoint.",
                tokens=12,
            ),
            Turn(
                role="tool_call",
                content="Reading app/routes/users.py",
                tool_name="Read",
                tool_input='{"path": "app/routes/users.py"}',
                tokens=5,
                duration_ms=100,
            ),
            Turn(role="tool_result", content="async def create_user(...):\n    ...", tokens=80),
            Turn(
                role="assistant",
                content="I think the issue is the missing validation. Let me add it.",
                tokens=20,
            ),
            Turn(
                role="tool_call",
                content="Editing users.py — adding validation",
                tool_name="Edit",
                tool_input='{"path": "app/routes/users.py", "new": "...validation..."}',
                tokens=25,
                duration_ms=90,
            ),
            Turn(
                role="tool_call",
                content="Running tests",
                tool_name="Bash",
                tool_input='{"command": "pytest tests/"}',
                tokens=5,
                duration_ms=3000,
            ),
            Turn(
                role="tool_result",
                content="FAILED tests/test_users.py::test_create - TypeError",
                tokens=30,
                error="Tests failed",
            ),
            Turn(
                role="assistant",
                content="That wasn't it. Let me look at the database model instead.",
                tokens=15,
            ),
            Turn(
                role="tool_call",
                content="Reading app/models/user.py",
                tool_name="Read",
                tool_input='{"path": "app/models/user.py"}',
                tokens=5,
                duration_ms=80,
            ),
            Turn(role="tool_result", content="class User(Base):\n    ...", tokens=60),
            Turn(
                role="assistant",
                content="Found it — the email column is NOT NULL but the schema allows None.",
                tokens=20,
            ),
            Turn(
                role="tool_call",
                content="Fixing the schema",
                tool_name="Edit",
                tool_input='{"path": "app/routes/users.py", "new": "...email required..."}',
                tokens=20,
                duration_ms=90,
            ),
            Turn(
                role="tool_call",
                content="Running tests again",
                tool_name="Bash",
                tool_input='{"command": "pytest tests/"}',
                tokens=5,
                duration_ms=2500,
            ),
            Turn(role="tool_result", content="All 12 tests passed", tokens=10),
            Turn(
                role="assistant",
                content="Fixed. The email field was optional in the Pydantic schema but required in the DB.",
                tokens=20,
            ),
        ],
    )

    failed = Trajectory.from_turns(
        trajectory_id="traj-003",
        source="claude-code",
        outcome="failure: could not resolve circular import",
        tags=["refactor", "python", "failed"],
        metadata={"repo": "acme/api", "model": "claude-sonnet-4-6"},
        turns=[
            Turn(
                role="user",
                content="Refactor the circular import between models and utils",
                tokens=12,
            ),
            Turn(
                role="assistant",
                content="I'll restructure the imports to break the cycle.",
                tokens=10,
            ),
            Turn(
                role="tool_call",
                content="Reading models/__init__.py",
                tool_name="Read",
                tool_input='{"path": "app/models/__init__.py"}',
                tokens=5,
                duration_ms=100,
            ),
            Turn(
                role="tool_result",
                content="from .user import User\nfrom .utils import ...",
                tokens=40,
            ),
            Turn(
                role="tool_call",
                content="Editing models/__init__.py",
                tool_name="Edit",
                tool_input='{"path": "app/models/__init__.py", "new": "...lazy imports..."}',
                tokens=30,
                duration_ms=90,
            ),
            Turn(
                role="tool_call",
                content="Running tests",
                tool_name="Bash",
                tool_input='{"command": "pytest tests/"}',
                tokens=5,
                duration_ms=2000,
                error="ImportError: circular import detected",
            ),
            Turn(
                role="assistant",
                content="I'm stuck on this circular dependency. The issue runs deeper than I initially thought.",
                tokens=25,
            ),
        ],
    )

    return [efficient, backtracking, failed]


# ── Main ────────────────────────────────────────────────────────────


async def main():
    has_openai_key = bool(os.getenv("OPENAI_API_KEY"))

    trajectories = make_trajectories()
    print(f"Created {len(trajectories)} synthetic trajectories\n")

    storage = StorageConfig(uri="./trajectory_data", namespace="trajectories")

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "trajectory-eval",
            storage=storage,
            processors=[
                SamplingProcessor(),
                *([LabelingProcessor()] if has_openai_key else []),
                ScoringProcessor(),
            ],
            resources=[SamplingConfig(min_turns=3), LabelingConfig(model="gpt-5-mini")],
        )

        label_specs = [
            (
                "efficiency",
                "Rate how directly the agent reached the solution without unnecessary "
                "backtracking or wasted steps. A perfect score means the agent identified the "
                "correct approach immediately.",
            ),
            (
                "correctness",
                "Did the agent produce the correct final result? Score 1.0 for fully "
                "correct, 0.5 for partially correct, 0.0 for incorrect or unresolved.",
            ),
        ]

        print("Ingesting trajectories...")
        for trajectory in trajectories:
            for technique, description in label_specs:
                label = Label(technique=technique, description=description)
                await world.spawn(trajectory, label)

        total = len(trajectories) * len(label_specs)
        print(
            f"  -> {len(trajectories)} trajectories x {len(label_specs)} techniques = {total} entities\n"
        )

        if not has_openai_key:
            print("OPENAI_API_KEY not set; running sampling/score pipeline without LLM labeling.\n")

        print("Running pipeline (sample -> label -> score)...")
        await world.step()
        print("  -> Pipeline completed\n")

        print("Results:")
        rows = (await world.query(Trajectory, Label)).collect().to_pylist()
        for row in rows:
            if not row.get("is_active", True):
                continue
            tid = row.get("trajectory__trajectory_id", "")
            tech = row.get("label__technique", "")
            score = row.get("label__score", 0.0)
            value = row.get("label__value", "")
            rationale = row.get("label__rationale", "")
            print(f"  [{tech}] {tid}: score={score:.2f} value={value!r}")
            if rationale:
                print(f"    rationale: {rationale}")
        print()

        print("Forking world to compare a stricter sampling threshold...")
        fork = await world.fork("strict-eval", storage=storage)
        print(f"  -> Forked world '{fork.name}', both worlds coexist in storage\n")


if __name__ == "__main__":
    asyncio.run(main())

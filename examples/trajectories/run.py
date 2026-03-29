# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Analysis Example
============================

Demonstrates the full pipeline: ingest sessions, label them with
natural language descriptions, compare techniques via world forking.

Usage:
    uv run python examples/trajectories/run.py

Set OPENAI_API_KEY for real LLM labeling, or run without it to see
the pipeline structure (labeling will fail gracefully).
"""

import asyncio

from archetype.trajectories import Session, TrajectoryPipeline
from archetype.trajectories.components import Turn

# ── Synthetic session data ──


def make_sessions() -> list[Session]:
    """Build a few synthetic agent sessions for demonstration."""

    # Session 1: Clean, efficient problem-solving
    efficient = Session.from_turns(
        session_id="session-001",
        source="claude-code",
        outcome="success: implemented feature correctly on first attempt",
        tags=["feature", "python", "clean"],
        metadata={"repo": "acme/api", "model": "claude-sonnet-4-6"},
        turns=[
            Turn(role="user", content="Add a /health endpoint to the API", tokens=12),
            Turn(role="assistant", content="I'll add a health endpoint to the FastAPI app.", tokens=8),
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
                content="Added the /health endpoint. It returns {\"status\": \"ok\"}.",
                tokens=15,
            ),
        ],
    )

    # Session 2: Backtracking, multiple attempts
    backtracking = Session.from_turns(
        session_id="session-002",
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

    # Session 3: Failed session
    failed = Session.from_turns(
        session_id="session-003",
        source="claude-code",
        outcome="failure: could not resolve circular import",
        tags=["refactor", "python", "failed"],
        metadata={"repo": "acme/api", "model": "claude-sonnet-4-6"},
        turns=[
            Turn(role="user", content="Refactor the circular import between models and utils", tokens=12),
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
            Turn(role="tool_result", content="from .user import User\nfrom .utils import ...", tokens=40),
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


# ── Main ──


async def main():
    sessions = make_sessions()
    print(f"Created {len(sessions)} synthetic sessions\n")

    # Build pipeline with two labeling techniques
    pipeline = (
        TrajectoryPipeline(name="trajectory-eval", storage_uri="./trajectory_data")
        .label("efficiency", "Rate how directly the agent reached the solution without unnecessary backtracking or wasted steps. A perfect score means the agent identified the correct approach immediately.")
        .label("correctness", "Did the agent produce the correct final result? Score 1.0 for fully correct, 0.5 for partially correct, 0.0 for incorrect or unresolved.")
        .sample(min_turns=3)
    )

    # Ingest
    print("Ingesting sessions...")
    await pipeline.ingest(sessions)
    print(f"  → {len(sessions)} sessions × {len(pipeline._labels)} techniques = {len(sessions) * len(pipeline._labels)} entities\n")

    # Run (this calls the LLM for labeling — skip if no API key)
    print("Running pipeline (sample → label → score)...")
    try:
        await pipeline.run()
        print("  → Pipeline completed\n")
    except Exception as e:
        print(f"  → Pipeline errored (expected without API key): {e}\n")

    # Show results
    print("Results:")
    results = await pipeline.results()
    for r in results:
        print(f"  [{r['technique']}] {r['session_id']}: score={r['score']:.2f} value={r['value']!r}")
        if r["rationale"]:
            print(f"    rationale: {r['rationale']}")
    print()

    # Show what forking looks like
    print("To compare a different labeling approach, fork the world:")
    print('  fork = await pipeline.fork("strict-eval")')
    print('  fork.label("correctness", "Binary only: 1.0 if output is exactly right, 0.0 otherwise")')
    print("  await fork.run()")
    print("  # Both worlds coexist in storage — query and compare")

    await pipeline.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

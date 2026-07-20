# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood a validator-gated coding mission on a real repository branch.

The author supplies an explicit task graph. Agent Missions installs the ECS
components, relationship edges, transition processors, and sandbox lifecycle.

Inspect the mission without external work:

    uv run --extra coding-agent python examples/11_coding_agent_mission.py --dry-run

Run the Modal/Codex dogfood:

    uv run --extra coding-agent python examples/11_coding_agent_mission.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import time

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.missions import AgentMissionConfig, AgentTask, CommandValidator
from archetype.missions.sandboxes import (
    ModalAgentMissionSandbox,
    ModalAgentSandboxConfig,
)

ISSUE = "https://github.com/VangelisTech/archetype/issues/543"
REPOSITORY = "VangelisTech/archetype"

MISSION_CONFIG = AgentMissionConfig(
    sandbox=ModalAgentMissionSandbox(
        ModalAgentSandboxConfig(
            app_name="archetype-agent-missions",
            auth_volume_name="archetype-codex-auth",
            github_secret_name="archetype-github",
            model=os.environ.get("CODING_AGENT_MODEL", ""),
            push=True,
        )
    ),
    max_ticks=40,
)

TASKS = (
    AgentTask(
        name="regression",
        prompt=(
            f"Reproduce issue #543 ({ISSUE}). Add a deterministic regression test named "
            "test_component_query_tolerates_added_fields in "
            "tests/app/test_query_schema_evolution.py. Reproduce a historical table "
            "written with an older component schema and query it through a fresh world "
            "using the same component name with one added field. Do not change production "
            "code in this task."
        ),
        validators=(
            CommandValidator(
                name="regression_is_red",
                command=(
                    "uv",
                    "run",
                    "pytest",
                    "-q",
                    "tests/app/test_query_schema_evolution.py::"
                    "test_component_query_tolerates_added_fields",
                ),
                expected_returncode=1,
            ),
            CommandValidator(
                name="regression_file_only",
                command=(
                    "bash",
                    "-lc",
                    'test "$(git status --porcelain --untracked-files=all | cut -c4-)" = '
                    "tests/app/test_query_schema_evolution.py",
                ),
                timeout_seconds=60,
            ),
        ),
    ),
    AgentTask(
        name="implementation",
        prompt=(
            f"Implement the smallest layer-correct fix for {ISSUE}. Preserve the red "
            "regression committed by the predecessor task. Querying historical tables "
            "whose matching component schema lacks a newly added field must no longer "
            "raise FieldNotFound; retain fail-closed catalog fingerprint validation and "
            "the lazy DataFrame plan."
        ),
        validators=(
            CommandValidator(
                name="focused_contract",
                command=(
                    "uv",
                    "run",
                    "pytest",
                    "-q",
                    "tests/app/test_query_schema_evolution.py",
                    "tests/app/test_runtime_contracts.py",
                ),
            ),
            CommandValidator(
                name="architecture",
                command=("uv", "run", "python", "scripts/check_architecture.py"),
            ),
            CommandValidator(
                name="lazy_audit",
                command=("uv", "run", "python", "scripts/check_lazy_audit.py"),
            ),
            CommandValidator(
                name="ruff",
                command=(
                    "uv",
                    "run",
                    "ruff",
                    "check",
                    "src/archetype/app/query/service.py",
                    "tests/app/test_query_schema_evolution.py",
                ),
                timeout_seconds=300,
            ),
            CommandValidator(
                name="diff_check",
                command=("git", "diff", "--check", "HEAD"),
                timeout_seconds=60,
            ),
        ),
        depends_on=("regression",),
    ),
)


def _default_branch() -> str:
    return f"agent/dogfood-543-{time.strftime('%Y%m%d-%H%M%S')}"


def _storage(branch: str) -> StorageConfig:
    namespace = re.sub(r"[^a-zA-Z0-9_]+", "_", branch).strip("_")
    return StorageConfig(uri=".context/agent-missions/data", namespace=namespace)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--branch", default=os.environ.get("CODING_AGENT_BRANCH", ""))
    return parser.parse_args()


async def main() -> None:
    arguments = _arguments()
    branch = arguments.branch or _default_branch()
    print(f"Mission: fix #543 ({ISSUE})")
    print(f"Branch:  {branch}")
    print("Tasks:")
    for task in TASKS:
        dependency = f" <- {', '.join(task.depends_on)}" if task.depends_on else ""
        print(f"  {task.name}{dependency}")
    if arguments.dry_run:
        return

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "dogfood-issue-543",
            config=MISSION_CONFIG,
            storage=_storage(branch),
        ) as missions:
            submitted = await missions.submit(
                name="component-schema-evolution",
                repository=REPOSITORY,
                branch=branch,
                tasks=TASKS,
            )
            result = await missions.run(submitted)

    print(f"Result:  {result.status} after {result.ticks_completed} ticks")
    for task in result.tasks:
        print(f"  {task.name}: {task.status} ({task.attempts} attempt(s), {task.commit_sha})")
    print(f"Pushed:  https://github.com/{REPOSITORY}/tree/{branch}")


if __name__ == "__main__":
    asyncio.run(main())

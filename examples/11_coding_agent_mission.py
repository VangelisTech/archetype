# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Dogfood a validator-gated Codex mission on a real repository branch.

The author supplies an explicit task graph. Agent Missions installs the ECS
components, relationship edges, transition processors, and sandbox lifecycle.

Inspect the mission without external work:

    uv run --extra coding-agent python examples/11_coding_agent_mission.py --dry-run

Agent Missions v0.5 admits the Modal backend. Modal uses the same Codex
subscription through a named remote Volume. Its
device-login flow is separate from an OpenAI API key and from the local Codex
session running this script. Set the explicit provider namespace, initialize it
once, then run and optionally stream the sandbox's durable live output:

    modal token set  # prompts without placing the token secret in argv
    export CODING_AGENT_MODAL_WORKSPACE="your-modal-workspace"
    export CODING_AGENT_MODAL_ENVIRONMENT="main"
    uv run --extra coding-agent python examples/11_coding_agent_mission.py \
        --login
    uv run --extra coding-agent python examples/11_coding_agent_mission.py \
        --follow

To attach from another terminal, copy the printed ``sb-...`` identity:

    uv run --extra coding-agent python examples/11_coding_agent_mission.py \
        --monitor sb-...

To open the same server-owned terminal in a browser, mint a transient,
authenticated Modal Connect Token for either the read-only spectate lane or
the separately writable takeover lane:

    uv run --extra coding-agent python examples/11_coding_agent_mission.py \
        --spectate sb-...
    uv run --extra coding-agent python examples/11_coding_agent_mission.py \
        --takeover sb-...

Each command deliberately displays its one bearer-capability browser URL.
Treat it like a password: do not copy it into durable logs or share it. It may
also remain in browser history until cleared, and sandbox teardown is its
practical revocation boundary.

    The subscription credential lives only in a dedicated broker volume. Use a
    different volume for each concurrently active runtime. Modal stages
    ``auth.json`` only through app-server thread admission, deletes it before the
    turn, TUI, or model-driven tools, and never writes a mission refresh back to
    the broker. ``GITHUB_TOKEN`` is leased separately only to the final push
    process in the non-agent broker sandbox.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time

from archetype import ArchetypeRuntime
from archetype.core.config import StorageConfig
from archetype.missions import AgentMissionConfig, AgentTask, CommandValidator
from archetype.missions.sandboxes import (
    MODAL_ACTIVITY_PROTOCOL_EPOCH,
    ModalSandboxBackend,
    ModalSandboxConfig,
    ModalSandboxSession,
    SandboxEvent,
    SandboxEventType,
)

ISSUE = "https://github.com/VangelisTech/archetype/issues/543"
REPOSITORY = "VangelisTech/archetype"
REGRESSION_TEST = "tests/world/test_query_schema_evolution.py"
QUERY_SOURCE = "src/archetype/world/query.py"

TASKS = (
    AgentTask(
        name="regression",
        prompt=(
            f"Reproduce issue #543 ({ISSUE}). Add a deterministic regression test named "
            "test_component_query_tolerates_added_fields in "
            f"{REGRESSION_TEST}. Reproduce a historical table "
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
                    f"{REGRESSION_TEST}::test_component_query_tolerates_added_fields",
                ),
                expected_returncode=1,
            ),
            CommandValidator(
                name="regression_file_only",
                command=(
                    "bash",
                    "-lc",
                    f'test "$(git status --porcelain --untracked-files=all | cut -c4-)" '
                    f'= "{REGRESSION_TEST}"',
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
                    REGRESSION_TEST,
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
                    QUERY_SOURCE,
                    REGRESSION_TEST,
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
    parser.add_argument(
        "--backend",
        choices=("modal",),
        default="modal",
        help="v0.5 admits only the Modal Activity backend",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="initialize Modal's Codex subscription credential and exit",
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help="stream live output while a new Modal mission runs",
    )
    viewport = parser.add_mutually_exclusive_group()
    viewport.add_argument(
        "--monitor",
        metavar="SANDBOX_ID",
        help="attach to an existing Modal sb-... identity and exit",
    )
    viewport.add_argument(
        "--spectate",
        metavar="SANDBOX_ID",
        help="print a transient authenticated read-only browser URL and exit",
    )
    viewport.add_argument(
        "--takeover",
        metavar="SANDBOX_ID",
        help="print a transient authenticated writable browser URL and exit",
    )
    return parser.parse_args()


def _backend(name: str) -> tuple[ModalSandboxBackend, str]:
    if name != "modal":
        raise ValueError("Agent Missions v0.5 admits only the Modal backend")
    auth_volume = os.environ.get("CODEX_AUTH_VOLUME", "archetype-codex-auth")
    image_id = os.environ.get("CODING_AGENT_MODAL_IMAGE_ID", "")
    backend = ModalSandboxBackend(
        ModalSandboxConfig(
            app_name=os.environ.get("CODING_AGENT_MODAL_APP", "archetype-agent-missions"),
            image_id=image_id,
            auth_volume_name=auth_volume,
            github_secret_name=os.environ.get("CODING_AGENT_GITHUB_SECRET", "archetype-github"),
            workspace_name=os.environ.get(
                "CODING_AGENT_MODAL_WORKSPACE",
                "example-workspace",
            ),
            environment_name=os.environ.get(
                "CODING_AGENT_MODAL_ENVIRONMENT",
                "main",
            ),
            operation_protocol_epoch=MODAL_ACTIVITY_PROTOCOL_EPOCH,
        )
    )
    return backend, backend.environment


async def run_demo(
    storage_uri: str,
    *,
    backend_name: str = "modal",
) -> dict[str, object]:
    """Return the credential-free typed authoring receipt without external work."""
    if not storage_uri:
        raise ValueError("storage_uri must be non-empty")
    backend, environment = _backend(backend_name)
    return {
        "mode": "dry_run",
        "repository": REPOSITORY,
        "backend": backend_name,
        "backend_type": type(backend).__name__,
        "environment_is_pinned": "sha256:" in environment,
        "tasks": [
            {
                "name": task.name,
                "depends_on": list(task.depends_on),
                "validators": [
                    {
                        "name": validator.name,
                        "expected_returncode": validator.expected_returncode,
                    }
                    for validator in task.validators
                ],
            }
            for task in TASKS
        ],
        "task_paths": {
            "implementation": {
                "path": QUERY_SOURCE,
            },
            "regression": {
                "path": REGRESSION_TEST,
            },
        },
        "external_work_started": False,
    }


async def main() -> None:
    arguments = _arguments()
    if (
        not arguments.dry_run
        and not arguments.login
        and not arguments.monitor
        and not arguments.spectate
        and not arguments.takeover
        and "CODING_AGENT_MODAL_WORKSPACE" not in os.environ
    ):
        raise ValueError(
            "set CODING_AGENT_MODAL_WORKSPACE to the authenticated Modal workspace name"
        )
    backend, environment = _backend(arguments.backend)
    if arguments.login:
        await backend.login_codex()
        print("Codex subscription credential initialized for Modal")
        return
    if arguments.monitor:
        await ModalSandboxSession.monitor(
            arguments.monitor,
            stdout_target=sys.stdout,
            stderr_target=sys.stderr,
            on_monitor_event=lambda event: print(json.dumps(event, sort_keys=True)),
        )
        return
    if arguments.spectate:
        grant = await ModalSandboxSession.issue_spectate_grant(arguments.spectate)
        print(grant.browser_url)
        return
    if arguments.takeover:
        grant = await ModalSandboxSession.issue_takeover_grant(arguments.takeover)
        print(grant.browser_url)
        return

    branch = arguments.branch or _default_branch()
    print(f"Mission: fix #543 ({ISSUE})")
    print(f"Branch:  {branch}")
    print(f"Backend: {arguments.backend} ({environment})")
    print("Tasks:")
    for task in TASKS:
        dependency = f" <- {', '.join(task.depends_on)}" if task.depends_on else ""
        print(f"  {task.name}{dependency}")
    if arguments.dry_run:
        return

    monitor_task: asyncio.Task[dict[str, object]] | None = None

    def sandbox_event(event: SandboxEvent) -> None:
        nonlocal monitor_task
        if event.kind is not SandboxEventType.READY:
            return
        identity = event.sandbox
        print(f"Sandbox: {identity.provider}/{identity.sandbox_id}", flush=True)
        if identity.provider == "modal":
            print(
                "Attach:  uv run --extra coding-agent python "
                "examples/11_coding_agent_mission.py --backend modal "
                f"--monitor {identity.sandbox_id}",
                flush=True,
            )
            if arguments.follow:
                monitor_task = asyncio.create_task(
                    ModalSandboxSession.monitor(
                        identity.sandbox_id,
                        stdout_target=sys.stdout,
                        stderr_target=sys.stderr,
                        on_monitor_event=lambda value: print(json.dumps(value, sort_keys=True)),
                    )
                )

    mission_config = AgentMissionConfig(
        sandbox_backend=backend,
        sandbox_environment=os.environ.get("CODING_AGENT_ENVIRONMENT", environment),
        model=os.environ.get("CODING_AGENT_MODEL", ""),
        max_ticks=40,
        on_sandbox_event=sandbox_event,
    )

    try:
        async with ArchetypeRuntime() as runtime:
            async with runtime.missions(
                "dogfood-issue-543",
                config=mission_config,
                storage=_storage(branch),
            ) as missions:
                submitted = await missions.submit(
                    name="component-schema-evolution",
                    repository=REPOSITORY,
                    branch=branch,
                    tasks=TASKS,
                )
                result = await missions.run(submitted)
    finally:
        if monitor_task is not None:
            await monitor_task

    print(f"Episode: {result.episode_id}")
    print(f"Result:  {result.status} after {result.ticks_completed} ticks")
    for task in result.tasks:
        print(
            f"  {task.name}: {task.status} "
            f"({task.dispatches} dispatch(es), {', '.join(task.commit_shas)})"
        )
    print(f"Pushed:  https://github.com/{REPOSITORY}/tree/{branch}")


if __name__ == "__main__":
    asyncio.run(main())

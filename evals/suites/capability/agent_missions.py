# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free end-to-end capability proof for Agent Missions V1."""

from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from archetype import ArchetypeRuntime
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.missions import (
    AgentExecution,
    AgentMissionConfig,
    AgentTask,
    CommandValidator,
    Commit,
    FrictionLog,
    Sandbox,
    TaskState,
    ValidationResult,
)
from archetype.missions.coding_agents import (
    AgentProcessObservation,
    TaskDispatchRequest,
)
from archetype.missions.sandboxes import (
    CheckpointRef,
    ProcessRequest,
    ProcessResult,
    SandboxCapabilities,
    SandboxIdentity,
    SandboxSession,
    SandboxSpec,
    SandboxStatus,
)
from archetype.projections import latest
from archetype.runtime.missions import RuntimeMissions
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult

SUITE = "capability"
_BRANCH = "agent/capability-transition-authority"


def _hermetic_environment(home: Path) -> dict[str, str]:
    home.mkdir(parents=True, exist_ok=True)
    return {
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "HOME": str(home),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": os.environ.get("PATH", os.defpath),
    }


def _git(*arguments: str, cwd: Path | None = None) -> str:
    home = (cwd or Path.cwd()) / ".capability-home"
    result = subprocess.run(
        ("git", *arguments),
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
        env=_hermetic_environment(home),
    )
    return result.stdout.strip()


def _local_bare_remote(root: Path) -> Path:
    seed = root / "seed"
    seed.mkdir()
    _git("init", "-b", "main", cwd=seed)
    _git("config", "user.name", "Capability Fixture", cwd=seed)
    _git("config", "user.email", "capability@example.com", cwd=seed)
    (seed / "README.md").write_text("mission capability fixture\n", encoding="utf-8")
    _git("add", "README.md", cwd=seed)
    _git("commit", "-m", "seed", cwd=seed)
    remote = root / "remote.git"
    _git("clone", "--bare", str(seed), str(remote), cwd=root)
    return remote


def _remote_revision(remote: Path) -> str:
    result = subprocess.run(
        ("git", "--git-dir", str(remote), "rev-parse", f"refs/heads/{_BRANCH}"),
        check=False,
        capture_output=True,
        text=True,
        env=_hermetic_environment(remote.parent / ".capability-home"),
    )
    return result.stdout.strip() if result.returncode == 0 else ""


async def _latest_rows(
    missions: RuntimeMissions,
    component: type[Component],
) -> list[dict[str, Any]]:
    try:
        frame = await missions.query(component)
    except KeyError:
        return []
    return latest(frame).to_pylist()


class _CredentialFreeSession:
    """Run the provider-neutral sandbox protocol in one temporary directory."""

    def __init__(self, spec: SandboxSpec) -> None:
        self.spec = spec
        self.close_calls = 0
        self._environment = _hermetic_environment(Path(spec.workdir).parent / ".capability-home")

    @property
    def identity(self) -> SandboxIdentity:
        return SandboxIdentity("capability-local", "mission-capability", self.spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        # The harness requests this symbolic lease for Git operations. A local
        # file remote needs no credential and the session injects no secret.
        return SandboxCapabilities(secret_names=("github",))

    async def status(self) -> SandboxStatus:
        return SandboxStatus.CLOSED if self.close_calls else SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        environment = self._environment.copy()
        environment.update(request.environment_dict())
        process = await asyncio.create_subprocess_exec(
            *request.argv,
            cwd=request.workdir,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=request.timeout_seconds,
            )
        except TimeoutError:
            process.kill()
            await process.communicate()
            raise
        if process.returncode is None:
            raise RuntimeError("sandbox process exited without a return code")
        return ProcessResult(
            argv=request.argv,
            returncode=process.returncode,
            stdout=stdout.decode(),
            stderr=stderr.decode(),
        )

    async def checkpoint(self) -> CheckpointRef:
        raise NotImplementedError

    async def close(self) -> None:
        self.close_calls += 1


class _CredentialFreeBackend:
    name = "capability-local"

    def __init__(self) -> None:
        self.create_calls = 0
        self.session: _CredentialFreeSession | None = None

    async def create(self, spec: SandboxSpec) -> _CredentialFreeSession:
        self.create_calls += 1
        self.session = _CredentialFreeSession(spec)
        return self.session

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> _CredentialFreeSession:
        raise NotImplementedError


class _DeterministicAgentDriver:
    """Produce one rejected revision, repair it, then complete its dependent."""

    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.requests: list[TaskDispatchRequest] = []

    async def run(
        self,
        session: SandboxSession,
        request: TaskDispatchRequest,
        prompt: str,
    ) -> AgentProcessObservation:
        del prompt
        self.requests.append(request)
        if request.task_name == "repair-gate":
            value = "rejected" if request.dispatch_sequence == 1 else "accepted"
            command = f"printf '%s\\n' {value} > gate.txt"
        else:
            command = "printf '%s\\n' downstream > downstream.txt"
        result = await session.exec(
            ProcessRequest(
                ("sh", "-c", command),
                workdir=str(self.workspace),
            )
        )
        return AgentProcessObservation(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            session_id=f"session-{request.task_name}",
        )


@dataclass(frozen=True)
class _MissionEvidence:
    mission_status: str
    task_dispatches: tuple[tuple[str, int], ...]
    request_order: tuple[tuple[str, int], ...]
    retry_saw_failed_validation: bool
    retry_resumed_agent_session: bool
    validation_returncodes: tuple[int, ...]
    execution_count: int
    pushed_final_commits: int
    friction_count: int
    prerequisite_accepted_tick: int | None
    dependent_dispatched_tick: int | None
    remote_revision: str
    projected_final_revision: str
    backend_create_calls: int
    session_close_calls: int
    sandbox_status: str


async def _run_mission(root: Path) -> _MissionEvidence:
    remote = _local_bare_remote(root)
    workspace = root / "sandbox" / "repo"
    backend = _CredentialFreeBackend()
    driver = _DeterministicAgentDriver(workspace)
    storage = StorageConfig(uri=str(root / "state"), namespace="mission_capability")

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "agent-mission-capability",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-eval@sha256:agent-mission-capability",
                driver=driver,
                workspace=str(workspace),
                max_ticks=40,
            ),
            storage=storage,
        ) as missions:
            submitted = await missions.submit(
                repository=str(remote),
                branch=_BRANCH,
                tasks=(
                    AgentTask(
                        name="repair-gate",
                        prompt="Create the gate marker required by the repository validator.",
                        validators=(
                            CommandValidator(
                                "gate-is-accepted",
                                ("sh", "-c", 'test "$(cat gate.txt)" = accepted'),
                            ),
                        ),
                        max_dispatches=2,
                    ),
                    AgentTask(
                        name="dependent",
                        prompt="Create the downstream marker after the gate task is accepted.",
                        validators=(
                            CommandValidator(
                                "downstream-exists",
                                ("sh", "-c", "test -f downstream.txt"),
                            ),
                        ),
                        depends_on=("repair-gate",),
                    ),
                ),
            )
            result = await missions.run(submitted)

            validation_rows = await _latest_rows(missions, ValidationResult)
            execution_rows = await _latest_rows(missions, AgentExecution)
            commit_rows = await _latest_rows(missions, Commit)
            friction_rows = await _latest_rows(missions, FrictionLog)
            sandbox_rows = await _latest_rows(missions, Sandbox)
            task_state_rows = (await missions.query(TaskState)).to_pylist()

            validation = ValidationResult.get_prefix()
            commit = Commit.get_prefix()
            sandbox = Sandbox.get_prefix()
            state = TaskState.get_prefix()
            state_ticks: dict[tuple[int, str], list[int]] = {}
            for row in sorted(task_state_rows, key=lambda item: int(item["tick"])):
                if row["is_active"]:
                    key = (int(row["entity_id"]), str(row[f"{state}status"]))
                    state_ticks.setdefault(key, []).append(int(row["tick"]))

            retry = driver.requests[1] if len(driver.requests) > 1 else None
            final_commits = result.tasks[-1].commit_shas
            projected_final_revision = final_commits[-1] if final_commits else ""
            evidence = _MissionEvidence(
                mission_status=result.status,
                task_dispatches=tuple((task.name, task.dispatches) for task in result.tasks),
                request_order=tuple(
                    (request.task_name, request.dispatch_sequence) for request in driver.requests
                ),
                retry_saw_failed_validation=(
                    retry is not None
                    and len(retry.previous_validation) == 1
                    and retry.previous_validation[0].passed is False
                ),
                retry_resumed_agent_session=(
                    retry is not None and retry.previous_agent_session_id == "session-repair-gate"
                ),
                validation_returncodes=tuple(
                    int(row[f"{validation}actual_returncode"]) for row in validation_rows
                ),
                execution_count=len(execution_rows),
                pushed_final_commits=sum(
                    bool(row[f"{commit}pushed"]) and bool(row[f"{commit}final_revision"])
                    for row in commit_rows
                ),
                friction_count=len(friction_rows),
                prerequisite_accepted_tick=(
                    min(accepted_ticks)
                    if (
                        accepted_ticks := state_ticks.get(
                            (submitted.task_id("repair-gate"), "accepted")
                        )
                    )
                    else None
                ),
                dependent_dispatched_tick=(
                    min(dispatched_ticks)
                    if (
                        dispatched_ticks := state_ticks.get(
                            (submitted.task_id("dependent"), "dispatched")
                        )
                    )
                    else None
                ),
                remote_revision="",
                projected_final_revision=projected_final_revision,
                backend_create_calls=backend.create_calls,
                session_close_calls=0,
                sandbox_status=str(sandbox_rows[-1][f"{sandbox}status"]),
            )

    if backend.session is None:
        raise RuntimeError("mission did not create its configured sandbox session")
    return replace(
        evidence,
        remote_revision=_remote_revision(remote),
        session_close_calls=backend.session.close_calls,
    )


def task_agent_mission_transition_authority() -> list[GraderResult]:
    """Run a real mission and grade persisted, published, terminal outcomes."""

    with tempfile.TemporaryDirectory(prefix="archetype-mission-capability-") as directory:
        evidence = asyncio.run(_run_mission(Path(directory)))

    return [
        state_check(
            {
                "mission_succeeded": evidence.mission_status == "succeeded",
                "retry_and_dependency_executed": evidence.task_dispatches
                == (("repair-gate", 2), ("dependent", 1)),
                "dispatch_order_is_durable": evidence.request_order
                == (("repair-gate", 1), ("repair-gate", 2), ("dependent", 1)),
                "dependent_waited_for_acceptance": (
                    evidence.dependent_dispatched_tick is not None
                    and evidence.prerequisite_accepted_tick is not None
                    and evidence.dependent_dispatched_tick > evidence.prerequisite_accepted_tick
                ),
            },
            name="mission_processors_own_transitions",
        ),
        state_check(
            {
                "failed_evidence_reached_retry": evidence.retry_saw_failed_validation,
                "agent_session_reached_retry": evidence.retry_resumed_agent_session,
                "repository_validators_rejected_then_passed": (
                    sorted(evidence.validation_returncodes) == [0, 0, 1]
                ),
                "every_dispatch_is_observed": evidence.execution_count == 3,
                "rejection_is_queryable_friction": evidence.friction_count == 1,
            },
            name="mission_retry_uses_repository_evidence",
        ),
        state_check(
            {
                "validated_revisions_were_pushed": evidence.pushed_final_commits == 2,
                "bare_remote_has_projected_final_revision": (
                    bool(evidence.projected_final_revision)
                    and evidence.remote_revision == evidence.projected_final_revision
                ),
                "sandbox_was_reused": evidence.backend_create_calls == 1,
                "sandbox_was_closed_once": evidence.session_close_calls == 1,
                "terminal_cleanup_was_persisted": (
                    evidence.sandbox_status == SandboxStatus.CLOSED.value
                ),
            },
            name="mission_publication_and_cleanup_are_real",
        ),
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "agent_mission_transition_authority",
        suite=SUITE,
        fn=task_agent_mission_transition_authority,
        desc=(
            "Runtime missions retry from repository evidence, honor dependency order, "
            "publish validated revisions, and clean up terminal resources"
        ),
    )

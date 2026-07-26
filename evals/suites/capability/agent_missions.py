# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free end-to-end capability proof for Agent Missions V1."""

from __future__ import annotations

import asyncio
import hashlib
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
    Candidate,
    CommandValidator,
    Commit,
    CriticExecution,
    CriticFinding,
    CriticReceipt,
    FrictionLog,
    Sandbox,
    TaskState,
    ValidationResult,
)
from archetype.missions.coding_agents import (
    AgentProcessObservation,
    TaskDispatchRequest,
)
from archetype.missions.critics import (
    CandidateReviewRequest,
    CriticProcessObservation,
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
        self.requests: list[ProcessRequest] = []
        self._environment = _hermetic_environment(Path(spec.workdir).parent / ".capability-home")

    @property
    def identity(self) -> SandboxIdentity:
        metadata = self.spec.metadata_dict()
        if metadata.get("role") == "critic":
            suffix = metadata["dispatch"][:12]
            sandbox_id = f"mission-critic-{suffix}"
        else:
            sandbox_id = f"mission-author-{metadata['mission']}"
        return SandboxIdentity("capability-local", sandbox_id, self.spec.environment)

    @property
    def capabilities(self) -> SandboxCapabilities:
        # The harness requests this symbolic lease for Git operations. A local
        # file remote needs no credential and the session injects no secret.
        return SandboxCapabilities(secret_names=("github",))

    async def status(self) -> SandboxStatus:
        return SandboxStatus.CLOSED if self.close_calls else SandboxStatus.READY

    async def exec(self, request: ProcessRequest) -> ProcessResult:
        self.requests.append(request)
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
        self.sessions: list[_CredentialFreeSession] = []

    async def create(self, spec: SandboxSpec) -> _CredentialFreeSession:
        self.create_calls += 1
        self.session = _CredentialFreeSession(spec)
        self.sessions.append(self.session)
        return self.session

    async def restore(
        self,
        spec: SandboxSpec,
        checkpoint: CheckpointRef,
    ) -> _CredentialFreeSession:
        raise NotImplementedError


class _DeterministicAgentDriver:
    """Produce a critic-rejected candidate, repair it, then complete a dependent."""

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
            values = {
                1: "critic-rejected",
                2: "accepted",
            }
            value = values[request.dispatch_sequence]
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


class _DeterministicCriticDriver:
    """Reject the first authored-green gate candidate, then approve exact heads."""

    driver_id = "codex"

    def __init__(self) -> None:
        self.requests: list[CandidateReviewRequest] = []
        self.sandbox_ids: list[str] = []
        self._gate_reviews = 0

    async def run(
        self,
        session: SandboxSession,
        request: CandidateReviewRequest,
        prompt: str,
    ) -> CriticProcessObservation:
        assert request.diff == ""
        assert request.subject_transport == "sandbox_file"
        assert request.subject_ref in prompt
        subject = Path(request.subject_ref).read_bytes()
        assert subject
        assert hashlib.sha256(subject).hexdigest() == request.diff_digest
        assert subject.decode() not in prompt
        self.requests.append(request)
        self.sandbox_ids.append(session.identity.sandbox_id)
        if request.task_name == "repair-gate":
            self._gate_reviews += 1
            if self._gate_reviews == 1:
                return CriticProcessObservation(
                    returncode=0,
                    stdout=(
                        '{"schema_version":1,"conclusion":"blocking",'
                        '"reviewed_scope":"exact task diff","findings":[{'
                        '"finding_id":"gate-value","severity":"blocking",'
                        '"category":"correctness","confidence":1.0,'
                        '"title":"Gate value is not accepted",'
                        '"detail":"gate.txt must contain accepted",'
                        '"evidence_location":"gate.txt:1",'
                        '"reproduction":"test $(cat gate.txt) = accepted"}]}'
                    ),
                )
        return CriticProcessObservation(
            returncode=0,
            stdout=(
                '{"schema_version":1,"conclusion":"approved",'
                '"reviewed_scope":"exact task diff","findings":[]}'
            ),
        )


@dataclass(frozen=True)
class _MissionEvidence:
    mission_status: str
    task_dispatches: tuple[tuple[str, int], ...]
    request_order: tuple[tuple[str, int], ...]
    repair_saw_passing_validation: bool
    retry_resumed_agent_session: bool
    repair_saw_critic_finding: bool
    validation_returncodes: tuple[int, ...]
    execution_count: int
    pushed_final_commits: int
    candidate_count: int
    critic_execution_count: int
    critic_finding_count: int
    critic_receipt_count: int
    critic_request_order: tuple[tuple[str, int], ...]
    author_critic_identities_are_distinct: bool
    critic_received_git_secret: bool
    critic_phase_timings_are_monotonic: bool
    critic_prewarm_finished_before_publication: bool
    friction_count: int
    prerequisite_candidate_ticks: tuple[int, ...]
    prerequisite_accepted_tick: int | None
    dependent_dispatched_tick: int | None
    remote_revision: str
    projected_final_revision: str
    backend_create_calls: int
    session_close_calls: int
    sandbox_statuses: tuple[str, ...]


async def _run_mission(root: Path) -> _MissionEvidence:
    remote = _local_bare_remote(root)
    workspace = root / "sandbox" / "repo"
    backend = _CredentialFreeBackend()
    driver = _DeterministicAgentDriver(workspace)
    critic = _DeterministicCriticDriver()
    storage = StorageConfig(uri=str(root / "state"), namespace="mission_capability")

    async with ArchetypeRuntime() as runtime:
        async with runtime.missions(
            "agent-mission-capability",
            config=AgentMissionConfig(
                sandbox_backend=backend,
                sandbox_environment="local-eval@sha256:agent-mission-capability",
                driver=driver,
                critic_driver=critic,
                workspace=str(workspace),
                critic_workspace=str(root / "critic"),
                checkpoint_after_dispatch=False,
                max_ticks=16,
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
                                (
                                    "sh",
                                    "-c",
                                    'test "$(cat gate.txt)" != validator-rejected',
                                ),
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
            candidate_rows = await _latest_rows(missions, Candidate)
            critic_execution_rows = await _latest_rows(missions, CriticExecution)
            critic_finding_rows = await _latest_rows(missions, CriticFinding)
            critic_receipt_rows = await _latest_rows(missions, CriticReceipt)
            friction_rows = await _latest_rows(missions, FrictionLog)
            sandbox_rows = await _latest_rows(missions, Sandbox)
            task_state_rows = (await missions.query(TaskState)).to_pylist()

            validation = ValidationResult.get_prefix()
            commit = Commit.get_prefix()
            critic_execution = CriticExecution.get_prefix()
            sandbox = Sandbox.get_prefix()
            state = TaskState.get_prefix()
            state_ticks: dict[tuple[int, str], list[int]] = {}
            for row in sorted(task_state_rows, key=lambda item: int(item["tick"])):
                if row["is_active"]:
                    key = (int(row["entity_id"]), str(row[f"{state}status"]))
                    state_ticks.setdefault(key, []).append(int(row["tick"]))

            retry = driver.requests[1] if len(driver.requests) > 1 else None
            repair = driver.requests[1] if len(driver.requests) > 1 else None
            final_commits = result.tasks[-1].commit_shas
            projected_final_revision = final_commits[-1] if final_commits else ""
            evidence = _MissionEvidence(
                mission_status=result.status,
                task_dispatches=tuple((task.name, task.dispatches) for task in result.tasks),
                request_order=tuple(
                    (request.task_name, request.dispatch_sequence) for request in driver.requests
                ),
                repair_saw_passing_validation=(
                    retry is not None
                    and len(retry.previous_validation) == 1
                    and retry.previous_validation[0].passed is True
                ),
                retry_resumed_agent_session=(
                    retry is not None and retry.previous_agent_session_id == "session-repair-gate"
                ),
                repair_saw_critic_finding=(
                    repair is not None
                    and len(repair.previous_critic_findings) == 1
                    and repair.previous_critic_findings[0].finding_id == "gate-value"
                ),
                validation_returncodes=tuple(
                    int(row[f"{validation}actual_returncode"]) for row in validation_rows
                ),
                execution_count=len(execution_rows),
                pushed_final_commits=sum(
                    bool(row[f"{commit}pushed"]) and bool(row[f"{commit}final_revision"])
                    for row in commit_rows
                ),
                candidate_count=len(candidate_rows),
                critic_execution_count=len(critic_execution_rows),
                critic_finding_count=len(critic_finding_rows),
                critic_receipt_count=len(critic_receipt_rows),
                critic_request_order=tuple(
                    (request.task_name, request.dispatch_sequence) for request in critic.requests
                ),
                author_critic_identities_are_distinct=all(
                    request.author_sandbox_id != sandbox_id
                    for request, sandbox_id in zip(
                        critic.requests,
                        critic.sandbox_ids,
                        strict=True,
                    )
                ),
                critic_received_git_secret=any(
                    "github" in process.secret_names
                    for session in backend.sessions
                    if session.spec.metadata_dict().get("role") == "critic"
                    for process in session.requests
                ),
                critic_phase_timings_are_monotonic=all(
                    0
                    < int(row[f"{critic_execution}provision_started_at_ms"])
                    <= int(row[f"{critic_execution}sandbox_ready_at_ms"])
                    <= int(row[f"{critic_execution}base_hydrated_at_ms"])
                    <= int(row[f"{critic_execution}candidate_published_at_ms"])
                    <= int(row[f"{critic_execution}head_ready_at_ms"])
                    <= int(row[f"{critic_execution}critic_started_at_ms"])
                    <= int(row[f"{critic_execution}ended_at_ms"])
                    <= int(row[f"{critic_execution}receipt_staged_at_ms"])
                    for row in critic_execution_rows
                ),
                critic_prewarm_finished_before_publication=all(
                    int(row[f"{critic_execution}base_hydrated_at_ms"])
                    <= int(row[f"{critic_execution}candidate_published_at_ms"])
                    for row in critic_execution_rows
                ),
                friction_count=len(friction_rows),
                prerequisite_candidate_ticks=tuple(
                    state_ticks.get((submitted.task_id("repair-gate"), "candidate"), ())
                ),
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
                sandbox_statuses=tuple(str(row[f"{sandbox}status"]) for row in sandbox_rows),
            )

    if not backend.sessions:
        raise RuntimeError("mission did not create its configured sandbox sessions")
    return replace(
        evidence,
        remote_revision=_remote_revision(remote),
        session_close_calls=sum(session.close_calls for session in backend.sessions),
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
                == (
                    ("repair-gate", 1),
                    ("repair-gate", 2),
                    ("dependent", 1),
                ),
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
                "validator_evidence_reached_repair": evidence.repair_saw_passing_validation,
                "agent_session_reached_retry": evidence.retry_resumed_agent_session,
                "repository_validators_ran_for_every_dispatch": (
                    evidence.validation_returncodes == (0, 0, 0)
                ),
                "every_dispatch_is_observed": evidence.execution_count == 3,
                "critic_rejection_is_not_author_friction": evidence.friction_count == 0,
            },
            name="mission_retry_uses_repository_evidence",
        ),
        state_check(
            {
                "authored_green_became_candidates": evidence.candidate_count == 3,
                "every_candidate_has_a_review_receipt": (
                    evidence.critic_execution_count == 3 and evidence.critic_receipt_count == 3
                ),
                "blocking_finding_reached_author_repair": (
                    evidence.critic_finding_count == 1 and evidence.repair_saw_critic_finding
                ),
                "critic_reviewed_exact_dispatches": evidence.critic_request_order
                == (("repair-gate", 1), ("repair-gate", 2), ("dependent", 1)),
                "candidate_preceded_acceptance": (
                    len(evidence.prerequisite_candidate_ticks) >= 2
                    and evidence.prerequisite_accepted_tick is not None
                    and max(evidence.prerequisite_candidate_ticks)
                    < evidence.prerequisite_accepted_tick
                ),
                "author_and_critic_are_independent": (
                    evidence.author_critic_identities_are_distinct
                    and not evidence.critic_received_git_secret
                ),
                "critic_phase_timings_are_durable": (
                    evidence.critic_phase_timings_are_monotonic
                    and evidence.critic_prewarm_finished_before_publication
                ),
            },
            name="mission_exact_head_critic_gates_promotion",
        ),
        state_check(
            {
                "validated_revisions_were_pushed": evidence.pushed_final_commits == 3,
                "bare_remote_has_projected_final_revision": (
                    bool(evidence.projected_final_revision)
                    and evidence.remote_revision == evidence.projected_final_revision
                ),
                "author_and_fresh_critics_were_created": evidence.backend_create_calls == 4,
                "every_sandbox_was_closed_once": evidence.session_close_calls == 4,
                "terminal_cleanup_was_persisted": (
                    len(evidence.sandbox_statuses) == 4
                    and set(evidence.sandbox_statuses) == {SandboxStatus.CLOSED.value}
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

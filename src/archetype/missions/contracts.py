# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed authoring and result contracts for coding-agent missions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from archetype.missions.coding_agents.contracts import CodingAgentDriver
    from archetype.missions.critics.contracts import CriticDriver
    from archetype.missions.sandboxes import SandboxBackend, SandboxEventObserver


@dataclass(frozen=True)
class CommandValidator:
    """One repository command whose exit code gates a task transition."""

    name: str
    command: tuple[str, ...]
    expected_returncode: int = 0
    timeout_seconds: int = 900

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("validator name must not be empty")
        if not self.command or any(not argument for argument in self.command):
            raise ValueError("validator command must contain non-empty arguments")
        if self.timeout_seconds < 1:
            raise ValueError("validator timeout_seconds must be positive")


class RepositoryPublicationPolicy(StrEnum):
    """Repository finalization required before a task may be accepted."""

    COMMIT_AND_PUSH = "commit_and_push"


@dataclass(frozen=True)
class CriticPolicy:
    """Stable, digestible policy for one independent exact-head review."""

    policy_id: str = "archetype.exact-head"
    version: str = "1"
    perspective: str = "repository-correctness"
    information_view: str = "task-diff-validators"
    driver: str = "codex"
    model: str = ""
    sampling: str = "provider-default"
    max_reviews: int = 2
    timeout_seconds: int = 45 * 60
    output_schema_version: int = 1
    max_output_chars: int = 16_000

    def __post_init__(self) -> None:
        for field_name in (
            "policy_id",
            "version",
            "perspective",
            "information_view",
            "driver",
            "sampling",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"critic {field_name} must not be empty")
        if self.max_reviews < 1:
            raise ValueError("critic max_reviews must be positive")
        if self.timeout_seconds < 1:
            raise ValueError("critic timeout_seconds must be positive")
        if self.output_schema_version < 1:
            raise ValueError("critic output_schema_version must be positive")
        if self.max_output_chars < 1:
            raise ValueError("critic max_output_chars must be positive")

    @property
    def digest(self) -> str:
        """Return the canonical identity of this atomic policy unit."""

        payload = {
            field_name: getattr(self, field_name) for field_name in self.__dataclass_fields__
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class AgentMissionConfig:
    """Process configuration bound once to a mission runtime handle."""

    sandbox_backend: SandboxBackend
    sandbox_environment: str
    driver: CodingAgentDriver | None = None
    critic_driver: CriticDriver | None = None
    workspace: str = "/workspace/repo"
    critic_workspace: str = "/workspace/review"
    model: str = ""
    max_ticks: int = 100
    checkpoint_after_dispatch: bool = True
    on_sandbox_event: SandboxEventObserver | None = None

    def __post_init__(self) -> None:
        if not self.sandbox_environment.strip():
            raise ValueError("sandbox_environment must be a pinned identity")
        if not self.workspace.startswith("/") or self.workspace == "/":
            raise ValueError("workspace must be a non-root absolute path")
        if not self.critic_workspace.startswith("/") or self.critic_workspace == "/":
            raise ValueError("critic_workspace must be a non-root absolute path")
        if self.max_ticks < 1:
            raise ValueError("AgentMissionConfig.max_ticks must be positive")


@dataclass(frozen=True)
class AgentTask:
    """One explicitly authored task and its incoming dependency names."""

    name: str
    prompt: str
    validators: tuple[CommandValidator, ...]
    depends_on: tuple[str, ...] = ()
    max_dispatches: int = 3
    publication_policy: RepositoryPublicationPolicy = RepositoryPublicationPolicy.COMMIT_AND_PUSH
    critic_policy: CriticPolicy = CriticPolicy()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("task name must not be empty")
        if not self.prompt.strip():
            raise ValueError(f"task {self.name!r} prompt must not be empty")
        if not self.validators:
            raise ValueError(f"task {self.name!r} requires at least one validator")
        validator_names = [validator.name for validator in self.validators]
        if len(set(validator_names)) != len(validator_names):
            raise ValueError(f"task {self.name!r} validator names must be unique")
        if self.max_dispatches < 1:
            raise ValueError(f"task {self.name!r} max_dispatches must be positive")
        try:
            policy = RepositoryPublicationPolicy(self.publication_policy)
        except ValueError as exc:
            raise ValueError(f"task {self.name!r} has an unsupported publication policy") from exc
        object.__setattr__(self, "publication_policy", policy)
        if len(set(self.depends_on)) != len(self.depends_on):
            raise ValueError(f"task {self.name!r} contains duplicate dependencies")


@dataclass(frozen=True)
class MissionSubmission:
    """Complete V1 mission input: repository context plus an explicit task DAG."""

    repository: str
    branch: str
    tasks: tuple[AgentTask, ...]
    name: str = "agent-mission"
    base_ref: str = "main"

    def __post_init__(self) -> None:
        if not self.repository.strip():
            raise ValueError("mission repository must not be empty")
        if not self.branch.strip():
            raise ValueError("mission branch must not be empty")
        if not self.base_ref.strip():
            raise ValueError("mission base_ref must not be empty")
        if not self.tasks:
            raise ValueError("mission requires at least one task")

        names = [task.name for task in self.tasks]
        if len(set(names)) != len(names):
            raise ValueError("mission task names must be unique")
        known = set(names)
        for task in self.tasks:
            unknown = sorted(set(task.depends_on) - known)
            if unknown:
                raise ValueError(
                    f"task {task.name!r} depends on unknown task(s): {', '.join(unknown)}"
                )
            if task.name in task.depends_on:
                raise ValueError(f"task {task.name!r} cannot depend on itself")
        self._require_acyclic()

    def _require_acyclic(self) -> None:
        dependencies = {task.name: task.depends_on for task in self.tasks}
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in visiting:
                raise ValueError("mission task relationships must form an acyclic graph")
            visiting.add(name)
            for dependency in dependencies[name]:
                visit(dependency)
            visiting.remove(name)
            visited.add(name)

        for name in dependencies:
            visit(name)


@dataclass(frozen=True)
class SubmittedMission:
    """Stable entity identities produced by ``missions.submit``."""

    mission_id: int
    task_ids: tuple[tuple[str, int], ...]
    repository: str = ""
    branch: str = ""
    base_ref: str = "main"

    def task_id(self, name: str) -> int:
        try:
            return dict(self.task_ids)[name]
        except KeyError as exc:
            raise KeyError(f"mission has no task named {name!r}") from exc


@dataclass(frozen=True)
class TaskResult:
    """Terminal task projection returned to mission authors."""

    task_id: int
    name: str
    status: str
    dispatches: int
    commit_shas: tuple[str, ...]
    reason: str = ""


@dataclass(frozen=True)
class MissionResult:
    """Terminal mission projection."""

    mission_id: int
    status: str
    repository: str
    branch: str
    ticks_completed: int
    tasks: tuple[TaskResult, ...]
    reason: str = ""

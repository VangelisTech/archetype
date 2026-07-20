# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed authoring and result contracts for coding-agent missions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


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
class AgentTask:
    """One explicitly authored task and its incoming dependency names."""

    name: str
    prompt: str
    validators: tuple[CommandValidator, ...]
    depends_on: tuple[str, ...] = ()
    max_dispatches: int = 3
    publication_policy: RepositoryPublicationPolicy = RepositoryPublicationPolicy.COMMIT_AND_PUSH

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("task name must not be empty")
        if not self.prompt.strip():
            raise ValueError(f"task {self.name!r} prompt must not be empty")
        if not self.validators:
            raise ValueError(f"task {self.name!r} requires at least one validator")
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

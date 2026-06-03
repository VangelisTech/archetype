# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Experiment Components
=====================

Experiment, Run, Result, BranchHead: the ECS schema for autoresearch
lifecycle state. Each is a first-class archetype Component, which means
runs become entities in an archetype world — forkable, time-travelable,
and queryable with the same tools as any other simulation state.

The module is deliberately minimal on scoring semantics: Result stores
eval outputs as an opaque JSON dict and BranchHead stores a free-form
JSON descriptor of *why* a commit is the current best. Users decide
what "better" means; the library stores and queries, it does not score.

Arrow serialization:
    - Scalar fields (str/int/float/bool) are stored directly
    - Complex types (dicts, lists, nested structures) are JSON-encoded
      into ``*_json`` fields, following the Trajectory convention
    - Timestamps are stored as Unix milliseconds (``*_at_ms``) for
      Arrow compatibility and efficient range filtering
"""

from __future__ import annotations

import json
import time
from enum import Enum
from typing import Any

from archetype.core.component import Component

# ============================================================================
# Helper types — NOT Components. Used for type-safe construction and for
# decoding JSON-encoded fields back into friendly Python objects.
# ============================================================================


class RunStatus(str, Enum):
    """Allowed values for ``Run.status``.

    Run stores status as a plain string for Arrow compatibility — enums
    are not directly Arrow-serializable. This helper provides type-safe
    construction (``RunStatus.RUNNING.value``) and classification
    (``RunStatus.is_active(run.status)``). The string values match
    archetype-runner's ``state.py`` registry exactly, so rows read from
    the runner's SQLite deserialize without translation.
    """

    BOOTING = "booting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    CRASHED = "crashed"

    @classmethod
    def is_active(cls, status: str) -> bool:
        """True while the run is still in flight."""
        return status in (cls.BOOTING.value, cls.RUNNING.value, cls.STOPPING.value)

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        """True once the run has reached a final state."""
        return status in (cls.STOPPED.value, cls.CRASHED.value)


def _now_ms() -> int:
    """Current time as Unix milliseconds."""
    return int(time.time() * 1000)


# ============================================================================
# Components
# ============================================================================


class Experiment(Component):
    """An autoresearch experiment — the setup for a family of runs.

    Deliberately contains no scoring or reward fields. An experiment
    describes the conditions under which runs happen (which repo, which
    branch, any user-defined metadata). How runs are compared or ranked
    is entirely the user's eval code's job — this Component does not
    pre-declare a metric.

    Fields:
        name:           Experiment identifier; matches Run.experiment_name
        repo_url:       Git repository URL the experiment evolves
        branch:         Branch the experiment evolves
        created_at_ms:  Creation timestamp in Unix milliseconds
        metadata_json:  Free-form user-defined metadata (JSON dict)
    """

    name: str = ""
    repo_url: str = ""
    branch: str = "main"
    created_at_ms: int = 0
    metadata_json: str = "{}"

    @classmethod
    def make(
        cls,
        name: str,
        repo_url: str,
        *,
        branch: str = "main",
        metadata: dict[str, Any] | None = None,
        created_at_ms: int | None = None,
    ) -> Experiment:
        """Convenience constructor that JSON-encodes metadata."""
        return cls(
            name=name,
            repo_url=repo_url,
            branch=branch,
            created_at_ms=created_at_ms if created_at_ms is not None else _now_ms(),
            metadata_json=json.dumps(metadata or {}),
        )

    def get_metadata(self) -> dict[str, Any]:
        """Decode the metadata JSON back to a dict."""
        return json.loads(self.metadata_json)


class Run(Component):
    """A single autoresearch attempt: one VM, one agent, one task.

    Pure facts about what happened — no evaluation or reward fields.
    Field shapes mirror archetype-runner's ``AgentRecord`` so that the
    runner's SQLite registry can be ingested into an archetype world
    row-for-row without translation.

    Fields:
        run_id:          Opaque run identifier; same value runner stores as agent_id
        experiment_name: Experiment this run belongs to (free-form label)
        vm_name:         masterblaster VM name backing this run
        status:          One of the RunStatus values (stored as string)
        harness:         'claude-code' | 'opencode' | 'hermes'
        repo_url:        Git repository URL
        branch:          Git branch
        task:            Natural-language task prompt given to the agent
        started_at_ms:   Start time in Unix milliseconds
        finished_at_ms:  Finish time in Unix milliseconds (0 if not finished)
        agent_name:      Composite identity, conventionally '{experiment_name}-{harness}'
        workspace_path:  Host filesystem path to the cloned repo
        commit_hash:     Git commit produced by the run ("" if not set yet)
    """

    run_id: str = ""
    experiment_name: str = "adhoc"
    vm_name: str = ""
    status: str = "booting"
    harness: str = ""
    repo_url: str = ""
    branch: str = "main"
    task: str = ""
    started_at_ms: int = 0
    finished_at_ms: int = 0
    agent_name: str = ""
    workspace_path: str = ""
    commit_hash: str = ""

    @property
    def is_active(self) -> bool:
        """True while the run is still in flight (booting/running/stopping).

        Matches archetype's entity-level ``active`` convention.
        """
        return RunStatus.is_active(self.status)

    @property
    def is_terminal(self) -> bool:
        """True once the run has reached a final state (stopped/crashed)."""
        return RunStatus.is_terminal(self.status)


class Result(Component):
    """An opaque eval envelope for a Run.

    The library persists; it does not interpret. User eval code puts
    whatever is meaningful into ``outputs_json`` — a scalar metric, a
    Pareto point, an LLM judge verdict, a full pytest report, a
    tournament record. Multiple Results per run are allowed (same
    ``run_id``, different ``evaluator``) so several eval harnesses can
    score the same run independently.

    Fields:
        run_id:         Foreign key to Run.run_id
        evaluator:      Free-form name of the eval source ('pytest', 'ruff', 'llm-judge', ...)
        outputs_json:   JSON dict of opaque user-defined eval output
        evaluated_at_ms: When the eval ran, in Unix milliseconds
    """

    run_id: str = ""
    evaluator: str = ""
    outputs_json: str = "{}"
    evaluated_at_ms: int = 0

    @classmethod
    def make(
        cls,
        run_id: str,
        outputs: dict[str, Any],
        *,
        evaluator: str = "",
        evaluated_at_ms: int | None = None,
    ) -> Result:
        """Convenience constructor that JSON-encodes outputs."""
        return cls(
            run_id=run_id,
            evaluator=evaluator,
            outputs_json=json.dumps(outputs),
            evaluated_at_ms=evaluated_at_ms if evaluated_at_ms is not None else _now_ms(),
        )

    def get_outputs(self) -> dict[str, Any]:
        """Decode the outputs JSON back to a dict."""
        return json.loads(self.outputs_json)


class BranchHead(Component):
    """Persisted 'current best commit' for an experiment.

    The autoresearch loop picks a winner each iteration using whatever
    comparison logic the user wants (scalar max, Pareto dominance, LLM
    judge, tournament, human vote). After each winner is selected, the
    loop writes a ``BranchHead`` so a host crash can recover and continue
    from the same declared-best commit instead of resetting to branch HEAD.

    The ``descriptor_json`` field is free-form and user-defined: it's
    the user's explanation of *why* this commit is the current best,
    in whatever shape they want. The library persists it verbatim and
    never interprets it.

    Fields:
        experiment_name: Experiment this head belongs to
        commit_hash:     Git commit currently declared best
        run_id:          Run that produced this commit ("" for initial seed)
        descriptor_json: Free-form user-defined JSON descriptor of why
        updated_at_ms:   Last update in Unix milliseconds
    """

    experiment_name: str = ""
    commit_hash: str = ""
    run_id: str = ""
    descriptor_json: str = "{}"
    updated_at_ms: int = 0

    @classmethod
    def make(
        cls,
        experiment_name: str,
        commit_hash: str,
        *,
        run_id: str = "",
        descriptor: dict[str, Any] | None = None,
        updated_at_ms: int | None = None,
    ) -> BranchHead:
        """Convenience constructor that JSON-encodes the descriptor."""
        return cls(
            experiment_name=experiment_name,
            commit_hash=commit_hash,
            run_id=run_id,
            descriptor_json=json.dumps(descriptor or {}),
            updated_at_ms=updated_at_ms if updated_at_ms is not None else _now_ms(),
        )

    def get_descriptor(self) -> dict[str, Any]:
        """Decode the descriptor JSON back to a dict."""
        return json.loads(self.descriptor_json)


# ============================================================================
# Contract Components — Archetype ↔ Runner bidirectional protocol
# ============================================================================


class RequestStatus(str, Enum):
    """Allowed values for ``RunRequest.status``.

    Tracks the request lifecycle: Archetype writes ``pending``, Runner
    transitions to ``claimed`` or ``rejected``.
    """

    PENDING = "pending"
    CLAIMED = "claimed"
    REJECTED = "rejected"


class RunRequest(Component):
    """A request from Archetype to Runner to execute a run.

    Mirrors the ``run_requests`` table in ``requests.db``. Archetype
    writes one row per desired execution; Runner reads pending rows,
    claims them, and provisions VMs accordingly.

    Fields:
        request_id:      UUID generated by Archetype
        experiment_name: Links to Experiment.name
        repo_url:        Git clone URL
        branch:          Branch to check out
        base_commit:     Specific commit to start from ('' = branch HEAD)
        task:            Natural-language prompt for the agent
        harness:         'claude-code' | 'opencode' | 'hermes'
        timeout_sec:     Hard wall-clock limit in seconds
        status:          One of the RequestStatus values (stored as string)
        created_at_ms:   Creation timestamp in Unix milliseconds
        metadata_json:   Free-form user-defined context (JSON dict)
    """

    request_id: str = ""
    experiment_name: str = ""
    repo_url: str = ""
    branch: str = "main"
    base_commit: str = ""
    task: str = ""
    harness: str = "claude-code"
    timeout_sec: int = 1800
    status: str = "pending"
    created_at_ms: int = 0
    metadata_json: str = "{}"

    @classmethod
    def make(
        cls,
        request_id: str,
        experiment_name: str,
        repo_url: str,
        task: str,
        *,
        branch: str = "main",
        base_commit: str = "",
        harness: str = "claude-code",
        timeout_sec: int = 1800,
        metadata: dict[str, Any] | None = None,
        created_at_ms: int | None = None,
    ) -> RunRequest:
        """Convenience constructor that JSON-encodes metadata."""
        return cls(
            request_id=request_id,
            experiment_name=experiment_name,
            repo_url=repo_url,
            branch=branch,
            base_commit=base_commit,
            task=task,
            harness=harness,
            timeout_sec=timeout_sec,
            status=RequestStatus.PENDING.value,
            created_at_ms=created_at_ms if created_at_ms is not None else _now_ms(),
            metadata_json=json.dumps(metadata or {}),
        )

    def get_metadata(self) -> dict[str, Any]:
        """Decode the metadata JSON back to a dict."""
        return json.loads(self.metadata_json)


class ExitStatus(str, Enum):
    """Allowed values for ``RunReport.exit_status``.

    Written by Runner when a run reaches a terminal state.
    """

    COMPLETED = "completed"
    CRASHED = "crashed"
    TIMED_OUT = "timed_out"


class RunReport(Component):
    """Immutable record of a completed run, written by Runner.

    Mirrors the ``run_reports`` table in ``state.db``. Runner writes
    one row when a run finishes, capturing the git commit produced,
    exit status, and tail logs. Archetype ingests these as entities
    for the evaluation pipeline.

    Fields:
        report_id:     UUID generated by Runner
        run_id:        Foreign key to Run.run_id (= agents.agent_id)
        request_id:    Foreign key to RunRequest.request_id ('' for ad-hoc)
        exit_status:   One of the ExitStatus values (stored as string)
        commit_hash:   Git SHA produced by the agent ('' if none)
        diff_stat:     Summary of changes (e.g. '3 files changed, 47+, 12-')
        stdout_tail:   Last N lines of agent stdout
        stderr_tail:   Last N lines of agent stderr
        metadata_json: Free-form run metadata (wall time, tokens, etc.)
        created_at_ms: When the report was written, Unix milliseconds
    """

    report_id: str = ""
    run_id: str = ""
    request_id: str = ""
    exit_status: str = ""
    commit_hash: str = ""
    diff_stat: str = ""
    stdout_tail: str = ""
    stderr_tail: str = ""
    metadata_json: str = "{}"
    created_at_ms: int = 0

    @classmethod
    def make(
        cls,
        report_id: str,
        run_id: str,
        exit_status: str,
        *,
        request_id: str = "",
        commit_hash: str = "",
        diff_stat: str = "",
        stdout_tail: str = "",
        stderr_tail: str = "",
        metadata: dict[str, Any] | None = None,
        created_at_ms: int | None = None,
    ) -> RunReport:
        """Convenience constructor that JSON-encodes metadata."""
        return cls(
            report_id=report_id,
            run_id=run_id,
            request_id=request_id,
            exit_status=exit_status,
            commit_hash=commit_hash,
            diff_stat=diff_stat,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            metadata_json=json.dumps(metadata or {}),
            created_at_ms=created_at_ms if created_at_ms is not None else _now_ms(),
        )

    def get_metadata(self) -> dict[str, Any]:
        """Decode the metadata JSON back to a dict."""
        return json.loads(self.metadata_json)

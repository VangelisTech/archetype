# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS-native lifecycle state for external trial execution.

The durable contract is ordinary Archetype state: intent, backend execution,
outcome, and artifact verification are component rows. Backends remain
resources so Modal, local subprocesses, or in-memory fakes can share the same
world-level orchestration model.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import daft
import pyarrow as pa

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component
from archetype.core.resources import Resources


class TrialExecutionStatus(StrEnum):
    """Lifecycle states for a trial execution."""

    PENDING = "pending"
    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    @classmethod
    def is_in_flight(cls, status: str) -> bool:
        return status in {cls.SUBMITTED.value, cls.RUNNING.value}

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        return status in {cls.SUCCEEDED.value, cls.FAILED.value}


class Trial(Component):
    """A trial intent row.

    ``spec_json`` is intentionally opaque. Domain adapters, such as a LIBERO
    backend, own its schema. Archetype persists and schedules it.
    """

    trial_id: str = ""
    run_id: str = ""
    suite: str = ""
    task_id: str = ""
    trial_idx: int = 0
    spec_json: str = "{}"


class TrialExecution(Component):
    """Backend execution state for one trial."""

    trial_id: str = ""
    backend: str = ""
    status: str = TrialExecutionStatus.PENDING.value
    job_id: str = ""
    attempt: int = 0
    submitted_at_ms: int = 0
    updated_at_ms: int = 0
    error: str = ""


class TrialOutcome(Component):
    """Task-level outcome once a backend finishes a trial."""

    trial_id: str = ""
    passed: bool = False
    score: float = 0.0
    steps: int = 0
    failure_mode: str = ""
    error: str = ""


class TrialManifest(Component):
    """Artifact and storage verification state for one trial."""

    trial_id: str = ""
    manifest_uri: str = ""
    storage_uri: str = ""
    verified: bool = False
    row_count: int = 0
    details_json: str = "{}"


@dataclass(frozen=True)
class TrialExecutionConfig:
    """World resource controlling bounded trial scheduling and polling."""

    max_submissions_per_tick: int = 100
    max_polls_per_tick: int = 100
    max_attempts: int = 1
    require_verified_manifest: bool = True


@dataclass(frozen=True)
class TrialRequest:
    """Serializable request passed from processors to an execution backend."""

    trial_id: str
    run_id: str
    suite: str
    task_id: str
    trial_idx: int
    spec_json: str


@dataclass(frozen=True)
class TrialJobRef:
    """Backend job handle for a submitted trial."""

    job_id: str
    status: str = TrialExecutionStatus.SUBMITTED.value


@dataclass(frozen=True)
class TrialBackendOutcome:
    """Backend-reported terminal or in-flight state for a trial."""

    status: str
    passed: bool = False
    score: float = 0.0
    steps: int = 0
    failure_mode: str = ""
    manifest_uri: str = ""
    storage_uri: str = ""
    verified: bool = False
    row_count: int = 0
    details_json: str = "{}"
    error: str = ""


class TrialExecutionBackend(Protocol):
    """Backend interface used by trial execution processors."""

    name: str

    async def submit(self, request: TrialRequest, *, idempotency_key: str) -> TrialJobRef: ...

    async def poll(self, job_id: str) -> TrialBackendOutcome | None: ...


@dataclass
class TrialExecutionBackendResource:
    """World resource wrapper for a trial execution backend."""

    backend: TrialExecutionBackend


def _now_ms() -> int:
    return int(time.time() * 1000)


def _request_from_row(row: dict) -> TrialRequest:
    return TrialRequest(
        trial_id=str(row.get("trial__trial_id") or ""),
        run_id=str(row.get("trial__run_id") or ""),
        suite=str(row.get("trial__suite") or ""),
        task_id=str(row.get("trial__task_id") or ""),
        trial_idx=int(row.get("trial__trial_idx") or 0),
        spec_json=str(row.get("trial__spec_json") or "{}"),
    )


def _rows_from_df(df) -> tuple[list[dict], pa.Schema]:
    materialized = df.collect()
    table = materialized.to_arrow()
    return materialized.to_pylist(), table.schema


def _df_from_rows(rows: list[dict], schema: pa.Schema):
    return daft.from_arrow(pa.Table.from_pylist(rows, schema=schema))


def _config_from_resources(resources: Resources) -> TrialExecutionConfig:
    return resources.get(TrialExecutionConfig) or TrialExecutionConfig()


def _mark_failure(row: dict, *, failure_mode: str, error: str) -> None:
    row["trialexecution__status"] = TrialExecutionStatus.FAILED.value
    row["trialexecution__error"] = error
    row["trialoutcome__passed"] = False
    row["trialoutcome__failure_mode"] = failure_mode
    row["trialoutcome__error"] = error
    row["trialmanifest__verified"] = False


class SubmitTrialsProcessor(AsyncProcessor):
    """Submit pending trials through the configured backend resource."""

    components = (Trial, TrialExecution, TrialOutcome, TrialManifest)
    priority = 10

    async def process(self, df, resources: Resources | None = None, **kwargs):
        resources = resources or kwargs.get("resources") or Resources()
        backend_resource = resources.get(TrialExecutionBackendResource)
        if backend_resource is None:
            return df

        config = _config_from_resources(resources)
        backend = backend_resource.backend
        rows, schema = _rows_from_df(df)
        now = _now_ms()
        submissions = 0

        rows.sort(key=lambda row: str(row.get("trial__trial_id") or ""))
        for row in rows:
            if submissions >= config.max_submissions_per_tick:
                break
            if row.get("trialexecution__status") != TrialExecutionStatus.PENDING.value:
                continue

            attempt = int(row.get("trialexecution__attempt") or 0)
            if attempt >= config.max_attempts:
                _mark_failure(
                    row,
                    failure_mode="max_attempts_exceeded",
                    error=f"max attempts exceeded ({config.max_attempts})",
                )
                row["trialexecution__updated_at_ms"] = now
                continue

            attempt += 1
            request = _request_from_row(row)
            idempotency_key = f"{request.trial_id}:attempt-{attempt}"
            job = await backend.submit(request, idempotency_key=idempotency_key)

            row["trialexecution__backend"] = backend.name
            row["trialexecution__status"] = job.status
            row["trialexecution__job_id"] = job.job_id
            row["trialexecution__attempt"] = attempt
            row["trialexecution__submitted_at_ms"] = now
            row["trialexecution__updated_at_ms"] = now
            row["trialexecution__error"] = ""
            submissions += 1

        return _df_from_rows(rows, schema)


class PollTrialsProcessor(AsyncProcessor):
    """Poll in-flight trials and update outcome/manifest lifecycle columns."""

    components = (Trial, TrialExecution, TrialOutcome, TrialManifest)
    priority = 20

    async def process(self, df, resources: Resources | None = None, **kwargs):
        resources = resources or kwargs.get("resources") or Resources()
        backend_resource = resources.get(TrialExecutionBackendResource)
        if backend_resource is None:
            return df

        config = _config_from_resources(resources)
        backend = backend_resource.backend
        rows, schema = _rows_from_df(df)
        polls = 0

        rows.sort(key=lambda row: str(row.get("trial__trial_id") or ""))
        for row in rows:
            if polls >= config.max_polls_per_tick:
                break
            status = str(row.get("trialexecution__status") or "")
            if not TrialExecutionStatus.is_in_flight(status):
                continue

            job_id = str(row.get("trialexecution__job_id") or "")
            if not job_id:
                continue

            outcome = await backend.poll(job_id)
            polls += 1
            if outcome is None:
                continue

            now = _now_ms()
            row["trialexecution__status"] = outcome.status
            row["trialexecution__updated_at_ms"] = now

            if not TrialExecutionStatus.is_terminal(outcome.status):
                continue

            row["trialexecution__error"] = outcome.error
            row["trialoutcome__passed"] = outcome.passed
            row["trialoutcome__score"] = outcome.score
            row["trialoutcome__steps"] = outcome.steps
            row["trialoutcome__failure_mode"] = outcome.failure_mode
            row["trialoutcome__error"] = outcome.error
            row["trialmanifest__manifest_uri"] = outcome.manifest_uri
            row["trialmanifest__storage_uri"] = outcome.storage_uri
            row["trialmanifest__verified"] = outcome.verified
            row["trialmanifest__row_count"] = outcome.row_count
            row["trialmanifest__details_json"] = outcome.details_json

            if (
                outcome.status == TrialExecutionStatus.SUCCEEDED.value
                and config.require_verified_manifest
                and not outcome.verified
            ):
                _mark_failure(
                    row,
                    failure_mode="unverified_manifest",
                    error="backend reported success without a verified manifest",
                )

        return _df_from_rows(rows, schema)


__all__ = [
    "PollTrialsProcessor",
    "SubmitTrialsProcessor",
    "Trial",
    "TrialBackendOutcome",
    "TrialExecution",
    "TrialExecutionBackend",
    "TrialExecutionBackendResource",
    "TrialExecutionConfig",
    "TrialExecutionStatus",
    "TrialJobRef",
    "TrialManifest",
    "TrialOutcome",
    "TrialRequest",
]

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Free evaluation operation handlers over storage-owned durable rows."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from typing import Any, Protocol

import daft
import pyarrow as pa
from pydantic_core import to_jsonable_python
from uuid_utils import uuid7

from archetype.evaluation import grading, views
from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import subject_digest
from archetype.evaluation.models import Evaluate, GraderContract, Outcome, RunGraders
from archetype.storage.interfaces import iStorageService

logger = logging.getLogger(__name__)

EVALUATION_LEASE_SECONDS = 300.0
EVALUATION_POLL_SECONDS = 0.05
EVALUATION_SCHEMA = pa.schema(
    [
        ("evaluation_id", pa.large_string()),
        ("subject_digest", pa.large_string()),
        ("contract_digest", pa.large_string()),
        ("grader_id", pa.large_string()),
        ("outcome", pa.large_string()),
        ("score", pa.float64()),
        ("graded_at_ms", pa.int64()),
        ("evidence_json", pa.large_string()),
    ]
)


class _EvaluationIdentity(Protocol):
    subject_digest: str
    contract_digest: str


class _EvaluationLease(_EvaluationIdentity, Protocol):
    world_id: str
    run_id: str
    evaluation_id: str
    status: str
    owner: str | None
    acquired: bool


async def run_graders(operation: RunGraders) -> list[object]:
    """Execute one exact ephemeral grading operation."""

    return await grading.run_graders(operation.df, operation.graders)


async def evaluate(
    storage: iStorageService,
    operation: Evaluate,
) -> EvalReceipt:
    """Grade a pinned snapshot once and append one typed evaluation result."""

    storage_config = operation.storage_config
    if storage_config is None:
        raise ValueError("Evaluate requires explicit storage_config")
    if not isinstance(operation.contract, GraderContract):
        raise ValueError(
            "persisted receipts require a GraderContract descriptor; "
            "use run_graders for ephemeral scoring"
        )

    world_id = str(operation.world_id)
    snapshot = await views.pin_snapshot(
        storage,
        world_id=world_id,
        storage_config=storage_config,
    )
    subject = subject_digest(
        world_id,
        snapshot.run_id,
        snapshot_tick=snapshot.tick,
        snapshot_tokens=list(snapshot.head_tokens),
        snapshot_segments=(
            [
                (
                    segment.world_id,
                    segment.run_id,
                    int(segment.up_to_tick),
                    list(segment.head_tokens),
                )
                for segment in snapshot.query_snapshot.effective_lineage
                if segment.up_to_tick is not None
            ]
            or None
        ),
        component_names=[component.__name__ for component in operation.components],
        ticks=list(operation.ticks) if operation.ticks is not None else None,
        entity_ids=(list(operation.entity_ids) if operation.entity_ids is not None else None),
    )
    contract_digest = operation.contract.digest()
    existing = await views.read_result(
        storage,
        world_id=world_id,
        evaluation_id=operation.evaluation_id,
        storage_config=storage_config,
    )
    if existing is not None:
        _require_same_identity(
            existing,
            operation.evaluation_id,
            subject,
            contract_digest,
        )

    catalog = storage.get_control_catalog(storage_config)
    owner = f"{socket.gethostname()}:{os.getpid()}:{uuid7()}"
    lease, settled = await _acquire_evaluation(
        storage,
        catalog,
        world_id=world_id,
        run_id=snapshot.run_id,
        evaluation_id=operation.evaluation_id,
        subject_digest=subject,
        contract_digest=contract_digest,
        owner=owner,
        storage_config=storage_config,
    )
    if settled is not None:
        return settled
    assert lease is not None and lease.acquired

    stop_heartbeat = asyncio.Event()
    lease_lost = asyncio.Event()
    heartbeat = asyncio.create_task(
        _heartbeat_evaluation(
            catalog,
            lease,
            owner=owner,
            stop=stop_heartbeat,
            lost=lease_lost,
        )
    )
    try:
        # Recovery check: a prior owner may have appended the Iceberg row and
        # crashed before completing its control record.
        existing = await views.read_result(
            storage,
            world_id=world_id,
            evaluation_id=operation.evaluation_id,
            storage_config=storage_config,
        )
        if existing is not None:
            _require_same_identity(
                existing,
                operation.evaluation_id,
                subject,
                contract_digest,
            )
            stop_heartbeat.set()
            await heartbeat
            if lease_lost.is_set():
                raise RuntimeError(
                    f"lost durable lease while recovering evaluation {operation.evaluation_id!r}"
                )
            await catalog.complete_evaluation(
                world_id,
                snapshot.run_id,
                operation.evaluation_id,
                owner,
            )
            return existing

        frame = await views.read_pinned_subject(
            storage,
            snapshot,
            world_id=world_id,
            components=operation.components,
            ticks=operation.ticks,
            entity_ids=operation.entity_ids,
        )
        outputs = await grading.run_graders(frame, [operation.grader])
        typed = [output for output in outputs if isinstance(output, Outcome)]
        if len(typed) != len(outputs):
            raise ValueError("persisted evaluations require typed Outcome results")
        if len(typed) != 1:
            raise ValueError("a persisted evaluation must produce exactly one Outcome")
        outcome = typed[0]
        result = EvalReceipt(
            evaluation_id=operation.evaluation_id,
            subject_digest=subject,
            contract_digest=contract_digest,
            grader_id=operation.contract.grader_id,
            outcome=outcome.status,
            score=outcome.score,
            graded_at_ms=int(time.time() * 1000),
            evidence_json=json.dumps(to_jsonable_python(outcome.evidence)),
        )
        if lease_lost.is_set():
            raise RuntimeError(f"lost durable lease for evaluation {operation.evaluation_id!r}")
        await storage.append_world_rows(
            storage_config,
            world_id,
            views.EVALUATION_RESULTS_TABLE,
            daft.from_arrow(pa.Table.from_pylist([result.model_dump()], schema=EVALUATION_SCHEMA)),
            key_columns=("evaluation_id",),
        )
        stop_heartbeat.set()
        await heartbeat
        if lease_lost.is_set():
            raise RuntimeError(f"lost durable lease for evaluation {operation.evaluation_id!r}")
        await catalog.complete_evaluation(
            world_id,
            snapshot.run_id,
            operation.evaluation_id,
            owner,
        )
        return result
    except BaseException:
        stop_heartbeat.set()
        if not heartbeat.done():
            await heartbeat
        try:
            await catalog.release_evaluation(
                world_id,
                snapshot.run_id,
                operation.evaluation_id,
                owner,
            )
        except Exception:
            logger.warning("failed to release durable evaluation lease")
        raise


async def _acquire_evaluation(
    storage: iStorageService,
    catalog: Any,
    *,
    world_id: str,
    run_id: str,
    evaluation_id: str,
    subject_digest: str,
    contract_digest: str,
    owner: str,
    storage_config: Any,
) -> tuple[_EvaluationLease | None, EvalReceipt | None]:
    """Wait for a racing result or return this process's durable lease."""

    while True:
        lease = await catalog.lease_evaluation(
            world_id,
            run_id,
            evaluation_id,
            subject_digest,
            contract_digest,
            owner,
            lease_seconds=EVALUATION_LEASE_SECONDS,
        )
        _require_same_identity(
            lease,
            evaluation_id,
            subject_digest,
            contract_digest,
        )
        if lease.status == "COMPLETE":
            result = await views.read_result(
                storage,
                world_id=world_id,
                evaluation_id=evaluation_id,
                storage_config=storage_config,
            )
            if result is None:
                raise RuntimeError(
                    f"evaluation {evaluation_id!r} is complete in the control catalog "
                    "but its Iceberg result is missing"
                )
            _require_same_identity(
                result,
                evaluation_id,
                subject_digest,
                contract_digest,
            )
            return None, result
        if lease.acquired:
            return lease, None
        await asyncio.sleep(EVALUATION_POLL_SECONDS)


async def _heartbeat_evaluation(
    catalog: Any,
    lease: _EvaluationLease,
    *,
    owner: str,
    stop: asyncio.Event,
    lost: asyncio.Event,
) -> None:
    """Renew a live grader lease; crash recovery still comes from expiry."""

    interval = max(EVALUATION_LEASE_SECONDS / 3, EVALUATION_POLL_SECONDS)
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except TimeoutError:
            pass
        try:
            renewed = await catalog.lease_evaluation(
                lease.world_id,
                lease.run_id,
                lease.evaluation_id,
                lease.subject_digest,
                lease.contract_digest,
                owner,
                lease_seconds=EVALUATION_LEASE_SECONDS,
            )
        except Exception:
            lost.set()
            return
        if not renewed.acquired or renewed.owner != owner:
            lost.set()
            return


def _require_same_identity(
    record: _EvaluationIdentity,
    evaluation_id: str,
    subject_digest: str,
    contract_digest: str,
) -> None:
    if record.subject_digest != subject_digest or record.contract_digest != contract_digest:
        raise ValueError(
            f"evaluation_id {evaluation_id!r} already names a different subject or grader contract"
        )


__all__ = ["evaluate", "run_graders"]

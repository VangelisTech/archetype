# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dataframe-first evaluation over persisted component rows.

``EvaluationService`` does not own simulation, experiment lifecycle, or a durable
scoring schema. It finds persisted rows through ``QueryService`` and runs
caller-provided graders over Daft DataFrames.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from collections.abc import Sequence
from dataclasses import dataclass
from inspect import isawaitable
from typing import Any, Protocol

import daft
import pyarrow as pa
from daft import DataFrame
from pydantic_core import to_jsonable_python
from uuid_utils import UUID, uuid7

from archetype.app.evaluation.interfaces import GraderOutput, TrajectoryGrader
from archetype.app.ingestion.interfaces import iIngestionService
from archetype.app.models import EpisodeResult
from archetype.app.query.interfaces import iQueryService
from archetype.app.storage.interfaces import iStorageService
from archetype.app.world.interfaces import iWorldService
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import (
    GraderContract,
    Outcome,
    subject_digest,
)
from archetype.ingestion.contracts import IngestionTable

logger = logging.getLogger(__name__)

_EVALUATION_RESULTS = IngestionTable("evaluation_results", key_columns=("evaluation_id",))
_EVALUATION_LEASE_SECONDS = 300.0
_EVALUATION_POLL_SECONDS = 0.05
_EVALUATION_SCHEMA = pa.schema(
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


@dataclass(frozen=True)
class _PinnedSnapshot:
    """Immutable simulation visibility captured for one evaluation."""

    run_id: str
    tick: int
    head_tokens: tuple[str, ...]
    visibility_tokens: tuple[str, ...]
    storage_config: StorageConfig


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


class EvaluationService:
    """Query persisted rows and execute graders over DataFrames.

    The returned analysis surface is a Daft DataFrame. Durable scores remain
    components written by the caller when a score should persist.
    """

    def __init__(
        self,
        query_service: iQueryService,
        ingestion_service: iIngestionService,
        storage_service: iStorageService,
        world_service: iWorldService,
    ) -> None:
        self._query_service = query_service
        self._ingestion = ingestion_service
        self._storage = storage_service
        self._worlds = world_service

    async def evaluate(
        self,
        world_id: str,
        components: Sequence[type[Component]],
        *,
        contract,
        grader: TrajectoryGrader,
        evaluation_id: str,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ):
        """Grade a pinned snapshot once and append one typed evaluation result.

        Iceberg owns the result. A narrow control-catalog lease serializes the
        potentially paid grader across processes while that result is absent.
        """
        if not isinstance(contract, GraderContract):
            raise ValueError(
                "persisted receipts require a GraderContract descriptor; "
                "use run_graders for ephemeral scoring"
            )

        snapshot = await self._snapshot(world_id, storage_config)
        subject = subject_digest(
            world_id,
            snapshot.run_id,
            snapshot_tick=snapshot.tick,
            snapshot_tokens=list(snapshot.head_tokens),
            component_names=[component.__name__ for component in components],
            ticks=ticks,
            entity_ids=entity_ids,
        )
        contract_digest = contract.digest()
        existing = await self._existing_result(
            world_id,
            evaluation_id,
            snapshot.storage_config,
        )
        if existing is not None:
            self._require_same_identity(existing, evaluation_id, subject, contract_digest)

        catalog = self._storage.get_control_catalog(snapshot.storage_config)
        owner = f"{socket.gethostname()}:{os.getpid()}:{uuid7()}"
        lease, settled = await self._acquire_evaluation(
            catalog,
            world_id=str(world_id),
            run_id=snapshot.run_id,
            evaluation_id=evaluation_id,
            subject_digest=subject,
            contract_digest=contract_digest,
            owner=owner,
            storage_config=snapshot.storage_config,
        )
        if settled is not None:
            return settled
        assert lease is not None and lease.acquired

        snapshot_ticks = (
            [tick for tick in ticks if tick <= snapshot.tick]
            if ticks is not None
            else list(range(snapshot.tick + 1))
        )

        stop_heartbeat = asyncio.Event()
        lease_lost = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat_evaluation(
                catalog,
                lease,
                owner=owner,
                stop=stop_heartbeat,
                lost=lease_lost,
            )
        )
        try:
            # Recovery check: a prior owner may have appended the Iceberg row
            # and crashed before completing its control record.
            existing = await self._existing_result(
                world_id,
                evaluation_id,
                snapshot.storage_config,
            )
            if existing is not None:
                self._require_same_identity(existing, evaluation_id, subject, contract_digest)
                stop_heartbeat.set()
                await heartbeat
                if lease_lost.is_set():
                    raise RuntimeError(
                        f"lost durable lease while recovering evaluation {evaluation_id!r}"
                    )
                await catalog.complete_evaluation(
                    str(world_id), snapshot.run_id, evaluation_id, owner
                )
                return existing

            frame = await self._query_service.query_components(
                list(components),
                world_id,
                snapshot.run_id,
                snapshot.storage_config,
                ticks=snapshot_ticks,
                entity_ids=entity_ids,
                visibility_tokens=list(snapshot.visibility_tokens),
            )
            outputs = await self.run_graders(frame, [grader])
            typed = [output for output in outputs if isinstance(output, Outcome)]
            if len(typed) != len(outputs):
                raise ValueError("persisted evaluations require typed Outcome results")
            if len(typed) != 1:
                raise ValueError("a persisted evaluation must produce exactly one Outcome")
            outcome = typed[0]
            result = EvalReceipt(
                evaluation_id=evaluation_id,
                subject_digest=subject,
                contract_digest=contract_digest,
                grader_id=contract.grader_id,
                outcome=outcome.status,
                score=outcome.score,
                graded_at_ms=int(time.time() * 1000),
                evidence_json=json.dumps(to_jsonable_python(outcome.evidence)),
            )
            if lease_lost.is_set():
                raise RuntimeError(f"lost durable lease for evaluation {evaluation_id!r}")
            await self._ingestion.append(
                world_id,
                _EVALUATION_RESULTS,
                daft.from_arrow(
                    pa.Table.from_pylist([result.model_dump()], schema=_EVALUATION_SCHEMA)
                ),
                storage_config=snapshot.storage_config,
            )
            stop_heartbeat.set()
            await heartbeat
            if lease_lost.is_set():
                raise RuntimeError(f"lost durable lease for evaluation {evaluation_id!r}")
            await catalog.complete_evaluation(str(world_id), snapshot.run_id, evaluation_id, owner)
            return result
        except BaseException:
            stop_heartbeat.set()
            if not heartbeat.done():
                await heartbeat
            try:
                await catalog.release_evaluation(
                    str(world_id), snapshot.run_id, evaluation_id, owner
                )
            except Exception:
                logger.warning("failed to release durable evaluation lease")
            raise

    async def _acquire_evaluation(
        self,
        catalog: Any,
        *,
        world_id: str,
        run_id: str,
        evaluation_id: str,
        subject_digest: str,
        contract_digest: str,
        owner: str,
        storage_config: StorageConfig,
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
                lease_seconds=_EVALUATION_LEASE_SECONDS,
            )
            self._require_same_identity(lease, evaluation_id, subject_digest, contract_digest)
            if lease.status == "COMPLETE":
                result = await self._existing_result(world_id, evaluation_id, storage_config)
                if result is None:
                    raise RuntimeError(
                        f"evaluation {evaluation_id!r} is complete in the control catalog "
                        "but its Iceberg result is missing"
                    )
                self._require_same_identity(result, evaluation_id, subject_digest, contract_digest)
                return None, result
            if lease.acquired:
                return lease, None
            await asyncio.sleep(_EVALUATION_POLL_SECONDS)

    async def _heartbeat_evaluation(
        self,
        catalog: Any,
        lease: _EvaluationLease,
        *,
        owner: str,
        stop: asyncio.Event,
        lost: asyncio.Event,
    ) -> None:
        """Renew a live grader lease; crash recovery still comes from expiry."""
        interval = max(_EVALUATION_LEASE_SECONDS / 3, _EVALUATION_POLL_SECONDS)
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
                    lease_seconds=_EVALUATION_LEASE_SECONDS,
                )
            except Exception:
                lost.set()
                return
            if not renewed.acquired or renewed.owner != owner:
                lost.set()
                return

    @staticmethod
    def _require_same_identity(
        record: _EvaluationIdentity,
        evaluation_id: str,
        subject_digest: str,
        contract_digest: str,
    ) -> None:
        if record.subject_digest != subject_digest or record.contract_digest != contract_digest:
            raise ValueError(
                f"evaluation_id {evaluation_id!r} already names a different "
                "subject or grader contract"
            )

    async def _snapshot(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> _PinnedSnapshot:
        wid = str(world_id)
        effective = self._resolve_storage(wid, storage_config)
        catalog = self._storage.get_control_catalog(effective)
        record = await catalog.get_world(wid)
        if record is None:
            raise KeyError(f"world {wid} is not recorded in catalog for {effective.uri}")
        if not record.run_id:
            raise RuntimeError(f"world {wid} has no recorded run; nothing to evaluate")
        manifests = await catalog.list_manifests(wid, str(record.run_id))
        if not manifests:
            raise RuntimeError(
                f"world {wid} has no published visibility to evaluate (step it at least once first)"
            )
        head = max(manifest.tick for manifest in manifests)
        visible = await catalog.visible_tokens(wid, str(record.run_id))
        visibility_tokens = {
            token for tick, tokens in (visible or {}).items() if tick <= head for token in tokens
        }
        return _PinnedSnapshot(
            run_id=str(record.run_id),
            tick=head,
            head_tokens=tuple(
                sorted(manifest.commit_token for manifest in manifests if manifest.tick == head)
            ),
            visibility_tokens=tuple(sorted(visibility_tokens)),
            storage_config=effective,
        )

    async def _existing_result(
        self,
        world_id: str,
        evaluation_id: str,
        storage_config: StorageConfig,
    ) -> EvalReceipt | None:
        try:
            rows = await self._ingestion.read(
                world_id,
                _EVALUATION_RESULTS,
                storage_config=storage_config,
            )
        except KeyError:
            return None
        values = (
            rows.where(
                rows["evaluation_id"] == evaluation_id  # ty: ignore[invalid-argument-type]
            )
            .limit(1)
            .to_pydict()
        )
        if not values.get("evaluation_id"):
            return None
        return EvalReceipt(**{name: values[name][0] for name in EvalReceipt.model_fields})

    def _resolve_storage(
        self,
        world_id: str,
        storage_config: StorageConfig | None,
    ) -> StorageConfig:
        if storage_config is not None:
            return storage_config
        live = self._worlds.storage_record(world_id)
        return live[0] if live is not None else StorageConfig()

    async def query_components(
        self,
        components: Sequence[type[Component]],
        *,
        world_id: str | UUID,
        run_id: str | UUID,
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame:
        """Return persisted component rows as a Daft DataFrame."""
        return await self._query_service.query_components(
            list(components),
            world_id=str(world_id),
            run_id=str(run_id),
            storage_config=storage_config,
            ticks=ticks,
            entity_ids=entity_ids,
            lineage=lineage,
        )

    async def query_episode(
        self,
        episode: EpisodeResult,
        *,
        components: Sequence[type[Component]],
        run_id: str | UUID | None = None,
        storage_config: StorageConfig | None = None,
        entity_ids: list[int] | None = None,
        lineage: list[tuple[str, str, int]] | None = None,
    ) -> DataFrame:
        """Return component rows produced during one episode."""
        active_run_id = run_id or episode.run_id
        if active_run_id is None:
            raise ValueError("query_episode requires episode.run_id or run_id")
        return await self.query_components(
            components,
            world_id=episode.world_id,
            run_id=active_run_id,
            storage_config=storage_config,
            ticks=list(range(int(episode.start_tick), int(episode.final_tick))),
            entity_ids=entity_ids,
            lineage=lineage,
        )

    async def run_graders(
        self,
        df: DataFrame,
        graders: Sequence[TrajectoryGrader],
    ) -> list[GraderOutput]:
        """Execute graders and flatten their non-empty outputs.

        An evaluation with no graders or no grader outputs is undefined, not a
        passing evaluation. Reject both cases here so callers cannot turn an
        empty result into a vacuous success via ``all([])``.
        """
        if not graders:
            raise ValueError("run_graders requires at least one grader")

        results: list[GraderOutput] = []
        for grader in graders:
            raw = grader(df)
            output = await raw if isawaitable(raw) else raw
            if isinstance(output, Sequence) and not isinstance(output, str | bytes):
                if not output:
                    name = getattr(grader, "__name__", type(grader).__name__)
                    raise ValueError(f"grader {name!r} returned no outputs")
                results.extend(output)
            else:
                results.append(output)
        return results

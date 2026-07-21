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

import json
import time
from collections.abc import Sequence
from dataclasses import dataclass
from inspect import isawaitable

import daft
import pyarrow as pa
from daft import DataFrame
from pydantic_core import to_jsonable_python
from uuid_utils import UUID

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

_EVALUATION_RESULTS = IngestionTable("evaluation_results", key_columns=("evaluation_id",))
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
        """Grade a pinned snapshot and append one typed evaluation result."""
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
            if existing.subject_digest != subject or existing.contract_digest != contract_digest:
                raise ValueError(
                    f"evaluation_id {evaluation_id!r} already names a different "
                    "subject or grader contract"
                )
            return existing
        snapshot_ticks = (
            [tick for tick in ticks if tick <= snapshot.tick]
            if ticks is not None
            else list(range(snapshot.tick + 1))
        )

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
        await self._ingestion.append(
            world_id,
            _EVALUATION_RESULTS,
            daft.from_arrow(pa.Table.from_pylist([result.model_dump()], schema=_EVALUATION_SCHEMA)),
            storage_config=snapshot.storage_config,
        )
        return result

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

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
from inspect import isawaitable

from daft import DataFrame
from pydantic_core import to_jsonable_python
from uuid_utils import UUID

from archetype.app.artifacts.interfaces import iArtifactService
from archetype.app.evaluation.interfaces import GraderOutput, TrajectoryGrader
from archetype.app.models import EpisodeResult
from archetype.app.query.interfaces import iQueryService
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.evaluation.components import EvalReceipt
from archetype.evaluation.contracts import (
    GraderContract,
    Outcome,
    evaluation_identity_digest,
    subject_digest,
)


class EvaluationService:
    """Query persisted rows and execute graders over DataFrames.

    The returned analysis surface is a Daft DataFrame. Durable scores remain
    components written by the caller when a score should persist.
    """

    def __init__(
        self,
        query_service: iQueryService,
        artifact_service: iArtifactService | None = None,
    ) -> None:
        self._query_service = query_service
        self._artifact_service = artifact_service

    async def evaluate(
        self,
        world_id: str,
        components: Sequence[type[Component]],
        *,
        contract,
        grader: TrajectoryGrader,
        evaluation_id: str,
        producer: str = "evals",
        storage_config: StorageConfig | None = None,
        ticks: list[int] | None = None,
        entity_ids: list[int] | None = None,
    ):
        """Claim, grade a pinned snapshot, and persist one typed receipt."""
        if self._artifact_service is None:
            raise RuntimeError("evaluation receipt persistence requires the artifact service")
        if not isinstance(contract, GraderContract):
            raise ValueError(
                "persisted receipts require a GraderContract descriptor; "
                "use run_graders for ephemeral scoring"
            )

        snapshot = await self._artifact_service.snapshot_ref(world_id, storage_config)
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
        identity = evaluation_identity_digest(subject, contract_digest)
        snapshot_ticks = (
            [tick for tick in ticks if tick <= snapshot.tick]
            if ticks is not None
            else list(range(snapshot.tick + 1))
        )

        async def grade_and_build(_claim):
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
                raise ValueError("persisted receipts require typed Outcome results")
            if len(typed) != 1:
                raise ValueError("a persisted evaluation must produce exactly one Outcome")
            outcome = typed[0]
            return [
                EvalReceipt(
                    evaluation_id=evaluation_id,
                    subject_digest=subject,
                    contract_digest=contract_digest,
                    grader_id=contract.grader_id,
                    outcome=outcome.status,
                    score=outcome.score,
                    graded_at_ms=int(time.time() * 1000),
                    evidence_json=json.dumps(to_jsonable_python(outcome.evidence)),
                )
            ]

        return await self._artifact_service.publish_evaluation(
            world_id,
            evaluation_id=evaluation_id,
            producer=producer,
            identity_digest=identity,
            component_types=[EvalReceipt],
            build_components=grade_and_build,
            storage_config=snapshot.storage_config,
        )

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

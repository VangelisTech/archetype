# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Trajectory Pipeline
===================

The high-level API. Describe what you want, it wires the world.

    pipeline = TrajectoryPipeline(name="eval-backtracking")
    pipeline.label("efficiency", "Rate how directly the agent solved the problem")
    pipeline.label("correctness", "Did the agent produce the right final answer?")
    pipeline.sample(min_turns=5, max_trajectories=100)
    await pipeline.ingest(trajectories)
    await pipeline.run()
    results = await pipeline.results()

Fork to compare techniques:

    fork = await pipeline.fork("eval-strict-correctness")
    fork.label("correctness", "Strictly binary: did the final output exactly match expected?")
    await fork.run()
"""

from __future__ import annotations

from typing import Any

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig
from archetype.trajectories.components import Label, Trajectory
from archetype.trajectories.processors import (
    LabelingConfig,
    LabelingProcessor,
    SamplingConfig,
    SamplingProcessor,
    ScoringProcessor,
)


class TrajectoryPipeline:
    """Wires up a trajectory analysis experiment as an archetype world.

    Each pipeline is a world. Each trajectory is an entity with (Trajectory, Label)
    components — one entity per (trajectory, technique) pair. Processors run
    in priority order: sample → label → score.

    All state lives in LanceDB. Fork to compare. Query to analyze.
    """

    def __init__(
        self,
        name: str,
        *,
        storage_uri: str = "./trajectory_data",
        model: str = "gpt-5-mini",
    ):
        self.name = name
        self._storage_uri = storage_uri
        self._model = model
        self._labels: list[tuple[str, str]] = []  # (technique, description)
        self._sampling = SamplingConfig()
        self._container: ServiceContainer | None = None
        self._world: Any = None  # AsyncWorld, set after init
        self._ctx = ActorCtx(id=uuid7(), roles={"admin"})
        self._initialized = False

    def label(self, technique: str, description: str) -> TrajectoryPipeline:
        """Add a labeling technique. Describe it in natural language."""
        self._labels.append((technique, description))
        return self

    def sample(
        self,
        *,
        max_trajectories: int = 0,
        min_turns: int = 0,
        max_turns: int = 0,
        require_tags: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        outcome_filter: str | None = None,
    ) -> TrajectoryPipeline:
        """Configure sampling. Chainable."""
        self._sampling = SamplingConfig(
            max_trajectories=max_trajectories,
            min_turns=min_turns,
            max_turns=max_turns,
            require_tags=require_tags,
            exclude_tags=exclude_tags,
            outcome_filter=outcome_filter,
        )
        return self

    async def _ensure_init(self) -> None:
        if self._initialized:
            return
        self._container = ServiceContainer()
        self._world = await self._container.world_service.create_world(
            WorldConfig(name=self.name),
            StorageConfig(uri=self._storage_uri, namespace="trajectories"),
        )
        # Wire processors
        await self._world.system.add_processor(SamplingProcessor())
        await self._world.system.add_processor(LabelingProcessor())
        await self._world.system.add_processor(ScoringProcessor())
        # Inject resources
        self._world.resources.insert(self._sampling)
        self._world.resources.insert(LabelingConfig(model=self._model))
        self._initialized = True

    async def ingest(self, trajectories: list[Trajectory]) -> list[int]:
        """Ingest trajectories into the world. Creates one entity per (trajectory, technique) pair."""
        await self._ensure_init()
        if not self._labels:
            raise ValueError("No labeling techniques defined. Call .label() first.")

        entity_ids = []
        for trajectory in trajectories:
            for technique, description in self._labels:
                label = Label(technique=technique, description=description)
                cmd = Command(
                    type=CommandType.SPAWN,
                    payload={
                        "components": [
                            trajectory.model_dump(),
                            label.model_dump(),
                        ],
                    },
                )
                await self._container.command_service.submit(
                    self._world.world_id, cmd, self._ctx
                )
                entity_ids.append(len(entity_ids))
        return entity_ids

    async def run(self, steps: int = 1) -> None:
        """Run the pipeline. One step = sample → label → score."""
        await self._ensure_init()
        await self._container.simulation_service.run(
            self._world.world_id,
            RunConfig(num_steps=steps, suite=self.name),
        )

    async def results(self) -> list[dict[str, Any]]:
        """Collect results as a list of dicts."""
        await self._ensure_init()
        rows = []
        for _sig, df in self._world._live.items():
            collected = df.collect().to_pylist()
            for row in collected:
                if not row.get("is_active", True):
                    continue
                rows.append(
                    {
                        "trajectory_id": row.get("trajectory__trajectory_id", ""),
                        "technique": row.get("label__technique", ""),
                        "description": row.get("label__description", ""),
                        "value": row.get("label__value", ""),
                        "score": row.get("label__score", 0.0),
                        "rationale": row.get("label__rationale", ""),
                        "sampled": row.get("label__sampled", True),
                        "total_turns": row.get("trajectory__total_turns", 0),
                        "outcome": row.get("trajectory__outcome", ""),
                        "source": row.get("trajectory__source", ""),
                    }
                )
        return rows

    async def results_df(self) -> Any:
        """Return raw Daft DataFrame of live state (for advanced queries)."""
        await self._ensure_init()
        dfs = list(self._world._live.values())
        if not dfs:
            return None
        return dfs[0] if len(dfs) == 1 else dfs

    async def fork(self, new_name: str) -> TrajectoryPipeline:
        """Fork this pipeline into a new world with the same data.

        Use this to compare labeling techniques on identical trajectories.
        Modify the fork's labels, then run it.
        """
        await self._ensure_init()
        forked = TrajectoryPipeline(
            name=new_name,
            storage_uri=self._storage_uri,
            model=self._model,
        )
        forked._labels = list(self._labels)
        forked._sampling = self._sampling
        # Initialize with a fresh world (fork semantics via archetype)
        forked._container = self._container
        forked._world = await self._container.world_service.fork_world(
            source_id=self._world.world_id,
            new_config=WorldConfig(name=new_name),
            storage_config=StorageConfig(uri=self._storage_uri, namespace="trajectories"),
        )
        await forked._world.system.add_processor(SamplingProcessor())
        await forked._world.system.add_processor(LabelingProcessor())
        await forked._world.system.add_processor(ScoringProcessor())
        forked._world.resources.insert(forked._sampling)
        forked._world.resources.insert(LabelingConfig(model=forked._model))
        forked._initialized = True
        return forked

    async def shutdown(self) -> None:
        """Clean up."""
        if self._container:
            await self._container.shutdown()

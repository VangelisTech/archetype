# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
EmbeddingService — Recursive Self-Improving Embeddings
=======================================================

Each call to run_improvement_loop() executes N generations:

  Generation 0:  embed all documents with model M → score against empty frontier
  Generation 1+: embed → score against previous centroid → advance frontier if better

The frontier tracks the best-seen avg_quality. spawn_world() can be used to
trial multiple model variants in parallel before committing the best one.

Usage:
    from archetype.app.embeddings.service import EmbeddingService
    from archetype.app.container import ServiceContainer

    container = ServiceContainer()
    svc = EmbeddingService(container)

    result = await svc.run_improvement_loop(
        documents=[{"doc_id": "a", "content": "hello world"}],
        generations=5,
        model_name="all-MiniLM-L6-v2",
    )
    print(result["final_avg_quality"], result["generations_improved"])
"""

from __future__ import annotations

import json
import logging
from typing import Any

from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

from .components import Document, Embedding, EmbeddingFrontier
from .processors import EmbedProcessor, FrontierProcessor, QualityProcessor

logger = logging.getLogger(__name__)


class EmbeddingService:
    """
    Manages the recursive embedding improvement loop.

    Each generation is one world tick. The EmbeddingFrontier component
    carries the best-seen state — if a generation beats it, the frontier
    advances; otherwise the generation is discarded and the next starts
    from the same baseline.
    """

    def __init__(self, container):
        self._container = container
        self._ctx = ActorCtx(id=uuid7(), roles={"admin"})

    async def run_improvement_loop(
        self,
        documents: list[dict[str, Any]],
        generations: int = 5,
        model_name: str = "all-MiniLM-L6-v2",
        storage_path: str = "./embeddings_data",
    ) -> dict[str, Any]:
        """
        Run N generations of recursive embedding improvement.

        Args:
            documents: list of {"doc_id": str, "content": str, "chunk_index": int}
            generations: number of improvement generations to run
            model_name: sentence-transformers model identifier
            storage_path: LanceDB storage path for this run

        Returns:
            {
                "final_avg_quality": float,
                "generations_improved": int,
                "frontier_generation": int,
                "world_id": str,
            }
        """
        storage_config = StorageConfig(uri=storage_path)
        world = await self._container.world_service.create_world(
            WorldConfig(name=f"embeddings-{uuid7()}"),
            storage_config,
        )
        wid = world.world_id

        # Wire processors
        await world.system.add_processor(EmbedProcessor(model_name))
        await world.system.add_processor(QualityProcessor())
        await world.system.add_processor(FrontierProcessor())

        # Spawn document + embedding entities
        for doc in documents:
            await self._container.command_service.submit(
                wid,
                Command(
                    type=CommandType.SPAWN,
                    payload={
                        "components": [
                            Document(
                                doc_id=doc.get("doc_id", str(uuid7())),
                                content=doc["content"],
                                chunk_index=doc.get("chunk_index", 0),
                            ).model_dump(),
                            Embedding(doc_id=doc.get("doc_id", "")).model_dump(),
                        ]
                    },
                ),
                self._ctx,
            )

        # Spawn the single frontier entity
        await self._container.command_service.submit(
            wid,
            Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        EmbeddingFrontier(model_name=model_name).model_dump(),
                    ]
                },
            ),
            self._ctx,
        )

        # Run all generations
        result = await self._container.simulation_service.run(
            wid, RunConfig(num_steps=generations)
        )
        logger.info(f"Completed {result.ticks_completed} ticks")

        # Read final frontier state (intentional collect — result read-out)
        final_quality = 0.0
        frontier_generation = 0
        for _sig, df in world._live.items():
            rows = df.collect().to_pylist()
            for row in rows:
                if "embeddingfrontier__avg_quality" in row:
                    final_quality = row["embeddingfrontier__avg_quality"] or 0.0
                    frontier_generation = row.get("embeddingfrontier__generation", 0) or 0

        return {
            "final_avg_quality": final_quality,
            "generations_improved": frontier_generation,
            "frontier_generation": frontier_generation,
            "ticks_completed": result.ticks_completed,
            "world_id": str(wid),
        }

    async def trial_models(
        self,
        documents: list[dict[str, Any]],
        model_names: list[str],
        generations: int = 3,
    ) -> dict[str, Any]:
        """
        Run improvement loops for multiple models and return the best.

        Each model gets its own world (parallel trials), then we pick
        the one with the highest final_avg_quality.
        """
        import asyncio

        tasks = [
            self.run_improvement_loop(
                documents=documents,
                generations=generations,
                model_name=m,
                storage_path=f"./embeddings_trial_{m.replace('/', '_')}",
            )
            for m in model_names
        ]
        results = await asyncio.gather(*tasks)

        best = max(zip(model_names, results), key=lambda x: x[1]["final_avg_quality"])
        return {
            "best_model": best[0],
            "best_result": best[1],
            "all_results": dict(zip(model_names, results)),
        }

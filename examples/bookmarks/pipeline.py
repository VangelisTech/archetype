# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Bookmarks Pipeline
==================

The high-level API. Point it at your X bookmarks, it builds a knowledge base.

    pipeline = BookmarksPipeline(
        name="my-knowledge-base",
        bearer_token="xox-...",
        storage_uri="s3://my-bucket/bookmarks",
    )
    await pipeline.fetch()                          # Pull from X API
    await pipeline.run()                            # Dedup → Enrich → Index
    results = await pipeline.search("daft dataframes")  # Query your knowledge

Incremental sync — only fetches new bookmarks since last run:

    await pipeline.fetch()   # First run: all bookmarks
    await pipeline.run()
    # ... later ...
    await pipeline.fetch()   # Only new bookmarks since last fetch
    await pipeline.run()

Fork to experiment with different enrichment models:

    fork = await pipeline.fork("gpt4-enrichment")
    fork.configure(model="gpt-4o")
    await fork.run()
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

import daft
from daft import col
from uuid_utils import uuid7

from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.core.config import RunConfig, StorageConfig, WorldConfig

from .client import XBookmarksClient
from .components import Bookmark, KnowledgeEntry
from .processors import (
    DeduplicationProcessor,
    EnrichmentConfig,
    EnrichmentProcessor,
    IndexingConfig,
    IndexingProcessor,
)

logger = logging.getLogger("bookmarks.pipeline")


class BookmarksPipeline:
    """Wires up an X bookmarks knowledge base as an archetype world.

    Each bookmark is an entity with (Bookmark, KnowledgeEntry) components.
    Processors run in priority order: dedup → enrich → index.

    All state lives in LanceDB (or Iceberg on S3). Fork to compare.
    Query to search your knowledge base.
    """

    def __init__(
        self,
        name: str,
        *,
        bearer_token: str,
        user_id: str | None = None,
        storage_uri: str = "./bookmarks_data",
        model: str = "gpt-5-mini",
        embedding_model: str = "text-embedding-3-small",
        categories: list[str] | None = None,
        ctx: ActorCtx | None = None,
    ):
        self.name = name
        self._bearer_token = bearer_token
        self._user_id = user_id
        self._storage_uri = storage_uri
        self._model = model
        self._embedding_model = embedding_model
        self._categories = categories
        self._container: ServiceContainer | None = None
        self._world: Any = None
        self._ctx = ctx or ActorCtx(id=uuid7(), roles={"operator"})
        self._initialized = False
        self._owns_container = False
        self._last_tweet_id: str | None = None  # For incremental sync

    def configure(
        self,
        *,
        model: str | None = None,
        embedding_model: str | None = None,
        categories: list[str] | None = None,
    ) -> BookmarksPipeline:
        """Update enrichment configuration. Chainable."""
        if model is not None:
            self._model = model
        if embedding_model is not None:
            self._embedding_model = embedding_model
        if categories is not None:
            self._categories = categories
        # Update live resources if already initialized
        if self._initialized and self._world is not None:
            enrichment_kwargs: dict[str, Any] = {"model": self._model}
            if self._categories:
                enrichment_kwargs["categories"] = self._categories
            self._world.resources.insert(EnrichmentConfig(**enrichment_kwargs))
            self._world.resources.insert(IndexingConfig(model=self._embedding_model))
        return self

    async def _ensure_init(self) -> None:
        if self._initialized:
            return
        self._container = ServiceContainer()
        self._owns_container = True
        self._world = await self._container.world_service.create_world(
            WorldConfig(name=self.name),
            StorageConfig(uri=self._storage_uri, namespace="bookmarks"),
        )
        await self._setup_processors()
        self._initialized = True

    async def _setup_processors(self) -> None:
        """Wire processors and resources into the world."""
        await self._world.system.add_processor(DeduplicationProcessor())
        await self._world.system.add_processor(EnrichmentProcessor())
        await self._world.system.add_processor(IndexingProcessor())

        enrichment_kwargs: dict[str, Any] = {"model": self._model}
        if self._categories:
            enrichment_kwargs["categories"] = self._categories
        self._world.resources.insert(EnrichmentConfig(**enrichment_kwargs))
        self._world.resources.insert(IndexingConfig(model=self._embedding_model))

    async def fetch(self) -> list[UUID]:
        """Fetch bookmarks from X API and ingest into the world.

        Performs incremental sync — only fetches bookmarks newer than the
        last seen tweet_id. Returns command UUIDs from spawn submissions.
        """
        await self._ensure_init()

        async with XBookmarksClient(
            self._bearer_token,
            user_id=self._user_id,
        ) as client:
            bookmarks = await client.fetch_all(since_id=self._last_tweet_id)

        if not bookmarks:
            logger.info("No new bookmarks found")
            return []

        # Track the newest tweet_id for next incremental fetch
        self._last_tweet_id = bookmarks[0].tweet_id

        return await self.ingest(bookmarks)

    async def ingest(self, bookmarks: list[Bookmark]) -> list[UUID]:
        """Ingest pre-built Bookmark objects into the world.

        Useful when you have bookmarks from a source other than the X API
        (e.g., an export file, a different social platform, manual entry).

        Returns command UUIDs from spawn submissions.
        """
        await self._ensure_init()

        command_ids = []
        for bookmark in bookmarks:
            entry = KnowledgeEntry()
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "Bookmark", **bookmark.model_dump()},
                        {"type": "KnowledgeEntry", **entry.model_dump()},
                    ],
                },
            )
            cmd_id = await self._container.command_service.submit(
                self._world.world_id, cmd, self._ctx
            )
            command_ids.append(cmd_id)

        logger.info("Ingested %d bookmarks into world %s", len(bookmarks), self.name)
        return command_ids

    async def run(self, steps: int = 1) -> None:
        """Run the pipeline. One step = dedup → enrich → index."""
        await self._ensure_init()
        for _ in range(steps):
            await self._container.simulation_service.step(
                self._world.world_id,
                RunConfig(num_steps=1, prefer_live_reads=True),
            )

    async def results(self) -> list[dict[str, Any]]:
        """Collect enriched bookmarks as a list of dicts."""
        await self._ensure_init()
        df = await self._world.get_components([Bookmark, KnowledgeEntry])
        if df is None:
            return []

        rows = []
        collected = df.collect().to_pylist()
        for row in collected:
            if not row.get("is_active", True):
                continue
            if row.get("knowledgeentry__is_duplicate", False):
                continue
            rows.append(
                {
                    "tweet_id": row.get("bookmark__tweet_id", ""),
                    "author": row.get("bookmark__author_username", ""),
                    "text": row.get("bookmark__text", ""),
                    "created_at": row.get("bookmark__created_at", ""),
                    "summary": row.get("knowledgeentry__summary", ""),
                    "category": row.get("knowledgeentry__category", ""),
                    "content_type": row.get("knowledgeentry__content_type", ""),
                    "topics": row.get("knowledgeentry__topics_json", "[]"),
                    "key_insights": row.get("knowledgeentry__key_insights_json", "[]"),
                    "relevance_score": row.get("knowledgeentry__relevance_score", 0.0),
                    "processed": row.get("knowledgeentry__processed", False),
                }
            )
        return rows

    async def results_df(self) -> Any:
        """Return raw Daft DataFrame of live state (for advanced queries)."""
        await self._ensure_init()
        return await self._world.get_components([Bookmark, KnowledgeEntry])

    async def search(
        self,
        query: str,
        *,
        category: str | None = None,
        min_relevance: float = 0.0,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search the knowledge base by keyword matching.

        For full semantic search, use results_df() and query LanceDB directly.

        Args:
            query:         Search string (matched against text, summary, topics)
            category:      Filter to a specific category
            min_relevance: Minimum relevance score threshold
            limit:         Max results to return
        """
        await self._ensure_init()
        df = await self._world.get_components([Bookmark, KnowledgeEntry])
        if df is None:
            return []

        # Filter: active, non-duplicate, processed
        df = df.where(col("is_active"))
        df = df.where(~col("knowledgeentry__is_duplicate"))
        df = df.where(col("knowledgeentry__processed"))

        if category:
            df = df.where(col("knowledgeentry__category") == category)

        if min_relevance > 0.0:
            df = df.where(col("knowledgeentry__relevance_score") >= min_relevance)

        # Text search: match against text, summary, and topics
        query_lower = query.lower()

        @daft.func
        def _matches_query(text: str, summary: str, topics_json: str) -> bool:
            searchable = (text + " " + summary + " " + topics_json).lower()
            return query_lower in searchable

        df = df.where(
            _matches_query(
                col("bookmark__text"),
                col("knowledgeentry__summary"),
                col("knowledgeentry__topics_json"),
            )
        )

        df = df.sort(col("knowledgeentry__relevance_score"), desc=True)
        df = df.limit(limit)

        rows = []
        for row in df.collect().to_pylist():
            rows.append(
                {
                    "tweet_id": row.get("bookmark__tweet_id", ""),
                    "author": row.get("bookmark__author_username", ""),
                    "text": row.get("bookmark__text", ""),
                    "summary": row.get("knowledgeentry__summary", ""),
                    "category": row.get("knowledgeentry__category", ""),
                    "topics": row.get("knowledgeentry__topics_json", "[]"),
                    "relevance_score": row.get("knowledgeentry__relevance_score", 0.0),
                }
            )
        return rows

    async def stats(self) -> dict[str, Any]:
        """Return summary statistics about the knowledge base."""
        await self._ensure_init()
        df = await self._world.get_components([Bookmark, KnowledgeEntry])
        if df is None:
            return {"total": 0, "processed": 0, "duplicates": 0}

        df = df.where(col("is_active"))
        collected = df.collect().to_pylist()

        total = len(collected)
        processed = sum(1 for r in collected if r.get("knowledgeentry__processed", False))
        duplicates = sum(1 for r in collected if r.get("knowledgeentry__is_duplicate", False))

        # Category breakdown
        categories: dict[str, int] = {}
        for r in collected:
            cat = r.get("knowledgeentry__category", "")
            if cat and not r.get("knowledgeentry__is_duplicate", False):
                categories[cat] = categories.get(cat, 0) + 1

        return {
            "total": total,
            "processed": processed,
            "duplicates": duplicates,
            "unique": total - duplicates,
            "categories": categories,
        }

    async def fork(self, new_name: str) -> BookmarksPipeline:
        """Fork this pipeline into a new world for experimentation.

        The forked pipeline shares the same bookmarks but can use
        different enrichment models or categories.
        """
        await self._ensure_init()
        forked = BookmarksPipeline(
            name=new_name,
            bearer_token=self._bearer_token,
            user_id=self._user_id,
            storage_uri=self._storage_uri,
            model=self._model,
            embedding_model=self._embedding_model,
            categories=self._categories,
        )
        forked._container = ServiceContainer()
        forked._owns_container = True
        forked._world = await forked._container.world_service.fork_world(
            source_world_id=self._world.world_id,
            name=new_name,
            storage_config=StorageConfig(uri=self._storage_uri, namespace="bookmarks"),
        )
        await forked._setup_processors()
        forked._initialized = True
        return forked

    async def shutdown(self) -> None:
        """Clean up. Only shuts down the container if we created it."""
        if self._container and self._owns_container:
            await self._container.shutdown()

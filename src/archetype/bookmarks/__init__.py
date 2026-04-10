# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
X Bookmarks Ingestion Pipeline
===============================

Fetch your X bookmarks, enrich them with LLM-powered topic extraction,
and build a searchable knowledge base backed by Daft catalog + lakehouse storage.

Bookmarks are entities. Processors are pipeline stages.
Fork a world to try a different enrichment model on the same data.
Storage is the version control.

Quick Start:
    >>> from archetype.bookmarks import BookmarksPipeline
    >>> pipeline = BookmarksPipeline(
    ...     name="my-knowledge-base",
    ...     bearer_token="your-x-bearer-token",
    ...     storage_uri="s3://your-bucket/bookmarks",
    ... )
    >>> await pipeline.fetch()          # Pull from X API
    >>> await pipeline.run()            # Dedup → Enrich → Index
    >>> results = await pipeline.search("machine learning")

Incremental sync:
    >>> await pipeline.fetch()          # Only new bookmarks since last run
    >>> await pipeline.run()

Fork to compare models:
    >>> fork = await pipeline.fork("gpt4-enrichment")
    >>> fork.configure(model="gpt-4o")
    >>> await fork.run()
"""

from archetype.bookmarks.client import XBookmarksClient, XClientError
from archetype.bookmarks.components import Bookmark, KnowledgeEntry, Media, UrlEntity
from archetype.bookmarks.pipeline import BookmarksPipeline
from archetype.bookmarks.processors import (
    DeduplicationProcessor,
    EnrichmentConfig,
    EnrichmentProcessor,
    IndexingConfig,
    IndexingProcessor,
)

__all__ = [
    "Bookmark",
    "KnowledgeEntry",
    "Media",
    "UrlEntity",
    "BookmarksPipeline",
    "XBookmarksClient",
    "XClientError",
    "DeduplicationProcessor",
    "EnrichmentProcessor",
    "IndexingProcessor",
    "EnrichmentConfig",
    "IndexingConfig",
]

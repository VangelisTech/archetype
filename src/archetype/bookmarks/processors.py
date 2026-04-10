# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Bookmark Processors
===================

Three pipeline stages, priority-ordered:

    DeduplicationProcessor  (priority=10) — detects duplicate bookmarks by tweet_id
    EnrichmentProcessor     (priority=20) — LLM extracts topics, summary, insights
    IndexingProcessor       (priority=30) — generates embeddings for semantic search

Each is a standard AsyncProcessor. They compose in a single tick:
one tick = dedup → enrich → index. Fork the world to try different configs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import daft
from daft import DataFrame, col

from archetype.bookmarks.components import Bookmark, KnowledgeEntry
from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.resources import Resources

# ── Resources (injected into world.resources) ──


@dataclass
class EnrichmentConfig:
    """Configures the LLM-based enrichment.

    Attributes:
        model:             LLM model to use for enrichment
        max_output_tokens: Max tokens for LLM response
        categories:        Allowed categories for classification
    """

    model: str = "gpt-5-mini"
    max_output_tokens: int = 1024
    categories: list[str] = field(
        default_factory=lambda: [
            "technology",
            "science",
            "business",
            "design",
            "culture",
            "politics",
            "health",
            "education",
            "entertainment",
            "other",
        ]
    )


@dataclass
class IndexingConfig:
    """Configures embedding generation.

    Attributes:
        model: Embedding model to use
    """

    model: str = "text-embedding-3-small"


# ── Processors ──


class DeduplicationProcessor(AsyncProcessor):
    """Detects duplicate bookmarks by tweet_id.

    Marks knowledgeentry__is_duplicate = True for bookmarks whose tweet_id
    appears more than once. Keeps the earliest entity (lowest entity_id).
    Never drops rows — all entities are preserved.
    """

    components = (Bookmark, KnowledgeEntry)
    priority = 10

    async def process(
        self, df: DataFrame, resources: Resources | None = None, **kwargs: Any
    ) -> DataFrame:
        # Mark all rows as dedup-checked
        df = df.with_columns({"knowledgeentry__deduplicated": daft.lit(True)})

        # Self-join dedup: mark rows where the same tweet_id appears
        # with a higher entity_id as duplicates.
        # We use a row-wise UDF closed over collected tweet_id->min_entity_id mapping.
        # Justified collect: cross-row visibility needed for dedup (LEARNINGS.md §4).
        id_rows = (
            df.select("bookmark__tweet_id", "entity_id")
            .where(col("bookmark__tweet_id") != "")
            .collect()
            .to_pylist()
        )

        first_seen: dict[str, int] = {}
        for row in id_rows:
            tid = row["bookmark__tweet_id"]
            eid = row["entity_id"]
            if tid not in first_seen or eid < first_seen[tid]:
                first_seen[tid] = eid

        @daft.func
        def _check_duplicate(tweet_id: str, entity_id: int) -> bool:
            if not tweet_id:
                return False
            min_eid = first_seen.get(tweet_id)
            return min_eid is not None and entity_id != min_eid

        df = df.with_columns(
            {
                "knowledgeentry__is_duplicate": _check_duplicate(
                    col("bookmark__tweet_id"), col("entity_id")
                ),
            }
        )

        return df


class EnrichmentProcessor(AsyncProcessor):
    """Enriches bookmarks with LLM-extracted topics, summary, and categorization.

    Reads bookmark__text (the tweet content) plus context (URLs, metrics, author),
    then calls an LLM to produce:
        - knowledgeentry__summary:           concise summary
        - knowledgeentry__topics_json:       JSON array of topic strings
        - knowledgeentry__key_insights_json: JSON array of key insight strings
        - knowledgeentry__category:          top-level category
        - knowledgeentry__content_type:      type of content
        - knowledgeentry__relevance_score:   0.0-1.0 relevance

    Only processes bookmarks where:
        - knowledgeentry__processed is False
        - knowledgeentry__is_duplicate is False
    """

    components = (Bookmark, KnowledgeEntry)
    priority = 20

    async def process(
        self, df: DataFrame, resources: Resources | None = None, **kwargs: Any
    ) -> DataFrame:
        resources = resources or kwargs.get("resources") or Resources()
        config = resources.get(EnrichmentConfig) or EnrichmentConfig()

        from daft.functions import prompt

        # Split: only enrich unprocessed, non-duplicate bookmarks
        needs_enrichment = df.where(
            ~col("knowledgeentry__processed") & ~col("knowledgeentry__is_duplicate")
        )
        already_done = df.where(
            col("knowledgeentry__processed") | col("knowledgeentry__is_duplicate")
        )

        categories_str = ", ".join(config.categories)

        enrich_prompt = (
            "You are a knowledge-base curator analyzing bookmarked content from X (Twitter).\n\n"
            "## Content\n"
            "Author: @"
            + col("bookmark__author_username")
            + " ("
            + col("bookmark__author_name")
            + ")\n"
            "Text: " + col("bookmark__text") + "\n"
            "URLs: " + col("bookmark__urls_json") + "\n"
            "Metrics: " + col("bookmark__metrics_json") + "\n"
            "Language: " + col("bookmark__lang") + "\n\n"
            "## Instructions\n"
            "Analyze this bookmarked content and extract structured knowledge.\n"
            "Respond in EXACTLY this format (no other text):\n"
            "SUMMARY: <1-2 sentence summary of the content and why it matters>\n"
            f"CATEGORY: <one of: {categories_str}>\n"
            "CONTENT_TYPE: <one of: thread, article, opinion, tutorial, announcement, "
            "discussion, resource, meme, other>\n"
            "TOPICS: <comma-separated list of 2-5 specific topics>\n"
            "INSIGHTS: <comma-separated list of 1-3 key takeaways>\n"
            "RELEVANCE: <float 0.0 to 1.0 — how valuable is this for a knowledge base>"
        )

        llm_output = prompt(
            enrich_prompt,
            model=config.model,
            max_output_tokens=config.max_output_tokens,
        )

        @daft.func
        def extract_summary(response: str) -> str:
            if not response:
                return ""
            for line in response.split("\n"):
                if line.startswith("SUMMARY:"):
                    return line[8:].strip()
            return ""

        @daft.func
        def extract_category(response: str) -> str:
            if not response:
                return "other"
            for line in response.split("\n"):
                if line.startswith("CATEGORY:"):
                    return line[9:].strip().lower()
            return "other"

        @daft.func
        def extract_content_type(response: str) -> str:
            if not response:
                return "other"
            for line in response.split("\n"):
                if line.startswith("CONTENT_TYPE:"):
                    return line[13:].strip().lower()
            return "other"

        @daft.func
        def extract_topics(response: str) -> str:
            if not response:
                return "[]"
            for line in response.split("\n"):
                if line.startswith("TOPICS:"):
                    topics = [t.strip() for t in line[7:].split(",") if t.strip()]
                    return json.dumps(topics)
            return "[]"

        @daft.func
        def extract_insights(response: str) -> str:
            if not response:
                return "[]"
            for line in response.split("\n"):
                if line.startswith("INSIGHTS:"):
                    insights = [i.strip() for i in line[9:].split(",") if i.strip()]
                    return json.dumps(insights)
            return "[]"

        @daft.func
        def extract_relevance(response: str) -> float:
            if not response:
                return 0.0
            for line in response.split("\n"):
                if line.startswith("RELEVANCE:"):
                    try:
                        return float(line[10:].strip())
                    except ValueError:
                        return 0.0
            return 0.0

        llm_col = llm_output

        needs_enrichment = needs_enrichment.with_columns(
            {
                "knowledgeentry__summary": extract_summary(llm_col),
                "knowledgeentry__category": extract_category(llm_col),
                "knowledgeentry__content_type": extract_content_type(llm_col),
                "knowledgeentry__topics_json": extract_topics(llm_col),
                "knowledgeentry__key_insights_json": extract_insights(llm_col),
                "knowledgeentry__relevance_score": extract_relevance(llm_col),
                "knowledgeentry__processed": daft.lit(True),
            }
        )

        return needs_enrichment.concat(already_done)


class IndexingProcessor(AsyncProcessor):
    """Generates embeddings for semantic search over the knowledge base.

    Combines bookmark text + enrichment summary into a search-friendly
    document, then generates an embedding vector. Only processes enriched,
    non-duplicate bookmarks.

    Embeds into knowledgeentry__summary field (the semantic handle).
    The embedding itself is stored by LanceDB's native vector indexing
    when the world persists — this processor ensures the summary field
    is populated with a clean, searchable document.
    """

    components = (Bookmark, KnowledgeEntry)
    priority = 30

    async def process(
        self, df: DataFrame, resources: Resources | None = None, **kwargs: Any
    ) -> DataFrame:
        # Build a clean search document from enriched data for downstream
        # LanceDB FTS / vector search. Only for processed, non-duplicate rows.
        processed = df.where(
            col("knowledgeentry__processed") & ~col("knowledgeentry__is_duplicate")
        )
        rest = df.where(~col("knowledgeentry__processed") | col("knowledgeentry__is_duplicate"))

        @daft.func
        def build_search_doc(text: str, summary: str, topics_json: str, author: str) -> str:
            """Compose a clean document for search indexing."""
            parts = []
            if author:
                parts.append(f"@{author}")
            if summary:
                parts.append(summary)
            elif text:
                parts.append(text)
            try:
                topics = json.loads(topics_json)
                if topics:
                    parts.append("Topics: " + ", ".join(topics))
            except (json.JSONDecodeError, TypeError):
                pass
            return " | ".join(parts) if parts else text

        # Overwrite summary with the richer search document
        processed = processed.with_columns(
            {
                "knowledgeentry__summary": build_search_doc(
                    col("bookmark__text"),
                    col("knowledgeentry__summary"),
                    col("knowledgeentry__topics_json"),
                    col("bookmark__author_username"),
                ),
            }
        )

        return processed.concat(rest)

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Bookmark Components
===================

Bookmark:       Raw X bookmark data — tweet content, author, media, metrics.
KnowledgeEntry: Enriched knowledge base entry — topics, summary, insights.

Both are Arrow-serializable. Complex nested data (media, URLs, metrics)
is stored as JSON strings for LanceDB compatibility.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from archetype.core.component import Component


@dataclass
class Media:
    """A media attachment on a bookmarked tweet. Not a Component — helper dataclass."""

    media_key: str = ""
    type: str = ""  # "photo", "video", "animated_gif"
    url: str = ""
    alt_text: str = ""
    duration_ms: int = 0
    width: int = 0
    height: int = 0

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"media_key": self.media_key, "type": self.type, "url": self.url}
        if self.alt_text:
            d["alt_text"] = self.alt_text
        if self.duration_ms:
            d["duration_ms"] = self.duration_ms
        if self.width:
            d["width"] = self.width
        if self.height:
            d["height"] = self.height
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Media:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class UrlEntity:
    """A URL entity extracted from a tweet. Not a Component — helper dataclass."""

    url: str = ""
    expanded_url: str = ""
    display_url: str = ""
    title: str = ""
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"url": self.url, "expanded_url": self.expanded_url}
        if self.display_url:
            d["display_url"] = self.display_url
        if self.title:
            d["title"] = self.title
        if self.description:
            d["description"] = self.description
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> UrlEntity:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class Bookmark(Component):
    """A bookmarked tweet from X.

    Stores the full tweet data — content, author, media, metrics, and threading
    info — as Arrow-serializable fields. Complex nested data uses JSON strings.

    Fields:
        tweet_id:               X's unique tweet ID
        author_id:              Author's X user ID
        author_username:        Author handle (without @)
        author_name:            Display name
        text:                   Tweet text content
        created_at:             When the tweet was posted (ISO 8601)
        bookmarked_at:          When the user bookmarked it (ISO 8601)
        lang:                   Language code (e.g., "en")
        conversation_id:        Thread conversation ID
        in_reply_to_user_id:    User ID this tweet replies to (if any)
        media_json:             JSON array of Media dicts
        urls_json:              JSON array of UrlEntity dicts
        metrics_json:           JSON dict of engagement metrics
        referenced_tweets_json: JSON array of {type, id} for quotes/replies
        context_annotations_json: JSON array of X's topic annotations
        tags_json:              JSON array of user-defined string tags
    """

    tweet_id: str = ""
    author_id: str = ""
    author_username: str = ""
    author_name: str = ""
    text: str = ""
    created_at: str = ""
    bookmarked_at: str = ""
    lang: str = ""
    conversation_id: str = ""
    in_reply_to_user_id: str = ""
    media_json: str = "[]"
    urls_json: str = "[]"
    metrics_json: str = "{}"
    referenced_tweets_json: str = "[]"
    context_annotations_json: str = "[]"
    tags_json: str = "[]"

    @classmethod
    def from_api_response(
        cls,
        tweet: dict[str, Any],
        *,
        includes: dict[str, Any] | None = None,
        bookmarked_at: str = "",
    ) -> Bookmark:
        """Build a Bookmark from an X API v2 tweet object + includes."""
        includes = includes or {}

        # Extract author from includes
        users_by_id = {u["id"]: u for u in includes.get("users", [])}
        author = users_by_id.get(tweet.get("author_id", ""), {})

        # Extract media from includes
        media_by_key = {m["media_key"]: m for m in includes.get("media", [])}
        media_list = []
        for key in tweet.get("attachments", {}).get("media_keys", []):
            if key in media_by_key:
                m = media_by_key[key]
                media_list.append(
                    Media(
                        media_key=key,
                        type=m.get("type", ""),
                        url=m.get("url", m.get("preview_image_url", "")),
                        alt_text=m.get("alt_text", ""),
                        duration_ms=m.get("duration_ms", 0),
                        width=m.get("width", 0),
                        height=m.get("height", 0),
                    ).to_dict()
                )

        # Extract URLs from entities
        url_entities = []
        for u in tweet.get("entities", {}).get("urls", []):
            url_entities.append(
                UrlEntity(
                    url=u.get("url", ""),
                    expanded_url=u.get("expanded_url", ""),
                    display_url=u.get("display_url", ""),
                    title=u.get("title", ""),
                    description=u.get("description", ""),
                ).to_dict()
            )

        # Public metrics
        metrics = tweet.get("public_metrics", {})

        # Referenced tweets (quotes, replies)
        refs = [
            {"type": r.get("type", ""), "id": r.get("id", "")}
            for r in tweet.get("referenced_tweets", [])
        ]

        # Context annotations (X's topic system)
        annotations = tweet.get("context_annotations", [])

        if not bookmarked_at:
            bookmarked_at = datetime.now().isoformat()

        return cls(
            tweet_id=tweet.get("id", ""),
            author_id=tweet.get("author_id", ""),
            author_username=author.get("username", ""),
            author_name=author.get("name", ""),
            text=tweet.get("text", ""),
            created_at=tweet.get("created_at", ""),
            bookmarked_at=bookmarked_at,
            lang=tweet.get("lang", ""),
            conversation_id=tweet.get("conversation_id", ""),
            in_reply_to_user_id=tweet.get("in_reply_to_user_id", ""),
            media_json=json.dumps(media_list),
            urls_json=json.dumps(url_entities),
            metrics_json=json.dumps(metrics),
            referenced_tweets_json=json.dumps(refs),
            context_annotations_json=json.dumps(annotations),
        )

    def get_media(self) -> list[Media]:
        """Deserialize media JSON back to Media objects."""
        return [Media.from_dict(d) for d in json.loads(self.media_json)]

    def get_urls(self) -> list[UrlEntity]:
        """Deserialize URLs JSON back to UrlEntity objects."""
        return [UrlEntity.from_dict(d) for d in json.loads(self.urls_json)]

    def get_metrics(self) -> dict[str, int]:
        """Deserialize metrics JSON."""
        return json.loads(self.metrics_json)


class KnowledgeEntry(Component):
    """An enriched knowledge base entry derived from a bookmark.

    Produced by the EnrichmentProcessor. Stores LLM-extracted topics,
    summary, and categorization for building a personal knowledge base.

    Fields:
        summary:            LLM-generated summary of the bookmark content
        topics_json:        JSON array of extracted topic strings
        key_insights_json:  JSON array of key takeaway strings
        category:           Top-level category (tech, science, business, etc.)
        relevance_score:    0.0-1.0 relevance to user's interests
        content_type:       Type of content (thread, article, opinion, tutorial, etc.)
        processed:          Whether this bookmark has been enriched
        deduplicated:       Whether dedup check has been performed
        is_duplicate:       True if this is a duplicate of an existing bookmark
    """

    summary: str = ""
    topics_json: str = "[]"
    key_insights_json: str = "[]"
    category: str = ""
    relevance_score: float = 0.0
    content_type: str = ""
    processed: bool = False
    deduplicated: bool = False
    is_duplicate: bool = False

    def get_topics(self) -> list[str]:
        """Deserialize topics JSON."""
        return json.loads(self.topics_json)

    def get_key_insights(self) -> list[str]:
        """Deserialize key insights JSON."""
        return json.loads(self.key_insights_json)

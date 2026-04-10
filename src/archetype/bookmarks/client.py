# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
X Bookmarks Client
==================

Async client for fetching bookmarks from the X API v2.

Requires OAuth 2.0 User Context with ``bookmark.read`` scope, or a Bearer
Token with appropriate access. Handles pagination automatically.

Usage::

    async with XBookmarksClient(bearer_token="xox-...") as client:
        bookmarks = await client.fetch_all()
        # bookmarks is a list[Bookmark]
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from archetype.bookmarks.components import Bookmark

logger = logging.getLogger("archetype.bookmarks")

X_API_BASE = "https://api.x.com/2"

# Fields to request from the X API v2
TWEET_FIELDS = [
    "id",
    "text",
    "author_id",
    "created_at",
    "lang",
    "conversation_id",
    "in_reply_to_user_id",
    "public_metrics",
    "entities",
    "context_annotations",
    "referenced_tweets",
    "attachments",
]

USER_FIELDS = ["id", "name", "username", "profile_image_url"]
MEDIA_FIELDS = ["media_key", "type", "url", "preview_image_url", "alt_text", "width", "height"]
EXPANSIONS = ["author_id", "attachments.media_keys", "referenced_tweets.id"]


class XClientError(Exception):
    """Raised when the X API returns an error."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"X API error {status_code}: {detail}")


class XBookmarksClient:
    """Async client for the X API v2 bookmarks endpoint.

    Args:
        bearer_token: OAuth 2.0 Bearer Token with bookmark.read scope.
        user_id:      Your X user ID. If not provided, fetched via /2/users/me.
        max_results:  Tweets per page (X API max is 100).
        max_pages:    Safety limit on pagination (0 = unlimited).
    """

    def __init__(
        self,
        bearer_token: str,
        *,
        user_id: str | None = None,
        max_results: int = 100,
        max_pages: int = 50,
    ):
        self._bearer_token = bearer_token
        self._user_id = user_id
        self._max_results = min(max_results, 100)
        self._max_pages = max_pages
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> XBookmarksClient:
        self._client = httpx.AsyncClient(
            base_url=X_API_BASE,
            headers={"Authorization": f"Bearer {self._bearer_token}"},
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("XBookmarksClient must be used as an async context manager")
        return self._client

    async def _resolve_user_id(self) -> str:
        """Resolve the authenticated user's ID via /2/users/me."""
        if self._user_id:
            return self._user_id
        client = self._ensure_client()
        resp = await client.get("/users/me")
        if resp.status_code != 200:
            raise XClientError(resp.status_code, resp.text)
        self._user_id = resp.json()["data"]["id"]
        return self._user_id

    async def fetch_all(
        self,
        *,
        since_id: str | None = None,
    ) -> list[Bookmark]:
        """Fetch all bookmarks, paginating automatically.

        Args:
            since_id: Only fetch bookmarks newer than this tweet ID (for incremental sync).

        Returns:
            List of Bookmark components, newest first.
        """
        client = self._ensure_client()
        user_id = await self._resolve_user_id()
        now = datetime.now().isoformat()

        bookmarks: list[Bookmark] = []
        pagination_token: str | None = None
        pages = 0

        while True:
            params: dict[str, str] = {
                "max_results": str(self._max_results),
                "tweet.fields": ",".join(TWEET_FIELDS),
                "user.fields": ",".join(USER_FIELDS),
                "media.fields": ",".join(MEDIA_FIELDS),
                "expansions": ",".join(EXPANSIONS),
            }
            if pagination_token:
                params["pagination_token"] = pagination_token

            resp = await client.get(f"/users/{user_id}/bookmarks", params=params)

            if resp.status_code != 200:
                raise XClientError(resp.status_code, resp.text)

            body = resp.json()
            tweets = body.get("data", [])
            includes = body.get("includes", {})

            if not tweets:
                break

            for tweet in tweets:
                # Stop if we've reached a previously-seen bookmark
                if since_id and tweet.get("id") == since_id:
                    return bookmarks

                bookmarks.append(
                    Bookmark.from_api_response(tweet, includes=includes, bookmarked_at=now)
                )

            # Pagination
            meta = body.get("meta", {})
            pagination_token = meta.get("next_token")
            pages += 1

            if not pagination_token:
                break
            if self._max_pages and pages >= self._max_pages:
                logger.warning("Hit max_pages limit (%d), stopping pagination", self._max_pages)
                break

        logger.info("Fetched %d bookmarks across %d pages", len(bookmarks), pages)
        return bookmarks

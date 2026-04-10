# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for X bookmarks client — mocked HTTP."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from bookmarks.client import XBookmarksClient, XClientError


def _mock_response(status_code: int, body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


@pytest.mark.asyncio
async def test_fetch_all_single_page():
    """Fetch bookmarks from a single page response."""
    tweet = {
        "id": "123",
        "author_id": "u1",
        "text": "Hello world",
        "created_at": "2026-01-01T00:00:00Z",
        "lang": "en",
    }
    api_response = _mock_response(
        200,
        {
            "data": [tweet],
            "includes": {"users": [{"id": "u1", "username": "testuser", "name": "Test User"}]},
            "meta": {"result_count": 1},
        },
    )

    client = XBookmarksClient(bearer_token="test-token", user_id="me123")
    client._client = AsyncMock()
    client._client.get = AsyncMock(return_value=api_response)

    bookmarks = await client.fetch_all()

    assert len(bookmarks) == 1
    assert bookmarks[0].tweet_id == "123"
    assert bookmarks[0].author_username == "testuser"
    assert bookmarks[0].text == "Hello world"


@pytest.mark.asyncio
async def test_fetch_all_pagination():
    """Fetch bookmarks across multiple pages."""
    page1 = _mock_response(
        200,
        {
            "data": [{"id": "1", "text": "first", "author_id": "u1"}],
            "includes": {"users": [{"id": "u1", "username": "a", "name": "A"}]},
            "meta": {"result_count": 1, "next_token": "page2"},
        },
    )
    page2 = _mock_response(
        200,
        {
            "data": [{"id": "2", "text": "second", "author_id": "u1"}],
            "includes": {"users": [{"id": "u1", "username": "a", "name": "A"}]},
            "meta": {"result_count": 1},
        },
    )

    client = XBookmarksClient(bearer_token="test-token", user_id="me123")
    client._client = AsyncMock()
    client._client.get = AsyncMock(side_effect=[page1, page2])

    bookmarks = await client.fetch_all()

    assert len(bookmarks) == 2
    assert bookmarks[0].tweet_id == "1"
    assert bookmarks[1].tweet_id == "2"


@pytest.mark.asyncio
async def test_fetch_all_since_id():
    """Incremental fetch stops at since_id."""
    page = _mock_response(
        200,
        {
            "data": [
                {"id": "3", "text": "new", "author_id": "u1"},
                {"id": "2", "text": "seen", "author_id": "u1"},
            ],
            "includes": {"users": [{"id": "u1", "username": "a", "name": "A"}]},
            "meta": {"result_count": 2},
        },
    )

    client = XBookmarksClient(bearer_token="test-token", user_id="me123")
    client._client = AsyncMock()
    client._client.get = AsyncMock(return_value=page)

    bookmarks = await client.fetch_all(since_id="2")

    assert len(bookmarks) == 1
    assert bookmarks[0].tweet_id == "3"


@pytest.mark.asyncio
async def test_fetch_all_empty():
    """Empty response returns empty list."""
    page = _mock_response(200, {"data": [], "meta": {"result_count": 0}})

    client = XBookmarksClient(bearer_token="test-token", user_id="me123")
    client._client = AsyncMock()
    client._client.get = AsyncMock(return_value=page)

    bookmarks = await client.fetch_all()
    assert bookmarks == []


@pytest.mark.asyncio
async def test_fetch_all_api_error():
    """API error raises XClientError."""
    error_resp = _mock_response(401, {"error": "Unauthorized"})

    client = XBookmarksClient(bearer_token="bad-token", user_id="me123")
    client._client = AsyncMock()
    client._client.get = AsyncMock(return_value=error_resp)

    with pytest.raises(XClientError) as exc_info:
        await client.fetch_all()
    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_context_manager_required():
    """Using the client without context manager should raise."""
    client = XBookmarksClient(bearer_token="test")
    with pytest.raises(RuntimeError, match="context manager"):
        client._ensure_client()


@pytest.mark.asyncio
async def test_resolve_user_id():
    """User ID should be resolved from /users/me if not provided."""
    me_resp = _mock_response(200, {"data": {"id": "resolved_123"}})
    bookmarks_resp = _mock_response(200, {"data": [], "meta": {}})

    client = XBookmarksClient(bearer_token="test-token")
    client._client = AsyncMock()
    client._client.get = AsyncMock(side_effect=[me_resp, bookmarks_resp])

    bookmarks = await client.fetch_all()

    assert client._user_id == "resolved_123"
    assert bookmarks == []

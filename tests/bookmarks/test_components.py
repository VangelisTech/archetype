# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for bookmark components."""

import json

from archetype.bookmarks.components import Bookmark, KnowledgeEntry, Media, UrlEntity


def test_media_round_trip():
    media = Media(media_key="mk_1", type="photo", url="https://example.com/img.jpg", width=800)
    d = media.to_dict()
    assert d["media_key"] == "mk_1"
    assert d["type"] == "photo"
    assert d["width"] == 800
    assert "height" not in d  # zero-valued optional omitted
    restored = Media.from_dict(d)
    assert restored.media_key == "mk_1"
    assert restored.url == "https://example.com/img.jpg"


def test_url_entity_round_trip():
    url = UrlEntity(
        url="https://t.co/abc",
        expanded_url="https://example.com/article",
        title="Great Article",
    )
    d = url.to_dict()
    assert d["expanded_url"] == "https://example.com/article"
    assert d["title"] == "Great Article"
    restored = UrlEntity.from_dict(d)
    assert restored.expanded_url == "https://example.com/article"


def test_bookmark_from_api_response():
    tweet = {
        "id": "123456",
        "author_id": "user_1",
        "text": "Daft DataFrames are awesome!",
        "created_at": "2026-01-15T10:00:00Z",
        "lang": "en",
        "conversation_id": "123456",
        "public_metrics": {"like_count": 42, "retweet_count": 7},
        "entities": {
            "urls": [
                {
                    "url": "https://t.co/abc",
                    "expanded_url": "https://getdaft.io",
                    "title": "Daft",
                }
            ]
        },
        "attachments": {"media_keys": ["mk_1"]},
        "referenced_tweets": [{"type": "quoted", "id": "111"}],
        "context_annotations": [{"domain": {"name": "Technology"}}],
    }
    includes = {
        "users": [{"id": "user_1", "username": "daftdev", "name": "Daft Dev"}],
        "media": [
            {
                "media_key": "mk_1",
                "type": "photo",
                "url": "https://example.com/img.jpg",
                "width": 1200,
                "height": 600,
            }
        ],
    }

    bookmark = Bookmark.from_api_response(
        tweet, includes=includes, bookmarked_at="2026-04-10T12:00:00"
    )
    assert bookmark.tweet_id == "123456"
    assert bookmark.author_username == "daftdev"
    assert bookmark.author_name == "Daft Dev"
    assert bookmark.text == "Daft DataFrames are awesome!"
    assert bookmark.lang == "en"
    assert bookmark.bookmarked_at == "2026-04-10T12:00:00"

    # Media deserialized correctly
    media = bookmark.get_media()
    assert len(media) == 1
    assert media[0].type == "photo"
    assert media[0].width == 1200

    # URLs deserialized correctly
    urls = bookmark.get_urls()
    assert len(urls) == 1
    assert urls[0].expanded_url == "https://getdaft.io"

    # Metrics
    metrics = bookmark.get_metrics()
    assert metrics["like_count"] == 42

    # Referenced tweets
    refs = json.loads(bookmark.referenced_tweets_json)
    assert refs[0]["type"] == "quoted"

    # Context annotations
    annotations = json.loads(bookmark.context_annotations_json)
    assert len(annotations) == 1


def test_bookmark_from_api_response_minimal():
    """Minimal tweet with no includes."""
    tweet = {"id": "999", "text": "hello world"}
    bookmark = Bookmark.from_api_response(tweet)
    assert bookmark.tweet_id == "999"
    assert bookmark.text == "hello world"
    assert bookmark.author_username == ""
    assert bookmark.get_media() == []
    assert bookmark.get_urls() == []
    assert bookmark.bookmarked_at != ""  # Should auto-fill


def test_bookmark_json_suffix_fields():
    """Verify JSON-encoded fields follow the _json suffix convention."""
    b = Bookmark()
    for field_name in [
        "media_json",
        "urls_json",
        "metrics_json",
        "referenced_tweets_json",
        "context_annotations_json",
        "tags_json",
    ]:
        assert hasattr(b, field_name)


def test_knowledge_entry_defaults():
    entry = KnowledgeEntry()
    assert entry.processed is False
    assert entry.deduplicated is False
    assert entry.is_duplicate is False
    assert entry.relevance_score == 0.0
    assert entry.category == ""
    assert entry.get_topics() == []
    assert entry.get_key_insights() == []


def test_knowledge_entry_json_accessors():
    entry = KnowledgeEntry(
        topics_json=json.dumps(["python", "daft", "ecs"]),
        key_insights_json=json.dumps(["Use expressions first", "Lazy evaluation"]),
    )
    assert entry.get_topics() == ["python", "daft", "ecs"]
    assert len(entry.get_key_insights()) == 2


def test_bookmark_component_prefix():
    """Component prefix should be 'bookmark__'."""
    assert Bookmark.get_prefix() == "bookmark__"


def test_knowledge_entry_component_prefix():
    """Component prefix should be 'knowledgeentry__'."""
    assert KnowledgeEntry.get_prefix() == "knowledgeentry__"


def test_bookmark_arrow_schema():
    """Bookmark should produce a valid PyArrow schema."""
    schema = Bookmark.to_pyarrow_schema()
    field_names = schema.names
    assert "tweet_id" in field_names
    assert "text" in field_names
    assert "media_json" in field_names


def test_knowledge_entry_arrow_schema():
    """KnowledgeEntry should produce a valid PyArrow schema."""
    schema = KnowledgeEntry.to_pyarrow_schema()
    field_names = schema.names
    assert "summary" in field_names
    assert "topics_json" in field_names
    assert "processed" in field_names

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for chronicle components."""

from __future__ import annotations

import json

from archetype.chronicle.components import Artifact, ChatMessage, Photo, Video


class TestArtifact:
    def test_make_sets_ingested_at(self):
        a = Artifact.make(kind="photo", source="folder", source_id="abc")
        assert a.kind == "photo"
        assert a.source == "folder"
        assert a.source_id == "abc"
        assert a.ingested_at_ms > 0

    def test_make_preserves_created_at(self):
        a = Artifact.make(
            kind="chat_message",
            source="claude_code",
            source_id="s1",
            created_at_ms=1000,
        )
        assert a.created_at_ms == 1000

    def test_arrow_serializable(self):
        Artifact.make(kind="video", source="apple_photos", source_id="v1")
        schema = Artifact.to_pyarrow_schema()
        assert "kind" in schema.names
        assert "created_at_ms" in schema.names


class TestChatMessage:
    def test_make_computes_totals(self):
        turns = [
            {"role": "user", "text": "Hello", "tokens_in": 10, "tokens_out": 0},
            {"role": "assistant", "text": "Hi there", "tokens_in": 0, "tokens_out": 20},
        ]
        cm = ChatMessage.make("conv-1", turns, title="Hello")
        assert cm.total_turns == 2
        assert cm.total_tokens_in == 10
        assert cm.total_tokens_out == 20
        assert "[user] Hello" in cm.content
        assert "[assistant] Hi there" in cm.content

    def test_get_turns_roundtrips(self):
        turns = [{"role": "user", "text": "test", "tokens_in": 0, "tokens_out": 0}]
        cm = ChatMessage.make("c1", turns)
        parsed = cm.get_turns()
        assert len(parsed) == 1
        assert parsed[0]["role"] == "user"

    def test_empty_turns_yields_empty_content(self):
        cm = ChatMessage.make("c2", [])
        assert cm.total_turns == 0
        assert cm.content == ""

    def test_arrow_serializable(self):
        schema = ChatMessage.to_pyarrow_schema()
        assert "conversation_id" in schema.names
        assert "turns_json" in schema.names


class TestPhoto:
    def test_defaults(self):
        p = Photo()
        assert p.width == 0
        assert p.gps_lat == 0.0
        assert p.favorite is False
        assert p.get_album_names() == []
        assert p.get_persons() == []

    def test_json_fields(self):
        p = Photo(
            album_names_json=json.dumps(["vacation", "2024"]),
            persons_json=json.dumps(["Alice"]),
        )
        assert p.get_album_names() == ["vacation", "2024"]
        assert p.get_persons() == ["Alice"]

    def test_arrow_serializable(self):
        schema = Photo.to_pyarrow_schema()
        assert "path" in schema.names
        assert "gps_lat" in schema.names


class TestVideo:
    def test_defaults(self):
        v = Video()
        assert v.duration_ms == 0
        assert v.get_album_names() == []

    def test_arrow_serializable(self):
        schema = Video.to_pyarrow_schema()
        assert "duration_ms" in schema.names

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Chronicle Components
====================

Artifact, ChatMessage, Photo, Video: the ECS schema for a personal
archive.  Every entity carries an ``Artifact`` component for
cross-kind timeline queries.  The second component determines the
artifact kind:

    (Artifact, ChatMessage)  — one per conversation / session
    (Artifact, Photo)        — one per image file
    (Artifact, Video)        — one per video file

Arrow serialization:
    - Scalar fields (str/int/float/bool) stored directly
    - Complex types (lists, dicts) JSON-encoded into ``*_json`` fields
    - Timestamps stored as Unix milliseconds (``*_at_ms``)
"""

from __future__ import annotations

import json
import time
from typing import Any

from archetype.core.component import Component


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Artifact — universal provenance, every entity gets one
# ---------------------------------------------------------------------------


class Artifact(Component):
    """Universal provenance envelope for every chronicled item.

    Fields:
        kind:           "chat_message", "photo", or "video"
        source:         Origin system ("claude_code", "claude_ai", "chatgpt",
                        "apple_photos", "folder")
        source_id:      External id (session UUID, photo UUID, etc.)
        created_at_ms:  Original creation timestamp (Unix ms)
        ingested_at_ms: When the item was chronicled (Unix ms)
        uri:            Canonical locator (file path or synthetic URI)
    """

    kind: str = ""
    source: str = ""
    source_id: str = ""
    created_at_ms: int = 0
    ingested_at_ms: int = 0
    uri: str = ""

    @classmethod
    def make(
        cls,
        kind: str,
        source: str,
        source_id: str,
        *,
        created_at_ms: int = 0,
        uri: str = "",
    ) -> Artifact:
        return cls(
            kind=kind,
            source=source,
            source_id=source_id,
            created_at_ms=created_at_ms,
            ingested_at_ms=_now_ms(),
            uri=uri,
        )


# ---------------------------------------------------------------------------
# ChatMessage — one per conversation / session
# ---------------------------------------------------------------------------


class ChatMessage(Component):
    """A complete conversation with an AI agent.

    Stores the full history as a JSON list of turns for structured
    access, plus concatenated text for full-text search.

    Fields:
        conversation_id: Session or conversation identifier
        title:           Conversation title (first user message, truncated)
        model:           Primary model used (e.g. "claude-opus-4-6")
        total_turns:     Number of user + assistant turns
        total_tokens_in: Aggregate input tokens
        total_tokens_out: Aggregate output tokens
        content:         Concatenated user+assistant text (searchable)
        turns_json:      JSON list of turn dicts for structured replay
        cwd:             Working directory (Claude Code sessions)
        git_branch:      Git branch (Claude Code sessions)
        metadata_json:   Free-form session metadata (JSON dict)
    """

    conversation_id: str = ""
    title: str = ""
    model: str = ""
    total_turns: int = 0
    total_tokens_in: int = 0
    total_tokens_out: int = 0
    content: str = ""
    turns_json: str = "[]"
    cwd: str = ""
    git_branch: str = ""
    metadata_json: str = "{}"

    @classmethod
    def make(
        cls,
        conversation_id: str,
        turns: list[dict[str, Any]],
        *,
        title: str = "",
        model: str = "",
        cwd: str = "",
        git_branch: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> ChatMessage:
        total_in = sum(t.get("tokens_in", 0) for t in turns)
        total_out = sum(t.get("tokens_out", 0) for t in turns)
        content_parts: list[str] = []
        for t in turns:
            role = t.get("role", "")
            text = t.get("text", "")
            if text:
                content_parts.append(f"[{role}] {text}")
        return cls(
            conversation_id=conversation_id,
            title=title,
            model=model,
            total_turns=len(turns),
            total_tokens_in=total_in,
            total_tokens_out=total_out,
            content="\n\n".join(content_parts),
            turns_json=json.dumps(turns),
            cwd=cwd,
            git_branch=git_branch,
            metadata_json=json.dumps(metadata or {}),
        )

    def get_turns(self) -> list[dict[str, Any]]:
        return json.loads(self.turns_json)


# ---------------------------------------------------------------------------
# Photo
# ---------------------------------------------------------------------------


class Photo(Component):
    """A single photograph.

    Fields:
        path:           Absolute file path (resolved)
        width:          Image width in pixels
        height:         Image height in pixels
        mime:           MIME type (e.g. "image/jpeg")
        file_size:      File size in bytes
        gps_lat:        GPS latitude (0.0 if unknown)
        gps_lon:        GPS longitude (0.0 if unknown)
        camera_make:    Camera manufacturer
        camera_model:   Camera model
        favorite:       Marked as favorite in source library
        hidden:         Hidden in source library
        album_names_json: JSON list of album memberships
        persons_json:   JSON list of recognized person names
    """

    path: str = ""
    width: int = 0
    height: int = 0
    mime: str = ""
    file_size: int = 0
    gps_lat: float = 0.0
    gps_lon: float = 0.0
    camera_make: str = ""
    camera_model: str = ""
    favorite: bool = False
    hidden: bool = False
    album_names_json: str = "[]"
    persons_json: str = "[]"

    def get_album_names(self) -> list[str]:
        return json.loads(self.album_names_json)

    def get_persons(self) -> list[str]:
        return json.loads(self.persons_json)


# ---------------------------------------------------------------------------
# Video
# ---------------------------------------------------------------------------


class Video(Component):
    """A single video file.

    Fields:
        path:           Absolute file path (resolved)
        width:          Frame width in pixels
        height:         Frame height in pixels
        duration_ms:    Duration in milliseconds
        mime:           MIME type (e.g. "video/mp4")
        file_size:      File size in bytes
        gps_lat:        GPS latitude (0.0 if unknown)
        gps_lon:        GPS longitude (0.0 if unknown)
        album_names_json: JSON list of album memberships
    """

    path: str = ""
    width: int = 0
    height: int = 0
    duration_ms: int = 0
    mime: str = ""
    file_size: int = 0
    gps_lat: float = 0.0
    gps_lon: float = 0.0
    album_names_json: str = "[]"

    def get_album_names(self) -> list[str]:
        return json.loads(self.album_names_json)

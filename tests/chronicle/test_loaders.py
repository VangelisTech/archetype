# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for chronicle loaders.

Unit tests use small on-disk fixtures.
Dogfood tests (marked skipif) run against real data on the developer's machine.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archetype.chronicle.components import Artifact, ChatMessage
from archetype.chronicle.loaders import (
    _extract_text_from_content,
    load_chatgpt_export,
    load_claude_ai_export,
    load_claude_code_sessions,
)

# ============================================================================
# Helpers
# ============================================================================


def _write_claude_code_session(tmp_path: Path, records: list[dict]) -> Path:
    """Write a minimal Claude Code JSONL file."""
    project_dir = tmp_path / "projects" / "-test-project"
    project_dir.mkdir(parents=True)
    session_file = project_dir / "session-abc.jsonl"
    session_file.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return tmp_path / "projects"


# ============================================================================
# _extract_text_from_content
# ============================================================================


class TestExtractText:
    def test_plain_string(self):
        assert _extract_text_from_content("hello") == "hello"

    def test_text_blocks(self):
        content = [
            {"type": "text", "text": "First"},
            {"type": "text", "text": "Second"},
        ]
        assert "First" in _extract_text_from_content(content)
        assert "Second" in _extract_text_from_content(content)

    def test_tool_use_block(self):
        content = [{"type": "tool_use", "name": "Read", "id": "x", "input": {}}]
        assert "[tool: Read]" in _extract_text_from_content(content)

    def test_thinking_blocks_skipped(self):
        content = [
            {"type": "thinking", "thinking": "deep thoughts"},
            {"type": "text", "text": "answer"},
        ]
        result = _extract_text_from_content(content)
        assert "deep thoughts" not in result
        assert "answer" in result

    def test_empty(self):
        assert _extract_text_from_content("") == ""
        assert _extract_text_from_content(None) == ""
        assert _extract_text_from_content([]) == ""


# ============================================================================
# Claude Code loader — unit tests with fixtures
# ============================================================================


class TestLoadClaudeCodeSessions:
    def test_basic_session(self, tmp_path):
        records = [
            {
                "type": "user",
                "uuid": "u1",
                "parentUuid": None,
                "timestamp": "2026-03-16T14:00:00.000Z",
                "sessionId": "session-abc",
                "cwd": "/home/test",
                "message": {"role": "user", "content": "Hello world"},
            },
            {
                "type": "assistant",
                "uuid": "a1",
                "parentUuid": "u1",
                "timestamp": "2026-03-16T14:00:05.000Z",
                "sessionId": "session-abc",
                "cwd": "/home/test",
                "gitBranch": "main",
                "message": {
                    "role": "assistant",
                    "model": "claude-sonnet-4-6",
                    "content": [{"type": "text", "text": "Hi!"}],
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            },
        ]
        projects_dir = _write_claude_code_session(tmp_path, records)
        pairs = load_claude_code_sessions(projects_dir)

        assert len(pairs) == 1
        artifact, chat = pairs[0]
        assert isinstance(artifact, Artifact)
        assert isinstance(chat, ChatMessage)
        assert artifact.kind == "chat_message"
        assert artifact.source == "claude_code"
        assert chat.total_turns == 2
        assert chat.model == "claude-sonnet-4-6"
        assert "Hello world" in chat.content
        assert "Hi!" in chat.content

    def test_skips_progress_records(self, tmp_path):
        records = [
            {"type": "progress", "data": {}, "uuid": "p1", "timestamp": "2026-01-01T00:00:00Z"},
            {
                "type": "user",
                "uuid": "u1",
                "timestamp": "2026-01-01T00:00:01Z",
                "message": {"role": "user", "content": "test"},
            },
        ]
        projects_dir = _write_claude_code_session(tmp_path, records)
        pairs = load_claude_code_sessions(projects_dir)
        assert len(pairs) == 1
        assert pairs[0][1].total_turns == 1

    def test_empty_session_skipped(self, tmp_path):
        records = [
            {"type": "progress", "data": {}, "uuid": "p1", "timestamp": "2026-01-01T00:00:00Z"},
        ]
        projects_dir = _write_claude_code_session(tmp_path, records)
        pairs = load_claude_code_sessions(projects_dir)
        assert len(pairs) == 0

    def test_missing_dir_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_claude_code_sessions(tmp_path / "nonexistent")


# ============================================================================
# Claude.ai export — unit tests with fixtures
# ============================================================================


class TestLoadClaudeAiExport:
    def test_basic_conversation(self, tmp_path):
        conversations = [
            {
                "uuid": "conv-1",
                "name": "Test Chat",
                "created_at": "2026-03-01T10:00:00Z",
                "chat_messages": [
                    {
                        "sender": "human",
                        "text": "What is 2+2?",
                        "created_at": "2026-03-01T10:00:00Z",
                    },
                    {
                        "sender": "assistant",
                        "text": "4",
                        "created_at": "2026-03-01T10:00:01Z",
                    },
                ],
            }
        ]
        conv_file = tmp_path / "conversations.json"
        conv_file.write_text(json.dumps(conversations))

        pairs = load_claude_ai_export(conv_file)
        assert len(pairs) == 1
        artifact, chat = pairs[0]
        assert artifact.source == "claude_ai"
        assert chat.title == "Test Chat"
        assert chat.total_turns == 2

    def test_empty_conversation_skipped(self, tmp_path):
        conversations = [{"uuid": "conv-2", "name": "Empty", "chat_messages": []}]
        conv_file = tmp_path / "conversations.json"
        conv_file.write_text(json.dumps(conversations))

        pairs = load_claude_ai_export(conv_file)
        assert len(pairs) == 0


# ============================================================================
# ChatGPT export — unit tests with fixtures
# ============================================================================


class TestLoadChatGptExport:
    def test_basic_conversation(self, tmp_path):
        conversations = [
            {
                "id": "gpt-conv-1",
                "title": "GPT Chat",
                "create_time": 1709290800.0,
                "mapping": {
                    "root": {
                        "id": "root",
                        "parent": None,
                        "children": ["msg1"],
                        "message": None,
                    },
                    "msg1": {
                        "id": "msg1",
                        "parent": "root",
                        "children": ["msg2"],
                        "message": {
                            "author": {"role": "user"},
                            "content": {"parts": ["Hello GPT"]},
                            "create_time": 1709290800.0,
                        },
                    },
                    "msg2": {
                        "id": "msg2",
                        "parent": "msg1",
                        "children": [],
                        "message": {
                            "author": {"role": "assistant"},
                            "content": {"parts": ["Hello!"]},
                            "create_time": 1709290801.0,
                            "metadata": {"model_slug": "gpt-4"},
                        },
                    },
                },
            }
        ]
        conv_file = tmp_path / "conversations.json"
        conv_file.write_text(json.dumps(conversations))

        pairs = load_chatgpt_export(conv_file)
        assert len(pairs) == 1
        artifact, chat = pairs[0]
        assert artifact.source == "chatgpt"
        assert chat.title == "GPT Chat"
        assert chat.total_turns == 2
        assert chat.model == "gpt-4"
        assert "Hello GPT" in chat.content
        assert "Hello!" in chat.content

    def test_empty_mapping_skipped(self, tmp_path):
        conversations = [{"id": "gpt-2", "title": "Empty", "mapping": {}}]
        conv_file = tmp_path / "conversations.json"
        conv_file.write_text(json.dumps(conversations))

        pairs = load_chatgpt_export(conv_file)
        assert len(pairs) == 0


# ============================================================================
# Dogfood tests — run against real data when available
# ============================================================================

_REAL_CLAUDE_CODE_DIR = Path.home() / ".claude" / "projects"


@pytest.mark.skipif(
    not _REAL_CLAUDE_CODE_DIR.exists(),
    reason="Claude Code projects dir not found",
)
class TestDogfoodClaudeCode:
    """Run the Claude Code loader against real session data."""

    def test_loads_sessions(self):
        pairs = load_claude_code_sessions(_REAL_CLAUDE_CODE_DIR)
        assert len(pairs) > 0, "Expected at least one session"

    def test_all_have_timestamps(self):
        pairs = load_claude_code_sessions(_REAL_CLAUDE_CODE_DIR)
        for artifact, _chat in pairs:
            assert artifact.created_at_ms > 0, f"Session {artifact.source_id} has no timestamp"

    def test_all_have_turns(self):
        pairs = load_claude_code_sessions(_REAL_CLAUDE_CODE_DIR)
        for _artifact, chat in pairs:
            assert chat.total_turns > 0, f"Session {chat.conversation_id} has 0 turns"

    def test_roles_are_expected(self):
        pairs = load_claude_code_sessions(_REAL_CLAUDE_CODE_DIR)
        for _artifact, chat in pairs:
            for turn in chat.get_turns():
                assert turn["role"] in ("user", "assistant"), f"Unexpected role: {turn['role']}"

    def test_kind_and_source(self):
        pairs = load_claude_code_sessions(_REAL_CLAUDE_CODE_DIR)
        for artifact, _chat in pairs:
            assert artifact.kind == "chat_message"
            assert artifact.source == "claude_code"

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Claude Code processor and AgentPrism export.

These are pure-Python tests — they exercise the stream-json parser, the
AgentPrism TraceRecord builder, and the processor's dataframe shape without
needing the ``claude`` CLI on PATH (the runner gracefully records a "skipped"
outcome when the binary is missing).
"""

from __future__ import annotations

import json

import pytest
from uuid_utils import uuid7

from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.container import ServiceContainer
from archetype.app.models import Command, CommandType
from archetype.contrib.agentprism import (
    claude_code_run_to_trace_record,
    turns_to_trace_record,
)
from archetype.contrib.claude_code import (
    ClaudeCodeProcessor,
    ClaudeCodePrompt,
    ClaudeCodeRun,
    ClaudeCodeTurn,
    _parse_event,
    _summarize_outcome,
)
from archetype.core.config import RunConfig, StorageConfig, WorldConfig


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


# ── stream-json parser ──────────────────────────────────────────────────────


class TestParseEvent:
    def test_system_event(self):
        turns = _parse_event({"type": "system", "subtype": "init", "session_id": "s-1"})
        assert len(turns) == 1
        assert turns[0].role == "system"
        assert turns[0].content == "init"

    def test_assistant_text_block(self):
        event = {
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "Hello"}],
                "usage": {"output_tokens": 7, "input_tokens": 3},
            },
        }
        turns = _parse_event(event)
        assert len(turns) == 1
        assert turns[0].role == "assistant"
        assert turns[0].content == "Hello"
        assert turns[0].tokens == 7

    def test_assistant_tool_use_and_text(self):
        event = {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "I'll read the file."},
                    {
                        "type": "tool_use",
                        "id": "tu-1",
                        "name": "Read",
                        "input": {"path": "a.py"},
                    },
                ],
                "usage": {"output_tokens": 25},
            },
        }
        turns = _parse_event(event)
        assert [t.role for t in turns] == ["assistant", "tool_call"]
        assert turns[1].tool_name == "Read"
        assert json.loads(turns[1].tool_input) == {"path": "a.py"}
        assert turns[1].metadata["tool_use_id"] == "tu-1"

    def test_user_tool_result(self):
        event = {
            "type": "user",
            "message": {
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tu-1",
                        "content": "file contents",
                        "is_error": False,
                    }
                ]
            },
        }
        turns = _parse_event(event)
        assert turns[0].role == "tool_result"
        assert turns[0].tool_output == "file contents"
        assert turns[0].error is None

    def test_result_event(self):
        event = {
            "type": "result",
            "subtype": "success",
            "result": "Done",
            "usage": {"output_tokens": 10},
            "duration_ms": 1234,
            "is_error": False,
            "num_turns": 5,
            "total_cost_usd": 0.01,
        }
        turns = _parse_event(event)
        assert turns[0].role == "result"
        assert turns[0].content == "Done"
        assert turns[0].duration_ms == 1234

    def test_outcome_summary(self):
        turns = [
            ClaudeCodeTurn(role="assistant", content="working..."),
            ClaudeCodeTurn(role="result", content="all green"),
        ]
        assert _summarize_outcome(turns).startswith("success: all green")

    def test_outcome_error(self):
        turns = [ClaudeCodeTurn(role="result", content="", error="exit 1")]
        assert _summarize_outcome(turns) == "failure: exit 1"

    def test_outcome_no_result(self):
        assert _summarize_outcome([]) == "no output"


# ── AgentPrism TraceRecord ──────────────────────────────────────────────────


class TestTraceRecord:
    def test_simple_turn_list(self):
        turns = [
            {"role": "user", "content": "hi", "tokens": 2, "duration_ms": 5},
            {"role": "assistant", "content": "hey", "tokens": 4, "duration_ms": 50},
        ]
        record = turns_to_trace_record(trace_id="t-1", name="demo", turns=turns)
        assert record["id"] == "t-1"
        assert record["status"] == "success"
        # 1 root + 2 turn spans
        assert len(record["spans"]) == 3
        types = [s["type"] for s in record["spans"]]
        assert types == ["agent_invocation", "chain", "llm_call"]

    def test_tool_call_result_pairing(self):
        turns = [
            {
                "role": "tool_call",
                "tool_name": "Read",
                "tool_input": "{}",
                "duration_ms": 5,
                "metadata": {"tool_use_id": "x"},
            },
            {
                "role": "tool_result",
                "tool_output": "abc",
                "duration_ms": 5,
                "metadata": {"tool_use_id": "x"},
            },
        ]
        record = turns_to_trace_record(trace_id="t-2", name="demo", turns=turns)
        # Find tool_call and tool_result spans (skip root)
        non_root = [s for s in record["spans"] if s["parentId"] is not None]
        call_span = next(s for s in non_root if s["name"].endswith("Read"))
        result_span = next(s for s in non_root if s["name"].endswith(".result"))
        assert result_span["parentId"] == call_span["id"]

    def test_error_propagates_to_root(self):
        turns = [
            {"role": "tool_result", "error": "boom", "duration_ms": 1},
        ]
        record = turns_to_trace_record(trace_id="t-3", name="demo", turns=turns)
        assert record["status"] == "error"

    def test_claude_code_run_to_trace_record(self):
        run = {
            "claudecoderun__session_id": "sess-1",
            "claudecoderun__model": "claude-sonnet-4-6",
            "claudecoderun__outcome": "success",
            "claudecoderun__exit_code": 0,
            "claudecoderun__duration_seconds": 1.5,
            "claudecoderun__turns_json": json.dumps(
                [{"role": "assistant", "content": "ok", "tokens": 1, "duration_ms": 10}]
            ),
            "claudecodeprompt__prompt": "do a thing",
        }
        record = claude_code_run_to_trace_record(run)
        assert record["id"] == "sess-1"
        assert record["name"] == "do a thing"
        assert record["attributes"]["gen_ai.system"] == "claude-code"
        assert record["attributes"]["gen_ai.request.model"] == "claude-sonnet-4-6"


# ── End-to-end processor (CLI gracefully missing) ───────────────────────────


@pytest.mark.asyncio
async def test_processor_runs_without_cli(tmp_path):
    """When ``claude`` is not on PATH the processor records a skipped outcome.

    This pins the dataframe contract: every entity gets an outcome and the
    ``ClaudeCodeRun`` columns are populated even if the binary is missing.
    """
    container = ServiceContainer()
    try:
        storage = StorageConfig(uri=str(tmp_path / "store"), namespace="cc")
        world = await container.world_service.create_world(WorldConfig(name="cc-test"), storage)

        await world.system.add_processor(ClaudeCodeProcessor())

        ctx = ActorCtx(id=uuid7(), roles={"operator"})

        for prompt in ["one", "two"]:
            cmd = Command(
                type=CommandType.SPAWN,
                payload={
                    "components": [
                        {"type": "ClaudeCodePrompt", "prompt": prompt, "max_turns": 1},
                        {"type": "ClaudeCodeRun"},
                    ],
                },
            )
            await container.command_service.submit(ctx, str(world.world_id), cmd)

        await container.simulation_service.step(world.world_id, RunConfig(num_steps=1))

        df = await world.get_components([ClaudeCodePrompt, ClaudeCodeRun])
        assert df is not None
        rows = df.collect().to_pylist()
        assert len(rows) == 2
        for row in rows:
            assert row["claudecoderun__outcome"] != ""
            # turns_json is always valid JSON
            json.loads(row["claudecoderun__turns_json"])
    finally:
        await container.shutdown()

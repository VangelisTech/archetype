# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""AgentPrism integration: render Archetype trajectories as TraceRecords.

`AgentPrism <https://github.com/evilmartians/agent-prism>`_ is an open-source
React component library that renders OpenTelemetry-style trace trees for AI
agent runs. It accepts JSON in two ways:

1. Direct ``TraceRecord`` JSON (its native shape) — fastest to wire up.
2. OTLP/JSON spans coming through any OTel exporter — what the
   :func:`emit_replay_spans` helper writes via the runtime's logfire client.

The :func:`turns_to_trace_record` helper maps a list of
:class:`~archetype.contrib.claude_code.ClaudeCodeTurn` dicts (the same shape
``ClaudeCodeRun.turns_json`` stores, and the same shape used by
``examples/06_trajectory_analysis.py``'s ``Trajectory.turns_json``) into the
TraceRecord shape AgentPrism expects:

.. code-block:: text

    TraceRecord {
        id, name, durationMs, startTimeUnixNano, endTimeUnixNano,
        status, attributes,
        spans: TraceSpan[] {
            id, name, type, status, durationMs, startTimeUnixNano,
            endTimeUnixNano, parentId, attributes
        }
    }

Span ``type`` values follow AgentPrism's enum: ``agent_invocation``,
``llm_call``, ``tool_execution``, ``chain``, ``retrieval``, ``embedding``,
``guardrail``, ``evaluator``, ``unknown``.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterable
from typing import Any

_NS_PER_MS = 1_000_000


def _new_id() -> str:
    return uuid.uuid4().hex[:16]


def _role_to_span_type(role: str) -> str:
    return {
        "user": "chain",
        "system": "chain",
        "assistant": "llm_call",
        "tool_call": "tool_execution",
        "tool_result": "tool_execution",
        "result": "agent_invocation",
        "raw": "unknown",
    }.get(role, "unknown")


def _turn_attributes(turn: dict[str, Any]) -> dict[str, Any]:
    attrs: dict[str, Any] = {
        "agent.role": turn.get("role", ""),
        "agent.tokens": int(turn.get("tokens", 0) or 0),
    }
    if turn.get("tool_name"):
        attrs["tool.name"] = turn["tool_name"]
    if turn.get("tool_input"):
        attrs["tool.input.value"] = turn["tool_input"]
    if turn.get("tool_output"):
        attrs["tool.output.value"] = turn["tool_output"]
    if turn.get("error"):
        attrs["error.message"] = turn["error"]
    if turn.get("metadata"):
        attrs["metadata"] = json.dumps(turn["metadata"], default=str)
    if turn.get("content"):
        attrs["agent.content"] = turn["content"][:4000]
    return attrs


def turns_to_trace_record(
    *,
    trace_id: str,
    name: str,
    turns: Iterable[dict[str, Any]],
    start_time_ms: float | None = None,
    root_attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an AgentPrism ``TraceRecord`` JSON dict from a flat turn list.

    Turns are placed sequentially in start-time order under one root span.
    Tool-call / tool-result pairs are linked: the result becomes a child of
    the matching call (matched by ``metadata.tool_use_id`` when present, else
    by adjacency).
    """
    turn_list = list(turns)
    if start_time_ms is None:
        start_time_ms = time.time() * 1000.0

    root_id = _new_id()
    cursor_ms = start_time_ms
    spans: list[dict[str, Any]] = []
    tool_call_index: dict[str, str] = {}
    last_tool_call_id: str | None = None

    for turn in turn_list:
        duration = float(turn.get("duration_ms", 0) or 0)
        if duration <= 0:
            duration = 1.0
        start_ns = int(cursor_ms * _NS_PER_MS)
        end_ns = int((cursor_ms + duration) * _NS_PER_MS)

        span_id = _new_id()
        role = turn.get("role", "")
        parent_id = root_id

        meta = turn.get("metadata") or {}
        if role == "tool_call":
            tool_use_id = meta.get("tool_use_id")
            if tool_use_id:
                tool_call_index[tool_use_id] = span_id
            last_tool_call_id = span_id
        elif role == "tool_result":
            tool_use_id = meta.get("tool_use_id")
            if tool_use_id and tool_use_id in tool_call_index:
                parent_id = tool_call_index[tool_use_id]
            elif last_tool_call_id is not None:
                parent_id = last_tool_call_id

        status = "error" if turn.get("error") else "success"

        spans.append(
            {
                "id": span_id,
                "name": _span_name_for(turn),
                "type": _role_to_span_type(role),
                "status": status,
                "durationMs": duration,
                "startTimeUnixNano": str(start_ns),
                "endTimeUnixNano": str(end_ns),
                "parentId": parent_id,
                "attributes": _turn_attributes(turn),
            }
        )
        cursor_ms += duration

    total_duration = cursor_ms - start_time_ms
    root_start_ns = int(start_time_ms * _NS_PER_MS)
    root_end_ns = int(cursor_ms * _NS_PER_MS)
    root_status = "error" if any(t.get("error") for t in turn_list) else "success"

    root_span = {
        "id": root_id,
        "name": name,
        "type": "agent_invocation",
        "status": root_status,
        "durationMs": total_duration,
        "startTimeUnixNano": str(root_start_ns),
        "endTimeUnixNano": str(root_end_ns),
        "parentId": None,
        "attributes": root_attributes or {},
    }

    return {
        "id": trace_id,
        "name": name,
        "durationMs": total_duration,
        "startTimeUnixNano": str(root_start_ns),
        "endTimeUnixNano": str(root_end_ns),
        "status": root_status,
        "attributes": root_attributes or {},
        "spans": [root_span, *spans],
    }


def _span_name_for(turn: dict[str, Any]) -> str:
    role = turn.get("role", "turn")
    if role == "tool_call":
        return f"tool.{turn.get('tool_name', 'tool')}"
    if role == "tool_result":
        return f"tool.{turn.get('tool_name', 'result')}.result"
    return role


def claude_code_run_to_trace_record(run: dict[str, Any]) -> dict[str, Any]:
    """Build a TraceRecord from a row of ``ClaudeCodeRun`` component data.

    Accepts a dict with the prefixed keys (``claudecoderun__turns_json`` etc.)
    or the unprefixed keys.
    """

    def pick(*keys: str, default: Any = "") -> Any:
        for k in keys:
            if k in run and run[k] not in (None, ""):
                return run[k]
        return default

    turns_json = pick("claudecoderun__turns_json", "turns_json", default="[]")
    try:
        turns = json.loads(turns_json) if isinstance(turns_json, str) else (turns_json or [])
    except json.JSONDecodeError:
        turns = []

    session_id = pick("claudecoderun__session_id", "session_id", default=_new_id())
    name = pick("claudecodeprompt__prompt", "prompt", default="claude_code.run")
    name = (name or "claude_code.run")[:80]

    return turns_to_trace_record(
        trace_id=str(session_id),
        name=name,
        turns=turns,
        root_attributes={
            "gen_ai.system": "claude-code",
            "gen_ai.request.model": pick("claudecoderun__model", "model"),
            "agent.outcome": pick("claudecoderun__outcome", "outcome"),
            "agent.exit_code": int(pick("claudecoderun__exit_code", "exit_code", default=0) or 0),
            "agent.duration_seconds": float(
                pick("claudecoderun__duration_seconds", "duration_seconds", default=0.0) or 0.0
            ),
        },
    )


def emit_replay_spans(record: dict[str, Any]) -> None:
    """Emit a stored TraceRecord as live logfire spans.

    Useful when you have trajectories sitting in LanceDB from a prior run and
    want them rendered through your live OTel pipeline (so AgentPrism picks
    them up via the same exporter as in-flight runs).
    """
    try:
        import logfire
    except ImportError:  # pragma: no cover — logfire is a hard dep
        return

    spans_by_id: dict[str, dict[str, Any]] = {s["id"]: s for s in record.get("spans", [])}
    children_of: dict[str | None, list[dict[str, Any]]] = {}
    for span in record.get("spans", []):
        children_of.setdefault(span.get("parentId"), []).append(span)

    def _walk(span: dict[str, Any]) -> None:
        with logfire.span(
            span["name"],
            **{
                k: v
                for k, v in span.get("attributes", {}).items()
                if isinstance(v, (str, int, float, bool))
            },
        ):
            for child in children_of.get(span["id"], []):
                _walk(child)

    roots = children_of.get(None) or list(spans_by_id.values())[:1]
    for root in roots:
        _walk(root)


__all__ = [
    "turns_to_trace_record",
    "claude_code_run_to_trace_record",
    "emit_replay_spans",
]

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Claude Code SDK processor for capturing agent trajectories.

This module wraps the ``claude`` CLI (or any binary that emits the same
``--output-format stream-json`` newline-delimited JSON protocol) inside a
``@daft.cls()`` UDF so the runtime can spawn many entities, each carrying a
prompt, and collect a structured ``Trajectory`` per row in a single tick.

Each row produces:

- A ``ClaudeCodeRun`` component populated with ``turns_json`` (one JSON object
  per parsed Claude Code event) and aggregate fields (token counts, duration,
  outcome, exit code).
- A logfire/OTel span tree under ``claude_code.run`` with one child span per
  turn — ready for AgentPrism to render directly when the configured OTLP
  exporter forwards spans to your observability backend.

Pair with :func:`archetype.contrib.agentprism.trajectory_to_trace_record` to
materialize an AgentPrism-shaped JSON document for offline rendering.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from typing import Any

import daft
from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from archetype.core.component import Component


@dataclass
class ClaudeCodeTurn:
    """One parsed event in a Claude Code stream-json trajectory.

    The shape is intentionally close to ``examples/06_trajectory_analysis.py``
    so trajectories produced here can flow into the existing labeling
    pipeline without translation.
    """

    role: str  # "user", "assistant", "tool_call", "tool_result", "system", "result"
    content: str = ""
    tool_name: str | None = None
    tool_input: str | None = None
    tool_output: str | None = None
    tokens: int = 0
    duration_ms: float = 0.0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if self.tool_name is None:
            d.pop("tool_name", None)
        if self.tool_input is None:
            d.pop("tool_input", None)
        if self.tool_output is None:
            d.pop("tool_output", None)
        if self.error is None:
            d.pop("error", None)
        if not self.metadata:
            d.pop("metadata", None)
        return d


class ClaudeCodePrompt(Component):
    """Input prompt for a Claude Code agent run.

    Spawn one entity per prompt you want to evaluate. The processor reads the
    fields by their prefixed column names (``claudecodeprompt__*``).
    """

    prompt: str = ""
    system_prompt: str = ""
    model: str = ""  # empty → CLI default
    cwd: str = ""  # empty → process cwd
    max_turns: int = 0  # 0 → no cap
    allowed_tools: str = ""  # comma-separated, e.g. "Read,Edit,Bash"; empty → all


class ClaudeCodeRun(Component):
    """Output of a Claude Code agent run, captured from stream-json stdout.

    Stored alongside :class:`ClaudeCodePrompt` on the same entity. ``turns_json``
    is a list of :class:`ClaudeCodeTurn` dicts.
    """

    run_id: str = ""
    session_id: str = ""
    model: str = ""
    turns_json: str = "[]"
    total_turns: int = 0
    total_tokens: int = 0
    duration_seconds: float = 0.0
    outcome: str = ""
    exit_code: int = 0
    error: str = ""


def _parse_event(event: dict[str, Any]) -> list[ClaudeCodeTurn]:
    """Translate one Claude Code stream-json event into zero or more turns.

    Stream-json events include ``system``, ``user``, ``assistant``, ``result``.
    Assistant messages may bundle text blocks and ``tool_use`` blocks; user
    messages may bundle ``tool_result`` blocks. We flatten each block into its
    own turn so the resulting trajectory mirrors AgentPrism's expected tree.
    """
    etype = event.get("type", "")
    turns: list[ClaudeCodeTurn] = []

    if etype == "system":
        return [
            ClaudeCodeTurn(
                role="system",
                content=event.get("subtype", "") or "system",
                metadata={k: v for k, v in event.items() if k not in {"type"}},
            )
        ]

    if etype == "result":
        usage = event.get("usage") or {}
        return [
            ClaudeCodeTurn(
                role="result",
                content=event.get("result", "") or event.get("subtype", ""),
                tokens=int(usage.get("output_tokens", 0) or 0),
                duration_ms=float(event.get("duration_ms", 0) or 0),
                error=event.get("error"),
                metadata={
                    "subtype": event.get("subtype"),
                    "is_error": event.get("is_error"),
                    "num_turns": event.get("num_turns"),
                    "total_cost_usd": event.get("total_cost_usd"),
                },
            )
        ]

    msg = event.get("message") or {}
    blocks = msg.get("content") or []
    if isinstance(blocks, str):
        blocks = [{"type": "text", "text": blocks}]

    usage = msg.get("usage") or {}
    out_tokens = int(usage.get("output_tokens", 0) or 0)
    in_tokens = int(usage.get("input_tokens", 0) or 0)

    for block in blocks:
        btype = block.get("type", "text")
        if btype == "text":
            turns.append(
                ClaudeCodeTurn(
                    role=etype or "assistant",
                    content=block.get("text", ""),
                    tokens=out_tokens if etype == "assistant" else in_tokens,
                )
            )
        elif btype == "tool_use":
            turns.append(
                ClaudeCodeTurn(
                    role="tool_call",
                    content=block.get("name", ""),
                    tool_name=block.get("name", ""),
                    tool_input=json.dumps(block.get("input", {}), default=str),
                    tokens=out_tokens,
                    metadata={"tool_use_id": block.get("id")},
                )
            )
        elif btype == "tool_result":
            content = block.get("content")
            if isinstance(content, list):
                content = "\n".join(
                    (c.get("text", "") if isinstance(c, dict) else str(c)) for c in content
                )
            turns.append(
                ClaudeCodeTurn(
                    role="tool_result",
                    content=str(content or ""),
                    tool_output=str(content or ""),
                    error="error" if block.get("is_error") else None,
                    metadata={"tool_use_id": block.get("tool_use_id")},
                )
            )
    if not turns:
        turns.append(
            ClaudeCodeTurn(
                role=etype or "unknown",
                content=json.dumps(event, default=str)[:500],
            )
        )
    return turns


def _summarize_outcome(turns: list[ClaudeCodeTurn]) -> str:
    """Pick a one-line outcome string for the run."""
    for turn in reversed(turns):
        if turn.role == "result":
            if turn.error:
                return f"failure: {turn.error}"
            return f"success: {turn.content[:140]}" if turn.content else "success"
    for turn in reversed(turns):
        if turn.role == "assistant" and turn.content:
            return f"assistant: {turn.content[:140]}"
    return "no output"


@daft.cls()
class _ClaudeCodeRunner:
    """Stateful runner that shells out to ``claude --output-format stream-json``.

    Lives once per worker. The CLI binary path and span emitter are cached.
    """

    def __init__(self, cli: str = "claude", emit_spans: bool = True) -> None:
        self._cli = cli
        self._emit_spans = emit_spans
        self._cli_path: str | None = None
        try:  # logfire is a hard dep of archetype-ecs but stay defensive in tests
            import logfire  # noqa: F401

            self._logfire_ok = True
        except ImportError:
            self._logfire_ok = False

    def _resolve_cli(self) -> str | None:
        if self._cli_path is not None:
            return self._cli_path or None
        path = shutil.which(self._cli)
        self._cli_path = path or ""
        return path

    async def run(
        self,
        prompt: str,
        system_prompt: str = "",
        model: str = "",
        cwd: str = "",
        max_turns: int = 0,
        allowed_tools: str = "",
    ) -> str:
        """Run one Claude Code session and return a JSON-encoded ``ClaudeCodeRun``.

        We return a single JSON string so the result can be a plain ``str``
        column. Callers ``json.loads`` it and feed the fields into the
        :class:`ClaudeCodeRun` component.
        """
        started = time.monotonic()
        cli = self._resolve_cli()
        if not cli or not prompt:
            return json.dumps(
                {
                    "turns": [],
                    "session_id": "",
                    "model": model,
                    "exit_code": 127 if not cli else 0,
                    "outcome": "skipped: no claude CLI on PATH"
                    if not cli
                    else "skipped: empty prompt",
                    "error": "claude CLI not found" if not cli else "empty prompt",
                    "duration_seconds": 0.0,
                }
            )

        argv = [cli, "-p", prompt, "--output-format", "stream-json", "--verbose"]
        if model:
            argv.extend(["--model", model])
        if max_turns:
            argv.extend(["--max-turns", str(max_turns)])
        if system_prompt:
            argv.extend(["--system-prompt", system_prompt])
        if allowed_tools:
            argv.extend(["--allowed-tools", allowed_tools])

        env = os.environ.copy()
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd or None,
            env=env,
        )

        turns: list[ClaudeCodeTurn] = []
        session_id = ""
        actual_model = model

        run_span_cm = self._open_run_span(prompt=prompt, model=model)
        with run_span_cm as run_span:
            assert proc.stdout is not None
            async for line in proc.stdout:
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    event = json.loads(text)
                except json.JSONDecodeError:
                    turns.append(ClaudeCodeTurn(role="raw", content=text[:500]))
                    continue

                if not session_id:
                    session_id = event.get("session_id", "") or session_id
                if not actual_model:
                    actual_model = (event.get("message") or {}).get("model", "") or actual_model

                for turn in _parse_event(event):
                    turns.append(turn)
                    self._emit_turn_span(run_span, turn)

            stderr = (
                (await proc.stderr.read()).decode("utf-8", errors="replace") if proc.stderr else ""
            )
            exit_code = await proc.wait()
            duration = time.monotonic() - started
            outcome = _summarize_outcome(turns)
            error_msg = stderr.strip() if exit_code != 0 else ""

            self._close_run_span(
                run_span,
                outcome=outcome,
                exit_code=exit_code,
                num_turns=len(turns),
                duration=duration,
                session_id=session_id,
                model=actual_model,
            )

        return json.dumps(
            {
                "turns": [t.to_dict() for t in turns],
                "session_id": session_id,
                "model": actual_model,
                "exit_code": exit_code,
                "outcome": outcome,
                "error": error_msg,
                "duration_seconds": duration,
            }
        )

    def _open_run_span(self, *, prompt: str, model: str):
        if not (self._emit_spans and self._logfire_ok):
            return _NullSpan()
        import logfire

        return logfire.span(
            "claude_code.run",
            _tags=["agent_invocation", "claude_code"],
            **{
                "gen_ai.system": "claude-code",
                "gen_ai.request.model": model or "default",
                "agent.input.value": (prompt or "")[:2000],
                "agent.span.kind": "agent_invocation",
            },
        )

    def _emit_turn_span(self, parent: object, turn: ClaudeCodeTurn) -> None:
        if not (self._emit_spans and self._logfire_ok):
            return
        if isinstance(parent, _NullSpan):
            return
        import logfire

        if turn.role == "tool_call":
            with logfire.span(
                "claude_code.tool.{name}",
                name=turn.tool_name or "tool",
                _tags=["tool_execution", "claude_code"],
                **{
                    "agent.span.kind": "tool_execution",
                    "tool.name": turn.tool_name or "",
                    "tool.input.value": (turn.tool_input or "")[:2000],
                },
            ):
                pass
        elif turn.role == "tool_result":
            logfire.info(
                "claude_code.tool_result",
                **{
                    "agent.span.kind": "tool_execution",
                    "tool.output.value": (turn.tool_output or "")[:2000],
                    "tool.error": bool(turn.error),
                },
            )
        elif turn.role == "assistant":
            logfire.info(
                "claude_code.assistant",
                **{
                    "agent.span.kind": "llm_call",
                    "gen_ai.response.output_tokens": turn.tokens,
                    "agent.output.value": (turn.content or "")[:2000],
                },
            )
        else:
            logfire.debug(
                f"claude_code.{turn.role}",
                role=turn.role,
                content=(turn.content or "")[:500],
            )

    def _close_run_span(
        self,
        span: object,
        *,
        outcome: str,
        exit_code: int,
        num_turns: int,
        duration: float,
        session_id: str,
        model: str,
    ) -> None:
        if not (self._emit_spans and self._logfire_ok):
            return
        if isinstance(span, _NullSpan):
            return
        try:
            span.set_attributes(
                {
                    "agent.session.id": session_id,
                    "agent.outcome": outcome,
                    "gen_ai.response.model": model,
                    "agent.num_turns": num_turns,
                    "agent.duration_seconds": duration,
                    "process.exit_code": exit_code,
                }
            )
        except AttributeError:
            pass


class _NullSpan:
    """Context-manager no-op used when logfire is unavailable in tests."""

    def __enter__(self) -> _NullSpan:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


# A module-level singleton: ``@daft.cls`` re-creates per worker, but having
# one Python object lets the processor reference its bound methods.
_runner = _ClaudeCodeRunner()


@daft.func
def _unpack_session_id(payload: str) -> str:
    return _safe_get_str(payload, "session_id")


@daft.func
def _unpack_model(payload: str) -> str:
    return _safe_get_str(payload, "model")


@daft.func
def _unpack_outcome(payload: str) -> str:
    return _safe_get_str(payload, "outcome")


@daft.func
def _unpack_error(payload: str) -> str:
    return _safe_get_str(payload, "error")


@daft.func
def _unpack_exit_code(payload: str) -> int:
    return _safe_get_int(payload, "exit_code")


@daft.func
def _unpack_duration(payload: str) -> float:
    return _safe_get_float(payload, "duration_seconds")


@daft.func
def _unpack_turns_json(payload: str) -> str:
    if not payload:
        return "[]"
    try:
        return json.dumps(json.loads(payload).get("turns", []))
    except (json.JSONDecodeError, TypeError):
        return "[]"


@daft.func
def _unpack_total_tokens(payload: str) -> int:
    if not payload:
        return 0
    try:
        turns = json.loads(payload).get("turns", []) or []
        return sum(int(t.get("tokens", 0) or 0) for t in turns)
    except (json.JSONDecodeError, TypeError, ValueError):
        return 0


@daft.func
def _unpack_total_turns(payload: str) -> int:
    if not payload:
        return 0
    try:
        return len(json.loads(payload).get("turns", []) or [])
    except (json.JSONDecodeError, TypeError):
        return 0


def _safe_get_str(payload: str, key: str) -> str:
    if not payload:
        return ""
    try:
        value = json.loads(payload).get(key, "")
        return "" if value is None else str(value)
    except (json.JSONDecodeError, TypeError):
        return ""


def _safe_get_int(payload: str, key: str) -> int:
    if not payload:
        return 0
    try:
        return int(json.loads(payload).get(key, 0) or 0)
    except (json.JSONDecodeError, TypeError, ValueError):
        return 0


def _safe_get_float(payload: str, key: str) -> float:
    if not payload:
        return 0.0
    try:
        return float(json.loads(payload).get(key, 0.0) or 0.0)
    except (json.JSONDecodeError, TypeError, ValueError):
        return 0.0


class ClaudeCodeProcessor(AsyncProcessor):
    """Run Claude Code on every entity with a :class:`ClaudeCodePrompt`.

    Skips rows where ``claudecoderun__run_id`` is already non-empty so reruns
    only execute new prompts. The trajectory lands in
    ``claudecoderun__turns_json`` and aggregate fields.
    """

    components = (ClaudeCodePrompt, ClaudeCodeRun)
    priority = 25

    async def process(self, df: DataFrame, tick: int = 0, **kwargs: Any) -> DataFrame:
        already_run = col("claudecoderun__run_id") != ""
        pending_df = df.where(~already_run)
        done_df = df.where(already_run)

        pending_df = pending_df.with_column(
            "_claude_payload",
            _runner.run(
                col("claudecodeprompt__prompt"),
                col("claudecodeprompt__system_prompt"),
                col("claudecodeprompt__model"),
                col("claudecodeprompt__cwd"),
                col("claudecodeprompt__max_turns"),
                col("claudecodeprompt__allowed_tools"),
            ),
        )

        pending_df = pending_df.with_columns(
            {
                "claudecoderun__run_id": _unpack_session_id(col("_claude_payload")),
                "claudecoderun__session_id": _unpack_session_id(col("_claude_payload")),
                "claudecoderun__model": _unpack_model(col("_claude_payload")),
                "claudecoderun__turns_json": _unpack_turns_json(col("_claude_payload")),
                "claudecoderun__total_turns": _unpack_total_turns(col("_claude_payload")),
                "claudecoderun__total_tokens": _unpack_total_tokens(col("_claude_payload")),
                "claudecoderun__duration_seconds": _unpack_duration(col("_claude_payload")),
                "claudecoderun__outcome": _unpack_outcome(col("_claude_payload")),
                "claudecoderun__exit_code": _unpack_exit_code(col("_claude_payload")),
                "claudecoderun__error": _unpack_error(col("_claude_payload")),
            }
        ).exclude("_claude_payload")

        return pending_df.concat(done_df)


__all__ = [
    "ClaudeCodePrompt",
    "ClaudeCodeRun",
    "ClaudeCodeProcessor",
    "ClaudeCodeTurn",
]

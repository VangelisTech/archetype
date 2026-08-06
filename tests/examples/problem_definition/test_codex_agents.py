# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Subprocess contracts for the example-local Codex CLI model adapters."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from examples.problem_definition_mission.codex_agents import (
    CodexInvocationError,
    CodexReflectionLanguageModel,
    CodexStructuredLanguageModel,
)
from examples.problem_definition_mission.live_agents import (
    StructuredModelContextError,
    StructuredModelOutputError,
)


class _StructuredAnswer(BaseModel):
    statement: str
    confidence: float


class _OptionalDetail(BaseModel):
    note: str | None = None


class _NestedAnswer(BaseModel):
    detail: _OptionalDetail
    feedback: tuple[str, ...] = ()


def _argument_path(command: list[str], option: str) -> Path:
    return Path(command[command.index(option) + 1])


def _tool_free_jsonl() -> str:
    return "\n".join(
        (
            json.dumps({"type": "thread.started", "thread_id": "thread-1"}),
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "reasoning", "text": "Consider the request."},
                }
            ),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "Done."},
                }
            ),
            json.dumps({"type": "turn.completed", "usage": {}}),
        )
    )


def test_structured_adapter_uses_one_ephemeral_read_only_process_per_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[list[str], dict[str, Any], dict[str, object]]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        schema_path = _argument_path(command, "--output-schema")
        output_path = _argument_path(command, "--output-last-message")
        calls.append(
            (
                command,
                kwargs,
                json.loads(schema_path.read_text(encoding="utf-8")),
            )
        )
        output_path.write_text(
            '{"statement":"The bounded problem.","confidence":0.91}',
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout=_tool_free_jsonl(), stderr="")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )
    model = CodexStructuredLanguageModel(
        codex_executable="/opt/codex",
        model="test-codex-model",
        timeout_seconds=42,
        cwd=tmp_path,
    )

    first = model.complete(
        system_prompt="SYSTEM: use only evidence α.",
        user_prompt="USER: define exactly one problem β.",
        response_model=_StructuredAnswer,
    )
    second = model.complete(
        system_prompt="second system",
        user_prompt="second user",
        response_model=_StructuredAnswer,
    )

    assert first == _StructuredAnswer(
        statement="The bounded problem.",
        confidence=0.91,
    )
    assert second == first
    assert len(calls) == 2
    assert model.provider_id == "codex.exec"
    assert model.model_id == "test-codex-model"
    assert model.codex_executable == "/opt/codex"
    assert model.timeout_seconds == 42.0
    assert model.cwd == tmp_path.resolve()

    output_paths = []
    execution_roots = []
    for command, kwargs, schema in calls:
        assert command[:2] == ["/opt/codex", "exec"]
        assert command[-1] == "-"
        assert command.count("--ephemeral") == 1
        assert command[command.index("--sandbox") + 1] == "read-only"
        assert "--skip-git-repo-check" in command
        assert "--ignore-user-config" in command
        assert "--ignore-rules" in command
        assert "--strict-config" in command
        assert "--json" in command
        disabled_features = {
            command[index + 1] for index, argument in enumerate(command) if argument == "--disable"
        }
        assert {
            "apps",
            "browser_use",
            "computer_use",
            "image_generation",
            "multi_agent",
            "shell_tool",
            "unified_exec",
        } <= disabled_features
        assert command[command.index("-c") + 1] == 'web_search="disabled"'
        assert command[command.index("--color") + 1] == "never"
        assert command[command.index("-m") + 1] == "test-codex-model"
        execution_roots.append(Path(command[command.index("-C") + 1]))
        assert "--output-schema" in command
        assert "--output-last-message" in command
        assert "resume" not in command
        assert kwargs["cwd"] == tmp_path.resolve()
        assert kwargs["timeout"] == 42.0
        assert kwargs["capture_output"] is True
        assert kwargs["check"] is False
        assert schema["title"] == _StructuredAnswer.model_json_schema()["title"]
        assert schema["properties"] == _StructuredAnswer.model_json_schema()["properties"]
        assert schema["required"] == ["statement", "confidence"]
        assert schema["additionalProperties"] is False
        output_paths.append(_argument_path(command, "--output-last-message"))

    assert calls[0][1]["input"] != calls[1][1]["input"]
    first_prompt = str(calls[0][1]["input"])
    assert "Do not call tools" in first_prompt
    assert "SYSTEM: use only evidence α." in first_prompt
    assert "USER: define exactly one problem β." in first_prompt
    assert output_paths[0] != output_paths[1]
    assert all(not path.exists() for path in output_paths)
    assert execution_roots[0] != execution_roots[1]
    assert all(not path.exists() for path in execution_roots)


def test_pydantic_schema_is_strictified_recursively_for_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_schema: dict[str, Any] = {}

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured_schema.update(
            json.loads(_argument_path(command, "--output-schema").read_text(encoding="utf-8"))
        )
        _argument_path(command, "--output-last-message").write_text(
            '{"detail":{"note":null},"feedback":[]}',
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout=_tool_free_jsonl(), stderr="")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )

    result = CodexStructuredLanguageModel().complete(
        system_prompt="system",
        user_prompt="user",
        response_model=_NestedAnswer,
    )

    assert result == _NestedAnswer(detail=_OptionalDetail(note=None))
    assert captured_schema["required"] == ["detail", "feedback"]
    assert captured_schema["additionalProperties"] is False
    nested = captured_schema["$defs"]["_OptionalDetail"]
    assert nested["required"] == ["note"]
    assert nested["additionalProperties"] is False
    assert {"type": "null"} in nested["properties"]["note"]["anyOf"]


def test_structured_adapter_uses_subscription_default_when_model_is_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        _argument_path(command, "--output-last-message").write_text(
            '{"statement":"answer","confidence":1.0}',
            encoding="utf-8",
        )
        assert "-m" not in command
        return subprocess.CompletedProcess(command, 0, stdout=_tool_free_jsonl(), stderr="")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )
    model = CodexStructuredLanguageModel()

    result = model.complete(
        system_prompt="system",
        user_prompt="user",
        response_model=_StructuredAnswer,
    )

    assert result.statement == "answer"
    assert model.model_id == "codex-default"


def test_reflection_adapter_uses_free_text_without_an_output_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        _argument_path(command, "--output-last-message").write_text(
            "A sharper candidate prompt.\n",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout=_tool_free_jsonl(), stderr="")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )
    reflection = CodexReflectionLanguageModel(
        model="reflection-model",
        cwd=tmp_path,
    )
    prompt: list[dict[str, object]] = [
        {
            "role": "user",
            "content": "Revise using evaluator feedback Ω.",
        }
    ]

    result = reflection(prompt)

    assert result == "A sharper candidate prompt.\n"
    assert reflection.provider_id == "codex.exec"
    assert reflection.model_id == "reflection-model"
    assert len(calls) == 1
    command, kwargs = calls[0]
    assert "--output-schema" not in command
    assert "--ephemeral" in command
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert command[-1] == "-"
    stdin = str(kwargs["input"])
    assert "Do not call tools" in stdin
    assert "bounded GEPA search" in stdin
    assert "Revise using evaluator feedback Ω." in stdin


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (
            subprocess.CompletedProcess(
                ["codex", "exec"],
                7,
                stdout="",
                stderr="subscription unavailable",
            ),
            "status 7: subscription unavailable",
        ),
        (FileNotFoundError("missing"), "executable was not found"),
        (
            subprocess.TimeoutExpired(["codex", "exec"], 3),
            "timed out after 3 seconds",
        ),
    ],
)
def test_invocation_failures_are_reported_clearly(
    monkeypatch: pytest.MonkeyPatch,
    failure: subprocess.CompletedProcess[str] | BaseException,
    message: str,
) -> None:
    def fake_run(
        command: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        if isinstance(failure, BaseException):
            raise failure
        return failure

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )
    model = CodexStructuredLanguageModel(timeout_seconds=3)

    with pytest.raises(CodexInvocationError, match=message):
        model.complete(
            system_prompt="system",
            user_prompt="user",
            response_model=_StructuredAnswer,
        )


@pytest.mark.parametrize(
    ("event_stream", "message"),
    [
        ("", "no JSONL events"),
        ("not-json", "malformed JSONL event"),
        (json.dumps({"type": "future.event"}), "unsupported event type"),
        (json.dumps({"type": "error", "message": "failed"}), "unsupported event type"),
        (
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "command_execution", "command": "pwd"},
                }
            ),
            "prohibited tool event",
        ),
        (
            json.dumps(
                {
                    "type": "item.updated",
                    "item": {"type": "file_change", "changes": []},
                }
            ),
            "prohibited tool event",
        ),
    ],
)
def test_successful_codex_process_fails_closed_on_unverifiable_or_tool_events(
    monkeypatch: pytest.MonkeyPatch,
    event_stream: str,
    message: str,
) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        _argument_path(command, "--output-last-message").write_text(
            '{"statement":"answer","confidence":1.0}',
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, stdout=event_stream, stderr="")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )

    with pytest.raises(CodexInvocationError, match=message):
        CodexStructuredLanguageModel().complete(
            system_prompt="system",
            user_prompt="user",
            response_model=_StructuredAnswer,
        )


@pytest.mark.parametrize(
    ("output", "message"),
    [
        (None, "did not write"),
        (" ", "empty"),
        ("not json", "invalid JSON"),
        ('{"statement":"missing confidence"}', "does not match"),
    ],
)
def test_structured_output_failures_are_reported_clearly(
    monkeypatch: pytest.MonkeyPatch,
    output: str | None,
    message: str,
) -> None:
    def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if output is not None:
            _argument_path(command, "--output-last-message").write_text(
                output,
                encoding="utf-8",
            )
        return subprocess.CompletedProcess(command, 0, stdout=_tool_free_jsonl(), stderr="")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fake_run,
    )
    model = CodexStructuredLanguageModel()

    with pytest.raises(StructuredModelOutputError, match=message):
        model.complete(
            system_prompt="system",
            user_prompt="user",
            response_model=_StructuredAnswer,
        )


def test_context_limit_fails_before_launch_without_truncation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_if_called(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("subprocess must not be called")

    monkeypatch.setattr(
        "examples.problem_definition_mission.codex_agents.subprocess.run",
        fail_if_called,
    )
    model = CodexStructuredLanguageModel(max_input_chars=100)

    with pytest.raises(StructuredModelContextError, match="never truncated"):
        model.complete(
            system_prompt="s" * 100,
            user_prompt="u" * 100,
            response_model=_StructuredAnswer,
        )

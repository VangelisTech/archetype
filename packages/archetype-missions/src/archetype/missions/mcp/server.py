# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Minimal MCP stdio server for Archetype Agent Missions.

JSON-RPC 2.0 frames are the only bytes this process writes to stdout;
diagnostics go to bounded, credential-redacted stderr. The server exposes
exactly the six asynchronous mission tools from issue #810 and returns
``-32601`` for any unsupported request method. Interactive attachment
tools (issue #811) are deliberately absent rather than stubbed.
"""

from __future__ import annotations

import json
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Any, TextIO

from archetype.missions.mcp.client import (
    MissionRunClient,
    MissionToolError,
    require_opaque_id,
)
from archetype.missions.mcp.config import McpHostConfig, McpHostConfigError

SERVER_NAME = "archetype-missions-mcp"
SUPPORTED_PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")

_MAX_COORDINATE_CHARS = 512
_MAX_TASK_NAME_CHARS = 200
_MAX_DIAGNOSTIC_BYTES = 512

_TASK_KEYS = {"name", "prompt", "validators", "depends_on"}

_OPAQUE_ID_SCHEMA = {"type": "string", "minLength": 1, "maxLength": 256}

_TASK_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "minLength": 1, "maxLength": _MAX_TASK_NAME_CHARS},
        "prompt": {"type": "string", "minLength": 1},
        "validators": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "argv": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["name", "argv"],
                "additionalProperties": False,
            },
        },
        "depends_on": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["name", "prompt"],
    "additionalProperties": False,
}

TOOLS: tuple[dict[str, Any], ...] = (
    {
        "name": "mission_submit",
        "description": (
            "Explicitly start a durable Archetype coding mission and return "
            "immediately with its run_id and status coordinates; the mission "
            "keeps running after this process exits. Reusing the same "
            "idempotency_key with identical inputs returns the original run. "
            "Execution authority comes from the server-owned profile, never "
            "from these arguments."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "profile_id": _OPAQUE_ID_SCHEMA,
                "repository": {"type": "string", "minLength": 1},
                "ref": {"type": "string", "minLength": 1},
                "mission": {"type": "string", "minLength": 1},
                "tasks": {"type": "array", "items": _TASK_SCHEMA, "minItems": 1},
                "idempotency_key": _OPAQUE_ID_SCHEMA,
            },
            "required": [
                "profile_id",
                "repository",
                "ref",
                "mission",
                "tasks",
                "idempotency_key",
            ],
            "additionalProperties": False,
        },
    },
    {
        "name": "mission_get",
        "description": "Read the bounded status projection of one mission run.",
        "inputSchema": {
            "type": "object",
            "properties": {"run_id": _OPAQUE_ID_SCHEMA},
            "required": ["run_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "mission_events",
        "description": (
            "Read ordered mission-run events after an opaque cursor; replay "
            "from the same cursor has no gaps or duplicates."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": _OPAQUE_ID_SCHEMA,
                "after": _OPAQUE_ID_SCHEMA,
                "limit": {"type": "integer", "minimum": 1},
            },
            "required": ["run_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "mission_result",
        "description": (
            "Read the immutable terminal result of one mission run; fails "
            "with not_ready while the run is nonterminal."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"run_id": _OPAQUE_ID_SCHEMA},
            "required": ["run_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "mission_cancel",
        "description": (
            "Record durable cancellation intent for one mission run; repeat "
            "calls are idempotent and completion races resolve to the "
            "committed execution fact."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"run_id": _OPAQUE_ID_SCHEMA},
            "required": ["run_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "mission_list",
        "description": "List mission runs owned by the authenticated principal.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "minimum": 1}},
            "additionalProperties": False,
        },
    },
)


def _server_version() -> str:
    try:
        return version("archetype-missions")
    except PackageNotFoundError:  # pragma: no cover - source-tree fallback
        return "0.0.0"


class _Diagnostics:
    """Bounded stderr sink that redacts the configured credential."""

    def __init__(self, stream: TextIO, secrets: tuple[str, ...]) -> None:
        self._stream = stream
        self._secrets = tuple(secret for secret in secrets if secret)

    def emit(self, text: str) -> None:
        for secret in self._secrets:
            text = text.replace(secret, "[redacted]")
        line = " ".join(text.split())
        encoded = line.encode("utf-8", errors="replace")[:_MAX_DIAGNOSTIC_BYTES]
        try:
            self._stream.write(f"{SERVER_NAME}: {encoded.decode('utf-8', errors='replace')}\n")
            self._stream.flush()
        except OSError:  # pragma: no cover - diagnostics must never kill frames
            pass


def _require_string(value: object, *, label: str, max_chars: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > max_chars
        or any(ord(char) < 0x20 for char in value)
    ):
        raise MissionToolError(
            "invalid_argument",
            f"{label} must be a non-empty single-line string of at most {max_chars} characters",
        )
    return value


def _reject_unknown(arguments: dict[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(arguments) - allowed)
    if unknown:
        raise MissionToolError(
            "invalid_argument",
            f"unknown argument(s): {', '.join(unknown[:5])}; tool arguments "
            "carry domain inputs and opaque ids only",
        )


def _string_keyed(value: object, *, label: str) -> dict[str, Any]:
    """Normalize an untrusted mapping into ``dict[str, Any]`` or fail closed."""

    if not isinstance(value, dict):
        raise MissionToolError("invalid_argument", f"{label} must be an object")
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise MissionToolError("invalid_argument", f"{label} keys must be strings")
        normalized[key] = item
    return normalized


def _require_arguments(params: object) -> dict[str, Any]:
    if params is None:
        return {}
    return _string_keyed(params, label="arguments")


class MissionMcpServer:
    """Stdio MCP server over one :class:`MissionRunClient`."""

    def __init__(
        self,
        config: McpHostConfig,
        *,
        client: MissionRunClient | None = None,
        stderr: TextIO | None = None,
    ) -> None:
        self._config = config
        self._client = client if client is not None else MissionRunClient(config)
        self._diagnostics = _Diagnostics(
            stderr if stderr is not None else sys.stderr,
            (config.credential or "",),
        )

    # -- protocol -----------------------------------------------------------

    def serve(self, stdin: TextIO | None = None, stdout: TextIO | None = None) -> int:
        """Run the newline-delimited JSON-RPC loop until stdin closes."""

        source = stdin if stdin is not None else sys.stdin
        sink = stdout if stdout is not None else sys.stdout
        self._diagnostics.emit(f"serving mission tools for {self._config.base_url}")
        try:
            for raw_line in source:
                line = raw_line.strip()
                if not line:
                    continue
                response = self.handle_line(line)
                if response is not None:
                    try:
                        sink.write(json.dumps(response, sort_keys=True) + "\n")
                        sink.flush()
                    except OSError:
                        # The host closed our stdout; stop serving cleanly.
                        break
        finally:
            self.close()
        return 0

    def close(self) -> None:
        self._client.close()

    def handle_line(self, line: str) -> dict[str, Any] | None:
        try:
            message = json.loads(line)
        except (ValueError, RecursionError):
            return _error_frame(None, -32700, "Parse error")
        return self.handle_message(message)

    def handle_message(self, message: object) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return _error_frame(None, -32600, "Invalid Request")
        request_id = message.get("id")
        method = message.get("method")
        if message.get("jsonrpc") != "2.0" or not isinstance(method, str):
            if request_id is None:
                return None
            return _error_frame(request_id, -32600, "Invalid Request")
        params = message.get("params")
        if request_id is None:
            # Notifications (including notifications/initialized) are accepted
            # as no-ops; JSON-RPC forbids responding to them.
            return None
        if method == "initialize":
            return _result_frame(request_id, self._initialize(params))
        if method == "ping":
            return _result_frame(request_id, {})
        if method == "tools/list":
            return _result_frame(request_id, {"tools": list(TOOLS)})
        if method == "tools/call":
            return self._tools_call(request_id, params)
        return _error_frame(request_id, -32601, "Method not found")

    def _initialize(self, params: object) -> dict[str, Any]:
        requested = None
        if isinstance(params, dict):
            requested = params.get("protocolVersion")
        negotiated = (
            requested
            if requested in SUPPORTED_PROTOCOL_VERSIONS
            else SUPPORTED_PROTOCOL_VERSIONS[0]
        )
        return {
            "protocolVersion": negotiated,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": _server_version()},
            "instructions": (
                "Asynchronous Archetype mission control. mission_submit starts "
                "real, budgeted work and must be an explicit decision; every "
                "other tool is read-only or idempotent."
            ),
        }

    def _tools_call(self, request_id: object, params: object) -> dict[str, Any]:
        if not isinstance(params, dict):
            return _error_frame(request_id, -32602, "Invalid params")
        name = params.get("name")
        if not isinstance(name, str):
            return _error_frame(request_id, -32602, "Invalid params")
        handlers = {
            "mission_submit": self._call_submit,
            "mission_get": self._call_get,
            "mission_events": self._call_events,
            "mission_result": self._call_result,
            "mission_cancel": self._call_cancel,
            "mission_list": self._call_list,
        }
        handler = handlers.get(name)
        if handler is None:
            return _error_frame(request_id, -32602, f"Unknown tool: {name}")
        try:
            arguments = _require_arguments(params.get("arguments"))
            payload = handler(arguments)
        except MissionToolError as exc:
            self._diagnostics.emit(f"tool {name} failed: {exc.code}")
            return _result_frame(
                request_id,
                self._tool_content(
                    {"error": {"code": exc.code, "message": exc.message}},
                    is_error=True,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - frames must stay protocol-pure
            self._diagnostics.emit(f"tool {name} failed unexpectedly: {type(exc).__name__}")
            return _result_frame(
                request_id,
                self._tool_content(
                    {
                        "error": {
                            "code": "internal",
                            "message": f"internal adapter failure ({type(exc).__name__})",
                        }
                    },
                    is_error=True,
                ),
            )
        return _result_frame(request_id, self._tool_content(payload, is_error=False))

    # -- tool handlers ------------------------------------------------------

    def _call_submit(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(
            arguments,
            {"profile_id", "repository", "ref", "mission", "tasks", "idempotency_key"},
        )
        for field in (
            "profile_id",
            "repository",
            "ref",
            "mission",
            "tasks",
            "idempotency_key",
        ):
            if field not in arguments:
                raise MissionToolError("invalid_argument", f"missing required argument: {field}")
        return self._client.submit(
            profile_id=require_opaque_id(arguments["profile_id"], label="profile_id"),
            repository=_require_string(
                arguments["repository"],
                label="repository",
                max_chars=_MAX_COORDINATE_CHARS,
            ),
            ref=_require_string(arguments["ref"], label="ref", max_chars=_MAX_COORDINATE_CHARS),
            mission=_require_string(
                arguments["mission"], label="mission", max_chars=_MAX_COORDINATE_CHARS
            ),
            tasks=self._validated_tasks(arguments["tasks"]),
            idempotency_key=require_opaque_id(
                arguments["idempotency_key"], label="idempotency_key"
            ),
        )

    def _validated_tasks(self, tasks: object) -> list[dict[str, Any]]:
        if not isinstance(tasks, list) or not tasks:
            raise MissionToolError("invalid_argument", "tasks must be a non-empty array")
        if len(tasks) > self._config.max_tasks:
            raise MissionToolError(
                "invalid_argument",
                f"tasks must contain at most {self._config.max_tasks} items",
            )
        validated: list[dict[str, Any]] = []
        for index, raw_task in enumerate(tasks):
            task = _string_keyed(raw_task, label=f"tasks[{index}]")
            _reject_unknown(task, _TASK_KEYS)
            name = task.get("name")
            if not isinstance(name, str) or not name or len(name) > _MAX_TASK_NAME_CHARS:
                raise MissionToolError(
                    "invalid_argument", f"tasks[{index}].name must be a short string"
                )
            prompt = task.get("prompt")
            if (
                not isinstance(prompt, str)
                or not prompt
                or len(prompt.encode("utf-8")) > self._config.max_prompt_bytes
            ):
                raise MissionToolError(
                    "invalid_argument",
                    f"tasks[{index}].prompt must be a non-empty string of at "
                    f"most {self._config.max_prompt_bytes} bytes",
                )
            clean: dict[str, Any] = {"name": name, "prompt": prompt}
            if "validators" in task:
                clean["validators"] = self._validated_validators(task["validators"], index=index)
            if "depends_on" in task:
                depends_on = task["depends_on"]
                if not isinstance(depends_on, list) or any(
                    not isinstance(item, str) or not item for item in depends_on
                ):
                    raise MissionToolError(
                        "invalid_argument",
                        f"tasks[{index}].depends_on must be an array of task names",
                    )
                clean["depends_on"] = depends_on
            validated.append(clean)
        return validated

    @staticmethod
    def _validated_validators(validators: object, *, index: int) -> list[dict[str, Any]]:
        if not isinstance(validators, list):
            raise MissionToolError(
                "invalid_argument", f"tasks[{index}].validators must be an array"
            )
        clean: list[dict[str, Any]] = []
        for position, raw_validator in enumerate(validators):
            label = f"tasks[{index}].validators[{position}]"
            validator = _string_keyed(raw_validator, label=label)
            _reject_unknown(validator, {"name", "argv"})
            name = validator.get("name")
            argv = validator.get("argv")
            if not isinstance(name, str) or not name:
                raise MissionToolError(
                    "invalid_argument", f"{label}.name must be a non-empty string"
                )
            if not isinstance(argv, list) or any(not isinstance(item, str) for item in argv):
                raise MissionToolError(
                    "invalid_argument", f"{label}.argv must be an array of strings"
                )
            clean.append({"name": name, "argv": argv})
        return clean

    def _call_get(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(arguments, {"run_id"})
        return self._client.get(self._run_id(arguments))

    def _call_events(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(arguments, {"run_id", "after", "limit"})
        after = arguments.get("after")
        if after is not None:
            after = require_opaque_id(after, label="after")
        return self._client.events(
            self._run_id(arguments),
            after=after,
            limit=self._limit_argument(arguments),
        )

    def _call_result(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(arguments, {"run_id"})
        return self._client.result(self._run_id(arguments))

    def _call_cancel(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(arguments, {"run_id"})
        return self._client.cancel(self._run_id(arguments))

    def _call_list(self, arguments: dict[str, Any]) -> dict[str, Any]:
        _reject_unknown(arguments, {"limit"})
        return self._client.list_runs(limit=self._limit_argument(arguments))

    @staticmethod
    def _limit_argument(arguments: dict[str, Any]) -> int | None:
        if "limit" not in arguments:
            return None
        value = arguments["limit"]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise MissionToolError("invalid_argument", "limit must be a positive integer")
        return value

    @staticmethod
    def _run_id(arguments: dict[str, Any]) -> str:
        if "run_id" not in arguments:
            raise MissionToolError("invalid_argument", "missing required argument: run_id")
        return require_opaque_id(arguments["run_id"], label="run_id")

    # -- rendering ----------------------------------------------------------

    def _tool_content(self, payload: dict[str, Any], *, is_error: bool) -> dict[str, Any]:
        rendered = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        if self._config.credential:
            # Defense in depth: even an upstream body that echoed the bearer
            # credential must never enter a model-visible tool result.
            rendered = rendered.replace(self._config.credential, "[redacted]")
        encoded = rendered.encode("utf-8")
        limit = self._config.max_result_bytes
        if len(encoded) > limit:
            prefix = encoded[:limit].decode("utf-8", errors="ignore")
            rendered = json.dumps(
                {
                    "truncated": True,
                    "limit_bytes": limit,
                    "content_prefix": prefix,
                },
                sort_keys=True,
                ensure_ascii=False,
            )
        return {
            "content": [{"type": "text", "text": rendered}],
            "isError": is_error,
        }


def _result_frame(request_id: object, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error_frame(request_id: object, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def main() -> int:
    """Console entry point: ``archetype-missions-mcp``."""

    try:
        config = McpHostConfig.from_env()
    except McpHostConfigError as exc:
        sys.stderr.write(f"{SERVER_NAME}: {exc}\n")
        return 2
    return MissionMcpServer(config).serve()

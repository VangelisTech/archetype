# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Minimal MCP stdio server for Archetype Agent Missions.

JSON-RPC 2.0 frames are the only bytes this process writes to stdout;
diagnostics go to bounded, credential-redacted stderr. The server exposes
exactly the six asynchronous mission tools from issue #810 and returns
``-32601`` for any unsupported request method. Interactive attachment
tools (issue #811) are deliberately absent rather than stubbed.

Each tool is one :class:`_ToolSpec` row: the advertised ``inputSchema``
and the runtime argument validation render from the same
``_ARGUMENT_KINDS`` table, so the two encodings cannot drift.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from typing import Any, TextIO

from archetype.missions.mcp.client import (
    OPAQUE_ID_PATTERN,
    MissionRunClient,
    MissionToolError,
    require_opaque_id,
)
from archetype.missions.mcp.config import McpHostConfig, McpHostConfigError

SERVER_NAME = "archetype-missions-mcp"
SUPPORTED_PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")

# Fixed input bounds (issue #810: "fixed byte/item limits"). Output bounds
# (events page, rendered result bytes) remain host-configured knobs.
_MAX_OPAQUE_ID_CHARS = 256
_MAX_COORDINATE_CHARS = 512
_MAX_TASK_NAME_CHARS = 200
_MAX_TASKS = 32
_MAX_PROMPT_BYTES = 65536
_MAX_DIAGNOSTIC_BYTES = 512
# Largest accepted stdin frame; a newline-less flood is rejected in bounded
# chunks instead of buffering an unbounded line.
_MAX_FRAME_CHARS = 16 * 1024 * 1024

_TASK_KEYS = {"name", "prompt", "validators", "depends_on"}

# Single-line coordinate: no ASCII control characters.
_LINE_PATTERN = "^[^\\u0000-\\u001f]+$"

_TASK_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "minLength": 1, "maxLength": _MAX_TASK_NAME_CHARS},
        "prompt": {"type": "string", "minLength": 1, "maxLength": _MAX_PROMPT_BYTES},
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
        "depends_on": {"type": "array", "items": {"type": "string", "minLength": 1}},
    },
    "required": ["name", "prompt"],
    "additionalProperties": False,
}


def _utf8_or_reject(value: str, label: str) -> bytes:
    """Encode ``value`` or fail closed: an unpaired surrogate is a permanently
    invalid argument, not a transient internal transport failure."""

    try:
        return value.encode("utf-8")
    except UnicodeEncodeError:
        raise MissionToolError(
            "invalid_argument",
            f"{label} must not contain unpaired surrogate code points",
        ) from None


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
    _utf8_or_reject(value, label)
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


def _validate_opaque_id(value: object, label: str) -> str:
    return require_opaque_id(value, label=label)


def _validate_line(value: object, label: str) -> str:
    return _require_string(value, label=label, max_chars=_MAX_COORDINATE_CHARS)


def _validate_limit(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise MissionToolError("invalid_argument", f"{label} must be a positive integer")
    return value


def _validate_validators(validators: object, *, label: str) -> list[dict[str, Any]]:
    if not isinstance(validators, list):
        raise MissionToolError("invalid_argument", f"{label} must be an array")
    clean: list[dict[str, Any]] = []
    for position, raw_validator in enumerate(validators):
        item_label = f"{label}[{position}]"
        validator = _string_keyed(raw_validator, label=item_label)
        _reject_unknown(validator, {"name", "argv"})
        name = validator.get("name")
        argv = validator.get("argv")
        if not isinstance(name, str) or not name:
            raise MissionToolError(
                "invalid_argument", f"{item_label}.name must be a non-empty string"
            )
        if not isinstance(argv, list):
            raise MissionToolError(
                "invalid_argument", f"{item_label}.argv must be an array of strings"
            )
        _utf8_or_reject(name, f"{item_label}.name")
        for position_argv, item in enumerate(argv):
            if not isinstance(item, str):
                raise MissionToolError(
                    "invalid_argument", f"{item_label}.argv must be an array of strings"
                )
            _utf8_or_reject(item, f"{item_label}.argv[{position_argv}]")
        clean.append({"name": name, "argv": argv})
    return clean


def _validate_tasks(tasks: object, label: str) -> list[dict[str, Any]]:
    if not isinstance(tasks, list) or not tasks:
        raise MissionToolError("invalid_argument", f"{label} must be a non-empty array")
    if len(tasks) > _MAX_TASKS:
        raise MissionToolError(
            "invalid_argument", f"{label} must contain at most {_MAX_TASKS} items"
        )
    validated: list[dict[str, Any]] = []
    for index, raw_task in enumerate(tasks):
        task = _string_keyed(raw_task, label=f"{label}[{index}]")
        _reject_unknown(task, _TASK_KEYS)
        name = task.get("name")
        if not isinstance(name, str) or not name or len(name) > _MAX_TASK_NAME_CHARS:
            raise MissionToolError(
                "invalid_argument", f"{label}[{index}].name must be a short string"
            )
        _utf8_or_reject(name, f"{label}[{index}].name")
        prompt = task.get("prompt")
        if (
            not isinstance(prompt, str)
            or not prompt
            or len(_utf8_or_reject(prompt, f"{label}[{index}].prompt")) > _MAX_PROMPT_BYTES
        ):
            raise MissionToolError(
                "invalid_argument",
                f"{label}[{index}].prompt must be a non-empty string of at "
                f"most {_MAX_PROMPT_BYTES} bytes",
            )
        clean: dict[str, Any] = {"name": name, "prompt": prompt}
        if "validators" in task:
            clean["validators"] = _validate_validators(
                task["validators"], label=f"{label}[{index}].validators"
            )
        if "depends_on" in task:
            depends_on = task["depends_on"]
            if not isinstance(depends_on, list):
                raise MissionToolError(
                    "invalid_argument",
                    f"{label}[{index}].depends_on must be an array of task names",
                )
            for position_dep, item in enumerate(depends_on):
                if not isinstance(item, str) or not item:
                    raise MissionToolError(
                        "invalid_argument",
                        f"{label}[{index}].depends_on must be an array of task names",
                    )
                _utf8_or_reject(item, f"{label}[{index}].depends_on[{position_dep}]")
            clean["depends_on"] = depends_on
        validated.append(clean)
    return validated


# One row per argument kind: (advertised JSON Schema fragment, runtime
# validator). Both sides of every tool argument come from this table.
_ARGUMENT_KINDS: dict[str, tuple[dict[str, Any], Callable[[object, str], Any]]] = {
    "opaque_id": (
        {
            "type": "string",
            "minLength": 1,
            "maxLength": _MAX_OPAQUE_ID_CHARS,
            "pattern": OPAQUE_ID_PATTERN,
        },
        _validate_opaque_id,
    ),
    "line": (
        {
            "type": "string",
            "minLength": 1,
            "maxLength": _MAX_COORDINATE_CHARS,
            "pattern": _LINE_PATTERN,
        },
        _validate_line,
    ),
    "limit": ({"type": "integer", "minimum": 1}, _validate_limit),
    "tasks": (
        {"type": "array", "items": _TASK_SCHEMA, "minItems": 1, "maxItems": _MAX_TASKS},
        _validate_tasks,
    ),
}


@dataclass(frozen=True, slots=True)
class _ToolSpec:
    """One mission tool: name, description, client method, argument rows."""

    name: str
    description: str
    client_method: str
    arguments: tuple[tuple[str, str, bool], ...]  # (argument, kind, required)


_TOOL_SPECS: tuple[_ToolSpec, ...] = (
    _ToolSpec(
        name="mission_submit",
        description=(
            "Explicitly start a durable Archetype coding mission and return "
            "immediately with its run_id and status coordinates; the mission "
            "keeps running after this process exits. Reusing the same "
            "idempotency_key with identical inputs returns the original run. "
            "Execution authority comes from the server-owned profile, never "
            "from these arguments."
        ),
        client_method="submit",
        arguments=(
            ("profile_id", "opaque_id", True),
            ("repository", "line", True),
            ("ref", "line", True),
            ("mission", "line", True),
            ("tasks", "tasks", True),
            ("idempotency_key", "opaque_id", True),
        ),
    ),
    _ToolSpec(
        name="mission_get",
        description="Read the bounded status projection of one mission run.",
        client_method="get",
        arguments=(("run_id", "opaque_id", True),),
    ),
    _ToolSpec(
        name="mission_events",
        description=(
            "Read ordered mission-run events after an opaque cursor; replay "
            "from the same cursor has no gaps or duplicates."
        ),
        client_method="events",
        arguments=(
            ("run_id", "opaque_id", True),
            ("after", "opaque_id", False),
            ("limit", "limit", False),
        ),
    ),
    _ToolSpec(
        name="mission_result",
        description=(
            "Read the immutable terminal result of one mission run; fails "
            "with not_ready while the run is nonterminal."
        ),
        client_method="result",
        arguments=(("run_id", "opaque_id", True),),
    ),
    _ToolSpec(
        name="mission_cancel",
        description=(
            "Record durable cancellation intent for one mission run; repeat "
            "calls are idempotent and completion races resolve to the "
            "committed execution fact."
        ),
        client_method="cancel",
        arguments=(("run_id", "opaque_id", True),),
    ),
    _ToolSpec(
        name="mission_list",
        description="List mission runs owned by the authenticated principal.",
        client_method="list_runs",
        arguments=(("limit", "limit", False),),
    ),
)

_SPECS_BY_NAME = {spec.name: spec for spec in _TOOL_SPECS}


def _input_schema(spec: _ToolSpec) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            argument: dict(_ARGUMENT_KINDS[kind][0]) for argument, kind, _ in spec.arguments
        },
        "additionalProperties": False,
    }
    required = [argument for argument, _, is_required in spec.arguments if is_required]
    if required:
        schema["required"] = required
    return schema


def _validate_arguments(spec: _ToolSpec, arguments: dict[str, Any]) -> dict[str, Any]:
    _reject_unknown(arguments, {argument for argument, _, _ in spec.arguments})
    validated: dict[str, Any] = {}
    for argument, kind, is_required in spec.arguments:
        if argument not in arguments:
            if is_required:
                raise MissionToolError("invalid_argument", f"missing required argument: {argument}")
            continue
        validated[argument] = _ARGUMENT_KINDS[kind][1](arguments[argument], argument)
    return validated


TOOLS: tuple[dict[str, Any], ...] = tuple(
    {"name": spec.name, "description": spec.description, "inputSchema": _input_schema(spec)}
    for spec in _TOOL_SPECS
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
            while True:
                raw_line = source.readline(_MAX_FRAME_CHARS + 1)
                if raw_line == "":
                    break
                if len(raw_line) > _MAX_FRAME_CHARS:
                    # Discard the rest of the oversized frame in bounded
                    # chunks; never buffer an unbounded line.
                    while raw_line and not raw_line.endswith("\n"):
                        raw_line = source.readline(_MAX_FRAME_CHARS + 1)
                    response = _error_frame(None, -32700, "Frame too large")
                    try:
                        sink.write(json.dumps(response, sort_keys=True) + "\n")
                        sink.flush()
                    except OSError:
                        break
                    continue
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
        spec = _SPECS_BY_NAME.get(name)
        if spec is None:
            return _error_frame(request_id, -32602, f"Unknown tool: {name}")
        try:
            arguments = _require_arguments(params.get("arguments"))
            validated = _validate_arguments(spec, arguments)
            payload = getattr(self._client, spec.client_method)(**validated)
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

    # -- rendering ----------------------------------------------------------

    def _tool_content(self, payload: dict[str, Any], *, is_error: bool) -> dict[str, Any]:
        # Defense in depth: redact structurally, before serialization, so a
        # credential containing JSON-escaped characters (quote, backslash)
        # can never survive as a non-contiguous escaped substring.
        if self._config.credential:
            payload = _redact_value(payload, self._config.credential)
        rendered = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        limit = self._config.max_result_bytes
        if len(rendered.encode("utf-8")) > limit:
            rendered = _truncation_envelope(rendered, limit)
        return {
            "content": [{"type": "text", "text": rendered}],
            "isError": is_error,
        }


def _redact_value(value: Any, secret: str) -> Any:
    """Replace ``secret`` inside every string of a JSON-shaped value."""

    if isinstance(value, str):
        return value.replace(secret, "[redacted]")
    if isinstance(value, list):
        return [_redact_value(item, secret) for item in value]
    if isinstance(value, dict):
        return {
            _redact_value(key, secret): _redact_value(item, secret) for key, item in value.items()
        }
    return value


def _truncation_envelope(rendered: str, limit: int) -> str:
    """Render an explicit truncation marker whose FULL serialized form fits
    ``limit`` bytes: shrink the raw prefix until the envelope, with all JSON
    escaping applied, measures within the bound."""

    prefix = rendered.encode("utf-8")[:limit].decode("utf-8", errors="ignore")
    while True:
        envelope = json.dumps(
            {"truncated": True, "limit_bytes": limit, "content_prefix": prefix},
            sort_keys=True,
            ensure_ascii=False,
        )
        overshoot = len(envelope.encode("utf-8")) - limit
        if overshoot <= 0 or not prefix:
            return envelope
        prefix = prefix[: -max(1, overshoot)]


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

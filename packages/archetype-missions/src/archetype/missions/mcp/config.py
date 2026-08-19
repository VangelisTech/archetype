# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Trusted host configuration for the Mission MCP server.

Issue #810 boundary: every transport coordinate — base URL, credential,
TLS trust, and limits — is fixed by the host environment that launched the
process. Tool arguments carry domain inputs and opaque ids only; nothing a
model supplies can reach or override a value defined here.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

ENV_BASE_URL = "ARCHETYPE_MISSIONS_MCP_URL"
ENV_CREDENTIAL = "ARCHETYPE_MISSIONS_MCP_CREDENTIAL"
ENV_CREDENTIAL_FILE = "ARCHETYPE_MISSIONS_MCP_CREDENTIAL_FILE"
ENV_TIMEOUT_SECONDS = "ARCHETYPE_MISSIONS_MCP_TIMEOUT_SECONDS"
ENV_MAX_EVENTS_PAGE = "ARCHETYPE_MISSIONS_MCP_MAX_EVENTS_PAGE"
ENV_MAX_RESULT_BYTES = "ARCHETYPE_MISSIONS_MCP_MAX_RESULT_BYTES"
ENV_MAX_TASKS = "ARCHETYPE_MISSIONS_MCP_MAX_TASKS"
ENV_MAX_PROMPT_BYTES = "ARCHETYPE_MISSIONS_MCP_MAX_PROMPT_BYTES"

_DEFAULT_BASE_URL = "http://localhost:8000"


class McpHostConfigError(ValueError):
    """Closed configuration failure; messages never carry credential bytes."""


def _positive_int(environ: dict[str, str], name: str, default: int) -> int:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise McpHostConfigError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise McpHostConfigError(f"{name} must be a positive integer")
    return value


def _positive_float(environ: dict[str, str], name: str, default: float) -> float:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw.strip())
    except ValueError as exc:
        raise McpHostConfigError(f"{name} must be a positive number") from exc
    if value <= 0:
        raise McpHostConfigError(f"{name} must be a positive number")
    return value


def _base_url(environ: dict[str, str]) -> str:
    raw = environ.get(ENV_BASE_URL, "").strip() or _DEFAULT_BASE_URL
    parts = urlsplit(raw)
    if parts.scheme not in {"http", "https"}:
        raise McpHostConfigError(f"{ENV_BASE_URL} must be an http(s) URL")
    if not parts.hostname:
        raise McpHostConfigError(f"{ENV_BASE_URL} must name a host")
    if parts.username is not None or parts.password is not None:
        raise McpHostConfigError(f"{ENV_BASE_URL} must not embed credentials")
    if parts.query or parts.fragment:
        raise McpHostConfigError(f"{ENV_BASE_URL} must not carry a query or fragment")
    return raw.rstrip("/")


def _credential(environ: dict[str, str]) -> str | None:
    inline = environ.get(ENV_CREDENTIAL, "").strip()
    file_path = environ.get(ENV_CREDENTIAL_FILE, "").strip()
    if inline and file_path:
        raise McpHostConfigError(f"set {ENV_CREDENTIAL} or {ENV_CREDENTIAL_FILE}, not both")
    if file_path:
        try:
            inline = Path(file_path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise McpHostConfigError(f"{ENV_CREDENTIAL_FILE} is not a readable file") from exc
        if not inline:
            raise McpHostConfigError(f"{ENV_CREDENTIAL_FILE} names an empty file")
    if not inline:
        return None
    if any(ord(char) < 0x21 or ord(char) > 0x7E for char in inline):
        raise McpHostConfigError(
            "the configured credential must be printable ASCII without whitespace"
        )
    return inline


@dataclass(frozen=True, slots=True)
class McpHostConfig:
    """Immutable process-lifetime transport configuration."""

    base_url: str
    credential: str | None
    timeout_seconds: float
    max_events_page: int
    max_result_bytes: int
    max_tasks: int
    max_prompt_bytes: int

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> McpHostConfig:
        """Resolve trusted host configuration once at process start."""

        source = dict(os.environ if environ is None else environ)
        return cls(
            base_url=_base_url(source),
            credential=_credential(source),
            timeout_seconds=_positive_float(source, ENV_TIMEOUT_SECONDS, 30.0),
            max_events_page=_positive_int(source, ENV_MAX_EVENTS_PAGE, 100),
            max_result_bytes=_positive_int(source, ENV_MAX_RESULT_BYTES, 65536),
            max_tasks=_positive_int(source, ENV_MAX_TASKS, 32),
            max_prompt_bytes=_positive_int(source, ENV_MAX_PROMPT_BYTES, 65536),
        )

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed HTTP adapter over the supported MissionRun REST contract.

The client exposes exactly the six documented mission-control operations
(issue #809). There is no generic request surface: paths are fixed
templates, opaque ids are validated before they may enter a URL or header,
and the client never follows a redirect, so a response cannot re-aim it at
another origin. Error text stays bounded and carries no credential bytes.

The submit body mirrors ``archetype.missions.api.MissionRunSubmitRequest``
field for field (issue #833). Optional fields the REST model defaults
(``base_ref``, ``name``, validator ``expected_returncode`` and
``timeout_seconds``, task ``max_dispatches``) are omitted when the caller
does not supply them, so the server stays the single owner of every
default. The offline contract tests in ``tests/missions/mcp/`` validate
the serialized body directly against that pydantic model, so any drift
from the shipped REST schema fails CI instead of 422ing in production.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from archetype.missions.mcp.config import McpHostConfig

# Opaque ids may enter a URL path segment, query value, or header value.
# The charset therefore excludes separators, whitespace, and control bytes,
# which closes path traversal, query/fragment smuggling, and header
# injection at the validation boundary.
_OPAQUE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")

#: Advertised in every opaque-id tool schema; the same expression
#: :func:`require_opaque_id` enforces at runtime.
OPAQUE_ID_PATTERN = _OPAQUE_ID.pattern

_RUNS_PATH = "/v1/mission-runs"


class MissionToolError(Exception):
    """Bounded, model-visible tool failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def require_opaque_id(value: object, *, label: str) -> str:
    """Return ``value`` when it is a safe opaque id; fail closed otherwise."""

    if not isinstance(value, str) or not _OPAQUE_ID.fullmatch(value):
        raise MissionToolError(
            "invalid_argument",
            f"{label} must be an opaque id of at most 256 URL-safe characters",
        )
    return value


def _bounded_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return ""
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if not isinstance(detail, str):
        return ""
    return detail[:200]


class MissionRunClient:
    """Six exact operations over trusted host transport configuration."""

    def __init__(
        self,
        config: McpHostConfig,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._config = config
        headers = {"Accept": "application/json"}
        if config.credential is not None:
            headers["Authorization"] = f"Bearer {config.credential}"
        self._http = httpx.Client(
            base_url=config.base_url,
            headers=headers,
            timeout=config.timeout_seconds,
            follow_redirects=False,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    # -- exact operations ---------------------------------------------------

    def submit(
        self,
        *,
        profile_id: str,
        repository: str,
        branch: str,
        tasks: list[dict[str, Any]],
        idempotency_key: str,
        base_ref: str | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        key = require_opaque_id(idempotency_key, label="idempotency_key")
        body: dict[str, Any] = {
            "profile_id": profile_id,
            "repository": repository,
            "branch": branch,
            "tasks": tasks,
        }
        if base_ref is not None:
            body["base_ref"] = base_ref
        if name is not None:
            body["name"] = name
        response = self._request(
            "POST",
            _RUNS_PATH,
            json=body,
            headers={"Idempotency-Key": key},
        )
        return self._payload(response, ok={202})

    def get(self, run_id: str) -> dict[str, Any]:
        path = f"{_RUNS_PATH}/{require_opaque_id(run_id, label='run_id')}"
        return self._payload(self._request("GET", path), ok={200})

    def events(
        self, run_id: str, *, after: str | None = None, limit: int | None = None
    ) -> dict[str, Any]:
        path = f"{_RUNS_PATH}/{require_opaque_id(run_id, label='run_id')}/events"
        params: dict[str, str] = {"limit": str(self._clamped_limit(limit))}
        if after is not None:
            params["after"] = require_opaque_id(after, label="after")
        return self._payload(self._request("GET", path, params=params), ok={200})

    def result(self, run_id: str) -> dict[str, Any]:
        path = f"{_RUNS_PATH}/{require_opaque_id(run_id, label='run_id')}/result"
        return self._payload(self._request("GET", path), ok={200})

    def cancel(self, run_id: str) -> dict[str, Any]:
        path = f"{_RUNS_PATH}/{require_opaque_id(run_id, label='run_id')}/cancel"
        return self._payload(self._request("POST", path), ok={200, 202})

    def list_runs(self, *, limit: int | None = None) -> dict[str, Any]:
        params = {"limit": str(self._clamped_limit(limit))}
        return self._payload(self._request("GET", _RUNS_PATH, params=params), ok={200})

    # -- transport ----------------------------------------------------------

    def _clamped_limit(self, limit: int | None) -> int:
        maximum = self._config.max_events_page
        if limit is None:
            return maximum
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise MissionToolError("invalid_argument", "limit must be a positive integer")
        return min(limit, maximum)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        try:
            return self._http.request(method, path, params=params, json=json, headers=headers)
        except httpx.HTTPError as exc:
            raise MissionToolError(
                "unavailable",
                f"mission control transport failure ({type(exc).__name__})",
            ) from exc

    def _payload(self, response: httpx.Response, *, ok: set[int]) -> dict[str, Any]:
        status = response.status_code
        if status in ok:
            try:
                payload = response.json()
            except ValueError as exc:
                raise MissionToolError(
                    "protocol_error", "mission control returned a non-JSON body"
                ) from exc
            if not isinstance(payload, dict):
                raise MissionToolError(
                    "protocol_error", "mission control returned a non-object body"
                )
            return payload
        if 300 <= status < 400:
            raise MissionToolError(
                "protocol_error",
                f"mission control returned an unexpected redirect ({status}); "
                "redirects are never followed",
            )
        code = {
            401: "unauthenticated",
            403: "forbidden",
            404: "not_found",
            409: "conflict",
            425: "not_ready",
            422: "invalid_argument",
        }.get(status, "upstream_error")
        detail = _bounded_detail(response)
        message = f"mission control rejected the request ({status})"
        if detail:
            message = f"{message}: {detail}"
        raise MissionToolError(code, message)

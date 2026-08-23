# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Offline fake MissionRun control surface for Mission MCP contract tests.

Contract binding (issue #833): :class:`FakeMissionControl` is pinned to the
shipped REST surface in ``archetype.missions.api``, not to a hand-mirrored
copy of the adapter's own expectations. Every submit body is validated
against the real ``MissionRunSubmitRequest`` pydantic model
(``extra="forbid"``) and answered with a real
``MissionRunAcceptedResponse`` payload, and run states use the real
``MissionRunState`` literals — so an adapter that drifts from the shipped
schema 422s in these offline tests exactly as it would in production.
Read-path payloads remain bounded fake projections.
"""

from __future__ import annotations

import hashlib
import io
import json
import threading
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import ValidationError

from archetype.missions.api import (
    MissionRunAcceptedResponse,
    MissionRunProfileResponse,
    MissionRunSubmitRequest,
)
from archetype.missions.mcp.config import McpHostConfig
from archetype.missions.mcp.server import MissionMcpServer

CREDENTIAL = "mcp-test-credential-0123456789abcdef"


@dataclass
class RecordedRequest:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: bytes


@dataclass
class FakeRun:
    run_id: str
    profile_id: str = "profile-default"
    state: str = "accepted"
    request_digest: str = ""
    events: list[dict[str, Any]] = field(default_factory=list)
    result: dict[str, Any] | None = None

    def projection(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "profile_id": self.profile_id,
            "state": self.state,
            "request_digest": self.request_digest,
        }


class FakeMissionControl:
    """Threading HTTP fake bound to loopback, recording every request."""

    def __init__(self) -> None:
        self.credential = CREDENTIAL
        self.requests: list[RecordedRequest] = []
        self.runs: dict[str, FakeRun] = {}
        self.submissions: dict[str, tuple[str, str]] = {}
        self.foreign_run_ids: set[str] = set()
        self.redirect_next = False
        self.fail_next_status: int | None = None
        self.fail_next_detail = ""
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        control = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: object) -> None:
                pass

            def do_GET(self) -> None:
                control._handle(self, "GET")

            def do_POST(self) -> None:
                control._handle(self, "POST")

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        assert self._server is not None
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    # -- seeding helpers ----------------------------------------------------

    def seed_run(
        self,
        *,
        state: str = "running",
        events: int = 0,
        result: dict[str, Any] | None = None,
        foreign: bool = False,
    ) -> FakeRun:
        run = FakeRun(run_id=f"run-{uuid.uuid4().hex[:12]}", state=state)
        run.events = [
            {
                "event_id": f"{run.run_id}-event-{index}",
                "cursor": str(index),
                "schema_version": 1,
                "ts": f"2026-08-18T00:00:{index:02d}Z",
                "phase": "task",
                "type": "progress",
                "payload": {"message": f"step {index}"},
            }
            for index in range(1, events + 1)
        ]
        run.result = result
        self.runs[run.run_id] = run
        if foreign:
            self.foreign_run_ids.add(run.run_id)
        return run

    # -- request handling ---------------------------------------------------

    def _handle(self, handler: BaseHTTPRequestHandler, method: str) -> None:
        parts = urlsplit(handler.path)
        length = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(length) if length else b""
        self.requests.append(
            RecordedRequest(
                method=method,
                path=parts.path,
                query=parse_qs(parts.query),
                headers={key: value for key, value in handler.headers.items()},
                body=body,
            )
        )
        if self.redirect_next:
            self.redirect_next = False
            self._respond(
                handler,
                302,
                {"detail": "moved"},
                extra_headers={"Location": f"{self.base_url}/v1/mission-runs/evil-target"},
            )
            return
        if self.fail_next_status is not None:
            status = self.fail_next_status
            self.fail_next_status = None
            self._respond(handler, status, {"detail": self.fail_next_detail})
            return
        if handler.headers.get("Authorization") != f"Bearer {self.credential}":
            self._respond(handler, 401, {"detail": "Authentication required"})
            return
        segments = [segment for segment in parts.path.split("/") if segment]
        if segments[:2] != ["v1", "mission-runs"]:
            self._respond(handler, 404, {"detail": "Not found"})
            return
        rest = segments[2:]
        if method == "POST" and not rest:
            self._submit(handler, body)
            return
        if method == "GET" and not rest:
            self._list(handler)
            return
        if not rest:
            self._respond(handler, 404, {"detail": "Not found"})
            return
        run = self.runs.get(rest[0])
        if run is None or rest[0] in self.foreign_run_ids:
            self._respond(handler, 404, {"detail": "Unknown mission run"})
            return
        if method == "GET" and len(rest) == 1:
            self._respond(handler, 200, run.projection())
            return
        if method == "GET" and rest[1:] == ["events"]:
            self._events(handler, run, parse_qs(parts.query))
            return
        if method == "GET" and rest[1:] == ["result"]:
            self._result(handler, run)
            return
        if method == "POST" and rest[1:] == ["cancel"]:
            self._cancel(handler, run)
            return
        self._respond(handler, 404, {"detail": "Not found"})

    def _submit(self, handler: BaseHTTPRequestHandler, body: bytes) -> None:
        key = handler.headers.get("Idempotency-Key")
        if not key:
            self._respond(handler, 422, {"detail": "Idempotency-Key header required"})
            return
        try:
            payload = json.loads(body.decode("utf-8"))
        except ValueError:
            self._respond(handler, 422, {"detail": "Body must be JSON"})
            return
        try:
            request = MissionRunSubmitRequest.model_validate(payload)
        except ValidationError as exc:
            # The real route rejects with 422 through the same model
            # (extra="forbid"); mirroring it here is what keeps the offline
            # suite honest about the shipped submit schema (issue #833).
            self._respond(
                handler,
                422,
                {"detail": f"MissionRunSubmitRequest rejected the body: {exc.error_count()}"},
            )
            return
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        existing = self.submissions.get(key)
        if existing is not None:
            recorded_digest, run_id = existing
            if recorded_digest != digest:
                self._respond(
                    handler,
                    409,
                    {"detail": "Idempotency-Key reuse with a different request"},
                )
                return
            self._respond(handler, 202, self._accepted(self.runs[run_id]))
            return
        run = FakeRun(
            run_id=f"run-{uuid.uuid4().hex[:12]}",
            profile_id=request.profile_id,
            state="accepted",
            request_digest=digest,
        )
        self.runs[run.run_id] = run
        self.submissions[key] = (digest, run.run_id)
        self._respond(handler, 202, self._accepted(run))

    @staticmethod
    def _accepted(run: FakeRun) -> dict[str, Any]:
        return MissionRunAcceptedResponse(
            run_id=run.run_id,
            state=run.state,  # type: ignore[arg-type]  # pydantic enforces the literal
            request_digest=run.request_digest,
            profile=MissionRunProfileResponse(
                profile_id=run.profile_id,
                version="1",
                digest="fake-profile-digest",
            ),
            status_url=f"/v1/mission-runs/{run.run_id}",
        ).model_dump(mode="json")

    def _list(self, handler: BaseHTTPRequestHandler) -> None:
        runs = [
            run.projection()
            for run_id, run in sorted(self.runs.items())
            if run_id not in self.foreign_run_ids
        ]
        self._respond(handler, 200, {"runs": runs})

    def _events(
        self,
        handler: BaseHTTPRequestHandler,
        run: FakeRun,
        query: dict[str, list[str]],
    ) -> None:
        after = int(query.get("after", ["0"])[0])
        limit = int(query.get("limit", ["100"])[0])
        selected = [event for event in run.events if int(event["cursor"]) > after][:limit]
        next_cursor = selected[-1]["cursor"] if selected else str(after)
        self._respond(handler, 200, {"events": selected, "next_cursor": next_cursor})

    def _result(self, handler: BaseHTTPRequestHandler, run: FakeRun) -> None:
        if run.result is None:
            self._respond(handler, 425, {"detail": "Mission run is not terminal"})
            return
        self._respond(
            handler,
            200,
            {"run_id": run.run_id, "state": run.state, "result": run.result},
        )

    def _cancel(self, handler: BaseHTTPRequestHandler, run: FakeRun) -> None:
        if run.state in {"succeeded", "failed", "cancelled", "interrupted"}:
            self._respond(handler, 200, {"run_id": run.run_id, "state": run.state})
            return
        run.state = "cancelling"
        self._respond(handler, 202, {"run_id": run.run_id, "state": "cancelling"})

    def _respond(
        self,
        handler: BaseHTTPRequestHandler,
        status: int,
        payload: dict[str, Any],
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(encoded)))
        for name, value in (extra_headers or {}).items():
            handler.send_header(name, value)
        handler.end_headers()
        handler.wfile.write(encoded)


@pytest.fixture
def fake_control():
    control = FakeMissionControl()
    control.start()
    yield control
    control.stop()


def host_environ(control: FakeMissionControl, **overrides: str) -> dict[str, str]:
    environ = {
        "ARCHETYPE_MISSIONS_MCP_URL": control.base_url,
        "ARCHETYPE_MISSIONS_MCP_CREDENTIAL": control.credential,
    }
    environ.update(overrides)
    return environ


@pytest.fixture
def host_config(fake_control):
    return McpHostConfig.from_env(host_environ(fake_control))


@pytest.fixture
def stderr_sink():
    return io.StringIO()


@pytest.fixture
def mcp_server(host_config, stderr_sink):
    server = MissionMcpServer(host_config, stderr=stderr_sink)
    yield server
    server.close()


def call_tool(
    server: MissionMcpServer, name: str, arguments: dict[str, Any] | None = None
) -> tuple[bool, dict[str, Any]]:
    """Invoke one tool on ``server`` through the full MCP frame path."""

    frame = server.handle_message(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}},
        }
    )
    assert frame is not None and "result" in frame, frame
    result = frame["result"]
    payload = json.loads(result["content"][0]["text"])
    return result["isError"], payload


@pytest.fixture
def call(mcp_server):
    def _call(name: str, arguments: dict[str, Any] | None = None):
        return call_tool(mcp_server, name, arguments)

    return _call


def submit_arguments(**overrides: Any) -> dict[str, Any]:
    arguments: dict[str, Any] = {
        "profile_id": "profile-default",
        "repository": "vangelis/archetype",
        "branch": "agent/demo-mission",
        "base_ref": "main",
        "name": "demo-mission",
        "tasks": [
            {
                "name": "fix-bug",
                "prompt": "Fix the reported bug.",
                "validators": [{"name": "pytest", "command": ["pytest", "-q"]}],
            }
        ],
        "idempotency_key": "key-0001",
    }
    arguments.update(overrides)
    return arguments

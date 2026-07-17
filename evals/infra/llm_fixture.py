# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free OpenAI-compatible transport fixture for LLM capability evals."""

from __future__ import annotations

import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_KNOWN_CASES = ("healthy", "timeout", "quota")


class _FixtureState:
    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._lock = threading.Lock()

    def record(self, case: str) -> None:
        with self._lock:
            self._counts[case] += 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {case: self._counts[case] for case in _KNOWN_CASES}


class _FixtureHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        state: _FixtureState,
        timeout_delay_seconds: float,
    ) -> None:
        self.fixture_state = state
        self.timeout_delay_seconds = timeout_delay_seconds
        super().__init__(address, _OpenAIHandler)


class _OpenAIHandler(BaseHTTPRequestHandler):
    server: _FixtureHTTPServer

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler protocol
        if self.path != "/v1/chat/completions":
            self._write_json(404, {"error": {"message": "unknown fixture route"}})
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        case = self._case_from(body)
        self.server.fixture_state.record(case)

        if case == "timeout":
            time.sleep(self.server.timeout_delay_seconds)
        if case == "quota":
            self._write_json(
                429,
                {
                    "error": {
                        "message": "fixture provider quota exhausted",
                        "type": "rate_limit_error",
                        "code": "rate_limit_exceeded",
                    }
                },
                headers={"Retry-After": "0"},
            )
            return

        self._write_json(
            200,
            {
                "id": f"chatcmpl-{case}",
                "object": "chat.completion",
                "created": 0,
                "model": body.get("model", "fixture-model"),
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": f"provider:{case}"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            },
        )

    @staticmethod
    def _case_from(body: dict[str, Any]) -> str:
        messages = body.get("messages", [])
        content = messages[-1].get("content", []) if messages else []
        if isinstance(content, list):
            text = " ".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
        else:
            text = str(content)
        for case in _KNOWN_CASES:
            if case in text:
                return case
        raise ValueError(f"fixture request did not name one of {_KNOWN_CASES!r}: {text!r}")

    def _write_json(
        self,
        status: int,
        payload: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            # The timeout case deliberately responds after the client has left.
            pass

    def log_message(self, _format: str, *args: Any) -> None:
        """Keep expected local fixture traffic out of eval output."""


class LocalOpenAIServer:
    """Serve deterministic success, timeout, and quota responses on loopback."""

    def __init__(self, *, timeout_delay_seconds: float = 0.25) -> None:
        self._state = _FixtureState()
        self._timeout_delay_seconds = timeout_delay_seconds
        self._server: _FixtureHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> LocalOpenAIServer:
        self._server = _FixtureHTTPServer(
            ("127.0.0.1", 0),
            self._state,
            self._timeout_delay_seconds,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="archetype-llm-eval-fixture",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join()
        self._server = None
        self._thread = None

    @property
    def base_url(self) -> str:
        if self._server is None:
            raise RuntimeError("LocalOpenAIServer is not running")
        return f"http://127.0.0.1:{self._server.server_port}/v1"

    @property
    def request_counts(self) -> dict[str, int]:
        return self._state.snapshot()

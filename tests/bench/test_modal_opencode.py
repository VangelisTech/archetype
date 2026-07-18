# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the paid Modal/OpenCode benchmark runner."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from archetype.experiments.modal_coding_agent import ModalSandboxSpec
from bench.agents.modal_opencode import (
    AgentBenchmarkConfig,
    EndpointBenchmarkConfig,
    build_agent_report,
    build_endpoint_report,
    load_shared_prefix,
    main,
    run_agent_benchmark,
    run_endpoint_benchmark,
)


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (EndpointBenchmarkConfig(concurrency_levels=()), "concurrency_levels"),
        (EndpointBenchmarkConfig(concurrency_levels=(4, 1)), "unique and increasing"),
        (EndpointBenchmarkConfig(requests_per_worker=0), "requests_per_worker"),
        (AgentBenchmarkConfig(setup_concurrency=0), "setup_concurrency"),
        (AgentBenchmarkConfig(snapshot_ttl_seconds=0), "snapshot_ttl_seconds"),
    ],
)
def test_modal_benchmark_rejects_degenerate_workloads(
    config: EndpointBenchmarkConfig | AgentBenchmarkConfig,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        config.validate()


def test_shared_prefix_repeats_repository_context_exactly(tmp_path: Path) -> None:
    source = tmp_path / "context.md"
    source.write_text("abc\ndef\n")

    prefix = load_shared_prefix(source, 25)

    assert len(prefix) == 25
    assert prefix == ("abc\ndef\n" * 4)[:25]


@pytest.mark.asyncio
async def test_endpoint_sweep_enforces_concurrency_and_emits_secret_free_report(
    tmp_path: Path,
) -> None:
    active = 0
    max_active = 0
    seen_headers: list[dict[str, str]] = []

    async def request(
        _client: Any,
        _endpoint: str,
        headers: Any,
        payload: Any,
        request_index: int,
    ) -> dict[str, Any]:
        nonlocal active, max_active
        seen_headers.append(dict(headers))
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.005)
        active -= 1
        assert payload["stream"] is True
        return {
            "request_index": request_index,
            "success": True,
            "status_code": 200,
            "error_type": "",
            "latency_s": 0.01,
            "ttft_s": 0.003,
            "prompt_tokens": 100,
            "completion_tokens": 10,
            "total_tokens": 110,
        }

    config = EndpointBenchmarkConfig(
        concurrency_levels=(1, 3),
        requests_per_worker=2,
        warmup_requests=1,
        target_prefix_chars=10,
    )
    results = await run_endpoint_benchmark(
        config,
        shared_prefix="0123456789",
        token_id="id-sensitive",
        token_secret="secret-sensitive",
        request=request,
    )

    assert [result["request_count"] for result in results] == [2, 6]
    assert [result["max_observed_client_concurrency"] for result in results] == [1, 3]
    assert results[1]["prompt_tokens"] == 600
    assert results[1]["completion_tokens"] == 60
    assert results[1]["success_rate"] == 1.0
    assert max_active == 3
    assert seen_headers[0] == {
        "Modal-Key": "id-sensitive",
        "Modal-Secret": "secret-sensitive",
    }

    report = build_endpoint_report(
        results,
        config=config,
        prefix_path=tmp_path / "context.md",
        shared_prefix="0123456789",
        runner_id="test-runner",
    )
    rendered = json.dumps(report)
    assert report["suite"] == "modal_openai_compatible_endpoint"
    assert "id-sensitive" not in rendered
    assert "secret-sensitive" not in rendered


@pytest.mark.asyncio
async def test_endpoint_warmup_retries_transient_unavailability_outside_measurement() -> None:
    calls = 0

    async def request(
        _client: Any,
        _endpoint: str,
        _headers: Any,
        _payload: Any,
        request_index: int,
    ) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        success = calls >= 3
        return {
            "request_index": request_index,
            "success": success,
            "status_code": 200 if success else 503,
            "error_type": "" if success else "HTTPStatusError",
            "latency_s": 0.001,
            "ttft_s": 0.001 if success else None,
            "prompt_tokens": 10 if success else 0,
            "completion_tokens": 1 if success else 0,
            "total_tokens": 11 if success else 0,
        }

    config = EndpointBenchmarkConfig(
        concurrency_levels=(1,),
        requests_per_worker=1,
        warmup_requests=1,
        warmup_timeout_seconds=1,
        warmup_retry_seconds=0.001,
        target_prefix_chars=1,
    )
    results = await run_endpoint_benchmark(
        config,
        shared_prefix="x",
        token_id="id",
        token_secret="secret",
        request=request,
    )

    assert calls == 4
    assert results[0]["request_count"] == 1
    assert results[0]["warmup_attempt_count"] == 3
    assert results[0]["warmup_elapsed_s"] > 0


class _FakeAgentClient:
    def __init__(self, spec: ModalSandboxSpec, object_id: str) -> None:
        self.spec = spec
        self._object_id = object_id
        self.closed = False

    @property
    def sandbox_id(self) -> str:
        return self._object_id

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(0.002)
        previous_session_id = str(kwargs.get("previous_session_id") or "")
        return {
            "accepted": True,
            "checkpoint_restorable": self.spec.snapshot_after_attempt,
            "sandbox_state_ref": (
                f"modal-image://im-{self._object_id}" if self.spec.snapshot_after_attempt else ""
            ),
            "agent_session_id": previous_session_id or f"session-{self._object_id}",
            "agent_returncode": 0,
            "agent_completed": True,
            "validator_details": [{"name": "exact_benchmark_change", "passed": True}],
            "friction": [],
            "sha": f"sha-{self._object_id}",
        }

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_agent_sweep_proves_new_sandbox_same_session_before_fanout() -> None:
    created: list[_FakeAgentClient] = []
    resumed: list[tuple[ModalSandboxSpec, str]] = []

    async def create(spec: ModalSandboxSpec) -> _FakeAgentClient:
        client = _FakeAgentClient(spec, f"sb-created-{len(created)}")
        created.append(client)
        return client

    async def resume(spec: ModalSandboxSpec, checkpoint_ref: str) -> _FakeAgentClient:
        resumed.append((spec, checkpoint_ref))
        client = _FakeAgentClient(spec, "sb-resumed")
        created.append(client)
        return client

    config = AgentBenchmarkConfig(
        concurrency_levels=(1, 3),
        setup_concurrency=2,
        resume_preflight=True,
    )
    results = await run_agent_benchmark(config, create=create, resume=resume)

    assert [result["name"] for result in results] == [
        "cross_sandbox_resume_preflight",
        "opencode_agents_concurrency_1",
        "opencode_agents_concurrency_3",
    ]
    assert results[0]["passed"] is True
    assert results[0]["source_sandbox_id"] != results[0]["resumed_sandbox_id"]
    assert resumed[0][1].startswith("modal-image://")
    assert results[1]["accepted_rate"] == 1.0
    assert results[2]["accepted_count"] == 3
    assert len(created) == 6
    assert all(client.closed for client in created)
    assert created[0].spec.snapshot_after_attempt is True
    assert all(not client.spec.snapshot_after_attempt for client in created[2:])

    report = build_agent_report(results, config=config, runner_id="test-runner")
    assert report["suite"] == "modal_opencode_agent_fanout"
    assert report["config"]["snapshot_during_sweep"] is False


def test_benchmark_cli_requires_explicit_paid_confirmation(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="confirm-paid-run"):
        main(["endpoint", "--out", str(tmp_path / "results.json")])

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Benchmark one protected Modal endpoint and OpenCode-on-Modal fanout.

The two subcommands deliberately answer different questions:

* ``endpoint`` measures streaming Chat Completions without an agent harness;
* ``agents`` measures one independent Modal Sandbox and OpenCode session per
  unit of concurrency, after proving cross-sandbox session continuation.

Both commands are paid, manual experiments. They never run in normal CI and
require ``--confirm-paid-run``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

import httpx

from archetype.experiments.modal_coding_agent import (
    ModalSandboxClient,
    ModalSandboxSpec,
    ValidatorSpec,
)
from bench.core.report import build_report, capture_environment, write_report

DEFAULT_ENDPOINT_URL = (
    "https://vangelis-tech--ep-qwen3-6-35b-a3b-fp8-server.us-west.modal.direct/v1"
)
DEFAULT_MODEL = "Qwen/Qwen3.6-35B-A3B-FP8"
DEFAULT_LEVELS = (1, 4, 8, 16, 24, 32)
DEFAULT_REPO_URL = "https://github.com/VangelisTech/archetype.git"


@dataclass(frozen=True)
class EndpointBenchmarkConfig:
    """Direct endpoint workload dimensions."""

    endpoint_url: str = DEFAULT_ENDPOINT_URL
    model: str = DEFAULT_MODEL
    concurrency_levels: tuple[int, ...] = DEFAULT_LEVELS
    requests_per_worker: int = 2
    warmup_requests: int = 1
    warmup_timeout_seconds: float = 10 * 60
    warmup_retry_seconds: float = 5.0
    target_prefix_chars: int = 120_000
    max_output_tokens: int = 192
    timeout_seconds: float = 330.0
    declared_gpu: str = "H200"
    declared_max_containers: int | None = None
    declared_target_concurrency: int = 32

    def validate(self) -> None:
        _validate_common(self.endpoint_url, self.model, self.concurrency_levels)
        if self.requests_per_worker < 1:
            raise ValueError("requests_per_worker must be positive")
        if self.warmup_requests < 0:
            raise ValueError("warmup_requests must be non-negative")
        if self.warmup_timeout_seconds <= 0 or self.warmup_retry_seconds <= 0:
            raise ValueError("warmup timeout and retry interval must be positive")
        if self.target_prefix_chars < 1:
            raise ValueError("target_prefix_chars must be positive")
        if self.max_output_tokens < 1:
            raise ValueError("max_output_tokens must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        _validate_deployment_declaration(
            self.declared_gpu,
            self.declared_max_containers,
            self.declared_target_concurrency,
        )


@dataclass(frozen=True)
class AgentBenchmarkConfig:
    """One-sandbox-per-agent workload dimensions."""

    endpoint_url: str = DEFAULT_ENDPOINT_URL
    model: str = DEFAULT_MODEL
    concurrency_levels: tuple[int, ...] = DEFAULT_LEVELS
    repo_url: str = DEFAULT_REPO_URL
    base_ref: str = "main"
    secret_name: str = "archetype-modal-endpoint"
    app_name: str = "archetype-opencode-benchmarks"
    image_name: str = ""
    setup_concurrency: int = 8
    agent_timeout_seconds: int = 20 * 60
    sandbox_timeout_seconds: int = 45 * 60
    snapshot_ttl_seconds: int = 24 * 60 * 60
    resume_preflight: bool = True
    declared_gpu: str = "H200"
    declared_max_containers: int | None = None
    declared_target_concurrency: int = 32

    def validate(self) -> None:
        _validate_common(self.endpoint_url, self.model, self.concurrency_levels)
        if not self.repo_url:
            raise ValueError("repo_url must not be empty")
        if not self.base_ref:
            raise ValueError("base_ref must not be empty")
        if not self.secret_name:
            raise ValueError("secret_name must not be empty")
        if self.setup_concurrency < 1:
            raise ValueError("setup_concurrency must be positive")
        if self.agent_timeout_seconds < 1 or self.sandbox_timeout_seconds < 1:
            raise ValueError("agent and sandbox timeouts must be positive")
        if self.snapshot_ttl_seconds < 1:
            raise ValueError("snapshot_ttl_seconds must be positive")
        _validate_deployment_declaration(
            self.declared_gpu,
            self.declared_max_containers,
            self.declared_target_concurrency,
        )


class AgentClient(Protocol):
    """Narrow client surface used by the fanout runner and its tests."""

    @property
    def sandbox_id(self) -> str: ...

    async def run_attempt(self, **kwargs: Any) -> dict[str, Any]: ...

    async def close(self) -> None: ...


EndpointRequest = Callable[
    [httpx.AsyncClient, str, Mapping[str, str], Mapping[str, Any], int],
    Awaitable[dict[str, Any]],
]
AgentFactory = Callable[[ModalSandboxSpec], Awaitable[AgentClient]]
ResumeFactory = Callable[[ModalSandboxSpec, str], Awaitable[AgentClient]]


def load_shared_prefix(path: str | Path, target_chars: int) -> str:
    """Repeat stable repository context to a requested shared-prefix size."""

    if target_chars < 1:
        raise ValueError("target prefix size must be positive")
    source = Path(path).read_text()
    if not source:
        raise ValueError("prefix source must not be empty")
    block = source.rstrip() + "\n"
    return (block * math.ceil(target_chars / len(block)))[:target_chars]


async def run_endpoint_benchmark(
    config: EndpointBenchmarkConfig,
    *,
    shared_prefix: str,
    token_id: str,
    token_secret: str,
    request: EndpointRequest | None = None,
) -> list[dict[str, Any]]:
    """Warm the endpoint, then measure each concurrency level sequentially."""

    config.validate()
    if not shared_prefix:
        raise ValueError("shared_prefix must not be empty")
    if not token_id or not token_secret:
        raise ValueError(
            "endpoint credentials require MODAL_ENDPOINT_TOKEN_ID and MODAL_ENDPOINT_TOKEN_SECRET"
        )
    request = request or _stream_chat_completion
    headers = {"Modal-Key": token_id, "Modal-Secret": token_secret}
    endpoint = f"{config.endpoint_url.rstrip('/')}/chat/completions"
    timeout = httpx.Timeout(config.timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        _progress(
            "endpoint_warmup_started",
            requests=config.warmup_requests,
            endpoint_url=config.endpoint_url,
            model=config.model,
        )
        warmup = await _warm_endpoint(
            client,
            endpoint,
            headers,
            config,
            shared_prefix,
            request,
        )
        results = []
        for concurrency in config.concurrency_levels:
            _progress("endpoint_level_started", concurrency=concurrency)
            result = await _run_endpoint_level(
                client,
                endpoint,
                headers,
                config,
                shared_prefix,
                concurrency,
                request,
            )
            result["warmup_attempt_count"] = warmup["attempt_count"]
            result["warmup_elapsed_s"] = warmup["elapsed_s"]
            results.append(result)
            _progress(
                "endpoint_level_finished",
                concurrency=concurrency,
                success_rate=result["success_rate"],
                total_tokens_per_s=result["total_tokens_per_s"],
            )
        return results


async def _warm_endpoint(
    client: httpx.AsyncClient,
    endpoint: str,
    headers: Mapping[str, str],
    config: EndpointBenchmarkConfig,
    shared_prefix: str,
    request: EndpointRequest,
) -> dict[str, Any]:
    if config.warmup_requests == 0:
        return {"attempt_count": 0, "elapsed_s": 0.0}
    started = time.perf_counter()
    deadline = started + config.warmup_timeout_seconds
    attempt_count = 0
    completed = 0
    last_error = "unknown error"
    while completed < config.warmup_requests:
        attempt_count += 1
        sample = await request(
            client,
            endpoint,
            headers,
            _endpoint_payload(config, shared_prefix, -attempt_count),
            -attempt_count,
        )
        if sample.get("success"):
            completed += 1
            continue
        last_error = str(sample.get("error_type") or "unknown error")
        remaining = deadline - time.perf_counter()
        _progress(
            "endpoint_warmup_retry",
            attempt=attempt_count,
            error_type=last_error,
            remaining_seconds=max(0.0, remaining),
        )
        if remaining <= 0:
            raise RuntimeError(
                f"endpoint warmup failed after {attempt_count} attempts: {last_error}"
            )
        await asyncio.sleep(min(config.warmup_retry_seconds, remaining))
    elapsed = time.perf_counter() - started
    _progress(
        "endpoint_warmup_finished",
        successful_requests=completed,
        attempt_count=attempt_count,
        elapsed_s=elapsed,
    )
    return {"attempt_count": attempt_count, "elapsed_s": elapsed}


async def _run_endpoint_level(
    client: httpx.AsyncClient,
    endpoint: str,
    headers: Mapping[str, str],
    config: EndpointBenchmarkConfig,
    shared_prefix: str,
    concurrency: int,
    request: EndpointRequest,
) -> dict[str, Any]:
    request_count = concurrency * config.requests_per_worker
    semaphore = asyncio.Semaphore(concurrency)
    lock = asyncio.Lock()
    active = 0
    max_active = 0

    async def execute(index: int) -> dict[str, Any]:
        nonlocal active, max_active
        async with semaphore:
            async with lock:
                active += 1
                max_active = max(max_active, active)
            try:
                return await request(
                    client,
                    endpoint,
                    headers,
                    _endpoint_payload(config, shared_prefix, index),
                    index,
                )
            finally:
                async with lock:
                    active -= 1

    started = time.perf_counter()
    samples = await asyncio.gather(*(execute(index) for index in range(request_count)))
    elapsed = time.perf_counter() - started
    return _summarize_endpoint_level(concurrency, elapsed, samples, max_active)


async def _stream_chat_completion(
    client: httpx.AsyncClient,
    endpoint: str,
    headers: Mapping[str, str],
    payload: Mapping[str, Any],
    request_index: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    first_token_at: float | None = None
    usage: dict[str, int] = {}
    status_code = 0
    try:
        async with client.stream("POST", endpoint, headers=headers, json=payload) as response:
            status_code = response.status_code
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if not data or data == "[DONE]":
                    continue
                event = json.loads(data)
                event_usage = event.get("usage")
                if isinstance(event_usage, dict):
                    usage = {
                        key: int(event_usage.get(key) or 0)
                        for key in ("prompt_tokens", "completion_tokens", "total_tokens")
                    }
                for choice in event.get("choices") or ():
                    delta = choice.get("delta") or {}
                    if delta.get("content") and first_token_at is None:
                        first_token_at = time.perf_counter()
    except Exception as exc:
        return {
            "request_index": request_index,
            "success": False,
            "status_code": status_code,
            "error_type": type(exc).__name__,
            "latency_s": time.perf_counter() - started,
            "ttft_s": None,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }
    finished = time.perf_counter()
    return {
        "request_index": request_index,
        "success": True,
        "status_code": status_code,
        "error_type": "",
        "latency_s": finished - started,
        "ttft_s": (first_token_at or finished) - started,
        "prompt_tokens": usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
    }


def _endpoint_payload(
    config: EndpointBenchmarkConfig, shared_prefix: str, request_index: int
) -> dict[str, Any]:
    return {
        "model": config.model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a repository coding agent. Use the following shared repository "
                    f"context when answering.\n\n{shared_prefix}"
                ),
            },
            {
                "role": "user",
                "content": (
                    "Identify three concrete engineering risks visible in the supplied context "
                    f"and give one terse mitigation for each. Request nonce: {request_index}."
                ),
            },
        ],
        "max_tokens": config.max_output_tokens,
        "temperature": 0,
        "stream": True,
        "stream_options": {"include_usage": True},
    }


def _summarize_endpoint_level(
    concurrency: int,
    elapsed: float,
    samples: Sequence[Mapping[str, Any]],
    max_active: int,
) -> dict[str, Any]:
    successful = [sample for sample in samples if sample.get("success")]
    latencies = [float(sample["latency_s"]) for sample in successful]
    ttfts = [float(sample["ttft_s"]) for sample in successful if sample.get("ttft_s") is not None]
    prompt_tokens = sum(int(sample.get("prompt_tokens") or 0) for sample in successful)
    completion_tokens = sum(int(sample.get("completion_tokens") or 0) for sample in successful)
    total_tokens = sum(int(sample.get("total_tokens") or 0) for sample in successful)
    errors = Counter(
        str(sample.get("error_type") or "unknown")
        for sample in samples
        if not sample.get("success")
    )
    return {
        "name": f"endpoint_concurrency_{concurrency}",
        "concurrency": concurrency,
        "max_observed_client_concurrency": max_active,
        "request_count": len(samples),
        "success_count": len(successful),
        "success_rate": len(successful) / len(samples),
        "elapsed_s": elapsed,
        "requests_per_s": len(successful) / elapsed,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "prompt_tokens_per_s": prompt_tokens / elapsed,
        "completion_tokens_per_s": completion_tokens / elapsed,
        "total_tokens_per_s": total_tokens / elapsed,
        "latency_p50_s": _percentile(latencies, 50),
        "latency_p95_s": _percentile(latencies, 95),
        "latency_max_s": max(latencies, default=0.0),
        "ttft_p50_s": _percentile(ttfts, 50),
        "ttft_p95_s": _percentile(ttfts, 95),
        "error_types": dict(sorted(errors.items())),
        "samples": sorted(
            (dict(sample) for sample in samples), key=lambda row: row["request_index"]
        ),
    }


async def run_agent_benchmark(
    config: AgentBenchmarkConfig,
    *,
    create: AgentFactory = ModalSandboxClient.create,
    resume: ResumeFactory = ModalSandboxClient.resume,
) -> list[dict[str, Any]]:
    """Prove continuation, then run independent OpenCode sandboxes by level."""

    config.validate()
    run_token = uuid4().hex[:12]
    results: list[dict[str, Any]] = []
    if config.resume_preflight:
        _progress("agent_resume_preflight_started")
        preflight = await _run_resume_preflight(config, run_token, create, resume)
        results.append(preflight)
        _progress(
            "agent_resume_preflight_finished",
            passed=preflight["passed"],
            source_sandbox_id=preflight["source_sandbox_id"],
            resumed_sandbox_id=preflight["resumed_sandbox_id"],
        )
        if not preflight["passed"]:
            return results
    for concurrency in config.concurrency_levels:
        _progress("agent_level_started", concurrency=concurrency)
        results.append(await _run_agent_level(config, run_token, concurrency, create))
        _progress(
            "agent_level_finished",
            concurrency=concurrency,
            accepted_rate=results[-1]["accepted_rate"],
            cleanup_error_count=results[-1]["cleanup_error_count"],
        )
    return results


async def _run_resume_preflight(
    config: AgentBenchmarkConfig,
    run_token: str,
    create: AgentFactory,
    resume: ResumeFactory,
) -> dict[str, Any]:
    spec = _agent_spec(config, f"{run_token}-resume", snapshots=True)
    first: AgentClient | None = None
    second: AgentClient | None = None
    started = time.perf_counter()
    source_sandbox_id = ""
    resumed_sandbox_id = ""
    session_id = ""
    checkpoint_ref = ""
    resumed_checkpoint_ref = ""
    phase_a_commit_sha = ""
    phase_b_commit_sha = ""
    result: dict[str, Any]
    try:
        first = await create(spec)
        source_sandbox_id = first.sandbox_id
        _progress(
            "agent_resume_phase_a_started",
            sandbox_id=source_sandbox_id,
            monitor_command=_monitor_command(source_sandbox_id),
        )
        phase_a = await _agent_attempt(first, f"{run_token}-resume-a", 1)
        if not phase_a.get("accepted") or not phase_a.get("checkpoint_restorable"):
            raise RuntimeError("resume phase A was not accepted with a restorable checkpoint")
        session_id = str(phase_a.get("agent_session_id") or "")
        checkpoint_ref = str(phase_a.get("sandbox_state_ref") or "")
        phase_a_commit_sha = str(phase_a.get("sha") or "")
        await first.close()
        first = None

        second = await resume(spec, checkpoint_ref)
        resumed_sandbox_id = second.sandbox_id
        _progress(
            "agent_resume_phase_b_started",
            sandbox_id=resumed_sandbox_id,
            monitor_command=_monitor_command(resumed_sandbox_id),
        )
        phase_b = await _agent_attempt(
            second,
            f"{run_token}-resume-b",
            2,
            previous_session_id=session_id,
            required_tokens=(f"{run_token}-resume-a",),
        )
        continued_session = str(phase_b.get("agent_session_id") or "")
        if not phase_b.get("accepted") or not phase_b.get("checkpoint_restorable"):
            raise RuntimeError("resume phase B was not accepted with a restorable checkpoint")
        resumed_checkpoint_ref = str(phase_b.get("sandbox_state_ref") or "")
        phase_b_commit_sha = str(phase_b.get("sha") or "")
        if not session_id or continued_session != session_id:
            raise RuntimeError("OpenCode session identity changed after sandbox resume")
        if resumed_sandbox_id == source_sandbox_id:
            raise RuntimeError("resume did not create a distinct sandbox")
        result = {
            "name": "cross_sandbox_resume_preflight",
            "passed": True,
            "elapsed_s": time.perf_counter() - started,
            "source_sandbox_id": source_sandbox_id,
            "resumed_sandbox_id": resumed_sandbox_id,
            "agent_session_id": session_id,
            "source_checkpoint_ref": checkpoint_ref,
            "resumed_checkpoint_ref": resumed_checkpoint_ref,
            "phase_a_commit_sha": phase_a_commit_sha,
            "phase_b_commit_sha": phase_b_commit_sha,
        }
    except Exception as exc:
        result = {
            "name": "cross_sandbox_resume_preflight",
            "passed": False,
            "elapsed_s": time.perf_counter() - started,
            "source_sandbox_id": source_sandbox_id,
            "resumed_sandbox_id": resumed_sandbox_id,
            "agent_session_id": session_id,
            "source_checkpoint_ref": checkpoint_ref,
            "resumed_checkpoint_ref": resumed_checkpoint_ref,
            "phase_a_commit_sha": phase_a_commit_sha,
            "phase_b_commit_sha": phase_b_commit_sha,
            "error_type": type(exc).__name__,
            "error": str(exc)[-1000:],
        }
    finally:
        cleanup_errors = await _close_clients(
            client for client in (second, first) if client is not None
        )
        result["cleanup_error_types"] = cleanup_errors
        result["cleanup_error_count"] = len(cleanup_errors)
        if cleanup_errors:
            result["passed"] = False
    return result


async def _run_agent_level(
    config: AgentBenchmarkConfig,
    run_token: str,
    concurrency: int,
    create: AgentFactory,
) -> dict[str, Any]:
    setup_started = time.perf_counter()
    specs = [
        _agent_spec(config, f"{run_token}-{concurrency}-{index}", snapshots=False)
        for index in range(concurrency)
    ]
    clients = await _create_clients(specs, create, config.setup_concurrency)
    setup_elapsed = time.perf_counter() - setup_started
    _progress(
        "agent_level_ready",
        concurrency=concurrency,
        setup_elapsed_s=setup_elapsed,
        sandboxes=[
            {
                "sandbox_id": client.sandbox_id,
                "monitor_command": _monitor_command(client.sandbox_id),
            }
            for client in clients
        ],
    )
    try:
        execution_started = time.perf_counter()
        samples = await asyncio.gather(
            *(
                _timed_agent_attempt(client, f"{run_token}-{concurrency}-{index}")
                for index, client in enumerate(clients)
            )
        )
        execution_elapsed = time.perf_counter() - execution_started
    finally:
        cleanup_started = time.perf_counter()
        cleanup_errors = await _close_clients(clients)
        cleanup_elapsed = time.perf_counter() - cleanup_started

    accepted = [sample for sample in samples if sample["accepted"]]
    latencies = [float(sample["latency_s"]) for sample in samples]
    errors = Counter(
        str(sample.get("error_type") or "rejected") for sample in samples if not sample["accepted"]
    )
    return {
        "name": f"opencode_agents_concurrency_{concurrency}",
        "passed": len(accepted) == len(samples) and not cleanup_errors,
        "concurrency": concurrency,
        "agent_count": len(samples),
        "accepted_count": len(accepted),
        "accepted_rate": len(accepted) / len(samples),
        "setup_elapsed_s": setup_elapsed,
        "execution_elapsed_s": execution_elapsed,
        "cleanup_elapsed_s": cleanup_elapsed,
        "cleanup_error_count": len(cleanup_errors),
        "cleanup_error_types": cleanup_errors,
        "agents_per_s": len(accepted) / execution_elapsed,
        "latency_p50_s": _percentile(latencies, 50),
        "latency_p95_s": _percentile(latencies, 95),
        "latency_max_s": max(latencies, default=0.0),
        "error_types": dict(sorted(errors.items())),
        "samples": samples,
    }


async def _create_clients(
    specs: Sequence[ModalSandboxSpec], create: AgentFactory, concurrency: int
) -> list[AgentClient]:
    semaphore = asyncio.Semaphore(concurrency)

    async def create_one(spec: ModalSandboxSpec) -> AgentClient:
        async with semaphore:
            return await create(spec)

    values = await asyncio.gather(*(create_one(spec) for spec in specs), return_exceptions=True)
    clients = [value for value in values if not isinstance(value, BaseException)]
    failures = [value for value in values if isinstance(value, BaseException)]
    if failures:
        await _close_clients(clients)
        raise RuntimeError(f"agent sandbox setup failed: {failures[0]}") from failures[0]
    return clients


async def _timed_agent_attempt(client: AgentClient, token: str) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        outcome = await _agent_attempt(client, token, 1)
    except Exception as exc:
        return {
            "sandbox_id": client.sandbox_id,
            "accepted": False,
            "latency_s": time.perf_counter() - started,
            "agent_session_id": "",
            "agent_returncode": None,
            "error_type": type(exc).__name__,
            "error": str(exc)[-1000:],
        }
    return {
        "sandbox_id": client.sandbox_id,
        "accepted": bool(outcome.get("accepted")),
        "latency_s": time.perf_counter() - started,
        "agent_session_id": str(outcome.get("agent_session_id") or ""),
        "agent_returncode": outcome.get("agent_returncode"),
        "agent_completed": bool(outcome.get("agent_completed")),
        "commit_sha": str(outcome.get("sha") or ""),
        "validator_details": list(outcome.get("validator_details") or ()),
        "friction": list(outcome.get("friction") or ()),
        "error_type": "" if outcome.get("accepted") else "validator_rejected",
    }


async def _agent_attempt(
    client: AgentClient,
    token: str,
    attempt_index: int,
    *,
    previous_session_id: str = "",
    required_tokens: Sequence[str] = (),
) -> dict[str, Any]:
    target = f"archetype_agent_benchmark_{token}.txt"
    expected = f"benchmark-ok:{token}\n"
    required = tuple(
        (f"archetype_agent_benchmark_{required_token}.txt", f"benchmark-ok:{required_token}\n")
        for required_token in required_tokens
    )
    validator_source = (
        "from pathlib import Path; import subprocess; "
        f"assert Path({target!r}).read_text() == {expected!r}; "
        + " ".join(f"assert Path({path!r}).read_text() == {value!r};" for path, value in required)
        + " status = subprocess.check_output(['git','status','--porcelain'], text=True); "
        f"assert [line[3:] for line in status.splitlines()] == [{target!r}], status"
    )
    prompt = (
        f"Create {target} with exactly this content, including the newline: {expected!r}. "
        "Do not modify any other tracked file. Do not delegate to a subagent."
    )
    return await client.run_attempt(
        prompt=prompt,
        validators=(
            ValidatorSpec(
                name="exact_benchmark_change",
                command=("python3", "-c", validator_source),
                timeout_seconds=60,
            ),
        ),
        step_name=f"modal-opencode-benchmark-{attempt_index}",
        attempt_index=attempt_index,
        idempotency_key=f"modal-opencode-benchmark:{token}:{attempt_index}",
        previous_session_id=previous_session_id,
    )


def _agent_spec(config: AgentBenchmarkConfig, token: str, *, snapshots: bool) -> ModalSandboxSpec:
    return ModalSandboxSpec(
        repo_url=config.repo_url,
        base_ref=config.base_ref,
        branch=f"benchmark/opencode-{token}",
        app_name=config.app_name,
        image_name=config.image_name,
        harness="opencode",
        auth_mode="api-key",
        opencode_secret_name=config.secret_name,
        model=config.model,
        opencode_base_url=config.endpoint_url,
        workspace="/workspace/repo",
        timeout_seconds=config.sandbox_timeout_seconds,
        idle_timeout_seconds=config.sandbox_timeout_seconds,
        agent_timeout_seconds=config.agent_timeout_seconds,
        snapshot_ttl_seconds=config.snapshot_ttl_seconds,
        snapshot_after_attempt=snapshots,
        capture_filesystem_manifests=False,
        stream_agent_output=False,
        push=False,
    )


async def _close_clients(clients: Iterable[AgentClient]) -> list[str]:
    values = list(clients)
    if not values:
        return []
    outcomes = await asyncio.gather(*(client.close() for client in values), return_exceptions=True)
    return [type(outcome).__name__ for outcome in outcomes if isinstance(outcome, BaseException)]


def build_endpoint_report(
    results: Sequence[Mapping[str, Any]],
    *,
    config: EndpointBenchmarkConfig,
    prefix_path: str | Path,
    shared_prefix: str,
    runner_id: str | None = None,
) -> dict[str, Any]:
    return build_report(
        results,
        suite="modal_openai_compatible_endpoint",
        config={
            **asdict(config),
            "workload": "shared-prefix-streaming-chat-completions-v1",
            "prefix_path": str(prefix_path),
            "prefix_sha256": hashlib.sha256(shared_prefix.encode()).hexdigest(),
            "prefix_chars": len(shared_prefix),
        },
        environment=capture_environment(runner_id=runner_id),
    )


def build_agent_report(
    results: Sequence[Mapping[str, Any]],
    *,
    config: AgentBenchmarkConfig,
    runner_id: str | None = None,
) -> dict[str, Any]:
    return build_report(
        results,
        suite="modal_opencode_agent_fanout",
        config={
            **asdict(config),
            "workload": "one-sandbox-one-opencode-session-v1",
            "snapshot_during_sweep": False,
            "filesystem_manifests_during_sweep": False,
        },
        environment=capture_environment(runner_id=runner_id),
    )


def _validate_common(endpoint_url: str, model: str, levels: Sequence[int]) -> None:
    if not endpoint_url.startswith(("http://", "https://")):
        raise ValueError("endpoint_url must be an http(s) URL")
    if not model:
        raise ValueError("model must not be empty")
    if not levels or any(level < 1 for level in levels):
        raise ValueError("concurrency_levels must contain positive values")
    if tuple(sorted(set(levels))) != tuple(levels):
        raise ValueError("concurrency_levels must be unique and increasing")


def _validate_deployment_declaration(
    gpu: str, max_containers: int | None, target_concurrency: int
) -> None:
    if not gpu:
        raise ValueError("declared_gpu must not be empty")
    if max_containers is not None and max_containers < 1:
        raise ValueError("declared_max_containers must be positive when supplied")
    if target_concurrency < 1:
        raise ValueError("declared_target_concurrency must be positive")


def _percentile(values: Sequence[float], percentile: int) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile / 100
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    return float(ordered[lower] * (upper - position) + ordered[upper] * (position - lower))


def _monitor_command(sandbox_id: str) -> str:
    return (
        "uv run --extra coding-agent python examples/11_coding_agent_mission.py "
        f"--monitor-sandbox {sandbox_id}"
    )


def _progress(event_type: str, **details: Any) -> None:
    print(json.dumps({"type": event_type, **details}, sort_keys=True), flush=True)


def _parse_levels(value: str) -> tuple[int, ...]:
    try:
        levels = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("levels must be comma-separated integers") from exc
    if not levels:
        raise argparse.ArgumentTypeError("levels must not be empty")
    return levels


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def common(command: argparse.ArgumentParser) -> None:
        command.add_argument("--endpoint-url", default=DEFAULT_ENDPOINT_URL)
        command.add_argument("--model", default=DEFAULT_MODEL)
        command.add_argument("--levels", type=_parse_levels, default=DEFAULT_LEVELS)
        command.add_argument("--runner-id", default=None)
        command.add_argument("--out", required=True)
        command.add_argument("--confirm-paid-run", action="store_true")
        command.add_argument("--declared-gpu", default="H200")
        command.add_argument("--declared-max-containers", type=int, default=None)
        command.add_argument("--declared-target-concurrency", type=int, default=32)

    endpoint = subparsers.add_parser("endpoint", help="Measure direct endpoint saturation")
    common(endpoint)
    endpoint.add_argument("--requests-per-worker", type=int, default=2)
    endpoint.add_argument("--warmup-requests", type=int, default=1)
    endpoint.add_argument("--warmup-timeout-seconds", type=float, default=10 * 60)
    endpoint.add_argument("--warmup-retry-seconds", type=float, default=5.0)
    endpoint.add_argument("--prefix-file", default="docs/guide/specification.md", type=Path)
    endpoint.add_argument("--target-prefix-chars", type=int, default=120_000)
    endpoint.add_argument("--max-output-tokens", type=int, default=192)
    endpoint.add_argument("--timeout-seconds", type=float, default=330.0)

    agents = subparsers.add_parser("agents", help="Measure real OpenCode sandbox fanout")
    common(agents)
    agents.add_argument("--repo-url", default=DEFAULT_REPO_URL)
    agents.add_argument("--base-ref", default="main")
    agents.add_argument("--secret-name", default="archetype-modal-endpoint")
    agents.add_argument("--app-name", default="archetype-opencode-benchmarks")
    agents.add_argument("--image-name", default="")
    agents.add_argument("--setup-concurrency", type=int, default=8)
    agents.add_argument("--agent-timeout-seconds", type=int, default=20 * 60)
    agents.add_argument("--sandbox-timeout-seconds", type=int, default=45 * 60)
    agents.add_argument("--snapshot-ttl-seconds", type=int, default=24 * 60 * 60)
    agents.add_argument("--skip-resume-preflight", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.confirm_paid_run:
        raise SystemExit("refusing paid benchmark without --confirm-paid-run")

    if args.command == "endpoint":
        config = EndpointBenchmarkConfig(
            endpoint_url=args.endpoint_url,
            model=args.model,
            concurrency_levels=args.levels,
            requests_per_worker=args.requests_per_worker,
            warmup_requests=args.warmup_requests,
            warmup_timeout_seconds=args.warmup_timeout_seconds,
            warmup_retry_seconds=args.warmup_retry_seconds,
            target_prefix_chars=args.target_prefix_chars,
            max_output_tokens=args.max_output_tokens,
            timeout_seconds=args.timeout_seconds,
            declared_gpu=args.declared_gpu,
            declared_max_containers=args.declared_max_containers,
            declared_target_concurrency=args.declared_target_concurrency,
        )
        shared_prefix = load_shared_prefix(args.prefix_file, config.target_prefix_chars)
        token_id = os.environ.get("MODAL_ENDPOINT_TOKEN_ID") or os.environ.get(
            "MODAL_PROXY_TOKEN_ID", ""
        )
        token_secret = os.environ.get("MODAL_ENDPOINT_TOKEN_SECRET") or os.environ.get(
            "MODAL_PROXY_TOKEN_SECRET", ""
        )
        results = asyncio.run(
            run_endpoint_benchmark(
                config,
                shared_prefix=shared_prefix,
                token_id=token_id,
                token_secret=token_secret,
            )
        )
        report = build_endpoint_report(
            results,
            config=config,
            prefix_path=args.prefix_file,
            shared_prefix=shared_prefix,
            runner_id=args.runner_id,
        )
        write_report(report, args.out)
        return 0 if all(result["success_rate"] == 1.0 for result in results) else 1

    config = AgentBenchmarkConfig(
        endpoint_url=args.endpoint_url,
        model=args.model,
        concurrency_levels=args.levels,
        repo_url=args.repo_url,
        base_ref=args.base_ref,
        secret_name=args.secret_name,
        app_name=args.app_name,
        image_name=args.image_name,
        setup_concurrency=args.setup_concurrency,
        agent_timeout_seconds=args.agent_timeout_seconds,
        sandbox_timeout_seconds=args.sandbox_timeout_seconds,
        snapshot_ttl_seconds=args.snapshot_ttl_seconds,
        resume_preflight=not args.skip_resume_preflight,
        declared_gpu=args.declared_gpu,
        declared_max_containers=args.declared_max_containers,
        declared_target_concurrency=args.declared_target_concurrency,
    )
    results = asyncio.run(run_agent_benchmark(config))
    report = build_agent_report(results, config=config, runner_id=args.runner_id)
    write_report(report, args.out)
    passed = all(bool(result.get("passed")) for result in results)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

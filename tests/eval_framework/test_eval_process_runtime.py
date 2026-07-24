# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Ownership contracts for the repository-eval process wrapper."""

from __future__ import annotations

import asyncio

import pytest

from evals.infra.runtime import EvalProcess


class _RetryableResources:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        self._events.append("workflow")
        if self.close_calls == 1:
            raise RuntimeError("workflow cleanup unavailable")
        self._events.append("audit")


class _Storage:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.close_calls = 0

    async def shutdown(self) -> None:
        self.close_calls += 1
        self._events.append("storage")


class _RetryableStorage(_Storage):
    async def shutdown(self) -> None:
        await super().shutdown()
        if self.close_calls == 1:
            raise RuntimeError("storage cleanup unavailable")


def _process(
    resources: _RetryableResources,
    storage: _Storage,
    *,
    owns_storage: bool,
) -> EvalProcess:
    process = object.__new__(EvalProcess)
    process.resources = resources  # type: ignore[assignment]
    process.storage = storage  # type: ignore[assignment]
    process._close_lock = asyncio.Lock()  # noqa: SLF001
    process._owns_storage = owns_storage  # noqa: SLF001
    process._storage_closed = False  # noqa: SLF001
    return process


@pytest.mark.asyncio
async def test_owned_storage_waits_for_retryable_resource_close() -> None:
    events: list[str] = []
    resources = _RetryableResources(events)
    storage = _Storage(events)
    process = _process(resources, storage, owns_storage=True)

    with pytest.raises(RuntimeError, match="workflow cleanup unavailable"):
        await process.aclose()

    assert events == ["workflow"]
    assert storage.close_calls == 0

    await process.aclose()
    await process.aclose()

    assert events[:4] == ["workflow", "workflow", "audit", "storage"]
    assert storage.close_calls == 1


@pytest.mark.asyncio
async def test_borrowed_storage_is_never_closed_by_eval_process() -> None:
    events: list[str] = []
    resources = _RetryableResources(events)
    resources.close_calls = 1
    storage = _Storage(events)
    process = _process(resources, storage, owns_storage=False)

    await process.aclose()

    assert events == ["workflow", "audit"]
    assert storage.close_calls == 0


@pytest.mark.asyncio
async def test_owned_storage_failure_remains_retryable_and_idempotent() -> None:
    events: list[str] = []
    resources = _RetryableResources(events)
    resources.close_calls = 1
    storage = _RetryableStorage(events)
    process = _process(resources, storage, owns_storage=True)

    with pytest.raises(RuntimeError, match="storage cleanup unavailable"):
        await process.aclose()

    await process.aclose()
    await process.aclose()

    assert storage.close_calls == 2
    assert events == [
        "workflow",
        "audit",
        "storage",
        "workflow",
        "audit",
        "storage",
        "workflow",
        "audit",
    ]

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Focused ownership contracts for the inert RuntimeMissions facade."""

from __future__ import annotations

import gc
import weakref
from typing import Any

import pytest

from archetype.missions.contracts import AgentMissionConfig
from archetype.runtime.runtime import ArchetypeRuntime


class _Backend:
    name = "owner-contract"

    async def create(self, spec: Any) -> Any:
        del spec
        raise NotImplementedError

    async def restore(self, spec: Any, checkpoint: Any) -> Any:
        del spec, checkpoint
        raise NotImplementedError


class _Dispatcher:
    def __init__(self) -> None:
        self.effects = 0

    async def apply(self, operation: object) -> object:
        del operation
        self.effects += 1
        return object()


class _Reservation:
    def __init__(self, *, fail_once: bool) -> None:
        self._fail_once = fail_once
        self._released = False
        self.close_calls = 0
        self.anchors: list[object] = []

    @property
    def released(self) -> bool:
        return self._released

    def retain_anchor[T](self, anchor: T) -> T:
        self.anchors.append(anchor)
        return anchor

    async def aclose(self) -> None:
        self.close_calls += 1
        if self._fail_once:
            self._fail_once = False
            raise RuntimeError("cleanup retry required")
        self.release()

    def release(self) -> None:
        self._released = True
        self.anchors.clear()


class _Resources:
    def __init__(self, reservation: _Reservation) -> None:
        self.dispatcher = _Dispatcher()
        self.reservation = reservation
        self.reservations: list[tuple[str, str]] = []

    def reserve_owner(self, owner: str, *, phase: str) -> _Reservation:
        self.reservations.append((owner, phase))
        return self.reservation


def _runtime(reservation: _Reservation) -> ArchetypeRuntime:
    runtime = object.__new__(ArchetypeRuntime)
    runtime._resources = _Resources(reservation)  # type: ignore[attr-defined]
    runtime._shutdown_started = False  # type: ignore[attr-defined]
    runtime._closed = False  # type: ignore[attr-defined]
    return runtime


def _config() -> AgentMissionConfig:
    return AgentMissionConfig(
        sandbox_backend=_Backend(),
        sandbox_environment="owner-contract",
    )


@pytest.mark.asyncio
async def test_mission_facade_anchor_survives_failed_close_until_retry_release() -> None:
    reservation = _Reservation(fail_once=True)
    runtime = _runtime(reservation)
    handle = runtime.missions("anchored", config=_config())

    assert reservation.anchors == [handle]
    assert runtime._resources.reservations[0][1] == "workflow-handles"  # type: ignore[attr-defined]
    handle_ref = weakref.ref(handle)
    del handle
    gc.collect()
    assert handle_ref() is not None

    owned = handle_ref()
    assert owned is not None
    try:
        await owned.close()
    except RuntimeError as error:
        assert str(error) == "cleanup retry required"
        error.__traceback__ = None
    else:
        pytest.fail("the first cleanup attempt must fail")
    del owned
    gc.collect()

    assert handle_ref() is not None
    assert not reservation.released

    owned = handle_ref()
    assert owned is not None
    await owned.close()
    del owned
    gc.collect()

    assert reservation.close_calls == 2
    assert reservation.released
    assert handle_ref() is None


@pytest.mark.asyncio
async def test_runtime_owned_release_closes_facade_before_dispatch_effect() -> None:
    reservation = _Reservation(fail_once=False)
    runtime = _runtime(reservation)
    handle = runtime.missions("released", config=_config())
    resources = runtime._resources  # type: ignore[attr-defined]

    reservation.release()

    with pytest.raises(RuntimeError, match="closed"):
        await handle.submit(
            repository="unused",
            branch="unused",
            tasks=(),
        )
    assert resources.dispatcher.effects == 0

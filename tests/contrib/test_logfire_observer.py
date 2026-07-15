# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Smoke coverage for contrib/logfire_observer (issue #278).

Runs only when the optional ``[logfire]`` extra is installed — the dev
group carries it so CI exercises this path; a bare install skips cleanly.
"""

import pytest

logfire = pytest.importorskip("logfire")

from archetype.contrib.logfire_observer import _tick_spans, logfire_hooks  # noqa: E402
from archetype.core.component import Component  # noqa: E402


class Blip(Component):
    x: float = 0.0


def test_logfire_hooks_shape():
    """One (event_type, handler) pair per lifecycle event, all coroutine fns."""
    import inspect

    from archetype.core.hooks import HookEvent

    hooks = logfire_hooks()
    assert hooks, "the observer must register at least the tick span pair"
    for event_type, handler in hooks:
        assert issubclass(event_type, HookEvent)
        assert inspect.iscoroutinefunction(handler)


@pytest.mark.asyncio
async def test_observer_rides_a_real_world_without_sending(monkeypatch, tmp_path):
    """Attach the hooks to a real runtime world, step it, and verify the
    span bookkeeping opens and closes — with network sending disabled."""
    monkeypatch.setenv("LOGFIRE_SEND_TO_LOGFIRE", "false")

    from archetype import ArchetypeRuntime
    from archetype.core.config import StorageConfig

    async with ArchetypeRuntime() as runtime:
        world = runtime.world(
            "observed",
            storage=StorageConfig(uri=str(tmp_path / "store"), namespace="ns"),
            hooks=logfire_hooks(),
        )
        await world.spawn(Blip(x=1.0))
        await world.run(steps=2)
        assert _tick_spans == {}, "every PreTick span must be closed by PostTick"

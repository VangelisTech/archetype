# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contract for the 0.6 removal of the parallel synchronous core kernel."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pytest

import archetype
import archetype.core as core
from archetype.core import hooks, interfaces

_LEGACY_ROOT_EXPORTS = frozenset(
    {
        "Processor",
        "Querier",
        "QueryManager",
        "Store",
        "SyncProcessor",
        "SyncStore",
        "SyncSystem",
        "SyncWorld",
        "System",
        "UpdateManager",
        "Updater",
        "World",
    }
)

_LEGACY_PROTOCOLS = frozenset(
    {
        "iProcessor",
        "iQueryManager",
        "iStore",
        "iSyncHookBus",
        "iSystem",
        "iUpdateManager",
        "iWorld",
    }
)


def test_parallel_sync_kernel_is_absent() -> None:
    core_dir = Path(core.__file__).resolve().parent

    assert not (core_dir / "sync").exists()
    with pytest.raises(ModuleNotFoundError):
        import_module("archetype.core.sync")

    assert _LEGACY_ROOT_EXPORTS.isdisjoint(archetype.__all__)
    assert _LEGACY_ROOT_EXPORTS.isdisjoint(archetype._EXPORTS)
    assert _LEGACY_ROOT_EXPORTS.isdisjoint(core.__all__)
    assert all(not hasattr(archetype, name) for name in _LEGACY_ROOT_EXPORTS)
    assert all(not hasattr(core, name) for name in _LEGACY_ROOT_EXPORTS)


def test_sync_only_protocols_and_hooks_are_absent() -> None:
    assert all(not hasattr(interfaces, name) for name in _LEGACY_PROTOCOLS)
    assert not hasattr(hooks, "SyncHookHandler")
    assert not hasattr(hooks, "SyncHookRegistry")


def test_blocking_runtime_facade_remains_supported() -> None:
    assert callable(archetype.ArchetypeRuntime.sync)
    assert archetype.SyncArchetypeRuntime is not None
    assert archetype.SyncRuntimeWorld is not None
    assert callable(archetype.run_sync)

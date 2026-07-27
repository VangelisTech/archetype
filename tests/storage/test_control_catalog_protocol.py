# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Structural parity contracts for local and remote control catalogs."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from archetype.storage.catalog import ControlCatalog, SqliteControlCatalog
from archetype.storage.catalog.remote import RemoteControlCatalog

_REQUIRED_METHODS = {
    "acquire_fence",
    "admit_commands",
    "cancel_commands",
    "close",
    "complete_evaluation",
    "current_fence_epoch",
    "fail_command",
    "get_world",
    "lease_commands",
    "lease_evaluation",
    "list_commands",
    "list_manifests",
    "list_signatures",
    "list_worlds",
    "mark_outbox_projected",
    "max_manifest_tick",
    "max_reserved_entity_id",
    "outbox_progress",
    "pending_command_count",
    "publish_manifest",
    "read_outbox",
    "register_signature",
    "register_world",
    "retire_world_registration",
    "release_commands",
    "release_evaluation",
    "set_world_status",
    "visible_tokens",
}


def _public_async_methods(owner: type[object]) -> set[str]:
    return {
        name
        for name, value in inspect.getmembers(owner)
        if not name.startswith("_") and inspect.iscoroutinefunction(value)
    }


def test_protocol_declares_every_catalog_operation_implemented_by_both_backends() -> None:
    protocol_methods = _public_async_methods(ControlCatalog)
    sqlite_methods = _public_async_methods(SqliteControlCatalog)
    remote_methods = _public_async_methods(RemoteControlCatalog)

    assert _REQUIRED_METHODS <= protocol_methods
    assert protocol_methods <= sqlite_methods
    assert protocol_methods <= remote_methods


@pytest.mark.asyncio
async def test_both_catalog_backends_satisfy_runtime_protocol(tmp_path: Path) -> None:
    sqlite = SqliteControlCatalog(tmp_path / "catalog.db")
    remote = RemoteControlCatalog("https://catalog.invalid", "namespace")
    try:
        assert isinstance(sqlite, ControlCatalog)
        assert isinstance(remote, ControlCatalog)
    finally:
        await sqlite.close()
        await remote.close()

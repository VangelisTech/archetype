# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Command-gated process-level runtime surface for durable ledgers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import get_type_hints

import pytest
from uuid_utils import uuid7

from archetype import ArchetypeRuntime, SyncArchetypeRuntime
from archetype.app.auth.errors import GuardrailError
from archetype.app.auth.guard import reset_daily_tokens, reset_tick_counters
from archetype.app.auth.models import ActorCtx
from archetype.app.command_service import CommandService
from archetype.core.config import StorageConfig


@pytest.fixture(autouse=True)
def _reset_quotas():
    reset_tick_counters()
    reset_daily_tokens()
    yield
    reset_tick_counters()
    reset_daily_tokens()


class _LedgerServiceSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.ref = SimpleNamespace(
            identity=SimpleNamespace(world_id="world", run_id="run"),
            manifest_digest="sha256:" + "1" * 64,
            manifest_generation=0,
        )
        self.head = object()
        self.infos = [object()]
        self.manifest = object()

    async def create_ledger(self, *args, **kwargs):
        self.calls.append(("create_ledger", args, kwargs))
        return self.ref

    async def get_head(self, *args, **kwargs):
        self.calls.append(("get_head", args, kwargs))
        return self.head

    async def list_ledgers(self, *args, **kwargs):
        self.calls.append(("list_ledgers", args, kwargs))
        return self.infos

    async def get_manifest(self, *args, **kwargs):
        self.calls.append(("get_manifest", args, kwargs))
        return self.manifest


def _command_service(ledgers: _LedgerServiceSpy) -> CommandService:
    unused = object()
    return CommandService(
        mutations=unused,
        worlds=unused,
        simulation=unused,
        queries=unused,
        broker=unused,
        ledgers=ledgers,
    )


@pytest.mark.asyncio
async def test_create_ledger_is_operator_gated_before_delegation(tmp_path):
    ledgers = _LedgerServiceSpy()
    commands = _command_service(ledgers)
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")

    viewer = ActorCtx(id=uuid7(), roles={"viewer"})
    with pytest.raises(GuardrailError):
        await commands.create_ledger(viewer, "denied", storage)
    assert ledgers.calls == []

    operator = ActorCtx(id=uuid7(), roles={"operator"})
    result = await commands.create_ledger(
        operator,
        "allowed",
        storage,
        world_id="world",
        run_id="run",
    )
    assert result is ledgers.ref
    assert ledgers.calls == [
        (
            "create_ledger",
            (),
            {
                "name": "allowed",
                "storage_config": storage,
                "world_id": "world",
                "run_id": "run",
            },
        )
    ]


@pytest.mark.asyncio
async def test_ledger_reads_are_viewer_gated_and_route_exactly(tmp_path):
    ledgers = _LedgerServiceSpy()
    commands = _command_service(ledgers)
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    viewer = ActorCtx(id=uuid7(), roles={"viewer"})
    identity = SimpleNamespace(world_id="world")
    storage_ref = object()
    ref = SimpleNamespace(identity=identity)

    assert await commands.get_ledger_head(viewer, identity, storage) is ledgers.head
    assert (
        await commands.list_ledgers(viewer, storage_ref, storage, name="only-this") is ledgers.infos
    )
    assert await commands.get_ledger_manifest(viewer, ref, storage) is ledgers.manifest
    assert ledgers.calls == [
        ("get_head", (identity,), {"storage_config": storage}),
        (
            "list_ledgers",
            (storage_ref,),
            {"storage_config": storage, "name": "only-this"},
        ),
        ("get_manifest", (ref,), {"storage_config": storage}),
    ]


class _RuntimeCommandSpy:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    async def _call(self, method: str, *args, **kwargs):
        self.calls.append((method, args, kwargs))
        return method

    async def create_ledger(self, *args, **kwargs):
        return await self._call("create_ledger", *args, **kwargs)

    async def get_ledger_head(self, *args, **kwargs):
        return await self._call("get_ledger_head", *args, **kwargs)

    async def list_ledgers(self, *args, **kwargs):
        return await self._call("list_ledgers", *args, **kwargs)

    async def get_ledger_manifest(self, *args, **kwargs):
        return await self._call("get_ledger_manifest", *args, **kwargs)


class _StorageRefSpy:
    def __init__(self) -> None:
        self.ref = object()

    def storage_ref(self, storage_config):
        return self.ref


@pytest.mark.asyncio
async def test_async_runtime_ledger_methods_route_only_through_gate(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    identity = object()
    ref = object()
    async with ArchetypeRuntime() as runtime:
        commands = _RuntimeCommandSpy()
        refs = _StorageRefSpy()
        runtime._container.command_service = commands
        runtime._container.storage_service = refs

        assert await runtime.create_ledger("name", storage=storage) == "create_ledger"
        assert await runtime.get_ledger_head(identity, storage=storage) == "get_ledger_head"
        assert await runtime.list_ledgers(storage=storage, name="name") == "list_ledgers"
        assert await runtime.get_ledger_manifest(ref, storage=storage) == "get_ledger_manifest"

        actor = runtime._actor_ctx
        assert commands.calls == [
            (
                "create_ledger",
                (actor, "name", storage),
                {"world_id": None, "run_id": None},
            ),
            ("get_ledger_head", (actor, identity, storage), {}),
            ("list_ledgers", (actor, refs.ref, storage), {"name": "name"}),
            ("get_ledger_manifest", (actor, ref, storage), {}),
        ]


def test_sync_runtime_ledger_methods_have_routing_parity(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="ns")
    identity = object()
    ref = object()
    with ArchetypeRuntime.sync() as runtime:
        commands = _RuntimeCommandSpy()
        refs = _StorageRefSpy()
        runtime._runtime._container.command_service = commands
        runtime._runtime._container.storage_service = refs

        assert runtime.create_ledger("name", storage=storage) == "create_ledger"
        assert runtime.get_ledger_head(identity, storage=storage) == "get_ledger_head"
        assert runtime.list_ledgers(storage=storage, name="name") == "list_ledgers"
        assert runtime.get_ledger_manifest(ref, storage=storage) == "get_ledger_manifest"

        actor = runtime._runtime._actor_ctx
        assert commands.calls == [
            (
                "create_ledger",
                (actor, "name", storage),
                {"world_id": None, "run_id": None},
            ),
            ("get_ledger_head", (actor, identity, storage), {}),
            ("list_ledgers", (actor, refs.ref, storage), {"name": "name"}),
            ("get_ledger_manifest", (actor, ref, storage), {}),
        ]


@pytest.mark.asyncio
async def test_runtime_ledger_methods_require_explicit_storage():
    async with ArchetypeRuntime() as runtime:
        with pytest.raises(TypeError, match="explicit storage"):
            await runtime.create_ledger("name", storage=None)  # type: ignore[arg-type]


def test_public_runtime_ledger_type_hints_resolve() -> None:
    method_names = (
        "create_ledger",
        "get_ledger_head",
        "list_ledgers",
        "get_ledger_manifest",
    )
    for runtime_type in (ArchetypeRuntime, SyncArchetypeRuntime):
        for method_name in method_names:
            hints = get_type_hints(getattr(runtime_type, method_name))
            assert "return" in hints

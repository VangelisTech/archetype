# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for app-layer Daft Iceberg table operations."""

import pytest
from daft.io import IOConfig
from daft.session import Session
from pyiceberg.exceptions import CommitFailedException

from archetype.app.iceberg import IcebergCatalogContext


def test_context_uses_daft_table_api_without_explicit_io_config():
    expected = object()

    class FakeTable:
        def read(self):
            return expected

    context = IcebergCatalogContext(Session(), None)

    assert context.read(FakeTable()) is expected


def test_context_passes_explicit_io_config_to_native_reads(monkeypatch):
    expected = object()
    native = object()
    io_config = IOConfig()
    seen = {}

    class FakeTable:
        _inner = native

    def fake_read_iceberg(table, *, io_config):
        seen.update(table=table, io_config=io_config)
        return expected

    monkeypatch.setattr("archetype.app.iceberg.read_iceberg", fake_read_iceberg)

    context = IcebergCatalogContext(Session(), io_config)

    assert context.read(FakeTable()) is expected
    assert seen == {"table": native, "io_config": io_config}


@pytest.mark.asyncio
async def test_context_passes_explicit_io_config_to_native_appends():
    native = object()
    io_config = IOConfig()
    seen = {}

    class FakeTable:
        _inner = native

    class FakeFrame:
        def write_iceberg(self, table, *, mode, io_config):
            seen.update(table=table, mode=mode, io_config=io_config)

    context = IcebergCatalogContext(Session(), io_config)
    await context.append(FakeTable(), FakeFrame())

    assert seen == {"table": native, "mode": "append", "io_config": io_config}


@pytest.mark.asyncio
async def test_context_refreshes_and_retries_optimistic_commit_conflicts(monkeypatch):
    attempts = 0
    refreshes = 0

    class FakeNative:
        def refresh(self):
            nonlocal refreshes
            refreshes += 1

    class FakeTable:
        _inner = FakeNative()

        def append(self, _frame):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise CommitFailedException("concurrent commit")

    async def no_wait(_delay):
        return None

    monkeypatch.setattr("archetype.app.iceberg.asyncio.sleep", no_wait)
    context = IcebergCatalogContext(Session(), None)

    await context.append(FakeTable(), object())

    assert attempts == 3
    assert refreshes == 2


def test_context_fails_closed_when_explicit_io_config_cannot_reach_native_table():
    context = IcebergCatalogContext(Session(), IOConfig())

    with pytest.raises(RuntimeError, match="Iceberg handle"):
        context.read(object())

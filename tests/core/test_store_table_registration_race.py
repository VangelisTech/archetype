# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import threading

import pytest

from archetype.core.aio.async_store import AsyncStore
from archetype.core.archetype import Archetype
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.storage.session import configure_session


class RegistrationProbe(Component):
    value: int


def _stores(tmp_path):
    storage = StorageConfig(uri=str(tmp_path / "store"), namespace="registration_race")
    first_session = configure_session(storage)
    second_session = configure_session(storage)
    sig = Archetype.sig_from_components([RegistrationProbe(value=1)])
    return storage, first_session, second_session, sig


def _synchronize_first_absence(monkeypatch, *sessions) -> None:
    """Force independent catalogs to decide absence before either creates."""
    barrier = threading.Barrier(len(sessions))
    for session in sessions:
        catalog = session.current_catalog()
        assert catalog is not None
        original_has_table = catalog.has_table

        def synchronized_has_table(identifier, *, _has_table=original_has_table):
            exists = _has_table(identifier)
            assert not exists
            barrier.wait(timeout=10)
            return exists

        monkeypatch.setattr(catalog, "has_table", synchronized_has_table)


@pytest.mark.asyncio
async def test_async_store_recovers_concurrent_table_registration_loser(tmp_path, monkeypatch):
    storage, first_session, second_session, sig = _stores(tmp_path)
    first = AsyncStore(first_session, io_config=storage.io_config)
    second = AsyncStore(second_session, io_config=storage.io_config)
    _synchronize_first_absence(monkeypatch, first.session, second.session)

    tables = await asyncio.wait_for(
        asyncio.gather(
            asyncio.to_thread(first._ensure_table, sig),
            asyncio.to_thread(second._ensure_table, sig),
        ),
        timeout=20,
    )

    table_name = Archetype.get_name(sig)
    assert [table.name for table in tables] == [table_name, table_name]
    assert await first.list_signatures() == [sig]
    assert await second.list_signatures() == [sig]

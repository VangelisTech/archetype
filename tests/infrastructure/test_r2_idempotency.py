# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real Archetype/Iceberg checks against Cloudflare R2 Data Catalog."""

from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen

import daft
import pytest
from daft.catalog import Catalog
from daft.session import Session
from pyiceberg.catalog.rest import RestCatalog
from uuid_utils import uuid7

from archetype.core.aio import AsyncStore, AsyncUpdateManager
from archetype.core.component import Component
from archetype.core.interfaces import CommitContext

TOKEN = os.environ.get("R2_CATALOG_TOKEN")
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not TOKEN or not ACCOUNT_ID,
        reason="GitHub Actions supplies the Cloudflare R2 catalog credentials",
    ),
]


class R2Reading(Component):
    value: float = 0.0


def _catalog_settings() -> tuple[str, str]:
    """Discover one explicitly selected—or unambiguous—active R2 catalog."""
    assert ACCOUNT_ID is not None
    assert TOKEN is not None
    request = Request(
        f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/r2-catalog",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed Cloudflare origin
        payload = json.load(response)

    if not payload.get("success"):
        pytest.fail(f"Cloudflare catalog discovery failed: {payload.get('errors', [])}")

    active = [
        item
        for item in payload.get("result", {}).get("warehouses", [])
        if item.get("status") == "active"
    ]
    requested_bucket = os.environ.get("R2_CATALOG_BUCKET")
    if requested_bucket:
        active = [item for item in active if item.get("bucket") == requested_bucket]
        if len(active) != 1:
            pytest.fail(f"No unique active R2 catalog found for bucket {requested_bucket!r}")
    elif len(active) != 1:
        pytest.fail("R2 catalog selection is ambiguous; set repository variable R2_CATALOG_BUCKET")

    selected = active[0]
    bucket = selected["bucket"]
    return (
        f"https://catalog.cloudflarestorage.com/{ACCOUNT_ID}/{bucket}",
        selected["name"],
    )


async def test_r2_iceberg_commit_visibility() -> None:
    """Exercise Archetype's real Iceberg store on one disposable R2 table."""
    assert TOKEN is not None
    catalog_uri, warehouse = _catalog_settings()
    catalog = RestCatalog(
        "archetype_ci",
        uri=catalog_uri,
        warehouse=warehouse,
        token=TOKEN,
    )
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "0")
    namespace = f"archetype_ci_{run_id}_{attempt}_{uuid7().hex[:8]}"
    catalog.create_namespace(namespace)

    try:
        session = Session()
        session.attach_catalog(Catalog.from_iceberg(catalog))
        session.set_namespace(namespace)
        store = AsyncStore(session)
        updater = AsyncUpdateManager(store)
        signature = (R2Reading,)
        world_id = f"r2-world-{uuid7().hex}"
        archetype_run_id = f"r2-run-{uuid7().hex}"
        raw = daft.from_pylist([{"entity_id": 1, "is_active": True, "r2reading__value": 4.0}])
        abandoned = CommitContext(commit_token=uuid7().hex, writer_epoch=1)
        published = CommitContext(commit_token=uuid7().hex, writer_epoch=2)

        # Two physical attempts really reach R2. Core append is intentionally
        # non-idempotent; the published-token allowlist supplies visibility.
        await updater.update(raw, signature, 0, world_id, archetype_run_id, commit=abandoned)
        await updater.update(raw, signature, 0, world_id, archetype_run_id, commit=published)

        physical = (await store.get_archetype_df(signature, world_id, archetype_run_id)).to_pylist()
        visible_a = (
            await store.get_archetype_df(
                signature,
                world_id,
                archetype_run_id,
                commit_tokens=[published.commit_token],
            )
        ).to_pylist()
        visible_b = (
            await store.get_archetype_df(
                signature,
                world_id,
                archetype_run_id,
                commit_tokens=[published.commit_token],
            )
        ).to_pylist()

        assert len(physical) == 2, "both retry attempts must exist in real R2 storage"
        assert len(visible_a) == 1, "only the published retry token may be visible"
        assert visible_b == visible_a, "replaying a fixed-snapshot read must be stable"
        assert visible_a[0]["commit_token"] == published.commit_token
    finally:
        for table in catalog.list_tables(namespace):
            catalog.drop_table(table)
        catalog.drop_namespace(namespace)

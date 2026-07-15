# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real Archetype/Iceberg checks against Cloudflare R2 Data Catalog."""

from __future__ import annotations

import json
import os
from urllib.error import HTTPError
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
DISCOVERY_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not TOKEN or not DISCOVERY_TOKEN or not ACCOUNT_ID,
        reason="GitHub Actions supplies the Cloudflare R2 catalog credentials",
    ),
]


class R2Reading(Component):
    value: float = 0.0


def _cloudflare_get(url: str, token: str) -> dict | None:
    request = Request(
        url,
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed Cloudflare origin
            return json.load(response)
    except HTTPError:
        return None


def _catalog_settings() -> tuple[str, str]:
    """Discover one explicitly selected—or unambiguous—active R2 catalog."""
    assert ACCOUNT_ID is not None
    assert DISCOVERY_TOKEN is not None
    assert TOKEN is not None

    account_ids = [ACCOUNT_ID]
    accounts = _cloudflare_get(
        "https://api.cloudflare.com/client/v4/accounts?per_page=50",
        DISCOVERY_TOKEN,
    )
    if accounts and accounts.get("success"):
        account_ids.extend(item["id"] for item in accounts.get("result", []))
    account_ids = list(dict.fromkeys(account_ids))

    active: list[tuple[str, dict]] = []
    for account_id in account_ids:
        for credential in dict.fromkeys((TOKEN, DISCOVERY_TOKEN)):
            payload = _cloudflare_get(
                f"https://api.cloudflare.com/client/v4/accounts/{account_id}/r2-catalog",
                credential,
            )
            if not payload or not payload.get("success"):
                continue
            active.extend(
                (account_id, item)
                for item in payload.get("result", {}).get("warehouses", [])
                if item.get("status") == "active"
            )
            break

    requested_bucket = os.environ.get("R2_CATALOG_BUCKET")
    if requested_bucket:
        active = [entry for entry in active if entry[1].get("bucket") == requested_bucket]
        if len(active) != 1:
            pytest.fail(f"No unique active R2 catalog found for bucket {requested_bucket!r}")
    elif len(active) != 1:
        pytest.fail("R2 catalog selection is ambiguous; set repository variable R2_CATALOG_BUCKET")

    selected_account, selected = active[0]
    bucket = selected["bucket"]
    return (
        f"https://catalog.cloudflarestorage.com/{selected_account}/{bucket}",
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

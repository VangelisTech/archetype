# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Real Archetype/Iceberg checks against Cloudflare R2 object storage."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import daft
import pytest
from daft.catalog import Catalog
from daft.io import IOConfig, S3Config
from daft.session import Session
from pyarrow.fs import S3FileSystem
from pyiceberg.catalog.sql import SqlCatalog
from uuid_utils import uuid7

from archetype.core.aio import AsyncStore, AsyncUpdateManager
from archetype.core.component import Component
from archetype.core.interfaces import CommitContext

ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY")
API_ENDPOINT = os.environ.get("R2_API_ENDPOINT")
BUCKET = os.environ.get("R2_BUCKET")
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not ACCESS_KEY_ID or not SECRET_ACCESS_KEY or not API_ENDPOINT or not BUCKET,
        reason="GitHub Actions supplies the Cloudflare R2 S3 configuration",
    ),
]


class R2Reading(Component):
    value: float = 0.0


async def test_r2_iceberg_commit_visibility(tmp_path: Path) -> None:
    """Exercise Archetype's real Iceberg store in one disposable R2 prefix."""
    assert ACCESS_KEY_ID is not None
    assert SECRET_ACCESS_KEY is not None
    assert API_ENDPOINT is not None
    assert BUCKET is not None

    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "0")
    prefix = f"archetype-ci/idempotency/{run_id}-{attempt}-{uuid7().hex}"
    warehouse = f"s3://{BUCKET}/{prefix}"
    namespace = "archetype_ci"
    catalog = SqlCatalog(
        "archetype_r2_ci",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=warehouse,
        **{
            "s3.endpoint": API_ENDPOINT,
            "s3.access-key-id": ACCESS_KEY_ID,
            "s3.secret-access-key": SECRET_ACCESS_KEY,
            "s3.region": "auto",
            "s3.force-virtual-addressing": "false",
        },
    )
    catalog.create_namespace(namespace)
    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=API_ENDPOINT,
            region_name="auto",
            key_id=ACCESS_KEY_ID,
            access_key=SECRET_ACCESS_KEY,
            use_ssl=True,
            force_virtual_addressing=False,
        )
    )

    try:
        session = Session()
        session.attach_catalog(Catalog.from_iceberg(catalog))
        session.set_namespace(namespace)
        store = AsyncStore(session, io_config=io_config)
        updater = AsyncUpdateManager(store)
        signature = (R2Reading,)
        world_id = f"r2-world-{uuid7().hex}"
        archetype_run_id = f"r2-run-{uuid7().hex}"
        raw = daft.from_pylist([{"entity_id": 1, "is_active": True, "r2reading__value": 4.0}])
        abandoned = CommitContext(commit_token=uuid7().hex, writer_epoch=1)
        published = CommitContext(commit_token=uuid7().hex, writer_epoch=2)

        # Both attempts physically reach R2. Core append is intentionally
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

        endpoint = urlparse(API_ENDPOINT)
        filesystem = S3FileSystem(
            access_key=ACCESS_KEY_ID,
            secret_key=SECRET_ACCESS_KEY,
            region="auto",
            scheme=endpoint.scheme,
            endpoint_override=endpoint.netloc,
            force_virtual_addressing=False,
        )
        filesystem.delete_dir(f"{BUCKET}/{prefix}")

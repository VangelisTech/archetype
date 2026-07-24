# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Phase-1 owner validation (issue #281 / #289).

Proves the remote substrate end to end with nothing but ``.env``:

1. ``--seed``: create a world on R2 (LanceDB over the S3-compatible
   endpoint) under the DEPLOYED Durable Objects control catalog — spawn and
   step twice.
2. default: from a completely fresh process, discover that world through
   production Cloudflare, read its visible rows through Archetype, and then
   read the SAME table straight off the R2 bucket with plain Daft — no
   archetype in the read path — to prove the bytes are really there.

Run it yourself:

    uv run python scripts/validate_r2_substrate.py --seed   # once
    uv run python scripts/validate_r2_substrate.py          # the proof
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

STATE_FILE = Path(__file__).parent / ".r2-validation-world.json"
BUCKET_URI = "s3://archetype-staging/validation"
NAMESPACE = "phase1"


def _load_env() -> None:
    """Load .env, then map R2 credentials onto the S3 names Lance/Daft read."""
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

    # LanceDB + Daft speak S3; R2 is S3-compatible at the account endpoint.
    endpoint = os.environ.get("R2_API_ENDPOINT", "")
    mapping = {
        "AWS_ACCESS_KEY_ID": os.environ.get("R2_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("R2_SECRET_ACCESS_KEY", ""),
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ENDPOINT": endpoint,
        "AWS_DEFAULT_REGION": "auto",
        "AWS_REGION": "auto",
    }
    for key, value in mapping.items():
        if value:
            os.environ[key] = value
    os.environ.pop("AWS_SESSION_TOKEN", None)  # R2 keys are static; a stale
    # session token from another provider poisons the chain.

    for required in ("R2_ACCESS_KEY_ID", "R2_API_ENDPOINT", "ARCHETYPE_CONTROL_CATALOG_URL"):
        if not os.environ.get(required):
            sys.exit(f"missing {required} in .env — cannot validate")


def _storage():
    from archetype.core.config import StorageConfig

    return StorageConfig(uri=BUCKET_URI, namespace=NAMESPACE)


async def seed() -> None:
    from archetype import ArchetypeRuntime
    from archetype.core.component import Component

    class Beacon(Component):
        value: float = 0.0

    async with ArchetypeRuntime() as runtime:
        world = runtime.world("r2-validation", storage=_storage())
        await world.spawn(Beacon(value=1.0))
        await world.step()
        await world.step()
        info = await world.info()
        STATE_FILE.write_text(
            json.dumps(
                {
                    "world_id": str(info.world_id),
                    "run_id": str(info.run_id),
                }
            )
        )
        print(f"seeded world {info.world_id} on {BUCKET_URI}")


async def validate() -> None:
    if not STATE_FILE.exists():
        sys.exit("no seeded world recorded — run with --seed first")
    state = json.loads(STATE_FILE.read_text())
    wid = state["world_id"]

    # ── 1. Cold discovery through PRODUCTION Cloudflare ─────────────────────
    from archetype import ArchetypeRuntime
    from archetype.core.component import Component

    class Beacon(Component):
        value: float = 0.0

    async with ArchetypeRuntime() as runtime:
        infos = await runtime.discover(_storage())
        assert wid in [str(i.world_id) for i in infos], "world not discovered via DO catalog"
        print(f"[1/3] deployed DO catalog knows the world ({len(infos)} world(s) in namespace)")

        cold = runtime.attach(wid, storage=_storage())
        df = await cold.query(Beacon)
        rows = df.to_pylist()
        ticks = sorted({r["tick"] for r in rows})
        assert len(rows) >= 3, f"expected >=3 visible rows, saw {len(rows)}"
        print(f"[2/3] archetype reads {len(rows)} visible rows from R2 (ticks {ticks})")

    # Table ids for the raw-Daft read are a substrate-validation detail. Use a
    # separately owned storage inspector instead of reaching through runtime.
    from archetype.storage.config import ControlCatalogConfig
    from archetype.storage.service import StorageService

    storage = StorageService(
        control_catalog_config=ControlCatalogConfig.from_env(),
    )
    try:
        catalog = storage.get_control_catalog(_storage())
        signatures = await catalog.list_signatures()
        table_ids = [signature.table_id for signature in signatures]
    finally:
        await storage.shutdown()

    # ── 2. Plain Daft, straight off the bucket — no archetype in the path ──
    import daft

    io_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            endpoint_url=os.environ["R2_API_ENDPOINT"],
            key_id=os.environ["R2_ACCESS_KEY_ID"],
            access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            region_name="auto",
        )
    )
    total = 0
    for table_id in table_ids:
        uri = f"{BUCKET_URI}/{NAMESPACE}/lance/{table_id}.lance"
        try:
            raw = daft.read_lance(uri, io_config=io_config)
            count = raw.count_rows()
        except Exception:
            continue
        total += count
        print(f"      daft.read_lance {table_id[:28]}…  {count} raw rows")
    assert total >= 3, "plain Daft saw no rows on the bucket"
    print(f"[3/3] plain Daft read {total} raw rows straight off {BUCKET_URI}")
    print("\nPHASE 1 VALIDATED: R2 data plane + deployed DO control catalog, end to end.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", action="store_true", help="write the validation world first")
    args = parser.parse_args()
    _load_env()
    asyncio.run(seed() if args.seed else validate())

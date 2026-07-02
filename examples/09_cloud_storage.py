# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Example 10 — Cloud storage configurations through the Archetype API
===================================================================

Builds StorageConfig objects for local and cloud-backed Iceberg warehouses,
then shows the same config flowing through ``ArchetypeRuntime.world(...,
storage=...)``. Cloud examples are configuration-only by default so the script
is safe to run without credentials.

Run:
    uv run python examples/09_cloud_storage.py
    uv run python examples/09_cloud_storage.py --smoke-local
"""

from __future__ import annotations

import argparse
import asyncio
import os
import tempfile
from dataclasses import dataclass

from daft.io import AzureConfig, CosConfig, GCSConfig, IOConfig, S3Config, TosConfig

from archetype import ArchetypeRuntime, Component
from archetype.core.config import StorageBackend, StorageConfig


class StorageProbe(Component):
    provider: str = ""
    note: str = ""


@dataclass(frozen=True)
class ProviderExample:
    name: str
    banner: str
    storage: StorageConfig
    env: tuple[str, ...]
    note: str


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def provider_examples() -> list[ProviderExample]:
    """Return cloud storage examples without opening network connections."""
    namespace = _env("ARCHETYPE_STORAGE_NAMESPACE", "product_demo")

    return [
        ProviderExample(
            name="AWS S3",
            banner="AWS S3 / Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_S3_URI", "s3://your-bucket/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    s3=S3Config(
                        region_name=_env("AWS_REGION", "us-east-1"),
                        profile_name=_env("AWS_PROFILE") or None,
                    )
                ),
            ),
            env=("ARCHETYPE_S3_URI", "AWS_REGION", "AWS_PROFILE"),
            note="Uses AWS credentials from the environment, profile, or default AWS chain.",
        ),
        ProviderExample(
            name="Google Cloud Storage",
            banner="Google Cloud Storage / Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_GCS_URI", "gs://your-bucket/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    gcs=GCSConfig(
                        project_id=_env("GOOGLE_CLOUD_PROJECT") or None,
                        token=_env("GOOGLE_OAUTH_ACCESS_TOKEN") or None,
                    )
                ),
            ),
            env=("ARCHETYPE_GCS_URI", "GOOGLE_CLOUD_PROJECT", "GOOGLE_OAUTH_ACCESS_TOKEN"),
            note="Uses ADC or an explicit OAuth token when provided.",
        ),
        ProviderExample(
            name="Azure Blob / ADLS",
            banner="Azure Blob or ADLS / Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_AZURE_URI", "az://container/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    azure=AzureConfig(
                        storage_account=_env("AZURE_STORAGE_ACCOUNT") or None,
                        access_key=_env("AZURE_STORAGE_ACCESS_KEY") or None,
                        tenant_id=_env("AZURE_TENANT_ID") or None,
                        client_id=_env("AZURE_CLIENT_ID") or None,
                        client_secret=_env("AZURE_CLIENT_SECRET") or None,
                    )
                ),
            ),
            env=(
                "ARCHETYPE_AZURE_URI",
                "AZURE_STORAGE_ACCOUNT",
                "AZURE_STORAGE_ACCESS_KEY",
                "AZURE_TENANT_ID",
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
            ),
            note="Supports account keys or service principal credentials.",
        ),
        ProviderExample(
            name="Cloudflare R2",
            banner="Cloudflare R2 / S3-Compatible Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_R2_URI", "s3://your-r2-bucket/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    s3=S3Config(
                        endpoint_url=_env(
                            "CLOUDFLARE_R2_ENDPOINT",
                            "https://<account-id>.r2.cloudflarestorage.com",
                        ),
                        key_id=_env("CLOUDFLARE_R2_ACCESS_KEY_ID") or None,
                        access_key=_env("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or None,
                        region_name="auto",
                    )
                ),
            ),
            env=(
                "ARCHETYPE_R2_URI",
                "CLOUDFLARE_R2_ENDPOINT",
                "CLOUDFLARE_R2_ACCESS_KEY_ID",
                "CLOUDFLARE_R2_SECRET_ACCESS_KEY",
            ),
            note="Uses Daft's S3 config with an R2 endpoint.",
        ),
        ProviderExample(
            name="MinIO",
            banner="MinIO / S3-Compatible Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_MINIO_URI", "s3://your-minio-bucket/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    s3=S3Config(
                        endpoint_url=_env("MINIO_ENDPOINT", "http://localhost:9000"),
                        key_id=_env("MINIO_ACCESS_KEY") or None,
                        access_key=_env("MINIO_SECRET_KEY") or None,
                        region_name=_env("MINIO_REGION", "us-east-1"),
                        use_ssl=_env("MINIO_ENDPOINT", "").startswith("https://"),
                    )
                ),
            ),
            env=("ARCHETYPE_MINIO_URI", "MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"),
            note="Works for local or hosted S3-compatible object storage.",
        ),
        ProviderExample(
            name="Tencent COS",
            banner="Tencent COS / Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_COS_URI", "cos://your-bucket/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    cos=CosConfig(
                        region=_env("TENCENT_COS_REGION") or None,
                        endpoint=_env("TENCENT_COS_ENDPOINT") or None,
                        secret_id=_env("TENCENT_COS_SECRET_ID") or None,
                        secret_key=_env("TENCENT_COS_SECRET_KEY") or None,
                    )
                ),
            ),
            env=(
                "ARCHETYPE_COS_URI",
                "TENCENT_COS_REGION",
                "TENCENT_COS_ENDPOINT",
                "TENCENT_COS_SECRET_ID",
                "TENCENT_COS_SECRET_KEY",
            ),
            note="Uses Daft's native COS configuration.",
        ),
        ProviderExample(
            name="Volcengine TOS",
            banner="Volcengine TOS / Iceberg Warehouse",
            storage=StorageConfig(
                uri=_env("ARCHETYPE_TOS_URI", "tos://your-bucket/archetype/warehouse"),
                namespace=namespace,
                backend=StorageBackend.ICEBERG,
                io_config=IOConfig(
                    tos=TosConfig(
                        region=_env("VOLCENGINE_TOS_REGION") or None,
                        endpoint=_env("VOLCENGINE_TOS_ENDPOINT") or None,
                        access_key=_env("VOLCENGINE_TOS_ACCESS_KEY") or None,
                        secret_key=_env("VOLCENGINE_TOS_SECRET_KEY") or None,
                    )
                ),
            ),
            env=(
                "ARCHETYPE_TOS_URI",
                "VOLCENGINE_TOS_REGION",
                "VOLCENGINE_TOS_ENDPOINT",
                "VOLCENGINE_TOS_ACCESS_KEY",
                "VOLCENGINE_TOS_SECRET_KEY",
            ),
            note="Uses Daft's native TOS configuration.",
        ),
    ]


def _print_banner(example: ProviderExample) -> None:
    width = max(72, len(example.banner) + 8)
    print("=" * width)
    print(f"  {example.banner}")
    print("=" * width)
    print(f"provider:  {example.name}")
    print(f"uri:       {example.storage.uri}")
    print(f"namespace: {example.storage.namespace}")
    print(f"backend:   {example.storage.backend.value}")
    print(f"env:       {', '.join(example.env)}")
    print(f"note:      {example.note}")
    print()


async def _smoke_local() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = StorageConfig(
            uri=tmp,
            namespace="cloud_storage_smoke",
            backend=StorageBackend.LANCEDB,
        )
        async with ArchetypeRuntime() as runtime:
            world = runtime.world("cloud-storage-smoke", storage=storage)
            await world.spawn(StorageProbe(provider="local", note="same storage API"))
            await world.step()
            rows = (await world.query(StorageProbe)).collect().to_pylist()
            active = [row for row in rows if row.get("is_active", True)]
    print("Local smoke")
    print(f"  spawned_entities={len(active)}")
    print(f"  provider={active[0]['storageprobe__provider']}")
    print()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--smoke-local",
        action="store_true",
        help="Run a local world through ArchetypeRuntime using the same storage API.",
    )
    args = parser.parse_args()

    for example in provider_examples():
        _print_banner(example)

    if args.smoke_local:
        await _smoke_local()


if __name__ == "__main__":
    asyncio.run(main())

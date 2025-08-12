"""
Unity Catalog temporary credential adapters to Daft IOConfig.

Transforms UC credential responses into Daft's IOConfig for cloud object
stores (S3/Azure/GCS). This enables per-operation scoped access to external
storage under UC governance.
"""

from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from daft.io import IOConfig, S3Config, AzureConfig, GCSConfig


def _to_datetime_ms(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def io_config_from_uc_credentials(resp: dict, *, default_region: Optional[str] = None) -> IOConfig:
    """Convert UC TemporaryCredentials payload to Daft IOConfig.

    UC schema (subset):
      {
        "aws_temp_credentials": {"access_key_id", "secret_access_key", "session_token"},
        "azure_user_delegation_sas": {"sas_token"},
        "gcp_oauth_token": {"oauth_token"},
        "expiration_time": 1712345678901
      }
    """
    aws = resp.get("aws_temp_credentials")
    az = resp.get("azure_user_delegation_sas")
    gcp = resp.get("gcp_oauth_token")
    # expiry = _to_datetime_ms(resp.get("expiration_time"))  # Not yet used by Daft IOConfig

    if aws:
        # For AWS S3 temporary creds
        return IOConfig(
            s3=S3Config(
                key_id=aws.get("access_key_id"),
                access_key=aws.get("secret_access_key"),
                session_token=aws.get("session_token"),
                region_name=default_region,
            )
        )

    if az:
        # For Azure Blob SAS tokens
        return IOConfig(
            azure=AzureConfig(
                sas_token=az.get("sas_token"),
            )
        )

    if gcp:
        # For GCS bearer token
        return IOConfig(
            gcs=GCSConfig(
                token=gcp.get("oauth_token"),
            )
        )

    # Fallback empty IOConfig if no known provider present
    return IOConfig()


__all__ = [
    "io_config_from_uc_credentials",
]



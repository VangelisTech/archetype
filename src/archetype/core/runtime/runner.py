from __future__ import annotations

from typing import Literal, Optional

from daft.io import IOConfig, S3Config, AzureConfig, GCSConfig, HTTPConfig, UnityConfig
from daft.context import (
    DaftContext,
    set_execution_config,
    set_planning_config,
    set_runner_native,
    set_runner_ray,
)

_INITIALIZED: bool = False


def initialize_runner(
    io_config: Optional[IOConfig],
    runner: Literal["native", "ray"] = "native",
    **ray_kwargs,
) -> None:
    """Initialize Daft's execution backend and default IOConfig once per process.

    - Sets the runner (native or ray)
    - Installs default IO credentials/config via planning config so downstream ops inherit it
    """
    global _INITIALIZED
    if _INITIALIZED:
        return

    if runner == "ray":
        set_runner_ray(**ray_kwargs)
    else:
        set_runner_native()

    if io_config is not None:
        set_planning_config(default_io_config=io_config)

    _INITIALIZED = True





class ExecutionContextFactory:
    """Helpers to initialize execution and produce per-operation IOConfig overrides."""

    @staticmethod
    def ensure(io_config: Optional[IOConfig], runner: Literal["native", "ray"] = "native", **ray_kwargs) -> None:
        """Idempotently initialize runner and install default IOConfig."""
        initialize_runner(io_config, runner=runner, **ray_kwargs)

    @staticmethod
    def set_default_io_config(io_config: IOConfig) -> DaftContext:
        """Override default IOConfig globally (rarely needed; prefer per-op overrides)."""
        return set_planning_config(default_io_config=io_config)

    @staticmethod
    def configure_execution(**kwargs) -> DaftContext:
        """Thin pass-through to Daft's execution config setter.

        Accepts the same kwargs as daft.context.set_execution_config.
        """
        return set_execution_config(**kwargs)

    @staticmethod
    def per_op_io_config(
        base: IOConfig,
        *,
        s3: Optional[S3Config] = None,
        azure: Optional[AzureConfig] = None,
        gcs: Optional[GCSConfig] = None,
        http: Optional[HTTPConfig] = None,
        unity: Optional[UnityConfig] = None,
    ) -> IOConfig:
        """Return a new IOConfig for a single operation by overriding subsets of providers."""
        return base.replace(s3=s3, azure=azure, gcs=gcs, http=http, unity=unity)

    # Convenience builders for common temporary-credentials shapes
    @staticmethod
    def s3_from_temp_credentials(
        *,
        key_id: str,
        access_key: str,
        session_token: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        max_connections: Optional[int] = None,
    ) -> S3Config:
        return S3Config(
            key_id=key_id,
            access_key=access_key,
            session_token=session_token,
            region_name=region_name,
            endpoint_url=endpoint_url,
            max_connections=max_connections,
        )

    @staticmethod
    def azure_from_bearer(
        *,
        bearer_token: str,
        storage_account: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> AzureConfig:
        return AzureConfig(
            bearer_token=bearer_token,
            storage_account=storage_account,
            endpoint_url=endpoint_url,
        )

    @staticmethod
    def azure_from_sas(
        *,
        sas_token: str,
        storage_account: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> AzureConfig:
        return AzureConfig(
            sas_token=sas_token,
            storage_account=storage_account,
            endpoint_url=endpoint_url,
        )

    @staticmethod
    def gcs_from_token(
        *,
        token: str,
        project_id: Optional[str] = None,
        max_connections: Optional[int] = None,
    ) -> GCSConfig:
        return GCSConfig(token=token, project_id=project_id, max_connections=max_connections)

    @staticmethod
    def gcs_from_credentials_json(
        *,
        credentials_json: str,
        project_id: Optional[str] = None,
        max_connections: Optional[int] = None,
    ) -> GCSConfig:
        return GCSConfig(credentials=credentials_json, project_id=project_id, max_connections=max_connections)
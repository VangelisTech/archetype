from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os

from daft.catalog import Catalog
from daft.session import Session
from pyiceberg.catalog.sql import SqlCatalog

from archetype.core.config import StorageConfig


@dataclass(frozen=True)
class StorageContext:
    """Runtime storage context created from a declarative StorageConfig.

    Carries initialized, owned runtime resources. Construction may perform I/O.
    """
    uri: str
    namespace: str
    session: Session
    catalog: Catalog
    io_config: Optional[object]


class StorageSessionFactory:
    """Centralizes side-effectful initialization of storage runtime resources.

    This replaces implicit setup inside StorageConfig validators/getters.
    """

    @staticmethod
    def build(config: StorageConfig) -> StorageContext:
        # Ensure base directory exists
        if not os.path.exists(config.uri):
            os.makedirs(config.uri, exist_ok=True)

        # Prepare catalog
        if getattr(config, "uc_mode", "off") == "attach":
            assert config.uc_endpoint, "Unity Catalog requires uc_endpoint"
            try:
                from daft.unity_catalog import UnityCatalog  # type: ignore
            except Exception as e:
                raise RuntimeError(
                    "Daft UC attach requested but 'unitycatalog' + daft[unity-catalog] not installed. "
                    "Set uc_mode='governance' to operate with UC governance only."
                ) from e
            uc = UnityCatalog(endpoint=config.uc_endpoint, token=(config.uc_token or ""))
            catalog = Catalog.from_unity(uc)
        else:
            catalog = getattr(config, "catalog", None) or Catalog.from_iceberg(
                SqlCatalog(
                    "archetype_iceberg_sql_catalog",
                    **{
                        "uri": f"sqlite:///{config.uri}/catalog.db",
                        "warehouse": f"file://{config.uri}",
                    },
                )
            )

        # Session: attach and set namespace (and ensure observability namespace exists)
        session = Session()
        session.attach_catalog(catalog)
        # If UC governance or attach and uc_catalog/uc_schema are provided, prefer those; else use local namespace
        if getattr(config, "uc_mode", "off") in ("governance", "attach") and config.uc_catalog and config.uc_schema:
            session.create_namespace_if_not_exists(f"{config.uc_catalog}.{config.uc_schema}")
            session.set_namespace(f"{config.uc_catalog}.{config.uc_schema}")
            # Observability schema under same catalog
            obs_schema = getattr(config, "uc_obs_schema", None) or config.obs_namespace
            if obs_schema:
                session.create_namespace_if_not_exists(f"{config.uc_catalog}.{obs_schema}")
        else:
            session.create_namespace_if_not_exists(config.namespace)
            session.set_namespace(config.namespace)
            # Observability: ensure separate namespace exists for logs/metrics
            if getattr(config, "obs_namespace", None):
                session.create_namespace_if_not_exists(config.obs_namespace)
        # Observability: ensure separate namespace exists for logs/metrics
        if getattr(config, "obs_namespace", None):
            session.create_namespace_if_not_exists(config.obs_namespace)

        return StorageContext(
            uri=config.uri,
            namespace=config.namespace,
            session=session,
            catalog=catalog,
            io_config=config.io_config,
        )



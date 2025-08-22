from __future__ import annotations

from dataclasses import dataclass
import pathlib
from urllib.parse import urlparse
from daft.catalog import Catalog
from daft.session import Session
from daft.io import IOConfig
from pyiceberg.catalog.sql import SqlCatalog

from archetype.core.config import StorageConfig
from archetype.core.runtime.runner import initialize_runner

IOConfig()

@dataclass(frozen=True)
class StorageContext:
    """Runtime storage context created from a declarative StorageConfig.

    Carries initialized, owned runtime resources. Construction may perform I/O.
    """
    uri: str
    namespace: str
    session: Session
    catalog: Catalog
    io_config: IOConfig


class StorageContextFactory:
    """Centralizes side-effectful initialization of storage runtime resources.

    This replaces implicit setup inside StorageConfig validators/getters.
    """

    @staticmethod
    def build(config: StorageConfig) -> StorageContext:
        """
        Builds a storage context from a storage config.
        """
        # Determine if the URI points to a remote object store
        scheme = urlparse(config.uri).scheme.lower()
        is_remote = scheme not in ("", "file")

        # Resolve local paths and ensure directories exist only for local URIs
        if not is_remote:
            base_path = pathlib.Path(config.uri)
            if not base_path.is_absolute():
                raise ValueError(f"uri must be an absolute path, got: {base_path}")
            base_path.mkdir(parents=True, exist_ok=True)
            sqlite_db_path = base_path / "catalog.db"
            warehouse_uri = f"file://{base_path}"
        else:
            # For remote warehouses (e.g., gs://, s3://), avoid local filesystem ops on the URI.
            # Keep a local SQLite catalog file and point the warehouse to the remote URI directly.
            local_meta_dir = pathlib.Path(".archetype_meta")
            local_meta_dir.mkdir(parents=True, exist_ok=True)
            sqlite_db_path = local_meta_dir / "catalog.db"
            warehouse_uri = config.uri

        # Initialize runner and default IOConfig once
        initialize_runner(config.io_config)

        # Prepare catalog based on IOConfig
        use_uc = bool(config.io_config and getattr(config.io_config, "unity", None))
        if use_uc:
            try:
                from daft.unity_catalog import UnityCatalog  # type: ignore
            except Exception as e:
                raise RuntimeError(
                    "Unity Catalog requested via IOConfig.unity but required extras are not installed. "
                    "Install with: pip install unitycatalog daft[unity-catalog]"
                ) from e
            unity = config.io_config.unity  # type: ignore[attr-defined]
            uc = UnityCatalog(endpoint=unity.endpoint, token=(unity.token or ""))
            catalog = Catalog.from_unity(uc)
        else:
            catalog = getattr(config, "catalog", None) or Catalog.from_iceberg(
                SqlCatalog(
                    "archetype_iceberg_sql_catalog",
                    **{
                        "uri": f"sqlite:///{sqlite_db_path}",
                        "warehouse": warehouse_uri,
                    },
                )
            )

        # Session: attach and set namespace
        session = Session()
        session.attach_catalog(catalog)
        if use_uc:
            # UC: don't auto-create namespaces; assume they exist and let UC enforce
            session.set_namespace(config.namespace)
        else:
            session.create_namespace_if_not_exists(config.namespace)
            session.set_namespace(config.namespace)

        return StorageContext(
            uri=config.uri,
            namespace=config.namespace,
            session=session,
            catalog=catalog,
            io_config=config.io_config,
        )



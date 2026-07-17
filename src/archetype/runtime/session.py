# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Daft session configuration for Archetype storage backends."""

from __future__ import annotations

import pathlib
from urllib.parse import urlparse

from daft.catalog import Catalog
from daft.session import Session

from archetype.core.config import StorageConfig
from archetype.core.paths import require_safe_namespace, resolve_local_root


def _resolve_storage_uri(uri: str) -> tuple[str, bool]:
    """Resolve local storage paths while preserving remote object-store URIs.

    Returns (resolved_uri, is_remote). Local paths route through
    ``resolve_local_root`` (issue #327): NUL bytes are rejected and, when
    ``ARCHETYPE_DATA_ROOT`` is set, escapes fail closed.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme not in ("", "file"):
        return uri, True

    base_path = resolve_local_root(parsed.path if scheme == "file" else uri)
    base_path.mkdir(parents=True, exist_ok=True)
    return str(base_path), False


def configure_session(
    config: StorageConfig,
    session: Session | None = None,
) -> Session:
    """Build Archetype's concrete local SQLite-catalog Iceberg session.

    Remote and managed catalogs are caller-owned. They enter through an
    already-configured ``Session`` passed to ``StorageService`` rather than
    being reconstructed from storage fields or environment variables.

    Args:
        config: Storage configuration with uri and namespace.
        session: Optional session to configure for backward-compatible local
            construction. Managed sessions should instead be injected through
            ``StorageService`` and are not reconfigured by Archetype.

    Returns:
        The configured session.
    """
    from pyiceberg.catalog.sql import SqlCatalog

    resolved_uri, is_remote = _resolve_storage_uri(str(config.uri))
    if is_remote:
        raise ValueError(
            "Archetype's built-in Iceberg factory supports local paths only; "
            "inject a preconfigured Daft Session through StorageService for "
            "remote or managed catalogs"
        )
    # The namespace becomes a warehouse directory under base_path
    # (pyiceberg Hive convention: <warehouse>/<namespace>.db/<table>).
    require_safe_namespace(config.namespace)
    # Same symlink-escape probe as the LanceDB branch: a pre-planted symlink
    # at the namespace directory must not redirect writes outside
    # ARCHETYPE_DATA_ROOT.
    resolve_local_root(str(pathlib.Path(resolved_uri) / f"{config.namespace}.db"))

    session = session if session is not None else Session()
    base_path = pathlib.Path(resolved_uri)
    sqlite_db_path = base_path / "catalog.db"
    warehouse_uri = f"file://{base_path}"

    catalog = Catalog.from_iceberg(
        SqlCatalog(
            "archetype_iceberg_sql_catalog",
            **{
                "uri": f"sqlite:///{sqlite_db_path}",
                "warehouse": warehouse_uri,
            },
        )
    )

    session.attach_catalog(catalog)
    session.create_namespace_if_not_exists(config.namespace)
    session.set_namespace(config.namespace)

    return session

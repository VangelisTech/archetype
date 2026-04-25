# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Backend-specific storage handles.

These are already-initialized inputs to concrete core stores. They do not
interpret user-facing storage configuration or allocate backend resources.
"""

from __future__ import annotations

from dataclasses import dataclass

from daft.catalog import Catalog
from daft.io import IOConfig
from daft.session import Session


@dataclass(frozen=True)
class DaftCatalogStorage:
    """Daft catalog/session handle used by the Daft-backed archetype store."""

    uri: str
    namespace: str
    session: Session
    catalog: Catalog
    io_config: IOConfig | None = None


@dataclass(frozen=True)
class LanceDbStorage:
    """LanceDB connection inputs used by the LanceDB-backed archetype store."""

    uri: str
    namespace: str
    io_config: IOConfig | None = None

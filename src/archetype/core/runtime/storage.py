# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Backward-compatible imports for legacy storage context paths."""

from __future__ import annotations

from dataclasses import dataclass

from daft.session import Session

from archetype.app.storage_service import StorageService
from archetype.core.config import StorageConfig


@dataclass(frozen=True)
class StorageContext:
    """Legacy Daft/Iceberg storage context.

    New code should use StorageService to create a native Daft Session for
    AsyncStore.
    """

    uri: str
    namespace: str
    session: Session


class StorageContextFactory:
    """Compatibility alias for StorageService storage builders.

    New code should use `StorageService.build_session`.
    """

    @staticmethod
    def build(config: StorageConfig) -> StorageContext:
        uri, namespace, session = StorageService.build_session_with_metadata(config)
        return StorageContext(
            uri=uri,
            namespace=namespace,
            session=session,
        )


__all__ = ["StorageContext", "StorageContextFactory"]

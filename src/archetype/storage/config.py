# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed bootstrap configuration for the durable control catalog."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

_DEFAULT_CATALOG_DIR = Path("~/.archetype/catalogs").expanduser()


@dataclass(frozen=True, slots=True)
class ControlCatalogConfig:
    """One immutable snapshot of application-level catalog configuration.

    Ordinary storage and catalog operations consume this value and never read
    ambient process configuration. Composition code may call :meth:`from_env`
    once while constructing the runtime dependency graph.
    """

    remote_url: str | None = None
    remote_token: str | None = field(default=None, repr=False, compare=False)
    catalog_dir: Path = _DEFAULT_CATALOG_DIR

    def __post_init__(self) -> None:
        remote_url = (self.remote_url or "").strip().rstrip("/") or None
        remote_token = (self.remote_token or "").strip() or None
        catalog_dir = Path(self.catalog_dir).expanduser()
        if remote_url is not None and remote_token is None:
            raise RuntimeError(
                "ARCHETYPE_CONTROL_CATALOG_TOKEN is required when "
                "ARCHETYPE_CONTROL_CATALOG_URL is configured"
            )
        object.__setattr__(self, "remote_url", remote_url)
        object.__setattr__(self, "remote_token", remote_token)
        object.__setattr__(self, "catalog_dir", catalog_dir)

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> ControlCatalogConfig:
        """Capture catalog settings from ``environ`` exactly once."""

        source = os.environ if environ is None else environ
        configured_dir = source.get("ARCHETYPE_CATALOG_DIR", "").strip()
        return cls(
            remote_url=source.get("ARCHETYPE_CONTROL_CATALOG_URL"),
            remote_token=source.get("ARCHETYPE_CONTROL_CATALOG_TOKEN"),
            catalog_dir=(
                Path(configured_dir).expanduser() if configured_dir else _DEFAULT_CATALOG_DIR
            ),
        )

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
World Registry

File-backed metadata store for world discovery. Persists world configs so that
CLI commands (which each spawn a fresh ``ServiceContainer``) can rediscover
worlds created by previous invocations.

The registry stores per-world metadata (name, storage URI, namespace, tick)
as JSON. Entity data still lives in the configured storage backend (LanceDB);
the registry is purely a catalog pointing at existing backends.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from uuid_utils import UUID

DEFAULT_REGISTRY_PATH = "./archetype_data/archetype_registry.json"
REGISTRY_ENV_VAR = "ARCHETYPE_REGISTRY_PATH"


def default_registry_path() -> Path:
    """Return the default registry path, honoring ``ARCHETYPE_REGISTRY_PATH``."""
    return Path(os.environ.get(REGISTRY_ENV_VAR, DEFAULT_REGISTRY_PATH))


class WorldRegistry:
    """File-backed registry for world metadata.

    Entries are stored under their string world_id key with at least:
      - world_id: str
      - name: str | None
      - storage_uri: str
      - namespace: str
      - tick: int
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def load(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(data, dict):
            return {}
        return data

    def save(self, data: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        tmp.replace(self.path)

    def upsert(self, world_id: UUID | str, entry: dict[str, Any]) -> None:
        data = self.load()
        data[str(world_id)] = entry
        self.save(data)

    def delete(self, world_id: UUID | str) -> None:
        data = self.load()
        if data.pop(str(world_id), None) is not None:
            self.save(data)

    def list_entries(self) -> list[dict[str, Any]]:
        return list(self.load().values())

    def get(self, world_id: UUID | str) -> dict[str, Any] | None:
        return self.load().get(str(world_id))

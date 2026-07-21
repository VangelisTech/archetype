# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Pure values shared by ingestion producers and the application authority."""

from __future__ import annotations

import re
from dataclasses import dataclass

_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


@dataclass(frozen=True)
class IngestionTable:
    """Describe one append-only table and its world/run-local identity columns."""

    name: str
    key_columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _TABLE_NAME.fullmatch(self.name):
            raise ValueError(
                "ingestion table names must start with a letter or underscore, contain "
                "only letters, digits, and underscores, and be at most 63 characters"
            )
        if not self.key_columns:
            raise ValueError("an ingestion table requires at least one key column")
        if len(self.key_columns) != len(set(self.key_columns)):
            raise ValueError("ingestion table key columns must be unique")
        for column in self.key_columns:
            if not _TABLE_NAME.fullmatch(column):
                raise ValueError(f"invalid ingestion key column: {column!r}")
            if column in {"world_id", "run_id"}:
                raise ValueError("world_id and run_id are service-owned envelope columns")


@dataclass(frozen=True)
class TableVersion:
    """Point to the catalog state made visible by one append."""

    table_name: str
    rows_written: int
    snapshot_id: int | None

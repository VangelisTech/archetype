# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Typed values used by the Smol teaching engine."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from pydantic import BaseModel, ConfigDict


class Component(BaseModel):
    """Immutable typed data attached to one Smol entity.

    Define a component by subclassing this type and declaring Pydantic fields.
    Smol prefixes those fields with the lowercase class name in processor
    DataFrames, matching the notation used by the full Archetype framework.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    @classmethod
    def prefix(cls) -> str:
        """Return the DataFrame column prefix for this component type."""

        return f"{cls.__name__.lower()}__"

    def to_row(self) -> dict[str, Any]:
        """Return this component's fields under their prefixed column names."""

        prefix = type(self).prefix()
        return {
            f"{prefix}{name}": deepcopy(getattr(self, name)) for name in type(self).model_fields
        }

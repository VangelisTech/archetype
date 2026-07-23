# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility re-export for :mod:`archetype.storage.signatures`."""

from archetype.storage.signatures import (
    _component_classes_by_name as _component_classes_by_name,
)
from archetype.storage.signatures import (
    match_signature_records,
    resolve_signature_records,
)

__all__ = ["match_signature_records", "resolve_signature_records"]

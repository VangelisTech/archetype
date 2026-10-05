# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Private loader seam. Only activation calls this factory."""

from typing import Any


def open_host(configuration: dict[str, Any]):
    from archetype.wiring import build_native_owner

    return build_native_owner(configuration)

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical Git diff identity shared by Mission authors and critics."""

from typing import Final

GIT_DIFF_IDENTITY_FLAGS: Final = (
    "--no-ext-diff",
    "--no-textconv",
    "--binary",
)

__all__ = ["GIT_DIFF_IDENTITY_FLAGS"]

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""HTTP error mapping for API handlers."""

from __future__ import annotations

from typing import NoReturn

from fastapi import HTTPException

from archetype.app.auth.errors import GuardrailError


def raise_api_error(exc: Exception, *, conflict: bool = False) -> NoReturn:
    """Map service-layer exceptions to stable REST errors."""
    if isinstance(exc, GuardrailError | PermissionError):
        raise HTTPException(status_code=403, detail=str(exc)) from None
    if isinstance(exc, KeyError):
        raise HTTPException(status_code=404, detail=str(exc)) from None
    if isinstance(exc, ValueError):
        status_code = 409 if conflict else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from None
    raise HTTPException(status_code=500, detail="Internal server error") from exc

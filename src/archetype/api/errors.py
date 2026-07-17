# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""HTTP error mapping for API handlers."""

from __future__ import annotations

import logging
from typing import NoReturn

from fastapi import HTTPException

from archetype.app.auth.errors import GuardrailError
from archetype.app.errors import WorldNotFoundError

logger = logging.getLogger(__name__)


def raise_api_error(exc: Exception, *, conflict: bool = False) -> NoReturn:
    """Map service-layer exceptions to stable REST errors."""
    if isinstance(exc, GuardrailError | PermissionError):
        raise HTTPException(status_code=403, detail=str(exc)) from None
    # WorldNotFoundError extends LookupError, not KeyError; without the
    # explicit branch it fell through to the 500 fallback (issue #180).
    if isinstance(exc, WorldNotFoundError | KeyError):
        raise HTTPException(status_code=404, detail=str(exc)) from None
    if isinstance(exc, ValueError):
        status_code = 409 if conflict else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from None
    logger.exception("Unhandled API exception")
    raise HTTPException(status_code=500, detail="Internal server error") from exc

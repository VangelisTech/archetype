# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Script decorator over the same 0.7 async or blocking native runtime.

Configuration is forwarded to ArchetypeRuntime. The decorator owns lifecycle
and injects the chosen facade as the function's first argument.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar

from archetype._api import public_api
from archetype.runtime.runtime import ArchetypeRuntime

_F = TypeVar("_F", bound=Callable[..., Any])


@public_api
def entrypoint(**configuration: Any) -> Callable[[_F], Callable[..., Any]]:
    """Wrap a script's main function with a managed ``ArchetypeRuntime``.

    The wrapped function is called with the runtime prepended to its
    arguments and may be sync or async. The wrapper itself is always sync
    (script boundary), returning whatever the function returns.
    """

    def decorate(fn: _F) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async def _run() -> Any:
                    async with ArchetypeRuntime(**configuration) as runtime:
                        return await fn(runtime, *args, **kwargs)

                return asyncio.run(_run())

            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            with ArchetypeRuntime.sync(**configuration) as runtime:
                return fn(runtime, *args, **kwargs)

        return sync_wrapper

    return decorate

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""``@archetype.entrypoint`` — the script boundary as a decorator.

An evolution of ``ArchetypeRuntime``, not a surface beside it: the decorator
owns the lifecycle ceremony every script (and every Modal GPU entrypoint) was
hand-rolling — construct the runtime, bridge sync/async, guarantee teardown —
and injects the runtime as the wrapped function's first argument.

    @archetype.entrypoint()
    def main(runtime: SyncArchetypeRuntime, n: int = 3) -> None:
        world = runtime.world("demo")
        ...

    @archetype.entrypoint()
    async def amain(runtime: ArchetypeRuntime, n: int = 3) -> None:
        world = runtime.world("demo")
        ...

Async functions run under ``asyncio.run`` inside ``async with
ArchetypeRuntime()``; sync functions receive the ``SyncArchetypeRuntime``
facade. Tracing/logging configuration follows the runtime's own env-driven
selection (R16/R17) — nothing extra to wire per script.
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
def entrypoint(*, log: str | None = None) -> Callable[[_F], Callable[..., Any]]:
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
                    async with ArchetypeRuntime(log=log) as runtime:
                        return await fn(runtime, *args, **kwargs)

                return asyncio.run(_run())

            return async_wrapper

        @functools.wraps(fn)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            with ArchetypeRuntime.sync(log=log) as runtime:
                return fn(runtime, *args, **kwargs)

        return sync_wrapper

    return decorate

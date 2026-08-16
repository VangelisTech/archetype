# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""In-process physical-AI processor protocols."""

from __future__ import annotations

from typing import Any, Protocol


class EnvClient(Protocol):
    """Boundary to an external manipulation simulator.

    This protocol is only for explicitly in-process processor composition.
    Hosted providers cross the whole-episode Activity boundary instead.
    """

    def reset(self, env_id: int, seed: int) -> dict[str, Any]:
        """Create/reset one environment and return its initial observation."""

        ...

    def step(
        self,
        env_ids: list[int],
        actions: list[list[float]],
    ) -> list[dict[str, Any]]:
        """Advance each environment once and return one observation per id."""

        ...

    async def aclose(self) -> None:
        """Release the provider and every resource it owns."""

        ...


class PolicyClient(Protocol):
    """Boundary to a policy with explicitly host-owned live state.

    This protocol is only for explicitly in-process processor composition.
    Distributed policies belong behind the whole-episode hosted operation.
    """

    def act(
        self,
        env_keys: list[int],
        instructions: list[str],
        observations: list[dict[str, Any]],
    ) -> list[list[float]]:
        """Return one action per environment observation."""

        ...

    async def aclose(self) -> None:
        """Release the provider and every resource it owns."""

        ...


__all__ = [
    "EnvClient",
    "PolicyClient",
]

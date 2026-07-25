# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Genuine physical-AI provider and lifetime-registration protocols."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Protocol

from uuid_utils import UUID

from archetype.world.registry import WorldCleanupLease


class EnvClient(Protocol):
    """Boundary to an external manipulation simulator.

    Construction must be inert. A registered physical operation transfers the
    exact object to process ownership before its first effect. The object must
    be serializable by Daft as a non-owning handle to the same host-owned
    backing authority; it may not create an independently owned worker resource.
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

    The object must be serializable by Daft as a non-owning handle to that
    authority; independently owned worker-local resources remain unsupported.
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


class PhysicalClientLifetimeRegistrar(Protocol):
    """Validate/own provider handles and serialize each identity's workflows."""

    def lease(
        self,
        env_client: EnvClient,
        policy_client: PolicyClient | None,
    ) -> AbstractAsyncContextManager[PhysicalWorkflowLifetime]:
        """Validate Daft serialization, own providers, then lease one workflow."""

        ...


class PhysicalEvidenceWorldRetirement(Protocol):
    """Exact process-owned retirement handle for one physical evidence world."""

    async def aclose(self) -> None:
        """Join or retry the complete cleanup transaction for the exact lease."""

        ...


class PhysicalWorkflowLifetime(Protocol):
    """Provider-scoped, pre-reserved authority for exact evidence cleanup."""

    def retain_evidence_world(
        self,
        world_id: str | UUID,
        lease: WorldCleanupLease,
    ) -> PhysicalEvidenceWorldRetirement:
        """Synchronously bind cleanup before the workflow's next await."""

        ...

    def retain_evidence_world_for_compensation(
        self,
        world_id: str | UUID,
        lease: WorldCleanupLease,
    ) -> PhysicalEvidenceWorldRetirement:
        """Recover the same pre-owned exact cleanup after retention failure."""

        ...


__all__ = [
    "EnvClient",
    "PhysicalClientLifetimeRegistrar",
    "PhysicalEvidenceWorldRetirement",
    "PhysicalWorkflowLifetime",
    "PolicyClient",
]

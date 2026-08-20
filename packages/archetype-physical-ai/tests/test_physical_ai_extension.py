# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Physical-AI world-library manifest and installation contracts."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from archetype.commands.registry import OperationRegistry
from archetype.core.config import StorageConfig
from archetype.physical_ai._extension import get_manifest
from archetype.physical_ai.config import PhysicalAIExtensionConfig
from archetype.physical_ai.models import RunHostedEpisode
from archetype.physical_ai.runtime import PhysicalAI
from archetype.world_libraries import WorldLibraryContext


def _context(
    registry: OperationRegistry,
    *,
    config: object | None = None,
) -> WorldLibraryContext:
    return WorldLibraryContext(
        registry=registry,
        resources=SimpleNamespace(),
        worlds=SimpleNamespace(),
        lifecycle=SimpleNamespace(),
        scheduler=SimpleNamespace(),
        storage=SimpleNamespace(),
        redaction=SimpleNamespace(),
        required_projectors=SimpleNamespace(),
        control_catalog_config=SimpleNamespace(),
        artifact_store_config=None,
        destroy_world=lambda _world_id: None,
        runtime_world_factory=lambda *args, **kwargs: (args, kwargs),
        config=config,
    )


def test_physical_ai_manifest_is_complete_and_side_effect_free() -> None:
    first = get_manifest()
    second = get_manifest()

    assert first == second
    assert first.name == "physical-ai"
    assert first.distribution == "archetype-physical-ai"
    assert first.version == "0.6.2"
    assert first.requires_framework == ">=0.6,<0.7"
    assert first.operation_models == (RunHostedEpisode,)
    assert first.operation_names == ("run_hosted_episode",)
    assert not first.api_router_factories


def test_physical_ai_install_registers_its_exact_operation() -> None:
    registry = OperationRegistry()
    installed = get_manifest().install(_context(registry, config=PhysicalAIExtensionConfig()))

    assert installed.name == "physical-ai"
    assert installed.world_adapter is PhysicalAI
    assert installed.runtime_adapter is None
    assert not hasattr(installed, "api_routers")
    assert len(registry.specs) == 1
    spec = registry.resolve_name("run_hosted_episode")
    assert spec.model is RunHostedEpisode
    assert spec.quota_scope == "live_world"
    assert spec.permission == "run_hosted_episode"
    assert spec.trusted
    assert not spec.untrusted
    assert spec.token_cost == 0


@pytest.mark.parametrize("lease", [0, -1, float("inf"), float("nan")])
def test_physical_ai_extension_config_rejects_invalid_lease(lease: float) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        PhysicalAIExtensionConfig(hosted_activity_lease_seconds=lease)


def test_physical_ai_install_rejects_foreign_config() -> None:
    with pytest.raises(TypeError, match="PhysicalAIExtensionConfig"):
        get_manifest().install(_context(OperationRegistry(), config=object()))


class _FakeWorld:
    def __init__(self) -> None:
        self.operations: list[Any] = []
        self._dispatcher = SimpleNamespace(apply=self._apply)

    async def _call_library(
        self,
        callback: Any,
        *,
        capability: str,
        require_storage: bool = False,
    ) -> Any:
        assert capability == "run_hosted_episode"
        assert require_storage
        return await callback("world-1", StorageConfig(), self._dispatcher)

    async def _apply(self, operation: Any) -> str:
        self.operations.append(operation)
        return "observed"


@pytest.mark.asyncio
async def test_physical_ai_adapter_dispatches_one_exact_operation() -> None:
    from archetype.physical_ai.models import (
        HostedEpisodeRequest,
        ModalHostedEpisodeConfig,
    )

    world = _FakeWorld()
    request = HostedEpisodeRequest(
        trial_id=1,
        suite="suite",
        task_id=2,
        seed=3,
        instruction="act",
        max_transitions=4,
        environment_id="environment-v1",
        policy_id="policy-v1",
    )
    provider = ModalHostedEpisodeConfig(
        workspace_name="workspace",
        environment_name="environment",
        app_name="app",
        function_name="function",
        result_dict_name="results",
        result_volume_name="volume",
    )

    result = await PhysicalAI(world).run_hosted_episode(
        [request],
        provider=provider,
        activity_id="activity-1",
    )

    assert result == "observed"
    assert len(world.operations) == 1
    operation = world.operations[0]
    assert type(operation) is RunHostedEpisode
    assert operation.world_id == "world-1"
    assert operation.storage_config == StorageConfig()
    assert operation.activity_id == "activity-1"
    assert operation.requests == (request,)
    assert operation.provider == provider

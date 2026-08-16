# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Research world-library manifest and installation contracts."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from archetype.commands.registry import OperationRegistry
from archetype.research._extension import get_manifest
from archetype.research.models import AutoResearch, AutoResearchConfig
from archetype.research.runtime import Research
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


def _config(*, max_iterations: int = 3) -> AutoResearchConfig:
    return AutoResearchConfig(
        experiment_name="experiment",
        experiment_id="experiment-1",
        evaluator_id="evaluator-v1",
        rollout_contract_id="rollout-v1",
        max_iterations=max_iterations,
    )


def test_research_manifest_is_complete_and_side_effect_free() -> None:
    first = get_manifest()
    second = get_manifest()

    assert first == second
    assert first.name == "research"
    assert first.distribution == "archetype-research"
    assert first.version == "0.6.1"
    assert first.requires_framework == ">=0.6,<0.7"
    assert first.operation_models == (AutoResearch,)
    assert first.operation_names == ("autoresearch",)
    assert not first.api_router_factories


def test_research_install_registers_its_exact_operation_and_cost() -> None:
    registry = OperationRegistry()
    installed = get_manifest().install(_context(registry))

    assert installed.name == "research"
    assert installed.world_adapter is Research
    assert installed.runtime_adapter is None
    assert not hasattr(installed, "api_routers")
    assert len(registry.specs) == 1
    spec = registry.resolve_name("autoresearch")
    assert spec.model is AutoResearch
    assert spec.quota_scope == "live_world"
    assert spec.permission == "autoresearch"
    assert spec.trusted and spec.untrusted
    assert callable(spec.token_cost)
    operation = AutoResearch(
        world_id="world-1",
        config=_config(max_iterations=3),
        evaluator=lambda _rollout: 1.0,
    )
    assert spec.token_cost(operation) == 600


def test_research_install_rejects_config() -> None:
    with pytest.raises(TypeError, match="does not accept"):
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
        assert capability == "autoresearch"
        assert not require_storage
        return await callback("world-1", None, self._dispatcher)

    async def _apply(self, operation: Any) -> str:
        self.operations.append(operation)
        return "researched"


@pytest.mark.asyncio
async def test_research_adapter_dispatches_one_exact_operation() -> None:
    world = _FakeWorld()
    config = _config()

    def evaluator(_rollout: object) -> float:
        return 1.0

    result = await Research(world).autoresearch(config, evaluator)

    assert result == "researched"
    assert len(world.operations) == 1
    operation = world.operations[0]
    assert type(operation) is AutoResearch
    assert operation.world_id == "world-1"
    assert operation.config is config
    assert operation.evaluator is evaluator

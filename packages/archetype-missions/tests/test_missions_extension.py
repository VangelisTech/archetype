# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""World-library manifest and typed-adapter contracts for Missions."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from archetype.commands.registry import OperationRegistry
from archetype.core.component import Component
from archetype.core.config import StorageConfig
from archetype.missions import Missions, MissionWorld
from archetype.missions._extension import MISSION_OPERATION_MODELS, get_manifest
from archetype.missions.config import MissionsExtensionConfig, installed_execution_profiles
from archetype.missions.contracts import AgentMissionConfig
from archetype.missions.execution_profiles import (
    ExecutionProfile,
    ExecutionProfileBinding,
    ExecutionProfileCatalog,
)
from archetype.missions.trajectories import ClaudeTranscriptSource, TrajectorySelection
from archetype.missions.trajectories.models import (
    GradeTrajectory,
    IngestClaudeTranscript,
    QueryTrajectory,
    QueryTranscriptRows,
)
from archetype.world_libraries import WorldLibraryContext


class Metric(Component):
    value: int = 0


class _Dispatcher:
    def __init__(self) -> None:
        self.operations: list[Any] = []

    async def apply(self, operation: Any) -> Any:
        self.operations.append(operation)
        if operation.operation == "get_world_info":
            return SimpleNamespace(run_id="run-1")
        return operation


class _World:
    def __init__(self, *, storage: StorageConfig | None) -> None:
        self.storage = storage
        self.dispatcher = _Dispatcher()
        self.calls: list[tuple[str, bool]] = []

    async def _call_library(
        self,
        callback: Any,
        *,
        capability: str,
        require_storage: bool = False,
    ) -> Any:
        self.calls.append((capability, require_storage))
        if require_storage and self.storage is None:
            raise ValueError(f"{capability} requires explicit storage coordinates")
        return await callback("world-1", self.storage, self.dispatcher)


def cast_backend(name: str) -> Any:
    return SimpleNamespace(name=name)


def cast_driver(driver_id: str) -> Any:
    return SimpleNamespace(driver_id=driver_id)


def _context(tmp_path: Path) -> WorldLibraryContext:
    return WorldLibraryContext(
        registry=OperationRegistry(),
        resources=SimpleNamespace(),
        worlds=SimpleNamespace(),
        lifecycle=SimpleNamespace(),
        scheduler=SimpleNamespace(),
        storage=SimpleNamespace(),
        redaction=SimpleNamespace(),
        required_projectors=SimpleNamespace(),
        control_catalog_config=SimpleNamespace(catalog_dir=tmp_path),
        artifact_store_config=None,
        destroy_world=lambda _world_id: None,
        runtime_world_factory=lambda *args, **kwargs: None,
    )


def test_manifest_declares_exact_missions_surface() -> None:
    first = get_manifest()
    second = get_manifest()

    assert first is second
    assert first.name == "missions"
    assert first.distribution == "archetype-missions"
    assert first.version == "0.6.3"
    assert first.requires_framework == ">=0.6,<0.7"
    assert first.operation_models == MISSION_OPERATION_MODELS
    assert first.operation_names == (
        "ingest_claude_transcript",
        "query_transcript_rows",
        "query_trajectory",
        "grade_trajectory",
        "submit_mission",
        "run_mission",
        "restore_mission_sandbox",
        "accept_mission_run",
        "get_mission_run",
        "cancel_mission_run",
        "get_mission_run_events",
        "list_mission_runs",
    )
    assert len(first.api_router_factories) == 1


def test_install_registers_only_the_declared_operations(tmp_path: Path) -> None:
    context = _context(tmp_path)

    installed = get_manifest().install(context)

    assert installed.name == "missions"
    assert installed.runtime_adapter is Missions
    assert installed.world_adapter is MissionWorld
    assert not hasattr(installed, "api_routers")
    assert isinstance(installed.config, MissionsExtensionConfig)
    with pytest.raises(KeyError, match="not configured"):
        installed_execution_profiles(installed).resolve("coding-default")
    assert tuple(spec.model for spec in context.registry.specs) == MISSION_OPERATION_MODELS
    assert tuple(spec.name for spec in context.registry.specs) == get_manifest().operation_names
    assert all(spec.trusted and not spec.untrusted for spec in context.registry.specs)


def test_install_rejects_unknown_library_configuration(tmp_path: Path) -> None:
    context = replace(_context(tmp_path), config=object())

    with pytest.raises(TypeError, match="MissionsExtensionConfig"):
        get_manifest().install(context)
    assert not context.registry.specs


def test_install_accepts_exact_missions_configuration(tmp_path: Path) -> None:
    context = replace(_context(tmp_path), config=MissionsExtensionConfig())

    installed = get_manifest().install(context)

    assert installed.name == "missions"
    assert installed.config is context.config
    assert tuple(spec.model for spec in context.registry.specs) == MISSION_OPERATION_MODELS


def test_installed_library_resolves_bound_profiles_to_live_configuration(
    tmp_path: Path,
) -> None:
    """profile_id -> ExecutionProfileBinding -> build_config through the seam."""

    profile = ExecutionProfile.model_validate(
        {
            "profile_id": "coding-default",
            "version": "1",
            "allowed_repositories": ("VangelisTech/archetype",),
            "allowed_base_refs": ("main",),
            "branch_namespace": "agent/",
            "sandbox_backend": "modal",
            "sandbox_environment": "coding-agent:v1",
            "agent_driver": "codex",
            "critic_driver": "codex",
            "model": "gpt-5",
            "timeout_seconds": 3600,
            "max_ticks": 100,
            "max_retries": 3,
            "max_concurrency": 1,
            "cost_ceiling_usd_cents": 5000,
            "max_validators_per_task": 8,
            "max_validator_timeout_seconds": 900,
            "publication_policy": "commit_and_push",
            "checkpoint_after_dispatch": True,
        }
    )

    def factory(selected: ExecutionProfile) -> AgentMissionConfig:
        return AgentMissionConfig(
            sandbox_backend=cast_backend(selected.sandbox_backend),
            sandbox_environment=selected.sandbox_environment,
            driver=cast_driver(selected.agent_driver),
            critic_driver=cast_driver(selected.critic_driver),
            model=selected.model,
            max_ticks=selected.max_ticks,
            checkpoint_after_dispatch=selected.checkpoint_after_dispatch,
        )

    binding = ExecutionProfileBinding(profile=profile, config_factory=factory)
    catalog = ExecutionProfileCatalog(
        (binding,),
        current_versions={profile.profile_id: profile.version},
    )
    context = replace(
        _context(tmp_path),
        config=MissionsExtensionConfig(execution_profiles=catalog),
    )

    installed = get_manifest().install(context)

    resolved = installed_execution_profiles(installed).resolve("coding-default")
    assert resolved is binding
    config = resolved.build_config()
    assert config.model == profile.model
    assert config.driver is not None and config.driver.driver_id == profile.agent_driver


@pytest.mark.asyncio
async def test_mission_world_constructs_exact_evidence_operations(tmp_path: Path) -> None:
    storage = StorageConfig(uri=str(tmp_path / "storage"))
    world = _World(storage=storage)
    adapter = MissionWorld(world)
    source = ClaudeTranscriptSource(path=tmp_path / "session.jsonl")
    selection = TrajectorySelection(episode_ids=("episode-1",))

    ingest = await adapter.ingest_claude_transcript(source)
    transcript_rows = await adapter.transcript_rows()
    query = await adapter.query_trajectory(
        Metric,
        selection=selection,
        ticks=[2],
        entity_ids=[3],
    )

    async def grader(_frame: Any) -> float:
        return 1.0

    grade = await adapter.grade_trajectory(
        Metric,
        graders=(grader,),
        selection=selection,
        ticks=[4],
        entity_ids=[5],
    )

    assert isinstance(ingest, IngestClaudeTranscript)
    assert ingest.source is source
    assert ingest.storage_config is storage
    assert isinstance(transcript_rows, QueryTranscriptRows)
    assert transcript_rows.storage_config is storage
    assert isinstance(query, QueryTrajectory)
    assert query.run_id == "run-1"
    assert query.selection is selection
    assert query.ticks == (2,)
    assert query.entity_ids == (3,)
    assert isinstance(grade, GradeTrajectory)
    assert grade.graders == (grader,)
    assert grade.ticks == (4,)
    assert grade.entity_ids == (5,)
    assert world.calls == [
        ("ingest_claude_transcript", True),
        ("query_transcript_rows", True),
        ("query_trajectory", False),
        ("grade_trajectory", False),
    ]


@pytest.mark.asyncio
async def test_mission_world_transcript_capabilities_require_storage(tmp_path: Path) -> None:
    adapter = MissionWorld(_World(storage=None))

    with pytest.raises(ValueError, match="explicit storage coordinates"):
        await adapter.ingest_claude_transcript(
            ClaudeTranscriptSource(path=tmp_path / "session.jsonl")
        )
    with pytest.raises(ValueError, match="explicit storage coordinates"):
        await adapter.transcript_rows()

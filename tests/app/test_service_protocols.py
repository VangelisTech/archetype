# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable checks that family protocols are active, complete boundaries."""

from __future__ import annotations

import inspect

import pytest

from archetype.app.application.interfaces import iRuntimeApplication
from archetype.app.application.service import RuntimeApplication
from archetype.app.artifacts.bundle_service import ArtifactBundleService
from archetype.app.artifacts.interfaces import (
    iArtifactBundleService,
    iArtifactService,
    iArtifactTableService,
)
from archetype.app.artifacts.service import ArtifactService
from archetype.app.artifacts.table_service import ArtifactTableService
from archetype.app.audit.interfaces import iAuditLog
from archetype.app.audit.service import AuditLog
from archetype.app.commands.interfaces import iCommandScheduler
from archetype.app.commands.service import CommandScheduler
from archetype.app.container import ServiceContainer
from archetype.app.evaluation.interfaces import iEvaluationService
from archetype.app.evaluation.service import EvaluationService
from archetype.app.gateway.interfaces import iCommandGateway
from archetype.app.gateway.service import CommandGateway
from archetype.app.missions.interfaces import (
    iMissionService,
    iTrajectoryService,
)
from archetype.app.missions.service import MissionService
from archetype.app.missions.trajectory_service import TrajectoryService
from archetype.app.query.interfaces import iQueryService
from archetype.app.query.service import QueryService
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.service import RedactionService
from archetype.app.research.interfaces import iResearchService
from archetype.app.research.service import AutoResearchService
from archetype.app.storage.interfaces import iStorageService
from archetype.app.storage.service import StorageService
from archetype.app.world.interfaces import iMutationService, iSimulationService, iWorldService
from archetype.app.world.mutation import MutationService
from archetype.app.world.service import WorldService
from archetype.app.world.simulation import SimulationService

pytestmark = pytest.mark.contract("architecture.protocols.complete")


SERVICE_PROTOCOLS = (
    (StorageService, iStorageService),
    (WorldService, iWorldService),
    (MutationService, iMutationService),
    (SimulationService, iSimulationService),
    (QueryService, iQueryService),
    (ArtifactService, iArtifactService),
    (ArtifactTableService, iArtifactTableService),
    (ArtifactBundleService, iArtifactBundleService),
    (MissionService, iMissionService),
    (TrajectoryService, iTrajectoryService),
    (RedactionService, iRedactionService),
    (EvaluationService, iEvaluationService),
    (AutoResearchService, iResearchService),
    (AuditLog, iAuditLog),
    (CommandScheduler, iCommandScheduler),
    (RuntimeApplication, iRuntimeApplication),
    (CommandGateway, iCommandGateway),
)


def _public_operations(cls: type) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(cls)
        if not name.startswith("_") and (inspect.isfunction(member) or inspect.ismethod(member))
    }


@pytest.mark.parametrize(("implementation", "protocol"), SERVICE_PROTOCOLS)
def test_family_protocol_covers_every_public_service_operation(implementation, protocol) -> None:
    missing = _public_operations(implementation) - _public_operations(protocol)
    assert not missing, f"{protocol.__name__} is missing {sorted(missing)}"


@pytest.mark.asyncio
async def test_container_wiring_conforms_to_every_family_protocol() -> None:
    container = ServiceContainer()
    try:
        bindings = (
            (container.storage_service, iStorageService),
            (container.world_service, iWorldService),
            (container.mutation_service, iMutationService),
            (container.simulation_service, iSimulationService),
            (container.query_service, iQueryService),
            (container.artifact_service, iArtifactService),
            (container.artifact_table_service, iArtifactTableService),
            (container.artifact_bundle_service, iArtifactBundleService),
            (container.trajectory_service, iTrajectoryService),
            (container.redaction_service, iRedactionService),
            (container.evaluation_service, iEvaluationService),
            (container.autoresearch_service, iResearchService),
            (container.audit_log, iAuditLog),
            (container.command_scheduler, iCommandScheduler),
            (container.application, iRuntimeApplication),
            (container.command_gateway, iCommandGateway),
        )
        assert all(isinstance(service, protocol) for service, protocol in bindings)
    finally:
        await container.shutdown()

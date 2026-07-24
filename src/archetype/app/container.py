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

"""Internal composition root for the application service graph."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial

from archetype.app.application.service import RuntimeApplication
from archetype.app.artifacts.service import ArtifactService
from archetype.app.audit.service import AuditLog
from archetype.app.commands.service import CommandScheduler
from archetype.app.evaluation.service import EvaluationService
from archetype.app.gateway.service import CommandGateway
from archetype.app.ingestion.service import IngestionService
from archetype.app.missions.service import MissionService
from archetype.app.missions.trajectory_service import TrajectoryService
from archetype.app.missions.transcript_service import TranscriptIngestionService
from archetype.app.physical_ai.service import PhysicalAIService
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.service import RedactionService
from archetype.app.research.service import AutoResearchService
from archetype.artifacts.contracts import ArtifactStoreConfig
from archetype.core.config import StorageConfig
from archetype.missions.contracts import AgentMissionConfig
from archetype.missions.sandboxes.service import SandboxService
from archetype.storage.config import ControlCatalogConfig
from archetype.storage.service import StorageService
from archetype.world import mutation
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry


class ServiceContainer:
    """Construct and own the internal service graph.

    Each service owns its internal composition.
    The container handles concrete construction and callback wiring. Application
    code uses ``ArchetypeRuntime`` or the REST/CLI adapters rather than calling
    these delegates directly.
    """

    def __init__(
        self,
        storage_service: StorageService | None = None,
        audit_storage_config: StorageConfig | None = None,
        artifact_store_config: ArtifactStoreConfig | None = None,
        redaction_service: iRedactionService | None = None,
    ):
        if storage_service is not None and storage_service.has_injected_session:
            if audit_storage_config is None:
                raise ValueError(
                    "audit_storage_config is required when ServiceContainer uses an "
                    "injected Daft Session"
                )
            storage_service.require_iceberg_identity(audit_storage_config)

        # Leaf services
        self._owns_storage_service = storage_service is None
        self.storage_service = (
            storage_service
            if storage_service is not None
            else StorageService(
                control_catalog_config=ControlCatalogConfig.from_env(),
            )
        )
        self.redaction_service = (
            redaction_service if redaction_service is not None else RedactionService()
        )

        # Canonical world ownership and exact tick materialization.
        self.world_registry = WorldRegistry()

        async def require_live_world(world_id) -> None:
            async with self.world_registry.operation(str(world_id)):
                pass

        async def resolve_control_catalog(world_id):
            record = await self.world_registry.storage_record(str(world_id))
            if record is None:
                raise KeyError(f"world {world_id} has no known storage identity")
            return self.storage_service.get_control_catalog(record[0])

        self.command_scheduler = CommandScheduler(
            require_live_world=require_live_world,
            resolve_control_catalog=resolve_control_catalog,
            list_catalog_world_ids=self.world_registry.catalog_world_ids,
            reserve_entity_ids=partial(
                mutation.reserve_entity_ids,
                self.world_registry,
            ),
        )
        self.world_lifecycle = WorldLifecycle(
            self.storage_service,
            self.world_registry,
            materialize_commands=self.command_scheduler.materialize,
        )

        # Storage-backed application workflows.
        self.audit_log = AuditLog(self.storage_service, audit_storage_config)
        self.ingestion_service = IngestionService(
            self.storage_service,
            self.world_registry,
        )
        self.artifact_service = ArtifactService(
            self.storage_service,
            self.world_registry,
            self.ingestion_service,
            artifact_store_config,
        )
        self.transcript_ingestion_service = TranscriptIngestionService(
            self.artifact_service,
            self.ingestion_service,
            self.redaction_service,
            self.storage_service,
            self.world_registry,
        )
        self.evaluation_service = EvaluationService(
            self.ingestion_service,
            self.storage_service,
            self.world_registry,
        )
        self.trajectory_service = TrajectoryService(
            self.storage_service,
            self.evaluation_service,
        )
        self.physical_ai_service = PhysicalAIService(
            self.world_registry,
            self.world_lifecycle,
            self.evaluation_service,
            self.storage_service,
        )
        self.audit_log.set_outbox_source(
            self.command_scheduler.read_outbox,
            self.command_scheduler.mark_outbox_projected,
        )

        async def destroy_rollout_world(world_id) -> None:
            # RuntimeApplication owns the cross-family teardown ordering. The
            # late-bound callback breaks the construction cycle while keeping
            # autoresearch fork cleanup on that one canonical path.
            await self.application.destroy_world(world_id)

        self.autoresearch_service = AutoResearchService(
            self.world_registry,
            self.world_lifecycle,
            self.storage_service,
            destroy_world=destroy_rollout_world,
        )

        self.application = RuntimeApplication(
            registry=self.world_registry,
            lifecycle=self.world_lifecycle,
            storage=self.storage_service,
            commands=self.command_scheduler,
            audit=self.audit_log,
            artifacts=self.artifact_service,
            transcripts=self.transcript_ingestion_service,
            evaluations=self.evaluation_service,
            trajectories=self.trajectory_service,
            research=self.autoresearch_service,
            physical_ai=self.physical_ai_service,
            agent_missions=self._agent_mission_service,
        )
        self.command_gateway = CommandGateway(
            self.application,
            self.audit_log,
            target_tick_for_world=lambda world_id: self.world_registry.target_tick(str(world_id)),
        )

    def _agent_mission_service(self, *, config: AgentMissionConfig, **kwargs) -> MissionService:
        """Compose one mission-owned sandbox lifetime beneath the app workflow."""

        return MissionService(
            config=config,
            sandbox_service=SandboxService((config.sandbox_backend,)),
            redaction_service=self.redaction_service,
            **kwargs,
        )

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        steps: list[tuple[str, Callable[[], Awaitable[None]]]] = [
            ("application admission", self.application.stop_admission),
            ("audit log", self.audit_log.shutdown),
        ]
        if self._owns_storage_service:
            steps.append(("storage service", self.storage_service.shutdown))

        failures: list[BaseException] = []
        for label, shutdown in steps:
            try:
                await shutdown()
            except BaseException as exc:
                exc.add_note(f"ServiceContainer shutdown step failed: {label}")
                failures.append(exc)
        if failures:
            raise BaseExceptionGroup(
                f"ServiceContainer shutdown failed for {len(failures)} step(s)",
                failures,
            )

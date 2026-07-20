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

from archetype.app.application.service import RuntimeApplication
from archetype.app.artifacts.bundle_service import ArtifactBundleService
from archetype.app.artifacts.service import ArtifactService
from archetype.app.artifacts.table_service import ArtifactTableService
from archetype.app.artifacts.transcript_service import ClaudeTranscriptIngestionService
from archetype.app.audit.service import AuditLog
from archetype.app.commands.service import CommandScheduler
from archetype.app.evaluation.service import EvaluationService
from archetype.app.gateway.auth import reset_tick_counters
from archetype.app.gateway.service import CommandGateway
from archetype.app.missions.service import MissionService
from archetype.app.missions.trajectory_service import TrajectoryService
from archetype.app.physical_ai.service import PhysicalAIService
from archetype.app.query.service import QueryService
from archetype.app.redaction.interfaces import iRedactionService
from archetype.app.redaction.service import RedactionService
from archetype.app.research.service import AutoResearchService
from archetype.app.storage.service import StorageService
from archetype.app.world.mutation import MutationService
from archetype.app.world.service import WorldService
from archetype.app.world.simulation import SimulationService
from archetype.artifacts.bundles import ArtifactSourceResolver, ArtifactStoreConfig
from archetype.core.config import StorageConfig
from archetype.missions.contracts import AgentMissionConfig
from archetype.missions.sandboxes.service import SandboxService


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
        artifact_source_resolver: ArtifactSourceResolver | None = None,
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
        self.storage_service = storage_service if storage_service is not None else StorageService()
        self.redaction_service = (
            redaction_service if redaction_service is not None else RedactionService()
        )

        # Storage-backed services
        self.world_service = WorldService(self.storage_service)
        self.audit_log = AuditLog(self.storage_service, audit_storage_config)
        self.query_service = QueryService(self.storage_service, self.audit_log)
        self.artifact_table_service = ArtifactTableService(self.storage_service, self.world_service)
        self.artifact_service = ArtifactService(self.storage_service, self.world_service)
        self.artifact_bundle_service = ArtifactBundleService(
            self.storage_service,
            self.world_service,
            artifact_store_config,
            artifact_source_resolver,
            redaction_service=self.redaction_service,
        )
        self.transcript_ingestion_service = ClaudeTranscriptIngestionService(
            self.artifact_service,
            self.artifact_table_service,
            self.redaction_service,
            self.world_service,
        )
        self.evaluation_service = EvaluationService(self.query_service, self.artifact_service)
        self.trajectory_service = TrajectoryService(
            self.query_service,
            self.evaluation_service,
        )

        # Services that depend on WorldService
        self.mutation_service = MutationService(self.world_service)
        self.simulation_service = SimulationService(self.world_service)
        self.physical_ai_service = PhysicalAIService(
            self.world_service,
            self.mutation_service,
            self.simulation_service,
            self.evaluation_service,
        )
        self.command_scheduler = CommandScheduler(self.world_service, self.mutation_service)
        self.audit_log.set_outbox_source(
            self.command_scheduler.read_outbox,
            self.command_scheduler.mark_outbox_projected,
        )

        # AutoResearch — depends on WorldService + SimulationService
        self.autoresearch_service = AutoResearchService(self.world_service, self.simulation_service)

        self.application = RuntimeApplication(
            mutations=self.mutation_service,
            worlds=self.world_service,
            simulation=self.simulation_service,
            queries=self.query_service,
            commands=self.command_scheduler,
            audit=self.audit_log,
            artifact_tables=self.artifact_table_service,
            artifacts=self.artifact_service,
            artifact_bundles=self.artifact_bundle_service,
            transcripts=self.transcript_ingestion_service,
            evaluations=self.evaluation_service,
            trajectories=self.trajectory_service,
            research=self.autoresearch_service,
            physical_ai=self.physical_ai_service,
            agent_missions=self._agent_mission_service,
        )
        self.command_gateway = CommandGateway(self.application, self.audit_log)
        self.simulation_service.set_command_drain(self.application.drain_and_apply)
        # Per-tick RBAC quota resets at each tick boundary (bug B1).
        self.simulation_service.set_quota_reset(reset_tick_counters)

    @staticmethod
    def _agent_mission_service(*, config: AgentMissionConfig, **kwargs) -> MissionService:
        """Compose one mission-owned sandbox lifetime beneath the app workflow."""

        return MissionService(
            config=config,
            sandbox_service=SandboxService((config.sandbox_backend,)),
            **kwargs,
        )

    async def shutdown(self) -> None:
        """Gracefully shut down all services."""
        steps: list[tuple[str, Callable[[], Awaitable[None]]]] = [
            ("application admission", self.application.stop_admission),
            ("audit log", self.audit_log.shutdown),
        ]
        if self._owns_storage_service:
            steps.append(("world and storage services", self.world_service.shutdown))

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

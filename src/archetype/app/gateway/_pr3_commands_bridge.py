# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Finite PR-3 bridge for operations whose final families land in PR-4.

This module is intentionally declarative and state-free.  It exists only so
the command-family move can delete the legacy gateway policy machinery without
pretending that later application-family operations already have canonical
registrations.  PR-4 registers the listed models at their final owners and
deletes this bridge.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from archetype.commands.models import ActorCtx


class _PurePolicy(Protocol):
    def preauthorize(
        self,
        actor: ActorCtx,
        *,
        permission: str,
    ) -> None: ...


PR3_BRIDGE_MODEL_LITERALS = MappingProxyType(
    {
        "IngestArtifacts": "ingest_artifacts",
        "QueryArtifacts": "query_artifacts",
        "RunGraders": "run_graders",
        "Evaluate": "evaluate",
        "AutoResearch": "autoresearch",
        "EvaluatePhysicalTask": "evaluate_physical_task",
        "SweepPhysicalInstructions": "sweep_physical_instructions",
        "IngestClaudeTranscript": "ingest_claude_transcript",
        "QueryTranscriptRows": "query_transcript_rows",
        "QueryTrajectory": "query_trajectory",
        "GradeTrajectory": "grade_trajectory",
        "SubmitMission": "submit_mission",
        "RunMission": "run_mission",
        "RestoreMissionSandbox": "restore_mission_sandbox",
    }
)

PR3_BRIDGE_UNTRUSTED_OPERATIONS = frozenset(
    {
        "autoresearch",
        "ingest_artifacts",
        "query_artifacts",
        "evaluate",
    }
)

PR3_BRIDGE_ALIASES = MappingProxyType(
    {
        "RuntimeMissions.query": "query_components",
    }
)

DELETE_BEFORE_PR4_WIRING = True

_BRIDGE_OPERATIONS = frozenset(PR3_BRIDGE_MODEL_LITERALS.values())


def preauthorize_pr3_bridge_actor_call(
    policy: _PurePolicy,
    actor: ActorCtx,
    *,
    operation: str,
) -> None:
    """Preauthorize one exact temporary actor route before any resource read."""
    if operation not in _BRIDGE_OPERATIONS:
        raise KeyError(f"unknown PR-3 bridge operation {operation!r} is not registered")

    policy.preauthorize(actor, permission=operation)
    if operation not in PR3_BRIDGE_UNTRUSTED_OPERATIONS:
        raise PermissionError(
            f"PR-3 bridge operation {operation!r} is trusted-only and "
            "not available to untrusted callers"
        )


__all__ = [
    "DELETE_BEFORE_PR4_WIRING",
    "PR3_BRIDGE_ALIASES",
    "PR3_BRIDGE_MODEL_LITERALS",
    "PR3_BRIDGE_UNTRUSTED_OPERATIONS",
    "preauthorize_pr3_bridge_actor_call",
]

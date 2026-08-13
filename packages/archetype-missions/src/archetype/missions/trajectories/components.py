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

"""Persistent episode-evidence schemas for mission and rollout evidence.

Evidence is stored as normalized, Arrow-friendly component rows keyed by
``episode_id`` and ``seq``. ``episode_id`` is the one persistent identity of a
bounded execution; a trajectory is a derived DataFrame view over these rows
(see :func:`archetype.missions.trajectories.transforms.trajectory`), never a
second persistent identity.
"""

from __future__ import annotations

from archetype.core.component import Component

CLAUDE_TRANSCRIPT_TABLE = "coding_agent_transcript_rows"


class TrajectoryTurn(Component):
    """One typed conversational or tool-use turn row of episode evidence."""

    episode_id: str = ""
    seq: int = 0
    role: str = ""
    content: str = ""
    tool_name: str = ""
    tool_input: str = ""
    tool_output: str = ""
    tokens: int = 0
    duration_ms: float = 0.0
    error: str = ""


class TranscriptArtifactRef(Component):
    """Lightweight link from episode evidence to redacted transcript artifacts.

    Narrative content never belongs in this Component. Transcript ingestion
    writes normalized narrative rows only through the artifact table boundary.
    """

    episode_id: str = ""
    mission_id: str = ""
    source_uri: str = ""
    source_content_hash: str = ""
    redaction_policy_id: str = ""
    redaction_status: str = "clean"
    redaction_count: int = 0
    redaction_rule_ids_json: str = "[]"
    table_name: str = CLAUDE_TRANSCRIPT_TABLE


class TrajectoryCommandEvent(Component):
    """One typed command/audit event row of episode evidence."""

    episode_id: str = ""
    seq: int = 0
    audit_id: str = ""
    command_id: str = ""
    world_id: str = ""
    actor_id: str = ""
    command_type: str = ""
    status: str = ""
    tick: int = 0
    priority: int = 0
    version: int = 0
    accepted_at: str = ""
    applied_at: str = ""


class TrajectoryObservation(Component):
    """One typed observation/event row of episode evidence."""

    episode_id: str = ""
    seq: int = 0
    world_id: str = ""
    tick: int = 0
    event_type: str = ""
    archetype_count: int = 0
    entity_count: int = 0


class TrajectoryAction(Component):
    """One typed action row of episode evidence."""

    episode_id: str = ""
    seq: int = 0
    tick: int = 0
    action_type: str = ""


class TrajectoryReward(Component):
    """One typed reward row of episode evidence."""

    episode_id: str = ""
    seq: int = 0
    reward: float = 0.0

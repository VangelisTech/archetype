# Copyright 2026 Vangelis Technologies Inc.
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

"""Single-ego trajectory rows and deterministic labeling helpers.

The ego schema is deliberately capture-adapter agnostic. Screen capture,
browser event streams, robotics traces, and conversational observers can all
emit ``EgoObservation`` rows without coupling the derivation to their capture
libraries.
"""

from __future__ import annotations

from typing import Protocol

from archetype.core.component import Component


class EgoObservationSource(Protocol):
    """Adapter protocol for sources that emit ego observations."""

    def observations(self) -> list[EgoObservation]:
        """Return normalized observations in source-local sequence order."""
        ...


class EgoObservation(Component):
    """One observation from a single ego trajectory.

    The component is intentionally modality-neutral. A screen capture adapter
    can store the frame URI here, while a conversation, browser, robot, or
    desktop observer can use the same row shape.
    """

    trajectory_id: str = ""
    seq: int = 0
    subject_id: str = ""
    modality: str = ""
    frame_uri: str = ""
    focus: str = ""
    context: str = ""
    captured_at_ms: int = 0

    salience: float = 0.0
    valence: float = 0.0
    arousal: float = 0.0
    effort: float = 0.0
    agency: float = 0.0
    external_pressure: float = 0.0

    @classmethod
    def from_screen_frame(
        cls,
        trajectory_id: str,
        seq: int,
        frame_uri: str,
        *,
        subject_id: str = "",
        focus: str = "",
        context: str = "",
        captured_at_ms: int = 0,
        salience: float = 0.0,
        valence: float = 0.0,
        arousal: float = 0.0,
        effort: float = 0.0,
        agency: float = 0.0,
        external_pressure: float = 0.0,
    ) -> EgoObservation:
        """Build an ego observation from a captured frame artifact."""
        return cls(
            trajectory_id=trajectory_id,
            seq=seq,
            subject_id=subject_id,
            modality="screen",
            frame_uri=frame_uri,
            focus=focus,
            context=context,
            captured_at_ms=captured_at_ms,
            salience=_clamp_unit(salience),
            valence=_clamp_signed(valence),
            arousal=_clamp_unit(arousal),
            effort=_clamp_unit(effort),
            agency=_clamp_unit(agency),
            external_pressure=_clamp_unit(external_pressure),
        )


class EgoLabel(Component):
    """A derived label for one ego observation."""

    trajectory_id: str = ""
    seq: int = 0
    phase: str = ""
    pattern: str = ""
    value: str = ""
    confidence: float = 0.0
    rationale: str = ""


class EgoTrajectoryPattern(Component):
    """A trajectory-level canonical pattern over derived ego labels."""

    trajectory_id: str = ""
    pattern: str = ""
    canonical_path: str = ""
    first_seq: int = 0
    last_seq: int = 0
    support: int = 0
    agency_delta: float = 0.0
    terminal_phase: str = ""


def _clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _clamp_signed(value: float) -> float:
    return max(-1.0, min(1.0, float(value)))


def _ego_phase(observation: EgoObservation) -> str:
    if observation.seq == 0:
        return "witness"
    if observation.external_pressure >= 0.65 and observation.agency <= 0.4:
        return "strain"
    if observation.external_pressure >= 0.55 and observation.agency >= 0.45:
        return "question"
    if observation.agency >= 0.7 and observation.effort >= 0.5:
        return "commit"
    if observation.agency >= 0.7 and observation.external_pressure < 0.4:
        return "depart"
    if observation.salience >= 0.55:
        return "orient"
    return "witness"


def _ego_pattern(observation: EgoObservation, phase: str) -> str:
    if (
        observation.external_pressure >= 0.65
        and observation.agency <= 0.45
        and observation.effort >= 0.6
    ):
        return "instrumentalized_intelligence"
    if phase == "question":
        return "rupture"
    if observation.agency >= 0.7 and observation.agency >= observation.external_pressure:
        return "self_authored_dream"
    if phase in {"witness", "orient"}:
        return "witnessing"
    return "adaptive_motion"


def derive_ego_label(observation: EgoObservation) -> EgoLabel:
    """Derive a canonical label from one ego observation.

    The derivation keeps the humanism question explicit: high effort under
    external pressure with low agency is not "optimization"; it is a captured
    trajectory.
    """
    phase = _ego_phase(observation)
    pattern = _ego_pattern(observation, phase)
    pressure_gap = abs(observation.external_pressure - observation.agency)
    confidence = _clamp_unit(max(observation.salience, pressure_gap))
    value = phase

    if pattern == "instrumentalized_intelligence":
        value = "captured"
        rationale = "effort is high while agency is low under external pressure"
    elif pattern == "self_authored_dream":
        value = "self-authored"
        rationale = "agency is high enough to carry the dream as its own trajectory"
    elif pattern == "rupture":
        value = "questioning"
        rationale = "agency rises inside pressure, turning compliance into inquiry"
    else:
        rationale = "observation remains within the ordinary attention path"

    return EgoLabel(
        trajectory_id=observation.trajectory_id,
        seq=observation.seq,
        phase=phase,
        pattern=pattern,
        value=value,
        confidence=confidence,
        rationale=rationale,
    )


def derive_ego_labels(observations: list[EgoObservation]) -> list[EgoLabel]:
    """Derive labels for ego observations in sequence order."""
    return [
        derive_ego_label(observation)
        for observation in sorted(observations, key=lambda row: row.seq)
    ]


def derive_ego_trajectory_pattern(
    observations: list[EgoObservation],
    labels: list[EgoLabel] | None = None,
) -> EgoTrajectoryPattern:
    """Summarize a single ego trajectory as a canonical path."""
    ordered = sorted(observations, key=lambda row: row.seq)
    if not ordered:
        raise ValueError("cannot derive an ego trajectory pattern without observations")

    derived = labels or derive_ego_labels(ordered)
    derived = sorted(derived, key=lambda row: row.seq)
    canonical_path = ">".join(label.phase for label in derived)
    terminal = ordered[-1]
    agency_delta = terminal.agency - ordered[0].agency
    label_patterns = {label.pattern for label in derived}

    if "instrumentalized_intelligence" in label_patterns and terminal.agency < 0.5:
        pattern = "captured_dream"
    elif (
        "instrumentalized_intelligence" in label_patterns
        and terminal.agency >= 0.5
        and agency_delta > 0
    ):
        pattern = "reclaimed_dream"
    elif terminal.agency >= 0.7 and terminal.external_pressure < 0.4:
        pattern = "self_authored_dream"
    else:
        pattern = "observed_trajectory"

    return EgoTrajectoryPattern(
        trajectory_id=ordered[0].trajectory_id,
        pattern=pattern,
        canonical_path=canonical_path,
        first_seq=ordered[0].seq,
        last_seq=terminal.seq,
        support=len(ordered),
        agency_delta=agency_delta,
        terminal_phase=derived[-1].phase,
    )

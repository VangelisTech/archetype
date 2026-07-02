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

"""Prompt contract for generating ego observations from unstructured traces."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from archetype.experiments.ego import EgoObservation

EGO_OBSERVATION_OUTPUT_GRAMMAR = r"""
ego_trajectory_output ::= object_start
  trajectory_id_pair comma
  subject_id_pair comma
  source_pair comma
  observations_pair
object_end

trajectory_id_pair ::= quote "trajectory_id" quote colon string
subject_id_pair ::= quote "subject_id" quote colon string
source_pair ::= quote "source" quote colon source_object
observations_pair ::= quote "observations" quote colon observations_array

source_object ::= object_start
  quote "modality" quote colon modality comma
  quote "artifact_uri" quote colon string comma
  quote "description" quote colon string
object_end

observations_array ::= array_start observation (comma observation)* array_end

observation ::= object_start
  quote "seq" quote colon integer comma
  quote "frame_uri" quote colon string comma
  quote "focus" quote colon string comma
  quote "context" quote colon string comma
  quote "captured_at_ms" quote colon integer comma
  quote "salience" quote colon unit_score comma
  quote "valence" quote colon signed_score comma
  quote "arousal" quote colon unit_score comma
  quote "effort" quote colon unit_score comma
  quote "agency" quote colon unit_score comma
  quote "external_pressure" quote colon unit_score
object_end

modality ::= quote ("screen" | "conversation" | "browser" | "robot" | "desktop" | "text") quote
unit_score ::= number_between_0_and_1
signed_score ::= number_between_minus_1_and_1

object_start ::= "{"
object_end ::= "}"
array_start ::= "["
array_end ::= "]"
comma ::= ","
colon ::= ":"
quote ::= "\""
string ::= valid_json_string
integer ::= non_negative_json_integer
""".strip()


EGO_OBSERVATION_JSON_SCHEMA: dict[str, object] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["trajectory_id", "subject_id", "source", "observations"],
    "properties": {
        "trajectory_id": {"type": "string"},
        "subject_id": {"type": "string"},
        "source": {
            "type": "object",
            "additionalProperties": False,
            "required": ["modality", "artifact_uri", "description"],
            "properties": {
                "modality": {
                    "type": "string",
                    "enum": ["screen", "conversation", "browser", "robot", "desktop", "text"],
                },
                "artifact_uri": {"type": "string"},
                "description": {"type": "string"},
            },
        },
        "observations": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "seq",
                    "frame_uri",
                    "focus",
                    "context",
                    "captured_at_ms",
                    "salience",
                    "valence",
                    "arousal",
                    "effort",
                    "agency",
                    "external_pressure",
                ],
                "properties": {
                    "seq": {"type": "integer", "minimum": 0},
                    "frame_uri": {"type": "string"},
                    "focus": {"type": "string"},
                    "context": {"type": "string"},
                    "captured_at_ms": {"type": "integer", "minimum": 0},
                    "salience": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "valence": {"type": "number", "minimum": -1.0, "maximum": 1.0},
                    "arousal": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "effort": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "agency": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "external_pressure": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                    },
                },
            },
        },
    },
}


EGO_OBSERVATION_PROMPT = """
You convert an unstructured trace into normalized EgoObservation rows.

Return only JSON matching the provided schema/grammar. Do not emit labels,
phases, or final trajectory patterns. Those are derived deterministically by
Archetype after your observations are parsed.

Scoring rubric:
- salience: how central this beat is to the ego trajectory.
- valence: negative to positive affect, from -1.0 to 1.0.
- arousal: intensity or activation, from 0.0 to 1.0.
- effort: how much optimization, work, or self-control is demanded.
- agency: how self-authored the subject's action or attention is.
- external_pressure: how much the subject is being shaped by another person,
  institution, metric, threat, promise, or dream.

Trajectory construction rules:
- Create 3 to 8 observations unless the trace has fewer distinct beats.
- seq starts at 0 and increments by 1.
- Use concise noun phrases for focus.
- Use context to name the pressure, question, choice, or witnessed event.
- Use frame_uri for a concrete capture path/URI when available; otherwise "".
- captured_at_ms is 0 when timing is unknown.
- Prefer honest uncertainty over overfitting: use middle scores when the trace
  does not justify an extreme.
""".strip()


def ego_observations_from_structured_output(
    output: Mapping[str, Any],
) -> list[EgoObservation]:
    """Convert structured prompt output into ``EgoObservation`` components."""
    trajectory_id = str(output.get("trajectory_id") or "")
    subject_id = str(output.get("subject_id") or "")
    source = output.get("source") or {}
    modality = str(source.get("modality") or "") if isinstance(source, Mapping) else ""
    observations = output.get("observations") or []
    if not isinstance(observations, list):
        raise TypeError("structured ego output must contain an observations list")

    rows: list[EgoObservation] = []
    for idx, item in enumerate(observations):
        if not isinstance(item, Mapping):
            raise TypeError("structured ego observations must be objects")
        rows.append(
            EgoObservation(
                trajectory_id=trajectory_id,
                seq=int(item.get("seq", idx)),
                subject_id=subject_id,
                modality=modality,
                frame_uri=str(item.get("frame_uri") or ""),
                focus=str(item.get("focus") or ""),
                context=str(item.get("context") or ""),
                captured_at_ms=int(item.get("captured_at_ms") or 0),
                salience=float(item.get("salience") or 0.0),
                valence=float(item.get("valence") or 0.0),
                arousal=float(item.get("arousal") or 0.0),
                effort=float(item.get("effort") or 0.0),
                agency=float(item.get("agency") or 0.0),
                external_pressure=float(item.get("external_pressure") or 0.0),
            )
        )
    return rows

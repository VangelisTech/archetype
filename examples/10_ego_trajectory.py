# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Example 09 — Ego trajectory derivation from structured output
=============================================================

Converts a structured-output fixture into EgoObservation rows, then derives
labels and a trajectory-level pattern deterministically.

This example is intentionally CI-safe: no live model call, no screen capture,
and no credentials. A real capture adapter only needs to produce the same JSON
shape as STRUCTURED_OUTPUT.

Run: uv run python examples/10_ego_trajectory.py
"""

from __future__ import annotations

import json

from archetype.experiments import (
    EGO_OBSERVATION_JSON_SCHEMA,
    EGO_OBSERVATION_OUTPUT_GRAMMAR,
    EGO_OBSERVATION_PROMPT,
    derive_ego_labels,
    derive_ego_trajectory_pattern,
    ego_observations_from_structured_output,
)

STRUCTURED_OUTPUT = {
    "trajectory_id": "yume-desu-001",
    "subject_id": "ego",
    "source": {
        "modality": "screen",
        "artifact_uri": "captures/yume-desu",
        "description": "A subject watches a dream become a demand, then asks whether it is theirs.",
    },
    "observations": [
        {
            "seq": 0,
            "frame_uri": "captures/yume-desu/000.png",
            "focus": "witnessing",
            "context": "the subject sees another person define a dream",
            "captured_at_ms": 0,
            "salience": 0.7,
            "valence": 0.0,
            "arousal": 0.4,
            "effort": 0.1,
            "agency": 0.2,
            "external_pressure": 0.2,
        },
        {
            "seq": 1,
            "frame_uri": "captures/yume-desu/001.png",
            "focus": "performance",
            "context": "the dream turns into a standard the subject must satisfy",
            "captured_at_ms": 1300,
            "salience": 0.9,
            "valence": -0.5,
            "arousal": 0.8,
            "effort": 0.9,
            "agency": 0.2,
            "external_pressure": 0.9,
        },
        {
            "seq": 2,
            "frame_uri": "captures/yume-desu/002.png",
            "focus": "question",
            "context": "the subject asks whether optimization without agency is still a life",
            "captured_at_ms": 2400,
            "salience": 0.95,
            "valence": -0.2,
            "arousal": 0.9,
            "effort": 0.7,
            "agency": 0.55,
            "external_pressure": 0.7,
        },
        {
            "seq": 3,
            "frame_uri": "captures/yume-desu/003.png",
            "focus": "departure",
            "context": "the subject chooses a trajectory that can become their own",
            "captured_at_ms": 3600,
            "salience": 0.9,
            "valence": 0.4,
            "arousal": 0.7,
            "effort": 0.65,
            "agency": 0.82,
            "external_pressure": 0.2,
        },
    ],
}


def main() -> None:
    observations = ego_observations_from_structured_output(STRUCTURED_OUTPUT)
    labels = derive_ego_labels(observations)
    pattern = derive_ego_trajectory_pattern(observations, labels)

    print("Structured-output contract")
    print(f"  prompt_chars={len(EGO_OBSERVATION_PROMPT)}")
    print(f"  grammar_root={'ego_trajectory_output' in EGO_OBSERVATION_OUTPUT_GRAMMAR}")
    print(f"  schema_required={json.dumps(EGO_OBSERVATION_JSON_SCHEMA['required'])}")
    print()

    print("Observations -> labels")
    for observation, label in zip(observations, labels, strict=True):
        print(
            "  "
            f"{observation.seq}: focus={observation.focus!r} "
            f"agency={observation.agency:.2f} "
            f"pressure={observation.external_pressure:.2f} "
            f"phase={label.phase} "
            f"value={label.value}"
        )

    print()
    print("Trajectory pattern")
    print(f"  pattern={pattern.pattern}")
    print(f"  path={pattern.canonical_path}")
    print(f"  agency_delta={pattern.agency_delta:.2f}")


if __name__ == "__main__":
    main()

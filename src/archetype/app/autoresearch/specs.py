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

"""Canonical specification shapes for AutoResearch experiments."""

from typing import Any


def get_canonical_world_spec() -> dict[str, Any]:
    """Get the canonical world specification for v0.1."""
    return {
        "world_name": "experiment-world",
        "storage": {
            "uri": "./archetype_data",
            "namespace": "archetypes",
            "backend": "lancedb"
        },
        "cache": {
            "flush_rows": 1000000,
            "flush_mb": 512,
            "idle_sec": 30.0
        },
        "resources": {},
        "metadata": {}
    }


def get_canonical_run_spec() -> dict[str, Any]:
    """Get the canonical run specification for v0.1."""
    return {
        "budget_kind": "steps",
        "budget_value": 1,
        "debug": False,
        "prefer_live_reads": True,
        "show_rows": 0,
        "suite": "autoresearch",
        "trial": None,
        "metadata": {}
    }


def get_canonical_evaluation_spec(
    primary_metric_name: str = "val_bpb",
    direction: str = "min"
) -> dict[str, Any]:
    """Get the canonical evaluation specification for v0.1."""
    return {
        "primary_metric_name": primary_metric_name,
        "direction": direction,
        "secondary_metric_names": [
            "peak_vram_mb",
            "training_seconds",
            "total_seconds"
        ],
        "crash_is_failure": True,
        "metadata": {}
    }
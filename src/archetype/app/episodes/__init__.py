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

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Episodes Layer: Parallel Rollouts with Sampled Initial Conditions
=================================================================

This layer provides abstractions for:
- Sampling initial conditions (flight controls perspective)
- Running single episodes
- Orchestrating parallel episodes for dataset curation

The key insight: episodes are initialized by SAMPLING initial conditions,
enabling Monte Carlo analysis and embarrassingly parallel execution.

Example:
    from archetype.app.episodes import (
        LatinHypercubeSampler,
        Episode,
        ParallelRollout,
    )

    # Sample initial conditions
    sampler = LatinHypercubeSampler(
        component_type=State,
        bounds={"x": (-10, 10), "y": (-10, 10)},
    )

    # Run parallel episodes
    rollout = ParallelRollout(config)
    trajectories = await rollout.run_episodes(
        num_episodes=1000,
        sampler=sampler,
        system_factory=create_system,
        horizon=500,
    )
"""

from archetype.app.episodes.episode import (
    Episode,
    EpisodeResult,
)
from archetype.app.episodes.sampler import (
    CompositeSampler,
    GridSampler,
    InitialConditionSampler,
    LatinHypercubeSampler,
    TrimPointSampler,
    UniformSampler,
)

__all__ = [
    # Samplers
    "InitialConditionSampler",
    "UniformSampler",
    "LatinHypercubeSampler",
    "GridSampler",
    "TrimPointSampler",
    "CompositeSampler",
    # Episode
    "Episode",
    "EpisodeResult",
]

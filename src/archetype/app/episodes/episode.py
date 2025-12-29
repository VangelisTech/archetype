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
Episode: Single Simulation Run with Sampled Initial Conditions
===============================================================

An Episode represents a single simulation run from sampled initial conditions
to completion (horizon or termination). Episodes are the atomic unit for
parallel rollout execution.
"""

from dataclasses import dataclass, field

import daft
from daft import DataFrame
from uuid_utils import UUID, uuid7

from archetype.core import Component
from archetype.core.aio import AsyncWorld
from archetype.core.config import RunConfig


@dataclass
class Episode:
    """
    A single simulation episode.

    Episodes encapsulate:
    - A world instance (owns entity state)
    - Initial conditions (from sampler)
    - Horizon (max steps)
    - Trajectory collection

    Example:
        episode = Episode(
            world=world,
            initial_conditions=[State(x=0, y=0), Action(thrust=0)],
            horizon=1000,
        )
        trajectory = await episode.run(run_config)
    """

    world: AsyncWorld
    initial_conditions: list[Component]
    horizon: int = 1000
    episode_id: UUID = field(default_factory=uuid7)

    # State
    _entity_id: int | None = field(default=None, init=False)
    _trajectory: list[dict] = field(default_factory=list, init=False)

    async def initialize(self) -> int:
        """
        Initialize the episode by spawning entity with initial conditions.

        Returns:
            The entity ID of the spawned entity
        """
        self._entity_id = await self.world.create_entity(self.initial_conditions)
        return self._entity_id

    async def run(
        self,
        run_config: RunConfig | None = None,
        components: list[type[Component]] | None = None,
        **step_kwargs,
    ) -> DataFrame:
        """
        Run the episode and collect trajectory.

        Args:
            run_config: Optional run config (uses defaults if not provided)
            components: Component types to include in trajectory
            **step_kwargs: Additional kwargs passed to processors (e.g., dt=0.1)

        Returns:
            DataFrame with trajectory: [tick, entity_id, component__field, ...]
        """
        if self._entity_id is None:
            await self.initialize()

        run_config = run_config or RunConfig(num_steps=1)
        components = components or [type(c) for c in self.initial_conditions]

        trajectory_rows = []

        # Collect initial state
        df = await self.world.get_components(components)
        rows = df.collect().to_pylist()
        for row in rows:
            row["tick"] = self.world.tick
            row["episode_id"] = str(self.episode_id)
            trajectory_rows.append(row)

        # Run steps and collect
        for _step in range(self.horizon):
            await self.world.step(run_config, **step_kwargs)

            df = await self.world.get_components(components)
            rows = df.collect().to_pylist()
            for row in rows:
                row["tick"] = self.world.tick
                row["episode_id"] = str(self.episode_id)
                trajectory_rows.append(row)

        self._trajectory = trajectory_rows
        return daft.from_pylist(trajectory_rows)

    async def run_until_done(
        self,
        done_component: type[Component],
        done_field: str = "done",
        components: list[type[Component]] | None = None,
        run_config: RunConfig | None = None,
        **step_kwargs,
    ) -> DataFrame:
        """
        Run episode until done flag is set or horizon reached.

        Args:
            done_component: Component containing done flag
            done_field: Field name for done flag
            components: Component types for trajectory
            run_config: Optional run config
            **step_kwargs: Additional kwargs for processors

        Returns:
            DataFrame with trajectory
        """
        if self._entity_id is None:
            await self.initialize()

        run_config = run_config or RunConfig(num_steps=1)
        components = components or [type(c) for c in self.initial_conditions]

        # Ensure done_component is in the list
        if done_component not in components:
            components = list(components) + [done_component]

        trajectory_rows = []
        done_col = f"{done_component.__name__.lower()}__{done_field}"

        for _step in range(self.horizon):
            # Get current state
            df = await self.world.get_components(components)
            rows = df.collect().to_pylist()

            for row in rows:
                row["tick"] = self.world.tick
                row["episode_id"] = str(self.episode_id)
                trajectory_rows.append(row)

            # Check done
            if any(row.get(done_col, False) for row in rows):
                break

            # Step
            await self.world.step(run_config, **step_kwargs)

        self._trajectory = trajectory_rows
        return daft.from_pylist(trajectory_rows)

    @property
    def trajectory(self) -> list[dict]:
        """Get the collected trajectory as a list of dicts."""
        return self._trajectory

    @property
    def entity_id(self) -> int | None:
        """Get the entity ID for this episode."""
        return self._entity_id


@dataclass
class EpisodeResult:
    """
    Result of an episode run.

    Contains trajectory data and metadata for analysis.
    """

    episode_id: UUID
    trajectory: DataFrame
    num_steps: int
    terminated: bool = False  # True if done flag was set
    truncated: bool = False  # True if horizon was reached

    # Optional metrics computed during episode
    total_reward: float = 0.0
    final_state: dict | None = None

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

"""
Simulation Service: API Surface for Multi-World Orchestration
==============================================================

High-level service for managing multi-world simulations and experiments.
External APIs (FastAPI, MCP) call this service to run simulations.
"""

import itertools
import random
from collections.abc import Callable
from typing import Any

from uuid_utils import UUID

from archetype.app.orchestrator import WorldOrchestrator
from archetype.app.world_service import WorldService
from archetype.core.aio import AsyncSystem
from archetype.core.config import (
    CacheConfig,
    RunConfig,
    StorageConfig,
    WorldConfig,
)


class SimulationService:
    """
    High-level service for managing multi-world simulations and experiments.

    Provides the API surface for:
    - Running parallel worlds (embarrassingly parallel)
    - Parameter sweeps across multiple worlds
    - Monte Carlo simulations with seeded trials
    - Experiment lifecycle management
    """

    def __init__(
        self,
        orchestrator: WorldOrchestrator,
        world_service: WorldService,
        storage_config: StorageConfig,
        cache_config: CacheConfig,
    ):
        self.orchestrator = orchestrator
        self.world_service = world_service
        self.storage_config = storage_config
        self.cache_config = cache_config

    async def run_parallel_worlds(
        self,
        num_worlds: int,
        run_config: RunConfig,
        system_factory: Callable[[int], AsyncSystem] | None = None,
    ) -> list[UUID]:
        """
        Create and run multiple worlds in parallel.

        Args:
            num_worlds: Number of worlds to create and run
            run_config: Configuration for how to run each world
            system_factory: Optional callable(index) -> AsyncSystem
                          If not provided, creates default AsyncSystem

        Returns:
            List of world IDs that were created and run
        """
        world_ids = []

        # Create all worlds
        for i in range(num_worlds):
            config = WorldConfig(name=f"world_{i}")

            # Create system (either from factory or default)
            system = system_factory(i) if system_factory else AsyncSystem()

            # Create world via orchestrator
            world = await self.orchestrator.create_world(
                config=config,
                system=system,
                storage_config=self.storage_config,
                cache_config=self.cache_config,
            )
            world_ids.append(world.world_id)

        # Run all worlds concurrently
        await self.orchestrator.run_all_worlds(run_config)

        return world_ids

    async def run_parameter_sweep(
        self,
        base_system_factory: Callable[..., AsyncSystem],
        parameters: dict[str, list[Any]],
        run_config: RunConfig,
    ) -> dict[str, UUID]:
        """
        Run a parameter sweep experiment across multiple worlds.

        Args:
            base_system_factory: Callable(**params) -> AsyncSystem
            parameters: Dict of parameter names to lists of values to sweep
            run_config: Configuration for running each world

        Returns:
            Dict mapping parameter string to world ID

        Example:
            results = await service.run_parameter_sweep(
                base_system_factory=lambda dt, gravity: create_physics_system(dt, gravity),
                parameters={"dt": [0.01, 0.1], "gravity": [9.8, 10.0]},
                run_config=RunConfig(num_steps=1000),
            )
        """
        results = {}

        # Generate all parameter combinations
        param_names = list(parameters.keys())
        param_values = list(parameters.values())
        combinations = list(itertools.product(*param_values))

        # Create a world for each parameter combination
        for combo in combinations:
            param_dict = dict(zip(param_names, combo, strict=False))

            # Create system with these parameters
            system = base_system_factory(**param_dict)

            # Create world with descriptive name
            param_str = "_".join(f"{k}={v}" for k, v in param_dict.items())
            config = WorldConfig(name=f"sweep_{param_str}")

            world = await self.orchestrator.create_world(
                config=config,
                system=system,
                storage_config=self.storage_config,
                cache_config=self.cache_config,
            )

            results[param_str] = world.world_id

        # Run all worlds in parallel
        await self.orchestrator.run_all_worlds(run_config)

        return results

    async def run_monte_carlo(
        self,
        num_trials: int,
        system_factory: Callable[[int], AsyncSystem],
        run_config: RunConfig,
        seed: int | None = None,
    ) -> dict[str, Any]:
        """
        Run Monte Carlo simulation with multiple trials.

        Args:
            num_trials: Number of trial worlds to run
            system_factory: Callable(seed) -> AsyncSystem
            run_config: Configuration for running each world
            seed: Optional random seed for reproducibility

        Returns:
            Dict with trial metadata and world IDs

        Example:
            results = await service.run_monte_carlo(
                num_trials=100,
                system_factory=lambda seed: create_stochastic_system(seed),
                run_config=RunConfig(num_steps=1000),
                seed=42,
            )
        """
        if seed is not None:
            random.seed(seed)

        world_ids = []
        trial_seeds = []

        for trial in range(num_trials):
            # Each trial gets a unique seed
            trial_seed = random.randint(0, 2**32 - 1)
            trial_seeds.append(trial_seed)

            # Create system with this seed
            system = system_factory(trial_seed)

            config = WorldConfig(name=f"monte_carlo_trial_{trial}")

            world = await self.orchestrator.create_world(
                config=config,
                system=system,
                storage_config=self.storage_config,
                cache_config=self.cache_config,
            )

            world_ids.append(world.world_id)

        # Run all trials in parallel
        await self.orchestrator.run_all_worlds(run_config)

        return {
            "num_trials": num_trials,
            "world_ids": world_ids,
            "trial_seeds": trial_seeds,
            "master_seed": seed,
        }

    async def cleanup_experiment(self, world_ids: list[UUID]):
        """Remove all worlds from an experiment."""
        for world_id in world_ids:
            self.orchestrator.remove_world(world_id)

    def list_worlds(self) -> list[UUID]:
        """List all managed world IDs."""
        return self.orchestrator.list_worlds()

    def get_world(self, world_id: UUID):
        """Get a world by ID."""
        return self.orchestrator.get_world(world_id)

    def get_world_by_name(self, name: str):
        """Get a world by name."""
        return self.orchestrator.get_world_by_name(name)

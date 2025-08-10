import asyncio
from typing import List, Dict, Any, Optional
from uuid_utils import UUID, uuid7

from archetype.core.orchestrator import WorldOrchestrator
from archetype.app.world_service import WorldService
from archetype.core.config import (
    WorldConfig, 
    StorageConfig, 
    CacheConfig, 
    RunConfig,
    SimulationConfig
)
from archetype.core.aio import AsyncSystem


class SimulationService:
    """
    High-level service for managing multi-world simulations and experiments.
    
    This service provides the primary interface for:
    - Running parameter sweeps across multiple worlds
    - Managing experiment lifecycles
    - Aggregating results from parallel simulations
    """
    
    def __init__(self, 
                 orchestrator: WorldOrchestrator,
                 world_service: WorldService,
                 storage_config: StorageConfig,
                 cache_config: CacheConfig):
        self.orchestrator = orchestrator
        self.world_service = world_service
        self.storage_config = storage_config
        self.cache_config = cache_config
    
    async def run_parallel_worlds(self, 
                                  num_worlds: int,
                                  run_config: RunConfig,
                                  system_factory=None) -> List[UUID]:
        """
        Create and run multiple worlds in parallel.
        
        Args:
            num_worlds: Number of worlds to create and run
            run_config: Configuration for how to run each world
            system_factory: Optional callable that returns a configured AsyncSystem
                          If not provided, creates a default AsyncSystem for each world
        
        Returns:
            List of world IDs that were created and run
        """
        world_ids = []
        
        # Create all worlds
        for i in range(num_worlds):
            config = WorldConfig(name=f"world_{i}")
            
            # Create system (either from factory or default)
            if system_factory:
                system = system_factory(i)  # Pass index for variation
            else:
                system = AsyncSystem()
            
            # Create world via orchestrator
            world = await self.orchestrator.create_world(
                config=config,
                system=system,
                storage_config=self.storage_config,
                cache_config=self.cache_config
            )
            world_ids.append(world.world_id)
        
        # Run all worlds concurrently
        await self.orchestrator.run_all_worlds(run_config)
        
        return world_ids
    
    async def run_parameter_sweep(self,
                                  base_system_factory,
                                  parameters: Dict[str, List[Any]],
                                  run_config: RunConfig
    ) -> Dict[str, Any]:
        """
        Run a parameter sweep experiment across multiple worlds.
        
        Args:
            base_system_factory: Callable that takes parameters and returns a system
            parameters: Dict of parameter names to lists of values to sweep
            run_config: Configuration for running each world
        
        Returns:
            Results aggregated by parameter combination
        """
        results = {}
        
        # Generate all parameter combinations
        import itertools
        param_names = list(parameters.keys())
        param_values = list(parameters.values())
        combinations = list(itertools.product(*param_values))
        
        world_ids = []
        
        # Create a world for each parameter combination
        for combo in combinations:
            param_dict = dict(zip(param_names, combo))
            
            # Create system with these parameters
            system = base_system_factory(**param_dict)
            
            # Create world with descriptive name
            param_str = "_".join(f"{k}={v}" for k, v in param_dict.items())
            config = WorldConfig(name=f"sweep_{param_str}")
            
            world = await self.orchestrator.create_world(
                config=config,
                system=system,
                storage_config=self.storage_config,
                cache_config=self.cache_config
            )
            
            world_ids.append(world.world_id)
            results[param_str] = world.world_id
        
        # Run all worlds in parallel
        await self.orchestrator.run_all_worlds(run_config)
        
        return results
    
    async def run_monte_carlo(self,
                              num_trials: int,
                              system_factory,
                              run_config: RunConfig,
                              seed: Optional[int] = None) -> Dict[str, Any]:
        """
        Run Monte Carlo simulation with multiple trials.
        
        Args:
            num_trials: Number of trial worlds to run
            system_factory: Callable that takes a seed and returns a system
            run_config: Configuration for running each world
            seed: Optional random seed for reproducibility
        
        Returns:
            Statistical summary of results across all trials
        """
        import random
        if seed is not None:
            random.seed(seed)
        
        world_ids = []
        
        for trial in range(num_trials):
            # Each trial gets a unique seed
            trial_seed = random.randint(0, 2**32 - 1)
            
            # Create system with this seed
            system = system_factory(trial_seed)
            
            config = WorldConfig(name=f"monte_carlo_trial_{trial}")
            
            world = await self.orchestrator.create_world(
                config=config,
                system=system,
                storage_config=self.storage_config,
                cache_config=self.cache_config
            )
            
            world_ids.append(world.world_id)
        
        # Run all trials in parallel
        await self.orchestrator.run_all_worlds(run_config)
        
        # Aggregate results (placeholder - would query world states)
        return {
            "num_trials": num_trials,
            "world_ids": world_ids,
            "seed": seed
        }
    
    async def cleanup_experiment(self, world_ids: List[UUID]):
        """Remove all worlds from an experiment."""
        for world_id in world_ids:
            self.orchestrator.remove_world(world_id)
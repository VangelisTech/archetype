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
AutoResearch State Machine Processors

Implements the experiment/run state machine transitions:
1. Create Experiment from current tracked BranchHead
2. Create Run for that experiment  
3. Both → running
4. Execution fails → Run → crashed|timed_out, Experiment → crashed
5. Execution completes → Run → completed, Experiment → succeeded
6. Compare Result to frontier
7. Result advances frontier → Experiment → kept, advance BranchHead
8. Otherwise → Experiment → discarded, BranchHead unchanged
"""

import json
from datetime import datetime
from typing import Optional

from daft import DataFrame, col

from archetype.core.aio.async_processor import AsyncProcessor
from .components import (
    BranchHead,
    Experiment, 
    ExperimentStatus,
    Result,
    Run,
    RunStatus,
)


class ExperimentStateProcessor(AsyncProcessor):
    """
    Manages experiment state transitions based on run outcomes.
    
    Transition rules:
    - pending → running when run starts
    - running → succeeded when run completes successfully
    - running → crashed when run fails
    """
    
    components = (Experiment, Run)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Filter for experiments that need state transitions
        experiments_with_runs = df.where(
            col("experiment__status").is_in([ExperimentStatus.PENDING, ExperimentStatus.RUNNING])
        )
        
        if experiments_with_runs.count_rows() == 0:
            return df
        
        # Update experiment status based on run status
        def update_experiment_status(experiment_status, run_status):
            if experiment_status == ExperimentStatus.PENDING and run_status == RunStatus.RUNNING:
                return ExperimentStatus.RUNNING
            elif experiment_status == ExperimentStatus.RUNNING and run_status == RunStatus.COMPLETED:
                return ExperimentStatus.SUCCEEDED
            elif experiment_status == ExperimentStatus.RUNNING and run_status in [RunStatus.CRASHED, RunStatus.TIMED_OUT]:
                return ExperimentStatus.CRASHED
            else:
                return experiment_status
        
        updated_df = df.with_columns({
            "experiment__status": update_experiment_status(
                col("experiment__status"), 
                col("run__status")
            )
        })
        
        return updated_df


class RunStateProcessor(AsyncProcessor):
    """
    Manages run state transitions during experiment execution.
    
    This processor would typically be driven by external execution
    events from the app layer git adapter and world execution.
    """
    
    components = (Run,)
    priority = 5

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # For Phase 1, this is a placeholder for run state management
        # The actual state transitions would be driven by:
        # 1. Git adapter materializing experiment worktree
        # 2. World instantiation from experiment specs
        # 3. Bounded run execution 
        # 4. Result collection and persistence
        
        # Update run timestamps for running runs
        current_time = datetime.now()
        
        updated_df = df.with_columns({
            "run__started_at": col("run__started_at").if_else(
                col("run__status") == RunStatus.RUNNING,
                current_time if col("run__started_at").is_null() else col("run__started_at"),
                col("run__started_at")
            ),
            "run__finished_at": col("run__finished_at").if_else(
                col("run__status").is_in([RunStatus.COMPLETED, RunStatus.CRASHED, RunStatus.TIMED_OUT]),
                current_time if col("run__finished_at").is_null() else col("run__finished_at"),
                col("run__finished_at")
            )
        })
        
        return updated_df


class FrontierEvaluationProcessor(AsyncProcessor):
    """
    Evaluates experiment results against the tracked frontier and decides keep/discard.
    
    Transition rules:
    - succeeded experiments with results get evaluated against frontier
    - If result advances frontier: experiment → kept, advance BranchHead
    - Otherwise: experiment → discarded, BranchHead unchanged
    """
    
    components = (Experiment, Result, BranchHead)
    priority = 15  # Run after experiment state updates

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        # Filter for succeeded experiments that need evaluation
        succeeded_experiments = df.where(
            col("experiment__status") == ExperimentStatus.SUCCEEDED
        )
        
        if succeeded_experiments.count_rows() == 0:
            return df
        
        # Evaluate results against frontier
        def evaluate_result(
            result_value: Optional[float],
            result_direction: str, 
            frontier_value: Optional[float],
            frontier_direction: str
        ) -> bool:
            """Returns True if result advances the frontier."""
            if result_value is None:
                return False
            if frontier_value is None:
                return True  # First result sets the frontier
            if result_direction != frontier_direction:
                return False  # Mismatched directions
                
            if frontier_direction == "min":
                return result_value < frontier_value
            elif frontier_direction == "max": 
                return result_value > frontier_value
            else:
                return False
        
        # Determine if results advance frontier
        advances_frontier = evaluate_result(
            col("result__primary_metric_value"),
            col("result__primary_metric_direction"), 
            col("branchhead__frontier_metric_value"),
            col("branchhead__frontier_metric_direction")
        )
        
        # Update experiment status based on frontier evaluation
        updated_df = df.with_columns({
            "experiment__status": col("experiment__status").if_else(
                advances_frontier & (col("experiment__status") == ExperimentStatus.SUCCEEDED),
                ExperimentStatus.KEPT,
                col("experiment__status").if_else(
                    col("experiment__status") == ExperimentStatus.SUCCEEDED,
                    ExperimentStatus.DISCARDED,
                    col("experiment__status")
                )
            )
        })
        
        # Update BranchHead frontier for kept experiments
        updated_df = updated_df.with_columns({
            "branchhead__frontier_metric_value": col("branchhead__frontier_metric_value").if_else(
                advances_frontier & (col("experiment__status") == ExperimentStatus.KEPT),
                col("result__primary_metric_value"),
                col("branchhead__frontier_metric_value")
            ),
            "branchhead__current_commit_hash": col("branchhead__current_commit_hash").if_else(
                advances_frontier & (col("experiment__status") == ExperimentStatus.KEPT),
                col("experiment__base_commit_hash"),  # In real implementation, this would be the new commit hash
                col("branchhead__current_commit_hash")
            )
        })
        
        return updated_df
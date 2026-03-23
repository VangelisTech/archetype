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

"""AutoResearch state machine processors."""

from datetime import datetime
from typing import Any

import polars as pl
from polars import DataFrame

from archetype.core.aio.async_processor import AsyncProcessor

from .components import BranchHead, Experiment, Result, Run


class ExperimentProcessor(AsyncProcessor):
    """
    Manages experiment state transitions.
    
    State transitions:
    - pending → running (when run starts)
    - running → succeeded (when run completes successfully)
    - running → crashed (when run fails)
    - succeeded → kept (when result advances frontier)
    - succeeded → discarded (when result does not advance frontier)
    """
    
    components = (Experiment, Run, Result, BranchHead)
    priority = 5

    async def process(self, df: DataFrame, **input_kwargs) -> DataFrame:
        """Process experiment state transitions."""
        
        # Process experiments that need to transition from succeeded to kept/discarded
        succeeded_experiments = df.filter(
            pl.col("experiment__status") == "succeeded"
        )
        
        if succeeded_experiments.height > 0:
            df = await self._evaluate_frontier_advancement(df, succeeded_experiments)
        
        return df

    async def _evaluate_frontier_advancement(
        self, 
        df: DataFrame, 
        succeeded_experiments: DataFrame
    ) -> DataFrame:
        """Evaluate whether successful experiments advance the frontier."""
        
        updates = []
        
        for experiment in succeeded_experiments.iter_rows(named=True):
            experiment_id = experiment["experiment__experiment_id"]
            branch_name = experiment["experiment__branch_name"]
            repository_id = experiment["experiment__repository_id"]
            
            # Find the result for this experiment
            experiment_results = df.filter(
                pl.col("result__experiment_id") == experiment_id
            )
            
            if experiment_results.height == 0:
                continue
                
            result = experiment_results.row(0, named=True)
            
            # Find the current branch head for comparison
            branch_heads = df.filter(
                (pl.col("branchhead__repository_id") == repository_id) &
                (pl.col("branchhead__branch_name") == branch_name)
            )
            
            if branch_heads.height == 0:
                continue
                
            branch_head = branch_heads.row(0, named=True)
            
            # Compare result to frontier
            primary_value = result["result__primary_metric_value"]
            frontier_value = branch_head["branchhead__frontier_metric_value"]
            direction = branch_head["branchhead__frontier_metric_direction"]
            
            if self._advances_frontier(primary_value, frontier_value, direction):
                # Mark experiment as kept and advance branch head
                updates.append({
                    "experiment_id": experiment_id,
                    "new_status": "kept",
                    "advance_frontier": True,
                    "new_commit_hash": experiment["experiment__base_commit_hash"],
                    "new_frontier_value": primary_value
                })
            else:
                # Mark experiment as discarded
                updates.append({
                    "experiment_id": experiment_id,
                    "new_status": "discarded",
                    "advance_frontier": False
                })
        
        # Apply updates to dataframe
        return self._apply_updates(df, updates)

    def _advances_frontier(
        self, 
        result_value: float | None, 
        frontier_value: float | None, 
        direction: str
    ) -> bool:
        """Check if result advances the frontier."""
        if result_value is None:
            return False
            
        if frontier_value is None:
            return True
            
        if direction == "min":
            return result_value < frontier_value
        else:  # direction == "max"
            return result_value > frontier_value

    def _apply_updates(self, df: DataFrame, updates: list[dict[str, Any]]) -> DataFrame:
        """Apply experiment and branch head updates to dataframe."""
        
        for update in updates:
            experiment_id = update["experiment_id"]
            new_status = update["new_status"]
            
            # Update experiment status
            df = df.with_columns(
                pl.when(pl.col("experiment__experiment_id") == experiment_id)
                .then(pl.lit(new_status))
                .otherwise(pl.col("experiment__status"))
                .alias("experiment__status")
            )
            
            # If advancing frontier, update branch head
            if update["advance_frontier"]:
                # This would need to be handled by creating a new BranchHead component
                # or updating the existing one - simplified for now
                pass
                
        return df


class RunProcessor(AsyncProcessor):
    """
    Manages run state transitions and updates experiment status accordingly.
    
    Run state transitions:
    - pending → running (when execution starts)
    - running → completed (when execution finishes successfully)
    - running → crashed (when execution fails)
    - running → timed_out (when execution exceeds time limit)
    """
    
    components = (Run, Experiment)
    priority = 6

    async def process(self, df: DataFrame, **input_kwargs) -> DataFrame:
        """Process run state transitions and update corresponding experiments."""
        
        # Find runs that have completed and need to update experiment status
        completed_runs = df.filter(
            pl.col("run__status") == "completed"
        )
        
        if completed_runs.height > 0:
            df = await self._update_experiment_status_for_completed_runs(df, completed_runs)
        
        # Find runs that have crashed/timed out and need to update experiment status
        failed_runs = df.filter(
            pl.col("run__status").is_in(["crashed", "timed_out"])
        )
        
        if failed_runs.height > 0:
            df = await self._update_experiment_status_for_failed_runs(df, failed_runs)
        
        return df

    async def _update_experiment_status_for_completed_runs(
        self, 
        df: DataFrame, 
        completed_runs: DataFrame
    ) -> DataFrame:
        """Update experiment status to 'succeeded' for completed runs."""
        
        for run in completed_runs.iter_rows(named=True):
            experiment_id = run["run__experiment_id"]
            
            # Update corresponding experiment to 'succeeded'
            df = df.with_columns(
                pl.when(pl.col("experiment__experiment_id") == experiment_id)
                .then(pl.lit("succeeded"))
                .otherwise(pl.col("experiment__status"))
                .alias("experiment__status")
            )
        
        return df

    async def _update_experiment_status_for_failed_runs(
        self, 
        df: DataFrame, 
        failed_runs: DataFrame
    ) -> DataFrame:
        """Update experiment status to 'crashed' for failed runs."""
        
        for run in failed_runs.iter_rows(named=True):
            experiment_id = run["run__experiment_id"]
            
            # Update corresponding experiment to 'crashed'
            df = df.with_columns(
                pl.when(pl.col("experiment__experiment_id") == experiment_id)
                .then(pl.lit("crashed"))
                .otherwise(pl.col("experiment__status"))
                .alias("experiment__status")
            )
        
        return df
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

"""Storage service for AutoResearch components."""

from typing import Any

from archetype.core.storage.lancedb import LanceDBStore

from .components import (
    BranchHead,
    Commit,
    Experiment,
    Repository,
    Result,
    Run,
)


class AutoResearchStorage:
    """Manages LanceDB storage for AutoResearch components."""
    
    def __init__(self, store: LanceDBStore):
        self.store = store
        self.components = [
            Repository,
            BranchHead, 
            Commit,
            Experiment,
            Run,
            Result
        ]
    
    async def initialize_tables(self) -> None:
        """Initialize all AutoResearch component tables."""
        for component_class in self.components:
            await self.store.ensure_table(component_class)
    
    async def get_current_frontier(
        self, 
        repository_id: str, 
        branch_name: str
    ) -> BranchHead | None:
        """Get the current branch head for a repository/branch."""
        results = await self.store.query(
            BranchHead,
            filter_condition=f"repository_id = '{repository_id}' AND branch_name = '{branch_name}'"
        )
        
        if results:
            return results[0]
        return None
    
    async def create_experiment(
        self,
        experiment_id: str,
        repository_id: str,
        branch_name: str,
        base_commit_hash: str,
        proposal_summary: str,
        world_spec: dict[str, Any],
        run_spec: dict[str, Any],
        evaluation_spec: dict[str, Any]
    ) -> Experiment:
        """Create a new experiment."""
        from datetime import datetime
        
        experiment = Experiment(
            experiment_id=experiment_id,
            repository_id=repository_id,
            branch_name=branch_name,
            base_commit_hash=base_commit_hash,
            proposal_summary=proposal_summary,
            world_spec_json=world_spec,
            run_spec_json=run_spec,
            evaluation_spec_json=evaluation_spec,
            created_at=datetime.now()
        )
        
        await self.store.insert([experiment])
        return experiment
    
    async def update_experiment_status(
        self, 
        experiment_id: str, 
        status: str
    ) -> None:
        """Update experiment status."""
        # This would need to be implemented with the store's update capabilities
        # For now, this is a placeholder
        pass
    
    async def advance_branch_head(
        self,
        repository_id: str,
        branch_name: str,
        new_commit_hash: str,
        new_frontier_value: float
    ) -> None:
        """Advance the branch head with new commit and frontier value."""
        # This would need to be implemented with the store's update capabilities
        # For now, this is a placeholder
        pass
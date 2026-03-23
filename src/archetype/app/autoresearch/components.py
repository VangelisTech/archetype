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

"""AutoResearch ECS components for tracking experiments and results."""

from datetime import datetime
from typing import Any, Literal, Optional

from archetype.core.component import Component


class Repository(Component):
    """Identifies the repository under optimization."""
    
    repository_id: str
    canonical_path: str
    default_branch: str = "main"


class BranchHead(Component):
    """Represents the single tracked frontier for the active loop."""
    
    repository_id: str
    branch_name: str
    current_commit_hash: str
    frontier_metric_name: str
    frontier_metric_direction: Literal["min", "max"]
    frontier_metric_value: Optional[float] = None


class Commit(Component):
    """Represents a concrete git state."""
    
    repository_id: str
    branch_name: str
    commit_hash: str
    parent_commit_hash: Optional[str] = None
    message: str
    created_at: datetime


class Experiment(Component):
    """Represents the proposed advancement of the tracked frontier."""
    
    experiment_id: str
    repository_id: str
    branch_name: str
    base_commit_hash: str
    proposal_summary: str
    world_spec_json: dict[str, Any]
    run_spec_json: dict[str, Any]
    evaluation_spec_json: dict[str, Any]
    status: Literal["pending", "running", "succeeded", "crashed", "kept", "discarded"] = "pending"
    created_at: datetime


class Run(Component):
    """Represents one bounded execution of an experiment."""
    
    run_id: str
    experiment_id: str
    world_id: str
    status: Literal["pending", "running", "completed", "crashed", "timed_out"] = "pending"
    budget_type: str
    budget_value: int
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    artifact_uri: Optional[str] = None


class Result(Component):
    """Represents the metrics and terminal outcome of a run."""
    
    result_id: str
    experiment_id: str
    run_id: str
    primary_metric_name: str
    primary_metric_value: Optional[float] = None
    primary_metric_direction: Literal["min", "max"]
    secondary_metrics_json: dict[str, Any] = {}
    runtime_metadata_json: dict[str, Any] = {}
    failure_metadata_json: dict[str, Any] = {}
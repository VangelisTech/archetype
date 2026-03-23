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
AutoResearch ECS Components

Core model components for the autonomous optimization loop:
- Repository: git repo under optimization
- BranchHead: tracked frontier for active loop
- Commit: concrete git state
- Experiment: proposed advancement of tracked frontier
- Run: bounded execution of an experiment
- Result: metrics and terminal outcome of a run
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from archetype.core.component import Component


class Repository(Component):
    """Identifies the repository under optimization."""
    
    repository_id: str
    canonical_path: str  # Local path or remote URL
    default_branch: str = "main"


class BranchHead(Component):
    """Represents the single tracked frontier for the active loop."""
    
    repository_id: str
    branch_name: str
    current_commit_hash: str
    frontier_metric_name: str
    frontier_metric_value: Optional[float] = None
    frontier_metric_direction: str = "min"  # "min" or "max"


class Commit(Component):
    """Represents a concrete git state."""
    
    repository_id: str
    branch_name: str
    commit_hash: str
    parent_commit_hash: str
    message: str
    created_at: datetime


class ExperimentStatus(str, Enum):
    """Experiment state machine states."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    CRASHED = "crashed"
    KEPT = "kept" 
    DISCARDED = "discarded"


class Experiment(Component):
    """
    Represents the proposed advancement of the tracked frontier.
    
    Carries the declarative recipe needed to create a world 
    and derive a concrete run configuration at execution time.
    """
    
    experiment_id: str
    repository_id: str
    branch_name: str
    base_commit_hash: str
    proposal_summary: str
    world_spec_json: str = '{"world_name": "experiment-world", "storage": {"uri": "./archetype_data", "backend": "lancedb"}, "resources": {}, "metadata": {}}'
    run_spec_json: str = '{"budget_kind": "steps", "budget_value": 1, "debug": false, "prefer_live_reads": true, "suite": "autoresearch"}'
    evaluation_spec_json: str = '{"primary_metric_name": "val_bpb", "direction": "min", "secondary_metric_names": ["peak_vram_mb", "training_seconds"], "crash_is_failure": true}'
    status: ExperimentStatus = ExperimentStatus.PENDING
    created_at: datetime


class RunStatus(str, Enum):
    """Run state machine states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CRASHED = "crashed"
    TIMED_OUT = "timed_out"


class Run(Component):
    """Represents one bounded execution of an experiment."""
    
    run_id: str
    experiment_id: str
    world_id: Optional[str] = None
    status: RunStatus = RunStatus.PENDING
    budget_type: str = "steps"
    budget_value: int = 1
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
    primary_metric_direction: str = "min"  # "min" or "max"
    secondary_metrics_json: str = "{}"  # JSON string of additional metrics
    runtime_metadata_json: str = "{}"   # JSON string of runtime metadata
    failure_metadata_json: str = "{}"   # JSON string of failure details
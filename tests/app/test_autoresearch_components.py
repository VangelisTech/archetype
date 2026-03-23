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

"""Tests for AutoResearch ECS components."""

import json
from datetime import datetime
from uuid import uuid4

import pytest

from archetype.app.autoresearch.components import (
    BranchHead,
    Commit,
    Experiment,
    ExperimentStatus,
    Repository,
    Result,
    Run,
    RunStatus,
)


def test_repository_component():
    """Repository component should store basic git repo information."""
    repo = Repository(
        repository_id="test-repo-123",
        canonical_path="/path/to/repo",
        default_branch="main"
    )
    assert repo.repository_id == "test-repo-123"
    assert repo.canonical_path == "/path/to/repo"
    assert repo.default_branch == "main"
    
    # Test prefixed schema
    row_dict = repo.to_row_dict()
    assert "repository__repository_id" in row_dict
    assert row_dict["repository__repository_id"] == "test-repo-123"


def test_branchhead_component():
    """BranchHead component should track frontier metrics."""
    branch_head = BranchHead(
        repository_id="test-repo-123",
        branch_name="autoresearch/v1",
        current_commit_hash="abc123",
        frontier_metric_name="val_bpb",
        frontier_metric_value=2.5,
        frontier_metric_direction="min"
    )
    assert branch_head.frontier_metric_value == 2.5
    assert branch_head.frontier_metric_direction == "min"
    
    # Test without initial frontier value
    branch_head_new = BranchHead(
        repository_id="test-repo-123",
        branch_name="autoresearch/v1",
        current_commit_hash="abc123",
        frontier_metric_name="val_bpb"
    )
    assert branch_head_new.frontier_metric_value is None


def test_experiment_component():
    """Experiment component should use canonical spec shapes."""
    experiment_id = str(uuid4())
    experiment = Experiment(
        experiment_id=experiment_id,
        repository_id="test-repo-123", 
        branch_name="autoresearch/v1",
        base_commit_hash="abc123",
        proposal_summary="Test optimization experiment",
        created_at=datetime.now()
    )
    
    # Check default status
    assert experiment.status == ExperimentStatus.PENDING
    
    # Check canonical JSON specs are valid JSON
    world_spec = json.loads(experiment.world_spec_json)
    assert world_spec["world_name"] == "experiment-world"
    assert world_spec["storage"]["backend"] == "lancedb"
    
    run_spec = json.loads(experiment.run_spec_json) 
    assert run_spec["budget_kind"] == "steps"
    assert run_spec["budget_value"] == 1
    assert run_spec["suite"] == "autoresearch"
    
    eval_spec = json.loads(experiment.evaluation_spec_json)
    assert eval_spec["primary_metric_name"] == "val_bpb"
    assert eval_spec["direction"] == "min"
    assert "peak_vram_mb" in eval_spec["secondary_metric_names"]


def test_run_component():
    """Run component should track execution state."""
    run_id = str(uuid4())
    experiment_id = str(uuid4())
    
    run = Run(
        run_id=run_id,
        experiment_id=experiment_id,
        world_id="world-123",
        budget_type="steps",
        budget_value=100
    )
    
    assert run.status == RunStatus.PENDING
    assert run.started_at is None
    assert run.finished_at is None


def test_result_component():
    """Result component should store metrics and metadata."""
    result_id = str(uuid4())
    experiment_id = str(uuid4()) 
    run_id = str(uuid4())
    
    result = Result(
        result_id=result_id,
        experiment_id=experiment_id,
        run_id=run_id,
        primary_metric_name="val_bpb",
        primary_metric_value=2.1,
        primary_metric_direction="min",
        secondary_metrics_json='{"peak_vram_mb": 1024, "training_seconds": 120}',
        runtime_metadata_json='{"model_size": "7B"}'
    )
    
    assert result.primary_metric_value == 2.1
    
    # Check JSON fields are valid
    secondary_metrics = json.loads(result.secondary_metrics_json)
    assert secondary_metrics["peak_vram_mb"] == 1024
    
    runtime_metadata = json.loads(result.runtime_metadata_json)
    assert runtime_metadata["model_size"] == "7B"


def test_experiment_status_enum():
    """Experiment status should follow state machine."""
    assert ExperimentStatus.PENDING == "pending"
    assert ExperimentStatus.RUNNING == "running"
    assert ExperimentStatus.SUCCEEDED == "succeeded"
    assert ExperimentStatus.CRASHED == "crashed"
    assert ExperimentStatus.KEPT == "kept"
    assert ExperimentStatus.DISCARDED == "discarded"


def test_run_status_enum():
    """Run status should follow state machine."""
    assert RunStatus.PENDING == "pending"
    assert RunStatus.RUNNING == "running"
    assert RunStatus.COMPLETED == "completed"
    assert RunStatus.CRASHED == "crashed"
    assert RunStatus.TIMED_OUT == "timed_out"


def test_component_schemas():
    """All AutoResearch components should generate valid PyArrow schemas."""
    import pyarrow as pa
    
    for component_type in [Repository, BranchHead, Commit, Experiment, Run, Result]:
        schema = component_type.to_arrow_schema()
        assert isinstance(schema, pa.Schema)
        
        prefixed_schema = component_type.get_prefixed_schema()
        assert isinstance(prefixed_schema, pa.Schema)
        
        # All prefixed fields should start with component name
        prefix = component_type.get_prefix()
        for field_name in prefixed_schema.names:
            assert field_name.startswith(prefix)
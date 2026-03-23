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

"""Test AutoResearch ECS components."""

from datetime import datetime

import pytest

from archetype.app.autoresearch.components import (
    BranchHead,
    Commit,
    Experiment,
    Repository,
    Result,
    Run,
)
from archetype.app.autoresearch.specs import (
    get_canonical_evaluation_spec,
    get_canonical_run_spec,
    get_canonical_world_spec,
)


class TestRepository:
    """Test Repository component."""

    def test_repository_creation(self):
        """Test Repository component can be created with required fields."""
        repo = Repository(
            repository_id="test-repo-1",
            canonical_path="/path/to/repo",
            default_branch="main"
        )
        
        assert repo.repository_id == "test-repo-1"
        assert repo.canonical_path == "/path/to/repo"
        assert repo.default_branch == "main"


class TestBranchHead:
    """Test BranchHead component."""

    def test_branch_head_creation(self):
        """Test BranchHead component can be created with required fields."""
        branch_head = BranchHead(
            repository_id="test-repo-1",
            branch_name="autoresearch/v1",
            current_commit_hash="abc123",
            frontier_metric_name="val_bpb",
            frontier_metric_direction="min",
            frontier_metric_value=2.5
        )
        
        assert branch_head.repository_id == "test-repo-1"
        assert branch_head.branch_name == "autoresearch/v1"
        assert branch_head.current_commit_hash == "abc123"
        assert branch_head.frontier_metric_name == "val_bpb"
        assert branch_head.frontier_metric_direction == "min"
        assert branch_head.frontier_metric_value == 2.5


class TestExperiment:
    """Test Experiment component."""

    def test_experiment_creation(self):
        """Test Experiment component can be created with canonical specs."""
        experiment = Experiment(
            experiment_id="exp-001",
            repository_id="test-repo-1",
            branch_name="autoresearch/v1",
            base_commit_hash="abc123",
            proposal_summary="Test hypothesis improvement",
            world_spec_json=get_canonical_world_spec(),
            run_spec_json=get_canonical_run_spec(),
            evaluation_spec_json=get_canonical_evaluation_spec(),
            created_at=datetime.now()
        )
        
        assert experiment.experiment_id == "exp-001"
        assert experiment.status == "pending"
        assert "world_name" in experiment.world_spec_json
        assert "budget_kind" in experiment.run_spec_json
        assert "primary_metric_name" in experiment.evaluation_spec_json


class TestRun:
    """Test Run component."""

    def test_run_creation(self):
        """Test Run component can be created with required fields."""
        run = Run(
            run_id="run-001",
            experiment_id="exp-001", 
            world_id="world-001",
            budget_type="steps",
            budget_value=100
        )
        
        assert run.run_id == "run-001"
        assert run.experiment_id == "exp-001"
        assert run.status == "pending"
        assert run.budget_type == "steps"
        assert run.budget_value == 100


class TestResult:
    """Test Result component."""

    def test_result_creation(self):
        """Test Result component can be created with required fields."""
        result = Result(
            result_id="result-001",
            experiment_id="exp-001",
            run_id="run-001",
            primary_metric_name="val_bpb",
            primary_metric_value=2.3,
            primary_metric_direction="min"
        )
        
        assert result.result_id == "result-001"
        assert result.experiment_id == "exp-001"
        assert result.primary_metric_name == "val_bpb"
        assert result.primary_metric_value == 2.3
        assert result.primary_metric_direction == "min"


class TestCanonicalSpecs:
    """Test canonical specification shapes."""

    def test_world_spec_structure(self):
        """Test canonical world spec has expected structure."""
        spec = get_canonical_world_spec()
        
        assert spec["world_name"] == "experiment-world"
        assert spec["storage"]["backend"] == "lancedb"
        assert "cache" in spec
        assert "resources" in spec
        assert "metadata" in spec

    def test_run_spec_structure(self):
        """Test canonical run spec has expected structure."""
        spec = get_canonical_run_spec()
        
        assert spec["budget_kind"] == "steps"
        assert spec["budget_value"] == 1
        assert spec["suite"] == "autoresearch"
        assert spec["prefer_live_reads"] is True

    def test_evaluation_spec_structure(self):
        """Test canonical evaluation spec has expected structure."""
        spec = get_canonical_evaluation_spec()
        
        assert spec["primary_metric_name"] == "val_bpb"
        assert spec["direction"] == "min"
        assert "secondary_metric_names" in spec
        assert spec["crash_is_failure"] is True
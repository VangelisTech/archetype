# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the generic Research ledger vocabulary."""

import json

import pyarrow as pa
import pytest

from archetype.core.component import Component
from archetype.research import BranchHead, Experiment, Result, Run, RunStatus


def test_run_status_is_research_specific() -> None:
    assert tuple(RunStatus) == (
        RunStatus.RUNNING,
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
    )
    assert RunStatus.is_active("running")
    assert not RunStatus.is_active("succeeded")
    assert RunStatus.is_terminal("succeeded")
    assert RunStatus.is_terminal("failed")


def test_experiment_is_generic_opaque_configuration() -> None:
    experiment = Experiment.make(
        "instruction-search",
        metadata={"strategy": "grid", "axes": ["clarity", "latency"]},
    )
    assert issubclass(Experiment, Component)
    assert experiment.name == "instruction-search"
    assert experiment.created_at_ms > 0
    assert experiment.get_metadata()["strategy"] == "grid"
    assert set(Experiment.model_fields) == {
        "name",
        "created_at_ms",
        "metadata_json",
    }
    assert isinstance(Experiment.to_pyarrow_schema(), pa.Schema)


def test_run_records_only_candidate_evaluation_identity() -> None:
    run = Run(
        run_id="exp:iter0",
        experiment_name="exp",
        status=RunStatus.RUNNING.value,
        candidate_world_id="candidate-world",
        started_at_ms=1,
    )
    assert run.is_active
    assert not run.is_terminal
    assert set(Run.model_fields) == {
        "run_id",
        "experiment_name",
        "status",
        "candidate_world_id",
        "started_at_ms",
        "finished_at_ms",
    }
    coding_fields = {
        "vm_name",
        "harness",
        "repo_url",
        "branch",
        "task",
        "agent_name",
        "workspace_path",
        "commit_hash",
    }
    assert not coding_fields & set(Run.model_fields)


def test_result_is_an_opaque_multi_evaluator_envelope() -> None:
    first = Result.make("run-1", {"score": 0.92}, evaluator="accuracy")
    second = Result.make("run-1", {"latency_ms": 12}, evaluator="latency")
    assert first.run_id == second.run_id
    assert first.evaluator != second.evaluator
    assert first.get_outputs() == {"score": 0.92}
    assert json.loads(first.outputs_json)["score"] == 0.92
    assert "score" not in Result.model_fields


def test_branch_head_points_to_an_incumbent_world_not_a_commit() -> None:
    head = BranchHead.make(
        "exp",
        candidate_world_id="world-best",
        run_id="run-1",
        descriptor={"score": 2.0, "reason": "highest finite score"},
    )
    assert head.candidate_world_id == "world-best"
    assert head.run_id == "run-1"
    assert head.get_descriptor()["score"] == 2.0
    assert "commit_hash" not in BranchHead.model_fields


def test_components_are_discoverable_with_distinct_prefixes() -> None:
    components = (Experiment, Run, Result, BranchHead)
    assert all(Component.get_type_by_name(value.__name__) is value for value in components)
    assert len({value.get_prefix() for value in components}) == len(components)
    with pytest.raises(ValueError, match="not found"):
        Component.get_type_by_name("NotAResearchComponent")

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Tests for research Components — Experiment, Run, Result, BranchHead."""

import json

import pyarrow as pa
import pytest

from archetype.core.component import Component
from archetype.research import (
    BranchHead,
    Experiment,
    Result,
    Run,
    RunStatus,
)


class TestRunStatus:
    def test_string_values_match_runner_registry(self):
        # Values must match archetype-runner's state.py exactly so that
        # SQLite rows from the runner registry can be used as-is.
        assert RunStatus.BOOTING.value == "booting"
        assert RunStatus.RUNNING.value == "running"
        assert RunStatus.STOPPING.value == "stopping"
        assert RunStatus.STOPPED.value == "stopped"
        assert RunStatus.CRASHED.value == "crashed"

    def test_is_active_true_while_in_flight(self):
        for s in ("booting", "running", "stopping"):
            assert RunStatus.is_active(s) is True

    def test_is_active_false_once_terminal(self):
        for s in ("stopped", "crashed"):
            assert RunStatus.is_active(s) is False

    def test_is_terminal_inverse_of_active(self):
        for s in ("booting", "running", "stopping"):
            assert RunStatus.is_terminal(s) is False
        for s in ("stopped", "crashed"):
            assert RunStatus.is_terminal(s) is True


class TestExperimentComponent:
    def test_is_archetype_component(self):
        assert issubclass(Experiment, Component)

    def test_default_construction(self):
        # Components follow the Trajectory convention: all fields have
        # defaults so partial rows from storage deserialize cleanly.
        exp = Experiment()
        assert exp.name == ""
        assert exp.repo_url == ""
        assert exp.branch == "main"
        assert exp.created_at_ms == 0
        assert exp.metadata_json == "{}"

    def test_make_convenience_constructor(self):
        exp = Experiment.make(
            name="frontend-perf",
            repo_url="https://github.com/foo/bar",
            metadata={"tags": ["benchmark"], "eval_cmd": "pytest tests/"},
        )
        assert exp.name == "frontend-perf"
        assert exp.repo_url == "https://github.com/foo/bar"
        assert exp.created_at_ms > 0
        # metadata is stored as JSON string for Arrow compatibility
        decoded = json.loads(exp.metadata_json)
        assert decoded["tags"] == ["benchmark"]
        assert decoded["eval_cmd"] == "pytest tests/"

    def test_get_metadata_round_trip(self):
        payload = {"a": 1, "nested": {"b": [1, 2, 3]}}
        exp = Experiment.make(name="x", repo_url="r", metadata=payload)
        assert exp.get_metadata() == payload

    def test_prefix_matches_arrow_convention(self):
        # Per Component.get_prefix(): field names get prefixed with
        # "{classname}__" when serialized to the world schema.
        assert Experiment.get_prefix() == "experiment__"

    def test_arrow_schema_is_materializable(self):
        schema = Experiment.to_pyarrow_schema()
        assert isinstance(schema, pa.Schema)
        field_names = set(schema.names)
        assert "name" in field_names
        assert "repo_url" in field_names
        assert "metadata_json" in field_names
        assert "created_at_ms" in field_names

    def test_no_metric_or_direction_fields(self):
        # The redesign deliberately omits scoring semantics.
        for forbidden in ("metric_name", "direction", "metric", "reward"):
            assert forbidden not in Experiment.model_fields


class TestRunComponent:
    def test_is_archetype_component(self):
        assert issubclass(Run, Component)

    def test_default_construction(self):
        run = Run()
        assert run.run_id == ""
        assert run.experiment_name == "adhoc"
        assert run.status == "booting"
        assert run.is_active is True
        assert run.is_terminal is False
        assert run.commit_hash == ""
        assert run.finished_at_ms == 0

    def test_constructs_from_runner_agent_record_shape(self):
        # Load-bearing: a dict shaped like archetype-runner's SQLite row
        # must construct a Run cleanly after mapping agent_id -> run_id
        # and parsing ISO timestamps. The loader module does this
        # transformation; this test asserts the Component surface accepts
        # the post-transformation shape.
        run = Run(
            run_id="claude-2026-04-10-abc123",
            vm_name="mb-vm-claude-2026-04-10-abc123",
            status="running",
            harness="claude-code",
            repo_url="https://github.com/Vangelis/archetype",
            branch="feat/foo",
            task="Add a feature flag for X",
            started_at_ms=1_744_291_696_000,
            finished_at_ms=0,
            agent_name="adhoc-claude-code",
            workspace_path="/home/vangelis/.archetype-runner/workspaces/claude-2026-04-10-abc123",
        )
        assert run.run_id == "claude-2026-04-10-abc123"
        assert run.status == "running"
        assert run.is_active is True
        assert run.harness == "claude-code"
        assert run.started_at_ms == 1_744_291_696_000

    def test_is_active_property_covers_all_in_flight_states(self):
        for s in ("booting", "running", "stopping"):
            run = Run(run_id="x", status=s)
            assert run.is_active is True
            assert run.is_terminal is False

    def test_is_active_false_once_stopped_or_crashed(self):
        for s in ("stopped", "crashed"):
            run = Run(run_id="x", status=s)
            assert run.is_active is False
            assert run.is_terminal is True

    def test_prefix(self):
        assert Run.get_prefix() == "run__"

    def test_arrow_schema_is_materializable(self):
        schema = Run.to_pyarrow_schema()
        assert isinstance(schema, pa.Schema)
        required = {
            "run_id",
            "experiment_name",
            "vm_name",
            "status",
            "harness",
            "repo_url",
            "branch",
            "task",
            "started_at_ms",
            "finished_at_ms",
            "agent_name",
            "workspace_path",
            "commit_hash",
        }
        assert required.issubset(set(schema.names))

    def test_no_eval_or_reward_fields(self):
        # Run holds execution records only. Evaluation lives in Result.
        for forbidden in (
            "metric",
            "metric_name",
            "value",
            "score",
            "beats_frontier",
            "direction",
            "reward",
        ):
            assert forbidden not in Run.model_fields


class TestResultComponent:
    def test_is_archetype_component(self):
        assert issubclass(Result, Component)

    def test_default_construction(self):
        result = Result()
        assert result.run_id == ""
        assert result.evaluator == ""
        assert result.outputs_json == "{}"
        assert result.evaluated_at_ms == 0

    def test_make_with_scalar_metric(self):
        result = Result.make(
            run_id="run-1",
            evaluator="pytest",
            outputs={"pass_rate": 0.92, "total": 500, "passed": 460},
        )
        assert result.run_id == "run-1"
        assert result.evaluator == "pytest"
        assert result.get_outputs()["pass_rate"] == 0.92
        assert result.evaluated_at_ms > 0

    def test_make_with_pareto_point(self):
        # Multi-objective: user puts a point or front in outputs.
        result = Result.make(
            run_id="run-1",
            evaluator="multi-obj",
            outputs={
                "pareto_point": [0.92, 0.78, 12.4],
                "axes": ["pass_rate", "coverage", "build_time_s"],
            },
        )
        assert result.get_outputs()["pareto_point"] == [0.92, 0.78, 12.4]

    def test_make_with_llm_judge_verdict(self):
        result = Result.make(
            run_id="run-1",
            evaluator="llm-judge-claude-4-6",
            outputs={
                "verdict": "better",
                "confidence": 0.85,
                "reasoning": "Handles an edge case the baseline missed",
            },
        )
        assert result.get_outputs()["verdict"] == "better"

    def test_multiple_results_per_run_are_independent(self):
        # Same run_id, different evaluator — multiple Results per run
        # are supported so different eval harnesses can score the same
        # run independently.
        r1 = Result.make(run_id="run-1", evaluator="pytest", outputs={"pass_rate": 0.92})
        r2 = Result.make(run_id="run-1", evaluator="ruff", outputs={"errors": 3})
        r3 = Result.make(run_id="run-1", evaluator="llm-judge", outputs={"verdict": "ok"})
        assert r1.run_id == r2.run_id == r3.run_id
        assert r1.evaluator != r2.evaluator != r3.evaluator
        assert r1.get_outputs() != r2.get_outputs()

    def test_prefix(self):
        assert Result.get_prefix() == "result__"

    def test_no_reward_fields(self):
        for forbidden in ("metric_name", "value", "beats_frontier", "score"):
            assert forbidden not in Result.model_fields


class TestBranchHeadComponent:
    def test_is_archetype_component(self):
        assert issubclass(BranchHead, Component)

    def test_default_construction(self):
        head = BranchHead()
        assert head.experiment_name == ""
        assert head.commit_hash == ""
        assert head.run_id == ""
        assert head.descriptor_json == "{}"
        assert head.updated_at_ms == 0

    def test_make_with_scalar_descriptor(self):
        head = BranchHead.make(
            experiment_name="exp",
            commit_hash="abc123",
            run_id="run-1",
            descriptor={"test_pass_rate": 0.92},
        )
        assert head.experiment_name == "exp"
        assert head.commit_hash == "abc123"
        assert head.get_descriptor()["test_pass_rate"] == 0.92
        assert head.updated_at_ms > 0

    def test_make_with_pareto_descriptor(self):
        head = BranchHead.make(
            experiment_name="exp",
            commit_hash="abc",
            run_id="run-1",
            descriptor={
                "pareto_point": [0.92, 0.78, 12.4],
                "dominates": ["run-prior-1", "run-prior-2"],
            },
        )
        assert head.get_descriptor()["dominates"] == ["run-prior-1", "run-prior-2"]

    def test_make_with_llm_verdict_descriptor(self):
        head = BranchHead.make(
            experiment_name="exp",
            commit_hash="abc",
            run_id="run-1",
            descriptor={
                "judge": "claude-4-6",
                "verdict": "better than baseline",
                "reasoning": "Handles null path correctly",
            },
        )
        assert head.get_descriptor()["verdict"] == "better than baseline"

    def test_initial_seed_has_no_run_id(self):
        # Before any runs have happened, the head is seeded from the
        # branch's actual git HEAD. run_id is "" for this seed state.
        head = BranchHead.make(
            experiment_name="exp",
            commit_hash="initial-branch-head",
        )
        assert head.run_id == ""

    def test_prefix(self):
        assert BranchHead.get_prefix() == "branchhead__"

    def test_no_frontier_value_or_metric_fields(self):
        # The redesign deliberately omits scoring semantics at the
        # Component level. descriptor_json is where user scoring lives.
        for forbidden in (
            "frontier_value",
            "metric_name",
            "direction",
            "value",
            "score",
        ):
            assert forbidden not in BranchHead.model_fields


class TestComponentDiscovery:
    def test_discoverable_by_name(self):
        # Component.get_type_by_name walks __subclasses__, so importing
        # the module is enough to register. No explicit registry.
        for name in ("Experiment", "Run", "Result", "BranchHead"):
            resolved = Component.get_type_by_name(name)
            assert resolved.__name__ == name

    def test_unknown_name_raises(self):
        with pytest.raises(ValueError, match="not found"):
            Component.get_type_by_name("NotAComponent")

    def test_prefixed_schemas_do_not_collide(self):
        prefixes = {
            Experiment.get_prefix(),
            Run.get_prefix(),
            Result.get_prefix(),
            BranchHead.get_prefix(),
        }
        assert len(prefixes) == 4  # all distinct

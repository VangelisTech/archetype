# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end contract for the problem-definition autoresearch example."""

from __future__ import annotations

import asyncio
import hashlib

import pytest

from archetype import ArchetypeRuntime
from examples.problem_definition_mission import (
    EvidenceItem,
    EvidenceSnapshot,
    GepaPromptConfig,
    PanelEvaluation,
    ProblemDefinitionPolicy,
    ProblemFramingCandidate,
    ProblemFramingEvaluation,
    ProblemFramingHead,
    ProblemFramingRun,
)
from examples.problem_definition_mission.mission import (
    IMPROVED_PROMPT,
    SEED_PROMPT,
    ProblemDefinitionMission,
    run_demo,
)


@pytest.mark.asyncio
async def test_problem_definition_example_improves_ratifies_and_orders_ledger(
    tmp_path,
) -> None:
    result = await run_demo(str(tmp_path / "problem-definition"), offline=True)

    assert result["mode"] == "offline"
    assert result["model"] == "deterministic"
    assert result["evaluator_id"] == ("archetype.problem-definition.panel-v2:deterministic-offline")
    assert result["snapshot_revision"] == 2
    assert result["seed_prompt"] == SEED_PROMPT
    assert result["head_prompt"] == IMPROVED_PROMPT
    assert result["improved"] is True
    assert result["decision_status"] == "ratified"
    assert result["unanimous"] is True
    assert result["hard_gate_passed"] is True
    assert result["active_challenges"] == ()
    assert result["counterexample_searches"]
    assert all(
        str(search).endswith(":not_found_within_budget")
        for search in result["counterexample_searches"]
    )
    assert result["perspectives"] == ("naive", "expert", "orthogonal")
    assert result["framing_statement"] == (
        "First-time users cannot confidently complete setup because the safe policy "
        "choice for a first run is unclear."
    )
    assert result["next_question"] == "What problem are we solving?"
    assert float(result["aggregate_score"]) > 0.8
    assert (
        int(result["intent_tick"]) < int(result["observation_tick"]) < int(result["decision_tick"])
    )


@pytest.mark.asyncio
async def test_problem_definition_demo_reports_an_unresolved_hard_gate(
    tmp_path,
) -> None:
    result = await run_demo(
        str(tmp_path / "unresolved"),
        offline=True,
        config=GepaPromptConfig(
            max_metric_calls=1,
            max_candidate_proposals=1,
            patience=1,
        ),
    )

    assert result["decision_status"] == "unresolved"
    assert result["head_prompt"] == SEED_PROMPT
    assert result["improved"] is False
    assert result["unanimous"] is False
    assert result["hard_gate_passed"] is False
    assert result["framing_statement"] == "Users have a setup problem."


class _FailingPanel:
    def evaluate(
        self,
        prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ):
        del prompt, snapshot, policy
        raise RuntimeError("deterministic panel failure")


class _CancellingPanel:
    def evaluate(
        self,
        prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ):
        del prompt, snapshot, policy
        raise asyncio.CancelledError


@pytest.mark.asyncio
async def test_new_evidence_reopens_with_fresh_bindings_and_crashes_are_terminal(
    tmp_path,
) -> None:
    storage = str(tmp_path / "reopen")
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(
            runtime,
            storage=storage,
            session_id="reopen-test",
            offline=True,
        )
        await mission.feed(
            EvidenceItem(
                evidence_id="interview",
                source="interview",
                content="Users leave setup at the policy selection step.",
            )
        )
        first = await mission.refine()
        assert first.head is not None
        assert first.evaluation is not None
        assert first.accepted is True
        world_id = str(mission.world.world_id)

    # A new runtime process resumes the durable writer and reconstructs the
    # exact snapshot and prompt head before accepting another occurrence.
    async with ArchetypeRuntime() as runtime:
        mission = await ProblemDefinitionMission.resume(
            runtime,
            world_id,
            storage=storage,
            offline=True,
        )
        assert mission.snapshot.revision == first.snapshot.revision
        assert mission.snapshot.digest == first.snapshot.digest
        assert mission.head_prompt == first.head.prompt

        await mission.feed(
            EvidenceItem(
                evidence_id="support",
                source="support",
                content="New users ask support which policy is safe.",
            )
        )
        second = await mission.refine()
        assert second.head is not None
        assert second.evaluation is not None
        assert second.snapshot.revision == first.snapshot.revision + 1
        assert second.evaluation.evidence_digest == second.snapshot.digest
        assert second.evaluation.evidence_digest != first.evaluation.evidence_digest
        assert all(
            observation.binding == second.evaluation.binding
            for observation in second.evaluation.observations
        )
        assert all(vote.binding == second.evaluation.binding for vote in second.evaluation.votes)
        assert {vote.vote_id for vote in first.evaluation.votes}.isdisjoint(
            vote.vote_id for vote in second.evaluation.votes
        )
        assert (
            first.decision_tick
            < second.intent_tick
            < second.observation_tick
            < second.decision_tick
        )

        joined_rows = (
            await mission.world.query(
                ProblemFramingCandidate,
                ProblemFramingEvaluation,
            )
        ).to_pylist()
        assert joined_rows
        assert all(
            row["problemframingcandidate__candidate_id"]
            == row["problemframingevaluation__candidate_id"]
            for row in joined_rows
        )
        first_candidates = [
            row
            for row in (await mission.world.query(ProblemFramingCandidate)).to_pylist()
            if row["problemframingcandidate__run_id"] == first.run_id
        ]
        seed_rows = [
            row for row in first_candidates if row["problemframingcandidate__prompt"] == SEED_PROMPT
        ]
        improved_rows = [
            row
            for row in first_candidates
            if row["problemframingcandidate__prompt"] == IMPROVED_PROMPT
        ]
        assert seed_rows
        assert seed_rows[-1]["problemframingcandidate__parent_prompt_digest"] == ""
        assert improved_rows
        assert improved_rows[-1]["problemframingcandidate__parent_prompt_digest"] == (
            hashlib.sha256(SEED_PROMPT.encode()).hexdigest()
        )

        with pytest.raises(RuntimeError, match="deterministic panel failure"):
            await mission.refine(panel_evaluator=_FailingPanel())

        run_rows = (await mission.world.query(ProblemFramingRun)).to_pylist()
        crashed = [row for row in run_rows if row["problemframingrun__status"] == "crashed"]
        assert crashed
        assert "deterministic panel failure" in crashed[-1]["problemframingrun__error"]
        assert int(crashed[-1]["tick"]) > second.decision_tick


@pytest.mark.asyncio
async def test_resume_reconstructs_custom_seed_provider_model_and_policy_before_a_head(
    tmp_path,
) -> None:
    storage = str(tmp_path / "custom-seed")
    custom_policy = ProblemDefinitionPolicy.default_three_perspective().model_copy(
        update={
            "evaluator_id": "custom-offline-panel",
            "counterexample_verifier_id": "custom-offline-verifier",
        }
    )
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(
            runtime,
            storage=storage,
            session_id="custom-seed-test",
            seed_prompt="Define the exact custom problem.",
            policy=custom_policy,
            offline=True,
        )
        await mission.feed(EvidenceItem(evidence_id="evidence", source="test", content="Signal."))
        world_id = str(mission.world.world_id)

    async with ArchetypeRuntime() as runtime:
        resumed = await ProblemDefinitionMission.resume(runtime, world_id, storage=storage)

        assert resumed.head_prompt == "Define the exact custom problem."
        assert resumed.provider == "offline"
        assert resumed.model == "deterministic"
        assert resumed.policy == custom_policy


@pytest.mark.asyncio
async def test_cancellation_after_intent_is_settled_as_crashed(tmp_path) -> None:
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(runtime, storage=tmp_path / "cancel", offline=True)
        await mission.feed(EvidenceItem(evidence_id="evidence", source="test", content="Signal."))

        with pytest.raises(asyncio.CancelledError):
            await mission.refine(panel_evaluator=_CancellingPanel())

        run_rows = (await mission.world.query(ProblemFramingRun)).to_pylist()
        latest = max(run_rows, key=lambda row: int(row["tick"]))
        assert latest["problemframingrun__status"] == "crashed"
        assert "CancelledError" in latest["problemframingrun__error"]


@pytest.mark.asyncio
async def test_cold_resume_settles_an_orphan_running_intent_and_allows_retry(
    tmp_path,
    monkeypatch,
) -> None:
    storage = str(tmp_path / "orphan-intent")
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(runtime, storage=storage, offline=True)
        await mission.feed(EvidenceItem(evidence_id="evidence", source="test", content="Signal."))
        world_id = str(mission.world.world_id)

        async def fail_settlement(*args, **kwargs) -> None:
            del args, kwargs
            raise RuntimeError("simulated hard death")

        monkeypatch.setattr(mission, "_settle_crashed_run", fail_settlement)
        with pytest.raises(RuntimeError, match="deterministic panel failure"):
            await mission.refine(panel_evaluator=_FailingPanel())

    async with ArchetypeRuntime() as runtime:
        resumed = await ProblemDefinitionMission.resume(runtime, world_id, storage=storage)
        run_rows = (await resumed.world.query(ProblemFramingRun)).to_pylist()
        run_one = [row for row in run_rows if row["problemframingrun__run_id"].endswith(":run-1")]
        assert (
            max(run_one, key=lambda row: int(row["tick"]))["problemframingrun__status"] == "crashed"
        )

        retried = await resumed.refine()
        assert retried.run_id.endswith(":run-2")
        assert retried.decision_tick > retried.observation_tick > retried.intent_tick


@pytest.mark.asyncio
async def test_cold_resume_completes_an_observed_run_without_model_calls(
    tmp_path,
    monkeypatch,
) -> None:
    storage = str(tmp_path / "orphan-observation")
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(runtime, storage=storage, offline=True)
        await mission.feed(EvidenceItem(evidence_id="evidence", source="test", content="Signal."))
        world_id = str(mission.world.world_id)

        async def fail_decision(**kwargs):
            del kwargs
            raise RuntimeError("decision failpoint")

        monkeypatch.setattr(mission, "_commit_decision", fail_decision)
        with pytest.raises(RuntimeError, match="decision failpoint"):
            await mission.refine()
        observed_rows = (await mission.world.query(ProblemFramingRun)).to_pylist()
        observation_tick = max(
            int(row["tick"])
            for row in observed_rows
            if row["problemframingrun__status"] == "observed"
        )

    def forbid_model_initialization(self):
        del self
        raise AssertionError("cold recovery must not initialize a model provider")

    monkeypatch.setattr(ProblemDefinitionMission, "_default_agents", forbid_model_initialization)
    async with ArchetypeRuntime() as runtime:
        resumed = await ProblemDefinitionMission.resume(runtime, world_id, storage=storage)

        assert resumed.head_prompt == IMPROVED_PROMPT
        head_rows = (await resumed.world.query(ProblemFramingHead)).to_pylist()
        assert len(head_rows) == 1
        assert int(head_rows[0]["tick"]) > observation_tick
        run_rows = (await resumed.world.query(ProblemFramingRun)).to_pylist()
        assert (
            max(run_rows, key=lambda row: int(row["tick"]))["problemframingrun__status"]
            == "stopped"
        )


@pytest.mark.asyncio
async def test_observed_run_recovery_fails_closed_on_a_corrupt_binding(
    tmp_path,
    monkeypatch,
) -> None:
    storage = str(tmp_path / "corrupt-observation")
    captured: dict[str, object] = {}
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(runtime, storage=storage, offline=True)
        await mission.feed(EvidenceItem(evidence_id="evidence", source="test", content="Signal."))
        world_id = str(mission.world.world_id)

        async def capture_and_fail(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("decision failpoint")

        monkeypatch.setattr(mission, "_commit_decision", capture_and_fail)
        with pytest.raises(RuntimeError, match="decision failpoint"):
            await mission.refine()

        evaluation = captured["head_evaluation"]
        assert isinstance(evaluation, PanelEvaluation)
        evaluation_rows = (await mission.world.query(ProblemFramingEvaluation)).to_pylist()
        row = next(
            candidate
            for candidate in evaluation_rows
            if candidate["problemframingevaluation__candidate_id"] == evaluation.candidate_digest
        )
        corrupted = mission._evaluation_component(
            str(row["problemframingevaluation__run_id"]),
            evaluation,
        ).model_copy(
            update={
                "binding_json": evaluation.binding.model_copy(
                    update={"candidate_digest": "corrupt-candidate"}
                ).model_dump_json()
            }
        )
        await mission.world.update(int(row["entity_id"]), corrupted)
        await mission.world.run(steps=1)

    async with ArchetypeRuntime() as runtime:
        with pytest.raises(ValueError, match="failed exact receipt validation"):
            await ProblemDefinitionMission.resume(runtime, world_id, storage=storage)

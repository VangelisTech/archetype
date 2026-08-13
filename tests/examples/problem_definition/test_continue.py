# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Continuation reporting contracts for unresolved problem-definition runs."""

from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest


class _Rows:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def to_pylist(self) -> list[dict[str, object]]:
        return self._rows


class _World:
    world_id = "world-123"

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    async def query(self, component: type[object]) -> _Rows:
        assert component.__name__ == "ProblemFramingEvaluation"
        return _Rows(self._rows)


@pytest.mark.asyncio
async def test_unresolved_continuation_reports_only_the_current_run(
    capsys,
    monkeypatch,
) -> None:
    examples_dir = Path(__file__).parents[3] / "examples"
    monkeypatch.syspath_prepend(str(examples_dir))
    continuation = importlib.import_module("problem_definition_continue")
    contracts = importlib.import_module("problem_definition_mission")
    framing = contracts.ProblemFraming(
        framing_id="current-framing",
        statement="Current provisional framing.",
        subject="Shared problem",
        current_state="The beneficiary and accepted outcome are unspecified.",
        desired_state="One bounded job has an observable accepted outcome.",
        gap="The transcript does not establish that concrete comparison.",
        stakes="The speakers may optimize a proxy instead of the intended result.",
        in_scope=["Defining the beneficiary and accepted outcome."],
        out_of_scope=["Choosing a product implementation."],
        success_criteria=["The outcome can be compared with a baseline."],
        claims=[
            contracts.AtomicClaim(
                claim_id="claim-1",
                kind=contracts.ClaimKind.OBSERVATION,
                statement="The accepted outcome remains unspecified.",
                evidence_ids=["transcript"],
                confidence=0.9,
                falsifier="The transcript names an accepted outcome.",
            )
        ],
        evidence_dispositions=[
            contracts.EvidenceDisposition(
                evidence_id="transcript",
                disposition=contracts.EvidenceDispositionKind.SUPPORTS,
                reason="The transcript leaves the accepted outcome open.",
            )
        ],
        contradictions=[],
        unknowns=["Who is the beneficiary?"],
        next_question="Who benefits?",
    )
    rows = [
        {
            "tick": 99,
            "problemframingevaluation__run_id": "older-run",
            "problemframingevaluation__evidence_revision": 2,
            "problemframingevaluation__evidence_digest": "current-digest",
            "problemframingevaluation__aggregate_score": 1.0,
            "problemframingevaluation__unanimous": True,
            "problemframingevaluation__hard_gate_passed": True,
            "problemframingevaluation__framing_json": framing.model_copy(
                update={"statement": "Stale framing."}
            ).model_dump_json(),
        },
        {
            "tick": 12,
            "problemframingevaluation__run_id": "current-run",
            "problemframingevaluation__evidence_revision": 1,
            "problemframingevaluation__evidence_digest": "historical-digest",
            "problemframingevaluation__aggregate_score": 1.0,
            "problemframingevaluation__unanimous": True,
            "problemframingevaluation__hard_gate_passed": True,
            "problemframingevaluation__framing_json": framing.model_copy(
                update={"statement": "Same-run historical framing."}
            ).model_dump_json(),
        },
        {
            "tick": 11,
            "problemframingevaluation__run_id": "current-run",
            "problemframingevaluation__evidence_revision": 2,
            "problemframingevaluation__evidence_digest": "current-digest",
            "problemframingevaluation__aggregate_score": 0.6,
            "problemframingevaluation__unanimous": False,
            "problemframingevaluation__hard_gate_passed": False,
            "problemframingevaluation__framing_json": framing.model_dump_json(),
        },
    ]
    mission = SimpleNamespace(
        world=_World(rows),
        provider="offline",
        model="deterministic",
        head_prompt="What problem are we solving?",
    )
    result = SimpleNamespace(
        run_id="current-run",
        head=None,
        evaluation=None,
        accepted=False,
        snapshot=SimpleNamespace(revision=2, digest="current-digest"),
    )

    await continuation._report_completed_result(
        mission,
        result,
        previous_revision=1,
    )

    output = capsys.readouterr().out
    assert "Decision: unresolved" in output
    assert "Consensus votes 3/3: False" in output
    assert "Hard gate passed: False" in output
    assert "Counterexample searches: none" in output
    assert "Active counterexample challenges: none" in output
    assert "Provisional framing: Current provisional framing." in output
    assert "Selected prompt: What problem are we solving?" in output
    assert "Stale framing." not in output
    assert "Same-run historical framing." not in output


def test_continuation_forwards_explicit_gepa_bounds_and_reports_snapshot_multiplier(
    monkeypatch,
) -> None:
    examples_dir = Path(__file__).parents[3] / "examples"
    monkeypatch.syspath_prepend(str(examples_dir))
    continuation = importlib.import_module("problem_definition_continue")
    args = continuation._parse_args(
        [
            "world-123",
            "--evidence-id",
            "new-evidence",
            "--source",
            "test",
            "--content",
            "Signal.",
            "--max-metric-calls",
            "2",
            "--max-candidate-proposals",
            "1",
            "--patience",
            "1",
            "--gepa-seed",
            "11",
            "--improvement-threshold",
            "0.25",
        ]
    )

    config = continuation._gepa_config(args)
    message = continuation._live_budget_message(
        SimpleNamespace(provider="codex", evaluation_snapshot_count=3),
        config,
    )

    assert args.provider is None
    assert config.max_metric_calls == 2
    assert config.max_candidate_proposals == 1
    assert config.patience == 1
    assert config.seed == 11
    assert config.improvement_threshold == 0.25
    assert "54 panel model calls" in message
    assert "9 × 2 metric calls × 3 evidence snapshots" in message
    assert "up to 1 GEPA reflection calls" in message

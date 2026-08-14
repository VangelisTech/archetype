# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free configuration contracts for the example mission CLI."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from archetype import ArchetypeRuntime
from examples.problem_definition_mission import EvidenceItem
from examples.problem_definition_mission.mission import (
    DEFAULT_CODEX_MODEL,
    ProblemDefinitionMission,
    _parse_args,
    evidence_items_from_files,
    resolve_provider,
)


def test_provider_selection_preserves_legacy_defaults_and_is_explicit() -> None:
    assert resolve_provider() == "openai"
    assert resolve_provider("openai") == "openai"
    assert resolve_provider("codex") == "codex"
    assert resolve_provider("offline") == "offline"
    assert resolve_provider(offline=True) == "offline"


def test_evidence_files_are_stable_ordered_and_never_truncated(tmp_path) -> None:
    transcript = ("speaker A: one\nspeaker B: two\n" * 3_200) + "EOF"
    transcript_path = tmp_path / "transcript.txt"
    transcript_path.write_text(transcript, encoding="utf-8")

    first = evidence_items_from_files((transcript_path,))
    second = evidence_items_from_files((transcript_path,))
    alias_path = tmp_path / "transcript-alias.txt"
    alias_path.symlink_to(transcript_path)
    aliased = evidence_items_from_files((alias_path,))

    assert first == second
    assert aliased == first
    assert len(first) > 1
    assert all(len(item.content) <= 32_000 for item in first)
    assert "".join(item.content for item in first) == transcript
    assert [item.evidence_id for item in first] == sorted(item.evidence_id for item in first)
    assert all(str(transcript_path) in item.source for item in first)


def test_cli_accepts_codex_transcript_question_and_bounded_gepa(tmp_path) -> None:
    transcript_path = tmp_path / "transcript.txt"
    transcript_path.write_text("JP and Everett discuss a problem.", encoding="utf-8")

    args = _parse_args(
        [
            "--provider",
            "codex",
            "--model",
            "gpt-5.6-sol",
            "--question",
            "What problem are JP and Everett solving?",
            "--seed-framing",
            "Define their shared problem from only the transcript.",
            "--evidence-file",
            str(transcript_path),
            "--max-metric-calls",
            "2",
            "--max-candidate-proposals",
            "1",
            "--patience",
            "1",
        ]
    )

    assert args.provider == "codex"
    assert args.model == "gpt-5.6-sol"
    assert args.question == "What problem are JP and Everett solving?"
    assert args.seed_prompt == "Define their shared problem from only the transcript."
    assert args.evidence_file == [transcript_path]
    assert args.max_metric_calls == 2
    assert args.max_candidate_proposals == 1
    assert args.patience == 1


@pytest.mark.smoke
def test_offline_entrypoint_executes_the_complete_mission(tmp_path) -> None:
    root = Path(__file__).parents[3]
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "examples" / "problem_definition_autoresearch.py"),
            "--provider",
            "offline",
            "--storage",
            str(tmp_path / "ledger"),
            "--max-metric-calls",
            "2",
            "--max-candidate-proposals",
            "1",
            "--patience",
            "1",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert "Decision: ratified" in completed.stdout
    assert "Hard gate passed: True" in completed.stdout
    assert "Counterexample searches:" in completed.stdout
    assert "Active counterexample challenges: none" in completed.stdout


@pytest.mark.asyncio
async def test_mission_wires_codex_provider_without_invoking_it(tmp_path) -> None:
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(
            runtime,
            provider="codex",
            model="gpt-5.6-sol",
            question="What problem are JP and Everett solving?",
            storage=tmp_path / "ledger",
        )

        assert mission.provider == "codex"
        assert mission.offline is False
        assert mission.model == "gpt-5.6-sol"
        assert mission.question == "What problem are JP and Everett solving?"
        assert mission.head_prompt == mission.question
        assert mission.policy.evaluator_id == (
            "archetype.problem-definition.panel-v2:codex.exec:gpt-5.6-sol"
        )
        assert mission.policy.counterexample_verifier_id == (
            "archetype.problem-definition.counterexample-verifier-v2:codex.exec:gpt-5.6-sol"
        )


@pytest.mark.asyncio
async def test_mission_pins_codex_default_model_in_durable_policy(tmp_path) -> None:
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(
            runtime,
            provider="codex",
            storage=tmp_path / "ledger",
        )

        assert mission.model == DEFAULT_CODEX_MODEL
        assert mission.policy.evaluator_id == (
            f"archetype.problem-definition.panel-v2:codex.exec:{DEFAULT_CODEX_MODEL}"
        )


@pytest.mark.asyncio
async def test_resume_uses_durable_provider_model_and_rejects_explicit_mismatch(tmp_path) -> None:
    storage = tmp_path / "durable-provider"
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(
            runtime,
            provider="codex",
            model="gpt-5.6-sol",
            storage=storage,
        )
        await mission.feed(EvidenceItem(evidence_id="evidence", source="test", content="Signal."))
        world_id = str(mission.world.world_id)

    async with ArchetypeRuntime() as runtime:
        resumed = await ProblemDefinitionMission.resume(runtime, world_id, storage=storage)
        assert resumed.provider == "codex"
        assert resumed.model == "gpt-5.6-sol"

    async with ArchetypeRuntime() as runtime:
        with pytest.raises(ValueError, match="provider does not match"):
            await ProblemDefinitionMission.resume(
                runtime,
                world_id,
                storage=storage,
                provider="offline",
            )

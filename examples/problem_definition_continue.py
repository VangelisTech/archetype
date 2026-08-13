# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Resume a durable problem-definition mission and append more evidence."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from problem_definition_mission import (
    EvidenceItem,
    GepaPromptConfig,
    ProblemFraming,
    ProblemFramingEvaluation,
)
from problem_definition_mission.mission import (
    DEFAULT_CODEX_MODEL,
    DEFAULT_MODEL,
    PROVIDERS,
    ProblemDefinitionMission,
    RefinementResult,
    evidence_items_from_files,
    resolve_provider,
)

from archetype import ArchetypeRuntime, StorageConfig


@dataclass(frozen=True)
class _CompletedOutcome:
    decision_status: str
    framing_statement: str
    unanimous: bool
    hard_gate_passed: bool
    counterexample_searches: tuple[str, ...]
    active_challenges: tuple[str, ...]
    selected_prompt: str


def _counterexample_outcome(framing: ProblemFraming) -> tuple[tuple[str, ...], tuple[str, ...]]:
    searches = tuple(
        f"{search.target_claim_id}:{search.outcome.value}"
        for search in framing.counterexample_searches
    )
    active = tuple(challenge.challenge_id for challenge in framing.challenges if challenge.active)
    return searches, active


async def _completed_outcome(
    mission: ProblemDefinitionMission,
    result: RefinementResult,
) -> _CompletedOutcome:
    if result.head is not None and result.evaluation is not None:
        searches, active = _counterexample_outcome(result.evaluation.framing)
        return _CompletedOutcome(
            decision_status="ratified" if result.accepted else "retained",
            framing_statement=result.evaluation.framing.statement,
            unanimous=result.evaluation.unanimous,
            hard_gate_passed=result.evaluation.hard_gate_passed,
            counterexample_searches=searches,
            active_challenges=active,
            selected_prompt=result.head.prompt,
        )

    evaluation_rows = (await mission.world.query(ProblemFramingEvaluation)).to_pylist()
    current_run_rows = [
        row
        for row in evaluation_rows
        if str(row["problemframingevaluation__run_id"]) == result.run_id
        and int(row["problemframingevaluation__evidence_revision"]) == result.snapshot.revision
        and str(row["problemframingevaluation__evidence_digest"]) == result.snapshot.digest
    ]
    if not current_run_rows:
        raise RuntimeError(
            f"completed unresolved run {result.run_id!r} has no persisted panel evaluation"
        )
    best_observation = max(
        current_run_rows,
        key=lambda row: (
            float(row["problemframingevaluation__aggregate_score"]),
            int(row["tick"]),
        ),
    )
    framing = ProblemFraming.model_validate_json(
        str(best_observation["problemframingevaluation__framing_json"])
    )
    searches, active = _counterexample_outcome(framing)
    return _CompletedOutcome(
        decision_status="unresolved",
        framing_statement=framing.statement,
        unanimous=bool(best_observation["problemframingevaluation__unanimous"]),
        hard_gate_passed=bool(best_observation["problemframingevaluation__hard_gate_passed"]),
        counterexample_searches=searches,
        active_challenges=active,
        selected_prompt=mission.head_prompt,
    )


async def _report_completed_result(
    mission: ProblemDefinitionMission,
    result: RefinementResult,
    *,
    previous_revision: int,
) -> None:
    outcome = await _completed_outcome(mission, result)
    framing_label = (
        "Provisional framing" if outcome.decision_status == "unresolved" else "Problem framing"
    )
    print(f"World ID: {mission.world.world_id}")
    print(f"Provider: {mission.provider} ({mission.model})")
    print(f"Evidence revision: {previous_revision} -> {result.snapshot.revision}")
    print(f"Decision: {outcome.decision_status}")
    print(f"Consensus votes 3/3: {outcome.unanimous}")
    print(f"Hard gate passed: {outcome.hard_gate_passed}")
    print("Counterexample searches: " + (", ".join(outcome.counterexample_searches) or "none"))
    print("Active counterexample challenges: " + (", ".join(outcome.active_challenges) or "none"))
    print(f"{framing_label}: {outcome.framing_statement}")
    print(f"Selected prompt: {outcome.selected_prompt}")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("world_id", help="World ID printed by the initial mission run.")
    parser.add_argument("--evidence-id")
    parser.add_argument("--source")
    parser.add_argument("--content")
    parser.add_argument(
        "--evidence-file",
        action="append",
        default=[],
        type=Path,
        metavar="PATH",
        help="UTF-8 evidence file to append; repeat for multiple files.",
    )
    parser.add_argument("--storage", default=".context/problem-definition")
    parser.add_argument("--namespace", default="problem_definition_demo")
    parser.add_argument(
        "--provider",
        choices=PROVIDERS,
        help="Optional provider override; defaults to the durable session provider.",
    )
    parser.add_argument(
        "--model",
        help=(
            "Provider model override. OpenAI defaults to "
            f"{DEFAULT_MODEL}; Codex defaults to {DEFAULT_CODEX_MODEL}."
        ),
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Backward-compatible alias for --provider offline.",
    )
    parser.add_argument("--max-metric-calls", type=int, default=6)
    parser.add_argument("--max-candidate-proposals", type=int, default=3)
    parser.add_argument("--patience", type=int, default=2)
    parser.add_argument("--gepa-seed", type=int, default=7)
    parser.add_argument("--improvement-threshold", type=float, default=0.1)
    args = parser.parse_args(argv)
    supplied_fields = (args.evidence_id, args.source, args.content)
    if any(value is not None for value in supplied_fields) and not all(
        value is not None for value in supplied_fields
    ):
        parser.error("--evidence-id, --source, and --content must be supplied together")
    if not args.evidence_file and not all(value is not None for value in supplied_fields):
        parser.error(
            "supply at least one --evidence-file or the --evidence-id/--source/--content triplet"
        )
    try:
        if args.offline or args.provider is not None:
            args.provider = resolve_provider(args.provider, offline=args.offline)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def _gepa_config(args: argparse.Namespace) -> GepaPromptConfig:
    try:
        return GepaPromptConfig(
            max_metric_calls=args.max_metric_calls,
            max_candidate_proposals=args.max_candidate_proposals,
            patience=args.patience,
            seed=args.gepa_seed,
            improvement_threshold=args.improvement_threshold,
        )
    except ValueError as exc:
        raise SystemExit(f"Invalid GEPA bounds: {exc}") from exc


def _live_budget_message(
    mission: ProblemDefinitionMission,
    config: GepaPromptConfig,
) -> str:
    snapshot_count = mission.evaluation_snapshot_count
    panel_call_budget = 9 * config.max_metric_calls * snapshot_count
    return (
        f"Live {mission.provider} mode: up to {panel_call_budget} panel model calls "
        f"(9 × {config.max_metric_calls} metric calls × {snapshot_count} evidence snapshots), "
        f"plus up to {config.max_candidate_proposals} GEPA reflection calls."
    )


async def main() -> None:
    args = _parse_args()
    config = _gepa_config(args)

    storage = StorageConfig(uri=args.storage, namespace=args.namespace)
    async with ArchetypeRuntime() as runtime:
        mission = await ProblemDefinitionMission.resume(
            runtime,
            args.world_id,
            storage=storage,
            provider=args.provider,
            model=args.model,
        )
        if mission.provider == "openai" and not os.environ.get("OPENAI_API_KEY"):
            raise SystemExit(
                "The durable openai provider requires OPENAI_API_KEY. Use the original "
                "credentials before continuing this mission."
            )
        previous_revision = mission.snapshot.revision
        evidence_items = list(evidence_items_from_files(args.evidence_file))
        if args.evidence_id is not None:
            evidence_items.append(
                EvidenceItem(
                    evidence_id=args.evidence_id,
                    source=args.source,
                    content=args.content,
                )
            )
        for item in evidence_items:
            await mission.feed(item)
        if mission.provider != "offline":
            print(_live_budget_message(mission, config))
        result = await mission.refine(config=config)
        await _report_completed_result(
            mission,
            result,
            previous_revision=previous_revision,
        )


if __name__ == "__main__":
    asyncio.run(main())

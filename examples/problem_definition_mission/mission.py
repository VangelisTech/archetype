# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Evidence-bound problem-definition autoresearch with a durable ECS ledger.

The optimized artifact is a reusable, single-shot problem-framing prompt.
OpenAI API and Codex subscription-backed agents are available as live
providers; deterministic providers are available only through the explicit
``--offline`` option used by CI:

    uv run --group problem-definition \
      python examples/problem_definition_autoresearch.py
    uv run --group problem-definition \
      python examples/problem_definition_autoresearch.py --offline

Each refinement crosses three separate world ticks:

1. intent: freeze the evidence revision, policy, seed, and search run;
2. observation: record every candidate's panel output; and
3. decision: append the exact prompt head selected by the pure promotion gate.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from archetype import ArchetypeRuntime, RuntimeWorld, StorageConfig

from . import (
    AtomicClaim,
    ClaimKind,
    EvaluationBinding,
    EvidenceDisposition,
    EvidenceDispositionKind,
    EvidenceItem,
    EvidenceSnapshot,
    GepaCandidateDiagnostic,
    GepaPromptConfig,
    OpenAIReflectionLanguageModel,
    OpenAIStructuredLanguageModel,
    PanelEvaluation,
    PanelEvaluator,
    PerspectiveObservation,
    ProblemDefinitionEvidence,
    ProblemDefinitionPolicy,
    ProblemDefinitionSession,
    ProblemFraming,
    ProblemFramingCandidate,
    ProblemFramingEvaluation,
    ProblemFramingHead,
    ProblemFramingRun,
    RatificationVote,
    ReflectionLanguageModel,
    ScoreVector,
    append_evidence,
    bounded_no_counterexample_search_receipts,
    build_model_backed_panel,
    optimize_problem_prompt,
    select_prompt_head,
)
from .codex_agents import CodexReflectionLanguageModel, CodexStructuredLanguageModel

QUESTION = "What problem are we solving?"
SEED_PROMPT = QUESTION
DEFAULT_MODEL = "gpt-5.6-terra"
DEFAULT_CODEX_MODEL = "gpt-5.6-sol"
LIVE_PROVIDERS = ("openai", "codex")
PROVIDERS = (*LIVE_PROVIDERS, "offline")
_EVIDENCE_FILE_CHUNK_CHARS = 30_000
IMPROVED_PROMPT = (
    "Given one immutable evidence snapshot, define exactly one solution-independent "
    "problem. Separate observations from hypotheses; express atomic, "
    "evidence-addressable claims; state the subject, current and desired states, gap, "
    "stakes, scope, non-goals, contradictions, unknowns, falsifiers, success criteria, "
    "and next question. Search every falsifiable material claim for a concrete "
    "counterexample; preserve each exact challenge until an independent verifier "
    "rejects it or the proposition is materially revised or removed. Return one "
    "framing that naive, research-equipped expert, and orthogonal perspectives can "
    "independently ratify."
)
OPTIMIZER_ID = "gepa.optimize_anything"


def resolve_provider(provider: str | None = None, *, offline: bool = False) -> str:
    """Resolve the provider while retaining the historical ``offline`` switch."""

    normalized = provider.strip().lower() if provider is not None else None
    if normalized == "":
        raise ValueError("provider must not be empty")
    if normalized is not None and normalized not in PROVIDERS:
        choices = ", ".join(PROVIDERS)
        raise ValueError(f"provider must be one of: {choices}")
    if offline and normalized not in {None, "offline"}:
        raise ValueError("--offline cannot be combined with a live --provider")
    if offline:
        return "offline"
    return normalized or "openai"


def evidence_items_from_files(paths: Sequence[str | Path]) -> tuple[EvidenceItem, ...]:
    """Read complete UTF-8 files into stable evidence items without truncation."""

    items: list[EvidenceItem] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        content = path.read_text(encoding="utf-8")
        if not content.strip():
            raise ValueError(f"evidence file is empty: {path}")
        path_label = str(path)
        file_digest = _text_digest(f"{path}\x1f{content}")
        chunks = tuple(
            content[index : index + _EVIDENCE_FILE_CHUNK_CHARS]
            for index in range(0, len(content), _EVIDENCE_FILE_CHUNK_CHARS)
        )
        for index, chunk in enumerate(chunks, start=1):
            suffix = f":part-{index:04d}-of-{len(chunks):04d}" if len(chunks) > 1 else ""
            source_suffix = f" (part {index}/{len(chunks)})" if len(chunks) > 1 else ""
            items.append(
                EvidenceItem(
                    evidence_id=f"file:{file_digest[:24]}{suffix}",
                    source=f"{path_label}{source_suffix}",
                    content=chunk,
                )
            )
    return tuple(items)


def _text_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _binding(
    prompt: str,
    snapshot: EvidenceSnapshot,
    policy: ProblemDefinitionPolicy,
) -> EvaluationBinding:
    return EvaluationBinding.for_candidate(prompt, snapshot, policy)


def _framing(
    snapshot: EvidenceSnapshot,
    *,
    framing_id: str,
    lens: str,
    explicit: bool,
) -> ProblemFraming:
    """Build one deterministic framing from the exact snapshot."""

    evidence_ids = list(snapshot.evidence_ids)
    observations = [
        AtomicClaim(
            claim_id=f"{framing_id}:observation:{index}",
            kind=ClaimKind.OBSERVATION,
            statement=item.content,
            evidence_ids=[item.evidence_id],
            confidence=0.95,
            falsifier="A source correction or retraction would revise this observation.",
        )
        for index, item in enumerate(snapshot.items)
    ]
    hypothesis = AtomicClaim(
        claim_id=f"{framing_id}:hypothesis",
        kind=ClaimKind.HYPOTHESIS,
        statement=(
            (
                "Uncertainty about a safe first-run policy contributes to setup "
                "abandonment and support demand."
            )
            if explicit
            else "The setup difficulty may involve the policy choice."
        ),
        evidence_ids=evidence_ids,
        confidence=0.72,
        falsifier=(
            "Setup completion and support demand remain unchanged when the safe first-run "
            "policy choice is made unambiguous."
        ),
    )
    current = " ".join(item.content for item in snapshot.items)
    return ProblemFraming(
        framing_id=framing_id,
        statement=(
            (
                "First-time users cannot confidently complete setup because the safe "
                "policy choice for a first run is unclear."
            )
            if explicit
            else "Users have a setup problem."
        ),
        subject=f"First-time setup users ({lens} lens)",
        current_state=current,
        desired_state=(
            (
                "A first-time user can choose a safe initial policy and complete setup "
                "with confidence."
            )
            if explicit
            else "Setup should be easier."
        ),
        gap=(
            (
                "The product asks for a consequential policy choice without making a "
                "safe first-run choice understandable."
            )
            if explicit
            else "The source of setup difficulty is not yet bounded."
        ),
        stakes="Users abandon setup or require support before receiving product value.",
        in_scope=[
            "First-run policy comprehension",
            "Setup completion",
            "Evidence-backed uncertainty",
        ],
        out_of_scope=[
            "Implementing a particular onboarding design",
            "Optimizing advanced-user policy configuration",
        ],
        success_criteria=[
            "First-time users can identify a safe policy without support.",
            "Policy-choice abandonment and related setup tickets materially decline.",
        ],
        claims=[*observations, hypothesis],
        evidence_dispositions=[
            EvidenceDisposition(
                evidence_id=item.evidence_id,
                disposition=EvidenceDispositionKind.SUPPORTS,
                reason="This item grounds the observed first-run policy uncertainty.",
            )
            for item in snapshot.items
        ],
        contradictions=[],
        unknowns=[
            "The baseline completion rate at the policy step is not yet measured.",
            "The safest default may vary by user context.",
        ],
        next_question=QUESTION,
    )


class DeterministicThreePerspectivePanel:
    """Credential-free panel whose score rewards the explicit framing protocol."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def evaluate(
        self,
        prompt: str,
        snapshot: EvidenceSnapshot,
        policy: ProblemDefinitionPolicy,
    ) -> PanelEvaluation:
        self.calls.append(prompt)
        explicit = prompt.strip() == IMPROVED_PROMPT
        quality = 0.92 if explicit else 0.38
        prompt_id = _text_digest(prompt)[:16]
        binding = _binding(prompt, snapshot, policy)

        observations = tuple(
            PerspectiveObservation(
                observation_id=_stable_id(
                    binding.candidate_digest,
                    perspective.value,
                    "observation",
                ),
                binding=binding,
                perspective=perspective,
                protocol_id=policy.protocol_for(perspective),
                framing=_framing(
                    snapshot,
                    framing_id=f"observation:{perspective.value}:{prompt_id}",
                    lens=perspective.value,
                    explicit=explicit,
                ),
                confidence=quality,
                feedback=(
                    f"{perspective.value} independently found the framing grounded; "
                    + (
                        "the instruction makes its epistemic boundaries explicit."
                        if explicit
                        else "the instruction should make atomicity and boundaries explicit."
                    )
                ),
            )
            for perspective in policy.required_perspectives
        )
        final_framing = _framing(
            snapshot,
            framing_id=f"synthesis:{snapshot.digest[:16]}:{prompt_id}",
            lens="synthesized",
            explicit=explicit,
        )
        final_framing = final_framing.model_copy(
            update={
                "counterexample_searches": bounded_no_counterexample_search_receipts(
                    final_framing,
                    snapshot,
                    policy,
                    detail=(
                        "The deterministic offline evaluator exhaustively applied its "
                        "configured bounded counterexample-search double and found no witness."
                    ),
                )
            }
        )
        observations_by_perspective = {
            observation.perspective: observation for observation in observations
        }
        votes = tuple(
            RatificationVote(
                vote_id=_stable_id(
                    binding.candidate_digest,
                    perspective.value,
                    "ratification",
                ),
                binding=binding,
                perspective=perspective,
                protocol_id=policy.ratification_protocol_id,
                approved=explicit,
                reason=(
                    (
                        "The exact synthesis is grounded, bounded, solution-independent, "
                        "and preserves explicit unknowns."
                    )
                    if explicit
                    else "The synthesis is still too broad to ratify as the exact problem."
                ),
                observation_id=observations_by_perspective[perspective].observation_id,
                observation_digest=observations_by_perspective[perspective].digest,
                framing_id=final_framing.framing_id,
                framing_digest=final_framing.digest,
            )
            for perspective in policy.required_perspectives
        )
        penalty = 0.0 if explicit else 0.12
        scores = ScoreVector(
            naive_clarity=quality,
            expert_grounding=quality,
            orthogonal_robustness=quality,
            consensus=quality,
            atomicity=quality,
            falsifiability=quality,
            scope_discipline=quality,
            solution_independence=quality,
            unsupported_claim_penalty=penalty,
            contradiction_penalty=0.0,
        )
        feedback = (
            (
                "Preserve the explicit evidence binding, atomic claims, scope, unknowns, "
                "falsifiers, bounded counterexample search, exact challenge continuity, "
                "success criteria, and independent three-role ratification."
            )
            if explicit
            else (
                "Rewrite the instruction to require atomic evidence-addressable claims, "
                "observations versus hypotheses, scope and non-goals, unknowns, "
                "falsifiers, bounded concrete counterexample search, independent challenge "
                "verification, success criteria, and independent naive/expert/orthogonal "
                "ratification."
            )
        )
        return PanelEvaluation(
            candidate_prompt=prompt,
            evidence_revision=snapshot.revision,
            evidence_ids=snapshot.evidence_ids,
            evidence_digest=snapshot.digest,
            policy=policy,
            observations=observations,
            synthesis_protocol_id=policy.synthesis_protocol_id,
            scoring_protocol_id=policy.scoring_protocol_id,
            framing=final_framing,
            votes=votes,
            scores=scores,
            feedback=(feedback,),
        )


class DeterministicReflectionModel:
    """GEPA reflection model that always proposes the stronger instruction."""

    def __init__(self) -> None:
        self.calls: list[str | list[dict[str, object]]] = []

    def __call__(self, prompt: str | list[dict[str, object]]) -> str:
        self.calls.append(prompt)
        return f"```\n{IMPROVED_PROMPT}\n```"


@dataclass(frozen=True)
class PromptHead:
    """Example-local projection of the selected durable head."""

    prompt: str
    evaluation: PanelEvaluation
    entity_id: int
    tick: int


@dataclass(frozen=True)
class RefinementResult:
    """One complete intent-observation-decision refinement."""

    snapshot: EvidenceSnapshot
    head: PromptHead | None
    accepted: bool
    run_id: str
    intent_tick: int
    observation_tick: int
    decision_tick: int

    @property
    def evaluation(self) -> PanelEvaluation | None:
        return self.head.evaluation if self.head is not None else None


class ProblemDefinitionMission:
    """Example-owned evidence ledger and bounded prompt hill climb."""

    def __init__(
        self,
        runtime: ArchetypeRuntime,
        *,
        question: str = QUESTION,
        seed_prompt: str | None = None,
        policy: ProblemDefinitionPolicy | None = None,
        storage: str | Path | StorageConfig | None = None,
        session_id: str = "problem-definition-demo",
        world_name: str = "problem-definition-autoresearch",
        offline: bool = False,
        provider: str | None = None,
        model: str | None = None,
        _world: RuntimeWorld | None = None,
    ) -> None:
        if not question.strip():
            raise ValueError("question must not be empty")
        selected_seed = question if seed_prompt is None else seed_prompt
        if not selected_seed.strip():
            raise ValueError("seed_prompt must not be empty")
        if not session_id.strip():
            raise ValueError("session_id must not be empty")
        if model is not None and not model.strip():
            raise ValueError("model must not be empty")

        selected_provider = resolve_provider(provider, offline=offline)
        if selected_provider == "offline":
            default_panel: PanelEvaluator | None = DeterministicThreePerspectivePanel()
            default_reflection: ReflectionLanguageModel | None = DeterministicReflectionModel()
            evaluator_id = "archetype.problem-definition.panel-v2:deterministic-offline"
            verifier_id = (
                "archetype.problem-definition.counterexample-verifier-v2:deterministic-offline"
            )
            selected_model = "deterministic"
        elif selected_provider == "codex":
            selected_model = model or DEFAULT_CODEX_MODEL
            default_panel = None
            default_reflection = None
            evaluator_id = f"archetype.problem-definition.panel-v2:codex.exec:{selected_model}"
            verifier_id = (
                "archetype.problem-definition.counterexample-verifier-v2:"
                f"codex.exec:{selected_model}"
            )
        else:
            selected_model = model or DEFAULT_MODEL
            default_panel = None
            default_reflection = None
            evaluator_id = (
                f"archetype.problem-definition.panel-v2:openai.responses:{selected_model}"
            )
            verifier_id = (
                "archetype.problem-definition.counterexample-verifier-v2:"
                f"openai.responses:{selected_model}"
            )

        self.world = _world or runtime.world(world_name, storage=storage)
        self.policy = policy or ProblemDefinitionPolicy.default_three_perspective().model_copy(
            update={
                "evaluator_id": evaluator_id,
                "counterexample_verifier_id": verifier_id,
            }
        )
        self.session_id = session_id
        self.question = question
        self.provider = selected_provider
        self.offline = selected_provider == "offline"
        self.model = selected_model
        self._default_panel = default_panel
        self._default_reflection = default_reflection
        self.snapshot = EvidenceSnapshot(revision=0)
        self._head_prompt = selected_seed
        self._session_entity_id: int | None = None
        self._run_sequence = 0
        self._evaluated_snapshots: list[EvidenceSnapshot] = []
        self._lock = asyncio.Lock()
        self._recovery_required = False

    @property
    def head_prompt(self) -> str:
        """Return the current reconstructed or newly selected prompt head."""

        return self._head_prompt

    @property
    def evaluation_snapshot_count(self) -> int:
        """Return the current plus distinct historical snapshots used by the next search."""

        return 1 + sum(
            snapshot.digest != self.snapshot.digest for snapshot in self._evaluated_snapshots
        )

    def _default_agents(self) -> tuple[PanelEvaluator, ReflectionLanguageModel]:
        """Initialize live providers lazily so durable state can be inspected offline."""

        if self._default_panel is not None and self._default_reflection is not None:
            return self._default_panel, self._default_reflection
        if self.provider == "codex":
            structured_model = CodexStructuredLanguageModel(model=self.model)
            self._default_panel = build_model_backed_panel(
                structured_model,
                verifier_id=self.policy.counterexample_verifier_id,
            )
            self._default_reflection = CodexReflectionLanguageModel(model=self.model)
        elif self.provider == "openai":
            structured_model = OpenAIStructuredLanguageModel(model=self.model)
            self._default_panel = build_model_backed_panel(
                structured_model,
                verifier_id=self.policy.counterexample_verifier_id,
            )
            self._default_reflection = OpenAIReflectionLanguageModel(model=self.model)
        else:  # pragma: no cover - initialized eagerly above
            raise RuntimeError(f"unsupported problem-definition provider: {self.provider}")
        return self._default_panel, self._default_reflection

    @classmethod
    async def resume(
        cls,
        runtime: ArchetypeRuntime,
        world_id: str,
        *,
        storage: str | Path | StorageConfig | None = None,
        policy: ProblemDefinitionPolicy | None = None,
        offline: bool = False,
        provider: str | None = None,
        model: str | None = None,
    ) -> ProblemDefinitionMission:
        """Resume a durable example world and rebuild its in-memory projection."""

        world = await runtime.resume(
            world_id,
            storage=storage,
            name="problem-definition-resumed",
        )
        session_rows = (await world.query(ProblemDefinitionSession)).to_pylist()
        if not session_rows:
            raise ValueError("world does not contain a problem-definition session")
        session_ids = {str(row["problemdefinitionsession__session_id"]) for row in session_rows}
        session_entities = {int(row["entity_id"]) for row in session_rows}
        if len(session_ids) != 1 or len(session_entities) != 1:
            raise ValueError("world must contain exactly one problem-definition session")
        latest_session = max(session_rows, key=lambda row: int(row["tick"]))
        session_id = next(iter(session_ids))
        question = str(latest_session["problemdefinitionsession__question"])
        stored_provider = resolve_provider(
            str(latest_session["problemdefinitionsession__provider"])
        )
        stored_model = str(latest_session["problemdefinitionsession__model"])
        stored_policy = ProblemDefinitionPolicy.model_validate_json(
            str(latest_session["problemdefinitionsession__policy_json"])
        )
        stored_policy_digest = str(latest_session["problemdefinitionsession__policy_digest"])
        if stored_policy.digest != stored_policy_digest:
            raise ValueError("durable problem-definition policy failed its digest")

        if offline:
            requested_provider = resolve_provider(provider, offline=True)
        elif provider is not None:
            requested_provider = resolve_provider(provider)
        else:
            requested_provider = stored_provider
        if requested_provider != stored_provider:
            raise ValueError("configured provider does not match the durable session")
        if model is not None and model != stored_model:
            raise ValueError("configured model does not match the durable session")
        if policy is not None and policy.digest != stored_policy.digest:
            raise ValueError("configured policy does not match the durable session")

        stored_head = str(latest_session["problemdefinitionsession__head_prompt"])
        if not stored_head.strip():
            raise ValueError("durable problem-definition session has no current prompt head")
        mission = cls(
            runtime,
            question=question,
            seed_prompt=stored_head,
            policy=policy or stored_policy,
            storage=storage,
            session_id=session_id,
            provider=stored_provider,
            model=stored_model,
            _world=world,
        )
        await mission._restore_from_ledger(session_rows)
        return mission

    @staticmethod
    def _run_number(run_id: str) -> int:
        marker = ":run-"
        if marker not in run_id:
            raise ValueError(f"invalid problem-definition run ID: {run_id!r}")
        try:
            return int(run_id.rsplit(marker, 1)[1])
        except ValueError as exc:
            raise ValueError(f"invalid problem-definition run ID: {run_id!r}") from exc

    @staticmethod
    def _latest_rows(
        rows: Sequence[dict[str, object]],
        key: str,
    ) -> dict[str, dict[str, object]]:
        latest: dict[str, dict[str, object]] = {}
        for row in rows:
            value = str(row[key])
            previous = latest.get(value)
            if previous is None or int(row["tick"]) > int(previous["tick"]):
                latest[value] = row
        return latest

    @staticmethod
    def _run_component_from_row(
        row: dict[str, object],
        *,
        status: str | None = None,
        error: str | None = None,
    ) -> ProblemFramingRun:
        return ProblemFramingRun(
            run_id=str(row["problemframingrun__run_id"]),
            session_id=str(row["problemframingrun__session_id"]),
            evidence_revision=int(row["problemframingrun__evidence_revision"]),
            evidence_digest=str(row["problemframingrun__evidence_digest"]),
            policy_digest=str(row["problemframingrun__policy_digest"]),
            seed_prompt_digest=str(row["problemframingrun__seed_prompt_digest"]),
            optimizer_id=str(row["problemframingrun__optimizer_id"]),
            optimizer_version=str(row["problemframingrun__optimizer_version"]),
            config_json=str(row["problemframingrun__config_json"]),
            historical_evidence_digests_json=str(
                row["problemframingrun__historical_evidence_digests_json"]
            ),
            status=status or str(row["problemframingrun__status"]),
            error=str(row["problemframingrun__error"]) if error is None else error,
        )

    @staticmethod
    def _snapshot_at_revision(
        evidence_with_revisions: Sequence[tuple[int, EvidenceItem]],
        revision: int,
    ) -> EvidenceSnapshot:
        return EvidenceSnapshot(
            revision=revision,
            items=tuple(
                item for item_revision, item in evidence_with_revisions if item_revision <= revision
            ),
        )

    async def _restore_from_ledger(
        self,
        session_rows: list[dict[str, object]],
    ) -> None:
        if {str(row["problemdefinitionsession__session_id"]) for row in session_rows} != {
            self.session_id
        } or len({int(row["entity_id"]) for row in session_rows}) != 1:
            raise ValueError("world must contain exactly one problem-definition session")
        latest_session = max(session_rows, key=lambda row: int(row["tick"]))
        stored_policy_digest = str(latest_session["problemdefinitionsession__policy_digest"])
        if stored_policy_digest != self.policy.digest:
            raise ValueError(
                "configured problem-definition policy does not match the durable session"
            )
        self._session_entity_id = int(latest_session["entity_id"])
        stored_head = str(latest_session["problemdefinitionsession__head_prompt"])
        if _text_digest(stored_head) != str(
            latest_session["problemdefinitionsession__head_prompt_digest"]
        ):
            raise ValueError("durable session prompt head failed its digest")
        self._head_prompt = stored_head

        all_evidence_rows = (await self.world.query(ProblemDefinitionEvidence)).to_pylist()
        evidence_rows = [
            row
            for row in all_evidence_rows
            if str(row["problemdefinitionevidence__session_id"]) == self.session_id
        ]
        evidence_entities: dict[str, set[int]] = {}
        for row in evidence_rows:
            evidence_entities.setdefault(
                str(row["problemdefinitionevidence__evidence_id"]), set()
            ).add(int(row["entity_id"]))
        duplicated_evidence = sorted(
            evidence_id
            for evidence_id, entity_ids in evidence_entities.items()
            if len(entity_ids) != 1
        )
        if duplicated_evidence:
            raise ValueError(
                "durable evidence IDs have multiple entities: " + ", ".join(duplicated_evidence)
            )
        latest_evidence = self._latest_rows(
            evidence_rows,
            "problemdefinitionevidence__evidence_id",
        )

        revision = int(latest_session["problemdefinitionsession__evidence_revision"])
        evidence_with_revisions: list[tuple[int, EvidenceItem]] = []
        for row in latest_evidence.values():
            item = EvidenceItem(
                evidence_id=str(row["problemdefinitionevidence__evidence_id"]),
                source=str(row["problemdefinitionevidence__source"]),
                content=str(row["problemdefinitionevidence__content"]),
            )
            stored_digest = str(row["problemdefinitionevidence__content_digest"])
            if item.digest != stored_digest:
                raise ValueError(f"durable evidence {item.evidence_id!r} failed its digest")
            evidence_with_revisions.append((int(row["problemdefinitionevidence__revision"]), item))

        stored_revisions = sorted(item_revision for item_revision, _ in evidence_with_revisions)
        if stored_revisions != list(range(1, revision + 1)):
            raise ValueError("durable evidence revisions are not contiguous and unique")

        snapshot = self._snapshot_at_revision(evidence_with_revisions, revision)
        stored_evidence_digest = str(latest_session["problemdefinitionsession__evidence_digest"])
        if snapshot.digest != stored_evidence_digest:
            raise ValueError("reconstructed evidence snapshot does not match the durable digest")
        self.snapshot = snapshot

        all_run_rows = (await self.world.query(ProblemFramingRun)).to_pylist()
        run_rows = [
            row
            for row in all_run_rows
            if str(row["problemframingrun__session_id"]) == self.session_id
        ]
        run_entities: dict[str, set[int]] = {}
        for row in run_rows:
            run_entities.setdefault(str(row["problemframingrun__run_id"]), set()).add(
                int(row["entity_id"])
            )
        duplicated_runs = sorted(
            run_id for run_id, entity_ids in run_entities.items() if len(entity_ids) != 1
        )
        if duplicated_runs:
            raise ValueError(
                "durable run IDs have multiple entities: " + ", ".join(duplicated_runs)
            )
        latest_runs = self._latest_rows(run_rows, "problemframingrun__run_id")
        self._run_sequence = max(
            (self._run_number(run_id) for run_id in latest_runs),
            default=0,
        )

        all_head_rows = (await self.world.query(ProblemFramingHead)).to_pylist()
        head_rows = [
            row
            for row in all_head_rows
            if str(row["problemframinghead__session_id"]) == self.session_id
        ]
        heads_by_run: dict[str, list[dict[str, object]]] = {}
        for row in head_rows:
            heads_by_run.setdefault(str(row["problemframinghead__run_id"]), []).append(row)
        for run_id, rows in heads_by_run.items():
            if run_id not in latest_runs:
                raise ValueError(f"durable head names unknown run {run_id!r}")
            if len(rows) != 1 or len({int(row["entity_id"]) for row in rows}) != 1:
                raise ValueError(f"run {run_id!r} has more than one durable prompt head")
            head_row = rows[0]
            run_row = latest_runs[run_id]
            head_prompt = str(head_row["problemframinghead__prompt"])
            head_matches = (
                _text_digest(head_prompt) == str(head_row["problemframinghead__prompt_digest"]),
                int(head_row["problemframinghead__evidence_revision"])
                == int(run_row["problemframingrun__evidence_revision"]),
                str(head_row["problemframinghead__evidence_digest"])
                == str(run_row["problemframingrun__evidence_digest"]),
                str(head_row["problemframinghead__policy_digest"])
                == str(run_row["problemframingrun__policy_digest"])
                == self.policy.digest,
                str(head_row["problemframinghead__evaluator_id"]) == self.policy.evaluator_id,
            )
            if not all(head_matches):
                raise ValueError(f"run {run_id!r} has an invalid durable prompt head")

        running = [
            row
            for run_id, row in latest_runs.items()
            if str(row["problemframingrun__status"]) == "running" and run_id not in heads_by_run
        ]
        running_with_heads = sorted(
            run_id
            for run_id, row in latest_runs.items()
            if str(row["problemframingrun__status"]) == "running" and run_id in heads_by_run
        )
        if running_with_heads:
            raise ValueError(
                "running runs already have prompt heads: " + ", ".join(running_with_heads)
            )
        if running:
            for row in running:
                await self.world.update(
                    int(row["entity_id"]),
                    self._run_component_from_row(
                        row,
                        status="crashed",
                        error="Interrupted before the durable observation boundary.",
                    ),
                )
            await self.world.run(steps=1)
            all_run_rows = (await self.world.query(ProblemFramingRun)).to_pylist()
            run_rows = [
                row
                for row in all_run_rows
                if str(row["problemframingrun__session_id"]) == self.session_id
            ]
            latest_runs = self._latest_rows(run_rows, "problemframingrun__run_id")

        orphan_observations = sorted(
            (
                row
                for run_id, row in latest_runs.items()
                if str(row["problemframingrun__status"]) in {"observed", "stopped"}
                and run_id not in heads_by_run
            ),
            key=lambda row: self._run_number(str(row["problemframingrun__run_id"])),
        )
        if orphan_observations:
            latest_existing_head = max(
                (self._run_number(run_id) for run_id in heads_by_run),
                default=0,
            )
            if any(
                self._run_number(str(row["problemframingrun__run_id"])) < latest_existing_head
                for row in orphan_observations
            ):
                raise ValueError("a decision gap precedes a later durable prompt head")
            candidate_rows = (await self.world.query(ProblemFramingCandidate)).to_pylist()
            evaluation_rows = (await self.world.query(ProblemFramingEvaluation)).to_pylist()
            for row in orphan_observations:
                await self._recover_observed_run(
                    row,
                    evidence_with_revisions=evidence_with_revisions,
                    candidate_rows=candidate_rows,
                    evaluation_rows=evaluation_rows,
                )

            session_rows = (await self.world.query(ProblemDefinitionSession)).to_pylist()
            session_rows = [
                row
                for row in session_rows
                if str(row["problemdefinitionsession__session_id"]) == self.session_id
            ]
            latest_session = max(session_rows, key=lambda row: int(row["tick"]))
            all_run_rows = (await self.world.query(ProblemFramingRun)).to_pylist()
            run_rows = [
                row
                for row in all_run_rows
                if str(row["problemframingrun__session_id"]) == self.session_id
            ]
            latest_runs = self._latest_rows(run_rows, "problemframingrun__run_id")
            all_head_rows = (await self.world.query(ProblemFramingHead)).to_pylist()
            head_rows = [
                row
                for row in all_head_rows
                if str(row["problemframinghead__session_id"]) == self.session_id
            ]
            heads_by_run = {}
            for row in head_rows:
                heads_by_run.setdefault(str(row["problemframinghead__run_id"]), []).append(row)

        historical: list[EvidenceSnapshot] = []
        stopped_by_revision: dict[int, dict[str, object]] = {}
        for run_id, row in sorted(
            latest_runs.items(),
            key=lambda item: self._run_number(item[0]),
        ):
            if row["problemframingrun__status"] != "stopped" or run_id not in heads_by_run:
                continue
            run_revision = int(row["problemframingrun__evidence_revision"])
            stopped_by_revision[run_revision] = row
        for run_revision, row in sorted(stopped_by_revision.items()):
            historical_snapshot = self._snapshot_at_revision(
                evidence_with_revisions,
                run_revision,
            )
            if historical_snapshot.digest != str(row["problemframingrun__evidence_digest"]):
                raise ValueError(
                    f"run revision {run_revision} does not match reconstructed evidence"
                )
            historical.append(historical_snapshot)
        self._evaluated_snapshots = historical

        if head_rows:
            latest_head = max(
                head_rows,
                key=lambda row: self._run_number(str(row["problemframinghead__run_id"])),
            )
            head_prompt = str(latest_head["problemframinghead__prompt"])
            if _text_digest(head_prompt) != str(latest_head["problemframinghead__prompt_digest"]):
                raise ValueError("durable prompt head failed its digest")
            self._head_prompt = head_prompt
        else:
            self._head_prompt = str(latest_session["problemdefinitionsession__head_prompt"])
        expected_head_digest = str(latest_session["problemdefinitionsession__head_prompt_digest"])
        if _text_digest(self._head_prompt) != expected_head_digest:
            raise ValueError("reconstructed prompt head does not match the durable session")

    async def _recover_observed_run(
        self,
        run_row: dict[str, object],
        *,
        evidence_with_revisions: Sequence[tuple[int, EvidenceItem]],
        candidate_rows: Sequence[dict[str, object]],
        evaluation_rows: Sequence[dict[str, object]],
    ) -> None:
        """Complete an observation-committed run from exact durable receipts only."""

        run = self._run_component_from_row(run_row)
        if run.session_id != self.session_id or run.policy_digest != self.policy.digest:
            raise ValueError(f"run {run.run_id!r} has the wrong session or policy binding")
        frozen = self._snapshot_at_revision(evidence_with_revisions, run.evidence_revision)
        if frozen.digest != run.evidence_digest:
            raise ValueError(f"run {run.run_id!r} has the wrong evidence binding")

        scoped_candidates = [
            row
            for row in candidate_rows
            if str(row["problemframingcandidate__run_id"]) == run.run_id
        ]
        scoped_evaluations = [
            row
            for row in evaluation_rows
            if str(row["problemframingevaluation__run_id"]) == run.run_id
        ]
        if not scoped_candidates or not scoped_evaluations:
            raise ValueError(f"observed run {run.run_id!r} has no durable evaluation receipts")

        candidate_entities: dict[str, set[int]] = {}
        for row in scoped_candidates:
            candidate_entities.setdefault(
                str(row["problemframingcandidate__candidate_id"]), set()
            ).add(int(row["entity_id"]))
        evaluation_entities: dict[str, set[int]] = {}
        for row in scoped_evaluations:
            evaluation_entities.setdefault(
                str(row["problemframingevaluation__candidate_id"]), set()
            ).add(int(row["entity_id"]))
        if any(len(entity_ids) != 1 for entity_ids in candidate_entities.values()):
            raise ValueError(f"run {run.run_id!r} has duplicate durable candidates")
        if any(len(entity_ids) != 1 for entity_ids in evaluation_entities.values()):
            raise ValueError(f"run {run.run_id!r} has duplicate durable evaluations")
        if set(candidate_entities) != set(evaluation_entities):
            raise ValueError(f"run {run.run_id!r} has unmatched candidate/evaluation receipts")

        candidates = self._latest_rows(
            scoped_candidates,
            "problemframingcandidate__candidate_id",
        )
        evaluations = self._latest_rows(
            scoped_evaluations,
            "problemframingevaluation__candidate_id",
        )
        rehydrated: list[PanelEvaluation] = []
        for candidate_id, evaluation_row in evaluations.items():
            candidate_row = candidates[candidate_id]
            if int(candidate_row["entity_id"]) != int(evaluation_row["entity_id"]):
                raise ValueError(
                    f"run {run.run_id!r} candidate {candidate_id!r} is not co-located "
                    "with its evaluation"
                )
            prompt = str(candidate_row["problemframingcandidate__prompt"])
            if _text_digest(prompt) != str(candidate_row["problemframingcandidate__prompt_digest"]):
                raise ValueError(
                    f"run {run.run_id!r} candidate {candidate_id!r} failed its prompt digest"
                )
            evaluation_revision = int(evaluation_row["problemframingevaluation__evidence_revision"])
            evaluation_snapshot = self._snapshot_at_revision(
                evidence_with_revisions,
                evaluation_revision,
            )
            stored_evidence_ids = tuple(
                str(item)
                for item in json.loads(
                    str(evaluation_row["problemframingevaluation__evidence_ids_json"])
                )
            )
            evaluation = PanelEvaluation(
                candidate_prompt=prompt,
                evidence_revision=evaluation_revision,
                evidence_ids=stored_evidence_ids,
                evidence_digest=str(evaluation_row["problemframingevaluation__evidence_digest"]),
                policy=self.policy,
                observations=tuple(
                    PerspectiveObservation.model_validate(item)
                    for item in json.loads(
                        str(evaluation_row["problemframingevaluation__observations_json"])
                    )
                ),
                synthesis_protocol_id=str(
                    evaluation_row["problemframingevaluation__synthesis_protocol_id"]
                ),
                scoring_protocol_id=str(
                    evaluation_row["problemframingevaluation__scoring_protocol_id"]
                ),
                framing=ProblemFraming.model_validate_json(
                    str(evaluation_row["problemframingevaluation__framing_json"])
                ),
                votes=tuple(
                    RatificationVote.model_validate(item)
                    for item in json.loads(
                        str(evaluation_row["problemframingevaluation__votes_json"])
                    )
                ),
                scores=ScoreVector.model_validate_json(
                    str(evaluation_row["problemframingevaluation__scores_json"])
                ),
                feedback=tuple(
                    str(item)
                    for item in json.loads(
                        str(evaluation_row["problemframingevaluation__feedback_json"])
                    )
                ),
            )
            stored_binding = EvaluationBinding.model_validate_json(
                str(evaluation_row["problemframingevaluation__binding_json"])
            )
            receipt_matches = (
                evaluation.evidence_ids == evaluation_snapshot.evidence_ids,
                evaluation.evidence_digest == evaluation_snapshot.digest,
                evaluation.policy_digest
                == str(evaluation_row["problemframingevaluation__policy_digest"])
                == self.policy.digest,
                evaluation.evaluator_id
                == str(evaluation_row["problemframingevaluation__evaluator_id"]),
                evaluation.synthesis_protocol_id == self.policy.synthesis_protocol_id,
                evaluation.scoring_protocol_id == self.policy.scoring_protocol_id,
                evaluation.binding == stored_binding,
                evaluation.candidate_digest == candidate_id,
                evaluation.aggregate_score
                == float(evaluation_row["problemframingevaluation__aggregate_score"]),
                evaluation.unanimous is bool(evaluation_row["problemframingevaluation__unanimous"]),
                evaluation.hard_gate_passed
                is bool(evaluation_row["problemframingevaluation__hard_gate_passed"]),
            )
            if not all(receipt_matches):
                raise ValueError(
                    f"run {run.run_id!r} candidate {candidate_id!r} failed exact receipt validation"
                )
            rehydrated.append(evaluation)

        current_records = [
            evaluation
            for evaluation in rehydrated
            if evaluation.evidence_revision == frozen.revision
            and evaluation.evidence_digest == frozen.digest
        ]
        seed_records = [
            evaluation
            for evaluation in current_records
            if _text_digest(evaluation.candidate_prompt) == run.seed_prompt_digest
        ]
        if len(seed_records) != 1:
            raise ValueError(f"run {run.run_id!r} does not have exactly one current seed receipt")
        seed_evaluation = seed_records[0]
        try:
            config_payload = json.loads(run.config_json)
            if not isinstance(config_payload, dict):
                raise TypeError("GEPA config receipt must be an object")
            config = GepaPromptConfig(**config_payload)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"run {run.run_id!r} has an invalid GEPA config receipt") from exc
        selected = select_prompt_head(
            seed_evaluation,
            current_records,
            snapshot=frozen,
            policy=self.policy,
            improvement_threshold=config.improvement_threshold,
        )
        accepted = (
            selected is not None and selected.candidate_prompt != seed_evaluation.candidate_prompt
        )
        selected_prompt = (
            selected.candidate_prompt if selected is not None else seed_evaluation.candidate_prompt
        )
        selected_id = (
            selected.candidate_digest if selected is not None else seed_evaluation.candidate_digest
        )
        await self._commit_decision(
            run_entity_id=int(run_row["entity_id"]),
            run=run,
            frozen=frozen,
            selected_prompt=selected_prompt,
            head_evaluation=selected,
            accepted=accepted,
            parent_prompt_digest=str(
                candidates[selected_id]["problemframingcandidate__parent_prompt_digest"]
            ),
            session_snapshot=self.snapshot,
        )

    async def _commit_decision(
        self,
        *,
        run_entity_id: int,
        run: ProblemFramingRun,
        frozen: EvidenceSnapshot,
        selected_prompt: str,
        head_evaluation: PanelEvaluation | None,
        accepted: bool,
        parent_prompt_digest: str,
        session_snapshot: EvidenceSnapshot,
    ) -> tuple[PromptHead | None, int]:
        """Atomically append one head, update the session, and stop its observed run."""

        selected_digest = _text_digest(selected_prompt)
        if head_evaluation is None:
            status = "unresolved"
            aggregate_score = 0.0
            framing_json = "{}"
        else:
            if (
                head_evaluation.candidate_prompt != selected_prompt
                or head_evaluation.evidence_revision != frozen.revision
                or head_evaluation.evidence_digest != frozen.digest
                or head_evaluation.policy_digest != self.policy.digest
            ):
                raise ValueError("selected head evaluation has the wrong durable binding")
            status = "ratified" if accepted else "retained"
            aggregate_score = head_evaluation.aggregate_score
            framing_json = head_evaluation.framing.model_dump_json()

        await self.world.update(
            run_entity_id,
            run.model_copy(update={"status": "stopped", "error": ""}),
        )
        assert self._session_entity_id is not None
        await self.world.update(
            self._session_entity_id,
            self._session_component(
                snapshot=session_snapshot,
                head_prompt=selected_prompt,
            ),
        )
        head_entity_id = await self.world.spawn(
            ProblemFramingHead(
                session_id=self.session_id,
                run_id=run.run_id,
                prompt=selected_prompt,
                prompt_digest=selected_digest,
                parent_prompt_digest=parent_prompt_digest,
                evidence_revision=frozen.revision,
                evidence_digest=frozen.digest,
                policy_digest=self.policy.digest,
                evaluator_id=self.policy.evaluator_id,
                aggregate_score=aggregate_score,
                framing_json=framing_json,
                status=status,
            )
        )
        await self.world.run(steps=1)
        decision_tick = (await self.world.info()).tick - 1
        self._head_prompt = selected_prompt
        if all(prior.digest != frozen.digest for prior in self._evaluated_snapshots):
            self._evaluated_snapshots.append(frozen)
        head = (
            PromptHead(
                prompt=selected_prompt,
                evaluation=head_evaluation,
                entity_id=head_entity_id,
                tick=decision_tick,
            )
            if head_evaluation is not None
            else None
        )
        return head, decision_tick

    def _session_component(
        self,
        *,
        snapshot: EvidenceSnapshot | None = None,
        head_prompt: str | None = None,
    ) -> ProblemDefinitionSession:
        bound_snapshot = snapshot or self.snapshot
        bound_head = self._head_prompt if head_prompt is None else head_prompt
        return ProblemDefinitionSession(
            session_id=self.session_id,
            question=self.question,
            status="open",
            provider=self.provider,
            model=self.model,
            evidence_revision=bound_snapshot.revision,
            evidence_digest=bound_snapshot.digest,
            policy_json=self.policy.model_dump_json(),
            policy_digest=self.policy.digest,
            head_prompt=bound_head,
            head_prompt_digest=_text_digest(bound_head),
        )

    async def _ensure_started(self) -> None:
        if self._session_entity_id is not None:
            return
        session_entity_id = await self.world.spawn(self._session_component())
        await self.world.run(steps=1)
        self._session_entity_id = session_entity_id

    async def feed(self, item: EvidenceItem) -> EvidenceSnapshot:
        """Append evidence as a new immutable revision and persist the occurrence."""

        async with self._lock:
            if self._recovery_required:
                raise RuntimeError("mission requires a cold resume before more work")
            await self._ensure_started()
            next_snapshot = append_evidence(self.snapshot, item)
            if next_snapshot is self.snapshot:
                return self.snapshot

            await self.world.spawn(
                ProblemDefinitionEvidence(
                    session_id=self.session_id,
                    evidence_id=item.evidence_id,
                    revision=next_snapshot.revision,
                    source=item.source,
                    content=item.content,
                    content_digest=item.digest,
                )
            )
            assert self._session_entity_id is not None
            await self.world.update(
                self._session_entity_id,
                self._session_component(snapshot=next_snapshot),
            )
            await self.world.run(steps=1)
            self.snapshot = next_snapshot
            return self.snapshot

    async def refine(
        self,
        *,
        panel_evaluator: PanelEvaluator | None = None,
        reflection_lm: ReflectionLanguageModel | None = None,
        config: GepaPromptConfig | None = None,
    ) -> RefinementResult:
        """Search one frozen revision and append intent, observations, then decision."""

        async with self._lock:
            if self._recovery_required:
                raise RuntimeError("mission requires a cold resume before more work")
            await self._ensure_started()
            if not self.snapshot.items:
                raise ValueError("refine requires at least one evidence item")

            frozen = self.snapshot.model_copy(deep=True)
            seed = self._head_prompt
            seed_digest = _text_digest(seed)
            historical_snapshots = tuple(
                snapshot
                for snapshot in self._evaluated_snapshots
                if snapshot.digest != frozen.digest
            )
            search_config = config or GepaPromptConfig(
                max_metric_calls=6,
                max_candidate_proposals=3,
                patience=2,
                seed=7,
                improvement_threshold=0.1,
            )
            config_json = _json(asdict(search_config))
            historical_digests_json = _json([snapshot.digest for snapshot in historical_snapshots])
            try:
                optimizer_version = version("gepa")
            except PackageNotFoundError:
                optimizer_version = "unavailable"
            next_run_sequence = self._run_sequence + 1
            run_id = f"{self.session_id}:revision-{frozen.revision}:run-{next_run_sequence}"
            run_intent = ProblemFramingRun(
                run_id=run_id,
                session_id=self.session_id,
                evidence_revision=frozen.revision,
                evidence_digest=frozen.digest,
                policy_digest=self.policy.digest,
                seed_prompt_digest=seed_digest,
                optimizer_id=OPTIMIZER_ID,
                optimizer_version=optimizer_version,
                config_json=config_json,
                historical_evidence_digests_json=historical_digests_json,
                status="running",
            )

            # Boundary 1: durable search intent precedes every external/model call.
            run_entity_id = await self.world.spawn(run_intent)
            seed_candidate_id = _binding(seed, frozen, self.policy).candidate_digest
            seed_entity_id = await self.world.spawn(
                ProblemFramingCandidate(
                    run_id=run_id,
                    candidate_id=seed_candidate_id,
                    prompt=seed,
                    prompt_digest=seed_digest,
                    parent_prompt_digest="",
                    gepa_index=0,
                    gepa_parent_indices_json=_json([None]),
                )
            )
            await self.world.run(steps=1)
            self._run_sequence = next_run_sequence
            intent_tick = (await self.world.info()).tick - 1

            observation_committed = False
            try:
                if panel_evaluator is None or reflection_lm is None:
                    default_panel, default_reflection = self._default_agents()
                else:
                    default_panel = panel_evaluator
                    default_reflection = reflection_lm
                panel = panel_evaluator or default_panel
                reflection = reflection_lm or default_reflection
                search = await asyncio.to_thread(
                    optimize_problem_prompt,
                    seed,
                    frozen,
                    self.policy,
                    panel,
                    reflection,
                    config=search_config,
                    historical_snapshots=historical_snapshots,
                )
                # Boundary 2: persist immutable model/panel observations.
                candidates_by_index = {
                    candidate.index: candidate for candidate in search.gepa_best.candidates
                }
                candidate_by_prompt = {
                    candidate.prompt: candidate for candidate in search.gepa_best.candidates
                }
                parent_digest_by_prompt: dict[str, str] = {}
                for candidate in search.gepa_best.candidates:
                    parent_index = next(
                        (index for index in candidate.parent_indices if index is not None),
                        None,
                    )
                    parent = (
                        candidates_by_index.get(parent_index) if parent_index is not None else None
                    )
                    parent_digest_by_prompt.setdefault(
                        candidate.prompt,
                        _text_digest(parent.prompt) if parent is not None else "",
                    )
                unique: dict[str, PanelEvaluation] = {}
                for evaluation in search.records:
                    unique.setdefault(evaluation.candidate_digest, evaluation)
                for evaluation in unique.values():
                    evaluation_component = self._evaluation_component(run_id, evaluation)
                    if (
                        evaluation.candidate_prompt == seed
                        and evaluation.evidence_digest == frozen.digest
                    ):
                        diagnostic = candidate_by_prompt.get(evaluation.candidate_prompt)
                        await self.world.update(
                            seed_entity_id,
                            self._candidate_component(
                                run_id,
                                evaluation,
                                parent_digest_by_prompt,
                                diagnostic,
                            ),
                        )
                        await self.world.add_components(seed_entity_id, evaluation_component)
                        continue
                    diagnostic = candidate_by_prompt.get(evaluation.candidate_prompt)
                    await self.world.spawn(
                        self._candidate_component(
                            run_id,
                            evaluation,
                            parent_digest_by_prompt,
                            diagnostic,
                        ),
                        evaluation_component,
                    )
                observed_run = run_intent.model_copy(
                    update={
                        "optimizer_version": search.gepa_best.optimizer_version,
                        "status": "observed",
                    }
                )
                await self.world.update(run_entity_id, observed_run)
                await self.world.run(steps=1)
                observation_committed = True
                observation_tick = (await self.world.info()).tick - 1

                # Boundary 3: only the pure provenance-safe selection becomes head.
                head_evaluation = search.head_evaluation
                selected_prompt = search.head_prompt or seed
                head, decision_tick = await self._commit_decision(
                    run_entity_id=run_entity_id,
                    run=observed_run,
                    frozen=frozen,
                    selected_prompt=selected_prompt,
                    head_evaluation=head_evaluation,
                    accepted=search.accepted,
                    parent_prompt_digest=parent_digest_by_prompt.get(selected_prompt, ""),
                    session_snapshot=self.snapshot,
                )
                return RefinementResult(
                    snapshot=frozen,
                    head=head,
                    accepted=search.accepted,
                    run_id=run_id,
                    intent_tick=intent_tick,
                    observation_tick=observation_tick,
                    decision_tick=decision_tick,
                )
            except BaseException as exc:
                if observation_committed:
                    self._recovery_required = True
                else:
                    try:
                        await asyncio.shield(
                            self._settle_crashed_run(
                                run_entity_id,
                                run_intent,
                                exc,
                            )
                        )
                    except BaseException:
                        self._recovery_required = True
                raise

    async def _settle_crashed_run(
        self,
        run_entity_id: int,
        run: ProblemFramingRun,
        exc: BaseException,
    ) -> None:
        """Best-effort terminal settlement before propagating a post-intent failure."""

        await self.world.update(
            run_entity_id,
            run.model_copy(
                update={
                    "status": "crashed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            ),
        )
        await self.world.run(steps=1)

    @staticmethod
    def _candidate_component(
        run_id: str,
        evaluation: PanelEvaluation,
        parent_digest_by_prompt: dict[str, str],
        diagnostic: GepaCandidateDiagnostic | None,
    ) -> ProblemFramingCandidate:
        return ProblemFramingCandidate(
            run_id=run_id,
            candidate_id=evaluation.candidate_digest,
            prompt=evaluation.candidate_prompt,
            prompt_digest=_text_digest(evaluation.candidate_prompt),
            parent_prompt_digest=parent_digest_by_prompt.get(
                evaluation.candidate_prompt,
                "",
            ),
            gepa_index=diagnostic.index if diagnostic is not None else -1,
            gepa_parent_indices_json=_json(
                list(diagnostic.parent_indices) if diagnostic is not None else []
            ),
            gepa_aggregate_score=(diagnostic.aggregate_score if diagnostic is not None else 0.0),
            discovery_evaluation_count=(
                diagnostic.discovery_evaluation_count if diagnostic is not None else 0
            ),
        )

    @staticmethod
    def _evaluation_component(
        run_id: str,
        evaluation: PanelEvaluation,
    ) -> ProblemFramingEvaluation:
        return ProblemFramingEvaluation(
            run_id=run_id,
            candidate_id=evaluation.candidate_digest,
            evidence_revision=evaluation.evidence_revision,
            evidence_ids_json=_json(list(evaluation.evidence_ids)),
            evidence_digest=evaluation.evidence_digest,
            policy_digest=evaluation.policy_digest,
            evaluator_id=evaluation.evaluator_id,
            synthesis_protocol_id=evaluation.synthesis_protocol_id,
            scoring_protocol_id=evaluation.scoring_protocol_id,
            binding_json=evaluation.binding.model_dump_json(),
            aggregate_score=evaluation.aggregate_score,
            unanimous=evaluation.unanimous,
            hard_gate_passed=evaluation.hard_gate_passed,
            framing_json=evaluation.framing.model_dump_json(),
            observations_json=_json(
                [item.model_dump(mode="json") for item in evaluation.observations]
            ),
            votes_json=_json([item.model_dump(mode="json") for item in evaluation.votes]),
            scores_json=evaluation.scores.model_dump_json(),
            feedback_json=_json(list(evaluation.feedback)),
        )


async def run_demo(
    storage_uri: str = ".context/problem-definition",
    *,
    offline: bool = False,
    provider: str | None = None,
    model: str | None = None,
    question: str = QUESTION,
    seed_prompt: str | None = None,
    evidence_items: Sequence[EvidenceItem] | None = None,
    config: GepaPromptConfig | None = None,
) -> dict[str, object]:
    """Run one provider-backed hill climb and return its durable ledger projection."""

    storage = StorageConfig(uri=storage_uri, namespace="problem_definition_demo")
    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(
            runtime,
            storage=storage,
            question=question,
            seed_prompt=seed_prompt,
            offline=offline,
            provider=provider,
            model=model,
        )
        supplied_evidence = (
            (
                EvidenceItem(
                    evidence_id="interview-001",
                    source="founder interview",
                    content="Users abandon setup after they are asked to configure a policy.",
                ),
                EvidenceItem(
                    evidence_id="support-2026-07",
                    source="support summary",
                    content="Most setup tickets ask which policy is safe for a first run.",
                ),
            )
            if evidence_items is None
            else evidence_items
        )
        for item in supplied_evidence:
            await mission.feed(item)
        result = await mission.refine(config=config)

        intent_rows = (await mission.world.query(ProblemFramingRun)).to_pylist()
        observation_rows = (await mission.world.query(ProblemFramingEvaluation)).to_pylist()
        decision_rows = (await mission.world.query(ProblemFramingHead)).to_pylist()
        latest_decision = max(decision_rows, key=lambda row: int(row["tick"]))
        decision_status = str(latest_decision["problemframinghead__status"])
        if result.head is None or result.evaluation is None:
            best_observation = max(
                observation_rows,
                key=lambda row: (
                    float(row["problemframingevaluation__aggregate_score"]),
                    int(row["tick"]),
                ),
            )
            framing = ProblemFraming.model_validate_json(
                str(best_observation["problemframingevaluation__framing_json"])
            )
            votes = tuple(
                RatificationVote.model_validate(item)
                for item in json.loads(
                    str(best_observation["problemframingevaluation__votes_json"])
                )
            )
            evaluator_id = str(best_observation["problemframingevaluation__evaluator_id"])
            aggregate_score = float(best_observation["problemframingevaluation__aggregate_score"])
            unanimous = bool(best_observation["problemframingevaluation__unanimous"])
            hard_gate_passed = bool(best_observation["problemframingevaluation__hard_gate_passed"])
            head_prompt = mission.head_prompt
            improved = False
        else:
            evaluation = result.evaluation
            framing = evaluation.framing
            votes = evaluation.votes
            evaluator_id = evaluation.evaluator_id
            aggregate_score = evaluation.aggregate_score
            unanimous = evaluation.unanimous
            hard_gate_passed = evaluation.hard_gate_passed
            head_prompt = result.head.prompt
            improved = result.accepted
        return {
            "world_id": str(mission.world.world_id),
            "mode": "offline" if mission.offline else "live",
            "provider": mission.provider,
            "model": mission.model,
            "evaluator_id": evaluator_id,
            "evidence_count": len(result.snapshot.items),
            "evidence_digest": result.snapshot.digest,
            "snapshot_revision": result.snapshot.revision,
            "question": mission.question,
            "seed_prompt": seed_prompt or question,
            "head_prompt": head_prompt,
            "improved": improved,
            "decision_status": decision_status,
            "aggregate_score": aggregate_score,
            "unanimous": unanimous,
            "hard_gate_passed": hard_gate_passed,
            "counterexample_searches": tuple(
                f"{search.target_claim_id}:{search.outcome.value}"
                for search in framing.counterexample_searches
            ),
            "active_challenges": tuple(
                challenge.challenge_id for challenge in framing.challenges if challenge.active
            ),
            "perspectives": tuple(vote.perspective.value for vote in votes),
            "framing_statement": framing.statement,
            "next_question": framing.next_question,
            "intent_tick": min(int(row["tick"]) for row in intent_rows),
            "observation_tick": min(int(row["tick"]) for row in observation_rows),
            "decision_tick": min(int(row["tick"]) for row in decision_rows),
        }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Hill-climb an evidence-bound problem-framing prompt with three "
            "independent model perspectives."
        )
    )
    parser.add_argument(
        "--provider",
        choices=PROVIDERS,
        help="Model provider (default: openai; use codex for saved ChatGPT authentication).",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Backward-compatible alias for --provider offline.",
    )
    parser.add_argument(
        "--model",
        help=(
            "Provider model override. OpenAI defaults to "
            f"{DEFAULT_MODEL}; Codex defaults to {DEFAULT_CODEX_MODEL}."
        ),
    )
    parser.add_argument(
        "--question",
        default=QUESTION,
        help="The durable question this mission is trying to answer.",
    )
    parser.add_argument(
        "--seed-prompt",
        "--seed-framing",
        dest="seed_prompt",
        help="Initial framing prompt to hill-climb (default: the question).",
    )
    parser.add_argument(
        "--evidence-file",
        action="append",
        default=[],
        type=Path,
        metavar="PATH",
        help=(
            "UTF-8 evidence file to freeze into the snapshot; repeat for multiple files. "
            "Large files are preserved as deterministic chunks."
        ),
    )
    parser.add_argument(
        "--storage",
        default=".context/problem-definition",
        help="Durable local storage path.",
    )
    parser.add_argument(
        "--max-metric-calls",
        type=int,
        default=6,
        help="Maximum panel metric calls in this GEPA run (default: 6).",
    )
    parser.add_argument(
        "--max-candidate-proposals",
        type=int,
        default=3,
        help="Maximum GEPA prompt proposals (default: 3).",
    )
    parser.add_argument(
        "--patience",
        type=int,
        default=2,
        help="Stop after this many non-improving proposals (default: 2).",
    )
    parser.add_argument(
        "--gepa-seed",
        type=int,
        default=7,
        help="Deterministic GEPA search seed (default: 7).",
    )
    parser.add_argument(
        "--improvement-threshold",
        type=float,
        default=0.1,
        help="Minimum score gain required to replace the head (default: 0.1).",
    )
    args = parser.parse_args(argv)
    try:
        args.provider = resolve_provider(args.provider, offline=args.offline)
    except ValueError as exc:
        parser.error(str(exc))
    return args


async def main() -> None:
    args = _parse_args()
    if args.provider == "openai" and not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit(
            "The openai provider requires OPENAI_API_KEY. Use --provider codex "
            "for saved ChatGPT authentication or --offline for deterministic tests."
        )
    evidence_items = evidence_items_from_files(args.evidence_file) if args.evidence_file else None
    try:
        config = GepaPromptConfig(
            max_metric_calls=args.max_metric_calls,
            max_candidate_proposals=args.max_candidate_proposals,
            patience=args.patience,
            seed=args.gepa_seed,
            improvement_threshold=args.improvement_threshold,
        )
    except ValueError as exc:
        raise SystemExit(f"Invalid GEPA bounds: {exc}") from exc
    if args.provider != "offline":
        panel_call_budget = 9 * config.max_metric_calls
        print(
            f"Live {args.provider} mode: up to {panel_call_budget} panel model calls "
            f"(9 × {config.max_metric_calls} metric calls × 1 evidence snapshot), plus "
            f"up to {config.max_candidate_proposals} GEPA reflection calls."
        )
    result = await run_demo(
        args.storage,
        provider=args.provider,
        model=args.model,
        question=args.question,
        seed_prompt=args.seed_prompt,
        evidence_items=evidence_items,
        config=config,
    )
    print(result["question"])
    print(f"World ID: {result['world_id']}")
    print(f"Mode: {result['mode']} ({result['provider']} / {result['model']})")
    print(
        f"Evidence: {result['evidence_count']} items, "
        f"revision {result['snapshot_revision']}, digest {result['evidence_digest']}"
    )
    print(f"Decision: {result['decision_status']}")
    print(f"Head improved: {result['improved']}")
    print(f"Consensus votes 3/3: {result['unanimous']}")
    print(f"Hard gate passed: {result['hard_gate_passed']}")
    print("Counterexample searches: " + (", ".join(result["counterexample_searches"]) or "none"))
    print("Active counterexample challenges: " + (", ".join(result["active_challenges"]) or "none"))
    print(f"Problem framing: {result['framing_statement']}")
    print(f"Next question: {result['next_question']}")
    print(
        "Ledger ticks: "
        f"intent={result['intent_tick']} < "
        f"observation={result['observation_tick']} < "
        f"decision={result['decision_tick']}"
    )
    print(f"Selected prompt: {result['head_prompt']}")


if __name__ == "__main__":
    asyncio.run(main())

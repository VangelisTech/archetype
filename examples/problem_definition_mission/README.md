# Problem-Definition Agent Mission

This is an example-local agent mission built on Archetype as it already
exists. Nothing in this directory is imported by the `archetype` package.
The example defines its own domain Components, uses `ArchetypeRuntime` as a
durable ledger, and keeps model and GEPA orchestration outside the runtime.

The mission repeatedly asks:

> What problem are we solving?

It hill-climbs the reusable prompt used to frame that problem. It does not
claim that agreement proves truth.

## Loop

```text
attributable inputs
      |
      v
immutable EvidenceSnapshot
      |
      +--> naive agent --------+
      +--> expert agent -------+--> synthesis
      +--> orthogonal agent ---+       |
                                      v
                         counterexample review
                      (bounded search + verification)
                                      |
                                      v
                              three exact votes
                                      |
                                      v
                         hard gate + multi-axis score
                                      |
                                      v
                              bounded GEPA mutation
                                      |
                                      v
                          retain / replace prompt head
```

Each live panel evaluation makes nine stateless model calls: three isolated
perspectives, one synthesis, one counterexample review, three ratifications,
and one scorer. GEPA makes additional reflection calls when it proposes prompt
mutations.

Every applicable call receives the complete frozen evidence IDs, source
labels, contents, revision, and digest. Evidence and candidate text are marked
as untrusted data. A model-authored citation that is not one of the supplied
evidence IDs cannot pass the deterministic promotion gate.

The expert does not browse secretly during evaluation. Research acquisition
must happen first, and verified results must become `EvidenceItem` values
before the snapshot is frozen. That keeps every observation and vote bound to
the evidence it claims to have seen.

## Counterexample discipline

A prospective `falsifier` says what could disprove a claim. A
`ClaimChallenge` records a concrete, reproducible witness that may already do
so. They are deliberately different records.

Every retained observation, inference, hypothesis, or constraint must have an
exact, budgeted `CounterexampleSearchReceipt`. The possible search outcomes
are `FOUND`, `NOT_FOUND_WITHIN_BUDGET`, and `INCONCLUSIVE`. “Not found within
budget” is evidence about one search, never proof that no counterexample
exists. Missing or inconclusive coverage fails the promotion gate.

A challenge is bound to the proposition rather than mutable annotations such
as confidence, evidence order, or its prospective falsifier. It therefore
cannot be erased with a metadata-only edit. The synthesis must preserve the
challenge, materially revise or remove the proposition, or fail closed.

Generators and the synthesizer cannot author verification receipts. The
counterexample-review stage mints evidence-, policy-, protocol-, proposition-,
and verifier-bound receipts only after a configured verifier returns a
decision. Unverified, inconclusive, or confirmed challenges remain active and
block promotion. A challenge becomes inactive only when all of its immutable
verification receipts reject it; conflicting or downgraded receipts fail the
gate.

The default live builder uses a separate stateless verifier role and call over
the selected model adapter. That role separation is not external proof. For a
stronger boundary, pass a separately configured `verifier_model` and identity
to `build_model_backed_panel`, or inject a custom `PanelEvaluator` whose
verifier can replay an executable witness. Ratifiers and the scorer see the
post-verification framing, and every vote binds both that exact framing and the
role's exact prior observation.

## Run

GEPA is isolated in the repository's `problem-definition` dependency group.
The group is lockfile-audited and is also included in the development test
environment; it is not a runtime dependency of the published package.

Run through Codex using the ChatGPT authentication already saved by
`codex login` (no `OPENAI_API_KEY` is involved):

```bash
codex login status
uv run \
  --group problem-definition \
  python examples/problem_definition_autoresearch.py \
  --provider codex \
  --model gpt-5.6-sol \
  --question "What problem are JP and Everett solving?" \
  --evidence-file .context/attachments/TRANSCRIPT.txt \
  --max-metric-calls 2 \
  --max-candidate-proposals 1 \
  --patience 1
```

`--evidence-file` may be repeated. A file larger than one `EvidenceItem` is
preserved as stable, digest-addressed chunks; it is never silently truncated.
The question is durable session state and is also the default prompt seed.
Use `--seed-prompt` (also spelled `--seed-framing`) when the initial instruction
should differ from the question.

The Codex adapter runs isolated `codex exec` calls and requests schema-bound
JSON for the structured panel roles. Codex has no single no-tools switch, so
the adapter explicitly disables its current tool-bearing features and web
search under strict configuration, uses an empty temporary root and read-only
sandbox, ignores user configuration and rules, and rejects any successful
JSONL trace containing a tool event. Omit `--model` to use the mission's pinned
`gpt-5.6-sol` default, or override it explicitly; the exact model is recorded
in the durable evaluator identity.

GEPA is bounded by `--max-metric-calls`, `--max-candidate-proposals`,
`--patience`, `--gepa-seed`, and `--improvement-threshold`. The transcript
example above intentionally starts with a small live budget; every panel
metric call invokes all nine roles. Continuations also evaluate historical
evidence revisions, so their upper-bound warning multiplies the metric budget
by the number of current and historical snapshots before any live call.

The OpenAI Responses API is also supported:

```bash
export OPENAI_API_KEY=...
uv run \
  --group problem-definition \
  python examples/problem_definition_autoresearch.py \
  --provider openai
```

The OpenAI provider defaults to `gpt-5.6-terra`. Its adapter uses the Responses
API with Pydantic structured outputs, `store=False`, bounded contexts, and no
silent evidence truncation.

Choose another model:

```bash
uv run \
  --group problem-definition \
  python examples/problem_definition_autoresearch.py \
  --provider openai \
  --model gpt-5.6-sol
```

Credential-free deterministic doubles are available only when explicitly
requested:

```bash
uv run \
  --group problem-definition \
  python examples/problem_definition_autoresearch.py --provider offline
```

The historical `--offline` flag remains an alias for `--provider offline`.

## Feed, refine, resume

The Python surface is intentionally example-owned:

```python
import asyncio

from archetype import ArchetypeRuntime, StorageConfig
from examples.problem_definition_mission import EvidenceItem
from examples.problem_definition_mission.mission import ProblemDefinitionMission

async def main() -> None:
    storage = StorageConfig(
        uri=".context/problem-definition",
        namespace="my_problem",
    )

    async with ArchetypeRuntime() as runtime:
        mission = ProblemDefinitionMission(runtime, storage=storage, provider="codex")
        await mission.feed(
            EvidenceItem(
                evidence_id="interview-001",
                source="customer interview 001",
                content="The participant stopped at the policy-selection step.",
            )
        )
        result = await mission.refine()
        world_id = str(mission.world.world_id)

    # A later process can reconstruct the exact provider, policy, snapshot,
    # and prompt head before appending new evidence.
    async with ArchetypeRuntime() as runtime:
        mission = await ProblemDefinitionMission.resume(
            runtime,
            world_id,
            storage=storage,
        )
        await mission.feed(
            EvidenceItem(
                evidence_id="support-042",
                source="support ticket 042",
                content="The user asked which first-run policy was safe.",
            )
        )
        result = await mission.refine()

asyncio.run(main())
```

The initial script prints its world ID. The same continuation is available as
a second script:

```bash
uv run \
  --group problem-definition \
  python examples/problem_definition_continue.py WORLD_ID \
  --storage .context/problem-definition \
  --provider codex \
  --evidence-id support-042 \
  --source "support ticket 042" \
  --content "The user asked which first-run policy was safe." \
  --max-metric-calls 2 \
  --max-candidate-proposals 1 \
  --patience 1
```

Continuation also accepts one or more `--evidence-file` arguments instead of
the explicit ID/source/content triplet.

The ledger records three causal boundaries on separate world ticks:

1. run intent before any external model call;
2. candidate observations, synthesis, votes, and scores; and
3. the selected, retained, or unresolved prompt head.

Crashes settle the already-persisted run as `crashed`. New evidence creates a
new immutable revision; old votes cannot ratify it.

A completed panel that lacks unanimity or fails a grounding hard gate is a
normal `unresolved` decision, not a process crash. The CLI prints the best
provisional framing, vote consensus, and hard-gate result while retaining every
candidate and dissenting receipt in the world. It also prints the bounded
counterexample-search outcomes and every active challenge ID.

If a process stops after intent, cold resume marks that run `crashed`. If it
stops after the observation boundary, resume validates the durable receipts and
finishes the pure decision without repeating model calls. Corrupt or ambiguous
receipts fail closed.

## Verify

```bash
uv run \
  --group problem-definition \
  pytest -q tests/examples/problem_definition

git diff --exit-code HEAD -- \
  packages/*/src/archetype mkdocs.yml docs/guide
```

The second command is the implementation-boundary acceptance test: this
experiment must not add changes to shipped Archetype code or normative guides.
Its GEPA test dependency and source operational scenario remain repository
harness concerns.

## Boundaries

- Consensus means the configured three roles approved one exact framing under
  one evidence snapshot and policy. It is not proof of truth.
- A verifier receipt is only as trustworthy as its configured verifier and
  reproduction protocol. The default model role is not a formal proof system.
- The current example does not automatically execute `next_question` or fetch
  the web.
- Model calls have real latency and cost. Start with small GEPA budgets.
- Evidence disclosure, admissibility, the orthogonal lens, and the final
  handoff to a solving agent remain user-owned choices.

References:

- [Counterexample-guided inductive synthesis](https://people.csail.mit.edu/asolar/papers/Solar-LezamaJB08.pdf)
- [Counterexample-guided abstraction refinement](https://www.cs.cmu.edu/~emc/papers/Conference%20Papers/Counterexample-guided%20Abstraction%20Refinement.pdf)
- [Counterexample-guided inductive definitions](https://arxiv.org/abs/1409.0393)
- [OpenAI model guidance](https://developers.openai.com/api/docs/guides/latest-model)
- [OpenAI structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs)
- [GEPA](https://github.com/gepa-ai/gepa)

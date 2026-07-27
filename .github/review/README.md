# Deterministic review contracts

This directory is the human entry point for reviewing model policy. Prompt
prose is deliberately separate from workflow YAML and backend interpolation.

| Stage | Prompt | Model return type | JSON Schema | Normalizer |
|---|---|---|---|---|
| Independent lens review | [`prompts/lens-review.md`](prompts/lens-review.md) | `FootgunLensResult` or `DesignLensResult` | `lens_result_schema()` | `normalize_lens_result()` |
| Bounded lens correction | [`prompts/lens-retry.md`](prompts/lens-retry.md) | same as the first attempt | `lens_result_schema()` | `normalize_lens_result()` |
| Selective falsification | [`prompts/adjudication.md`](prompts/adjudication.md) | `AdjudicationResult` | `adjudication_result_schema()` | `normalize_adjudication_result()` |
| Human design review | [`prompts/design-brief.md`](prompts/design-brief.md) | `HumanDesignBrief` | `human_design_brief_schema()` | `normalize_human_design_brief()` |
| Bounded design correction | [`prompts/design-brief-retry.md`](prompts/design-brief-retry.md) | same as the first attempt | `human_design_brief_schema()` | `normalize_human_design_brief()` |

The named Python types, schemas, lens/category assignments, and reviewer matrix
are adjacent in `scripts/review_contracts.py`. The model-authored schemas omit
reviewer identity, backend/model identity, assigned-category coverage, and the
changed-file manifest. `scripts/review_aggregation.py` stamps that trusted
provenance into digest-bound receipts.

Each lens result also declares `review_status` as `complete` or `blocked`.
Schema validity is not review evidence by itself: only `complete` can be
normalized into a reviewer receipt. A reviewer MUST use `blocked` when a tool,
sandbox, permission, or other admission failure prevents any required
repository inspection. The validator rejects that result as a verdict, gives
the same seat its one bounded retry, and then records an infrastructure-failure
receipt if inspection remains blocked. That receipt is neutral; if every seat
for a lens is neutral, aggregation fails closed.

Aggregation uses one exact key:

```text
(lens, category, path, side, line)
```

Every original finding remains in its reviewer receipt and is referenced by
the cluster. Distinct reviewers at the same key corroborate; repeated findings
from one reviewer do not. Missing evidence is not disagreement. Singleton
blocking claims and severity conflicts receive targeted adjudication.
Refutation and uncertainty become human decisions rather than deleting the
original claim.

`design-coherence` is a separate advisory-only lens. Its rulebook is
`.claude/skills/design-coherence/SKILL.md`; its findings can inform the human
brief but can never become a blocking gate disposition.

The human-brief prompt has a repo-owned 900,000-character budget beneath the
provider limit. It always keeps the finalized bundle, exact scope, and exact
diff. It first attempts the complete protected-base guidance set; if that does
not fit, it keeps whole mandatory policy files and architecture fragments,
including the umbrella specification, then whole changed guidance files in
canonical order while space remains. A complete path, digest, character-count,
and inclusion manifest records every guidance source. Nothing is truncated
silently. If the authoritative inputs and mandatory guidance cannot fit,
prompt materialization fails before calling the provider and the change must
be split or the reviewed contract redesigned.
If the first structured brief fails deterministic normalization, the workflow
permits one schema-bound correction using the rejected result and exact
validation feedback. The original prompt remains authoritative; a second
invalid result fails the gate. Only a successfully extracted JSON object that
fails semantic normalization is eligible for correction; missing or malformed
provider output is an infrastructure failure and fails directly.

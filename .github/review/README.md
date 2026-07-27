# Deterministic review contracts

This directory is the human entry point for reviewing model policy. Prompt
prose is deliberately separate from workflow YAML and backend interpolation.

| Stage | Prompt | Model return type | JSON Schema | Normalizer |
|---|---|---|---|---|
| Independent lens review | [`prompts/lens-review.md`](prompts/lens-review.md) | `FootgunLensResult` or `DesignLensResult` | `lens_result_schema()` | `normalize_lens_result()` |
| Bounded lens correction | [`prompts/lens-retry.md`](prompts/lens-retry.md) | same as the first attempt | `lens_result_schema()` | `normalize_lens_result()` |
| Selective falsification | [`prompts/adjudication.md`](prompts/adjudication.md) | `AdjudicationResult` | `adjudication_result_schema()` | `normalize_adjudication_result()` |
| Human design review | [`prompts/design-brief.md`](prompts/design-brief.md) | `HumanDesignBrief` | `human_design_brief_schema()` | `normalize_human_design_brief()` |

The named Python types, schemas, lens/category assignments, and reviewer matrix
are adjacent in `scripts/review_contracts.py`. The model-authored schemas omit
reviewer identity, backend/model identity, assigned-category coverage, and the
changed-file manifest. `scripts/review_aggregation.py` stamps that trusted
provenance into digest-bound receipts.

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

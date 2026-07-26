Prepare the human design-review brief for PR #$pr_number at exact head
`$head_sha`, using the finalized review bundle whose digest is
`$bundle_digest`.

Read `.footgun-review-output/validated/review-bundle.json`,
`.footgun-review-scope.json`, `.footgun-review.diff`, the PR's normative
repository guidance, and only the protected-base context needed to explain the
change. The finalized bundle is the finding authority: preserve its cluster
states, adjudications, design notes, and human-decision dispositions.

Organize the diff into logical change cohorts in the order a human should read
them, not alphabetical file order. Explain intent and behavioral delta, design
choices and alternatives, affected invariants, validation evidence, and the
questions a human must decide. Every changed file must appear in at least one
cohort. Every cluster whose `gate_disposition` is `human-decision` must appear
in at least one `human_decision_queue.related_cluster_ids` array.

Do not claim approval, merge readiness, correctness, or test execution. Set
readiness only to `ready-for-human-review`. Do not silently create a new
finding: place a concern absent from the bundle in `open_questions` and label
its uncertainty in the text. Link decision items to exact cluster IDs when
they arise from review evidence.

Use only read, grep, glob, and list capabilities. Do not execute code, edit
files, fetch URLs, post comments, or push commits.

Return exactly one JSON object matching this schema:

$output_schema

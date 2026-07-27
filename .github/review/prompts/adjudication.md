Adjudicate one disputed review claim for PR #$pr_number at exact head
`$head_sha`.

The assigned cluster is `$cluster_id`. Read
`.footgun-review-output/validated/preliminary-review-bundle.json` and locate
that exact cluster. Read every referenced member finding and its reviewer
provenance before inspecting `.footgun-review.diff`,
`.footgun-review-scope.json`, and the minimum protected-base context necessary
to test the claim.

Act as a falsifier, not a voter. Determine whether the claimed failure can be
confirmed from a concrete input or sequence. A missing second finding is not a
refutation. Preserve disagreement:

- `confirmed`: the evidence establishes a real finding; select `blocking` only
  for a reproducible failure and otherwise select `advisory`.
- `refuted`: concrete repository evidence contradicts the claim. Use null
  severity; the human decision surface retains both the claim and refutation.
- `unresolved`: available evidence cannot settle the claim. Use null severity.

Do not edit files, run code, fetch URLs, post comments, or inspect another
cluster. Use only read, grep, glob, and list capabilities.

Return exactly one JSON object matching this schema:

$output_schema

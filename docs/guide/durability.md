# Durability and retry

Native completion does not establish Iceberg visibility. Observe an admission's
`frozen` state, retain its `Boundary`, publish the complete cut and confirm that
exact cut. Keep the admission key, generation, boundary and cut receipt until
settlement. On uncertain publication use `world.reconcile(boundary)`. Inspect and
confirm the exact returned result; do not submit the inputs again.

Every cut includes all declared components, even empty outputs. Reads select a
cut before tables. Retractions therefore stay absent in a later empty cut.
`world.resume(cut, expected_generation=...)` verifies the bound native checkpoint
and publication identity. Analytical parent cut IDs and native receipt digests
are distinct identities and both remain verified internally.

`RuntimeOperationError.code` is bounded. Its `outcome` is `not_dispatched` for
local rejection or `unknown` after dispatch when completion is uncertain. Neither
message wording nor cancellation proves rollback. Stop and close drain through
the native owner and preserve unresolved publication evidence.

Distributed fencing, generic administrative uncertainty resolution and garbage
collection are outside this contract. Transport envelope limits do not bound
native execution time or analytical scan memory.

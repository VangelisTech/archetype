# Modal Mission author Activity proof — 2026-07-26

**Status:** A4 passed against the supported runtime path and real Modal
sandboxes.

## Result

Archetype source `ae4eae3d` ran one disposable coding-agent Mission through the
Activity-backed Modal author path. The public fixture was
[`everettVT/archetype-modal-proof-20260726-a4-1`](https://github.com/everettVT/archetype-modal-proof-20260726-a4-1).

The base branch remained at
`9a94713a93e3f4f49cb5129259faa87dc1a11296` with `proof.txt` equal to
`unproved`. The author changed only that file, both exact-revision validators
passed, and the proof branch published
[`67ed4f0c`](https://github.com/everettVT/archetype-modal-proof-20260726-a4-1/commit/67ed4f0cbc20e1e024de8ad049f804e458fa4ea6)
with `proof.txt` equal to `modal activity proved`.

## Durable ordering

| Evidence | Value |
|---|---|
| World | `019f9fc7-a7da-71d0-82b1-fe6a3f48390b` |
| Run | `019f9fc7-a7da-71d0-82b1-fe7be9782c96` |
| Dispatch source tick | 1 |
| Activity observation/settlement tick | 2 |
| Final world head | 7 |
| Activity ID | `70a37d8f972f2494837f9dba8364cbb418b203558cbe0166f69ae925bb544f2d` |
| Activity result digest | `695a5e337e8b83d0c65df453265801e7559a0c238973138e1d76a7054c408eb9` |
| Complete fact-bundle digest | `74eeeeb68bd74de069fd4d25aa4f49081f48e90bd1f503a3add0a226df259568` |
| Attempt / fence | 1 / 1 |

The Activity catalog retained the immutable tick-1 admission, exact provider
operation, bounded result reference, and tick-2 settlement. The tick-2
`CompleteAuthorActivityObservation` bound one sandbox, one author execution,
two validations, one commit, one candidate, eight provenance relations, and
zero friction facts to the same result digest. The task became `accepted` only
after a separate critic approved the exact base-to-head diff.

## Provider evidence

| Evidence | Value |
|---|---|
| Modal workspace / Environment | `vangelis-tech` / `main` |
| Modal App | `ap-13Icl2bVNWkY4672goRJfg` (`archetype-agent-missions-a4-proof`) |
| Author sandbox | `sb-BBHOAIlQCNBeqIPLXXN7c8` |
| Critic sandbox | `sb-YucMchzdILbWDmXMe6S9MN` |
| Provider operation | `missions.author:38326552c51ddeaee066345dac3c2bcd00d897f874b4271ce70906ef9f6c80a9` |
| First-result Dict | `arc-author-results-v1-askxhkteilmybcgk55f7hj2uscamhehz` |
| Candidate digest | `8963804e3450f0452e82248a95079836b55cdf4c3fdc9c190a006df9872281a3` |
| Diff digest | `79ddc92ae2d8dec5ed220217a1e36aaf73d1c9383da620c1d1374af462ae6b21` |
| Validator bundle digest | `06f1d7a490ea9b1473068ca9a2e4e099105dfc09b3ec9ad672fc3027463e8efa` |

The author and critic sandbox identities differ. The critic receipt reviewed
the exact candidate head, diff digest, validator bundle digest, and policy
digest and concluded `approved` with zero findings.

## Cold-process recovery

After the Mission and its runtime closed, a separate Python process
reconstructed the Modal adapter from the durable Activity request and provider
operation identity. `reconcile()` read the exact first-result Dict entry and
returned the original author sandbox, final revision, and result digest. It did
not call the author harness or start another sandbox.

The branch was then built as a wheel and installed into an empty virtual
environment. From `/tmp`, with no source checkout on the import path, that
installed artifact reconstructed the same request and adapter and recovered
the same provider operation, result digest, and final Git revision.

## Fault found by the proof

The first attempt stopped before provider execution because the receipt-pinned
Mission reader returned append-only history rather than the state at the
receipt's latest tick. Validators and relations therefore appeared twice at
tick 1. A3 now applies the lazy `latest` projection before materialization and
has a two-tick regression. The failed attempt left the committed tick pending,
failed shutdown closed, and created no Git proof branch or provider result.

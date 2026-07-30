# Retired review gate

This directory preserves the deterministic AI review and merge-queue
orchestration retired during the 2026-07-30 SEV-1 response. Files here are
historical evidence only: GitHub does not execute workflows outside
`.github/workflows/`, and pytest does not collect these tests.

Do not restore this system by moving files back into active paths. Any future
review automation requires a new design with an explicit cost budget, an
adaptive scope policy, measured false-positive and latency targets, and a
manual-review fallback that cannot block repository recovery.

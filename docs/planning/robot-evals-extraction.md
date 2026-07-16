# robot-evals extraction runbook

Target repo (created 2026-07-16, private — deliberately not hyperlinked: the
link checker 404s on private repos): `everettVT/robot-evals` on GitHub.

The LIBERO/VLA-JEPA harness leaves archetype; the machinery it forced into
existence stays. Rule: **anything that imports robosuite/LIBERO/VLA/Modal or
needs a GPU moves; anything any benchmark needs stays.**

## Already done (Phase 1 — graduate, this PR)

- `bench/libero/eval_run.py` → `archetype.experiments.eval_rollouts`
- `bench/libero/instruction_sweep.py` → `archetype.experiments.instruction_sweep`
- their tests → `tests/experiments/`
- `EnvClient`/`PolicyClient` protocols, processors, Manip components were
  already in `src/archetype/experiments/`.

## Phase 2 — extract (run AFTER PR #303 merges to main)

What moves (with history, via `git filter-repo`):

- `bench/libero/` (image.py, in_process.py, in_process_policy.py, clients.py,
  pro_suite.py, upstream_probe.py)
- `tests/bench/test_in_process_frames.py`, `test_vla_jepa_client.py`,
  `test_pro_suite.py` (+ any policy/env tests the direct-model refactor added)
- `docs/guide/libero-recipe.md`, `docs/planning/eval-libero-roadmap.md`,
  `docs/planning/paper-readiness-dod.md`, this file

Runbook:

```bash
git clone https://github.com/VangelisTech/archetype /tmp/archetype-extract
cd /tmp/archetype-extract
git filter-repo \
  --path bench/libero --path tests/bench/test_in_process_frames.py \
  --path tests/bench/test_vla_jepa_client.py --path tests/bench/test_pro_suite.py \
  --path docs/guide/libero-recipe.md --path docs/planning/eval-libero-roadmap.md \
  --path docs/planning/paper-readiness-dod.md \
  --path-rename bench/libero:src/robot_evals \
  --path-rename tests/bench:tests --path-rename docs/guide:docs \
  --path-rename docs/planning:docs/planning
git remote add target https://github.com/everettVT/robot-evals
git push target HEAD:main
```

Then in robot-evals: `pyproject.toml` depending on **`archetype-ecs` from
PyPI** (the keystone — dogfood through the front door; every gap becomes an
archetype issue), `modal`/`daft` deps, import rewrites
(`bench.libero.*` → `robot_evals.*`), CI = the credential-free unit tests
(numerics, loader, frame-path, language-trap); GPU runs stay manual
`modal run`. Secrets pattern carries over unchanged: repo `.env` with
`HF_TOKEN` + `LOGFIRE_TOKEN`, launcher pass-through via `Secret.from_dict`.

Back in archetype: delete the moved paths, leave a tombstone in
`docs/guide/libero-recipe.md`'s slot pointing at robot-evals, keep the
LEARNINGS entries (they are archetype lessons), keep `bench/mujoco`
(substrate perf guard, not a robot benchmark).

## Sequencing

1. #303 merges (Everett).
2. Direct-model verification run stamps the RUN LEDGER (see PR threads).
3. Phase 2 extraction above.
4. #289 GEPA sweeps run **in robot-evals** against PyPI archetype — receipts
   born in the citable repo.

Coordination note: a parallel session rewrote the policy to direct in-process
PyTorch calls mid-branch (landed in commits 219cfdf/3e04fb6); the extraction
must take the tree as it stands post-merge, not this runbook's snapshot of
file contents.

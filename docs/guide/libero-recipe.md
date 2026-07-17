# The LIBERO Recipe → moved to robot-evals

The LIBERO/VLA-JEPA benchmark harness — the blessed image recipe, the
in-process env + policy clients, LIBERO-Pro suites, GPU entrypoints, RUN
LEDGERs, and the receipts (libero_spatial 99/100; LIBERO-Pro baselines) —
was extracted on 2026-07-16 to its own repository, `everettVT/robot-evals`
(history preserved), which consumes archetype as a package dependency.

What stayed here, because every benchmark needs it:

- `archetype.experiments.eval_rollouts` — batched control-plane task eval
  (one world, N trial entities, ledger-graded)
- `archetype.experiments.instruction_sweep` — instruction-variant sweeps +
  the greedy optimize baseline
- the `EnvClient` / `PolicyClient` protocols, processors, and Manip
  components in `archetype.experiments`
- the GL-thread-affinity rule in `LEARNINGS.md` (renderers driven from
  processors marshal every call onto one thread per context)

Extraction record: `docs/planning/robot-evals-extraction.md`.

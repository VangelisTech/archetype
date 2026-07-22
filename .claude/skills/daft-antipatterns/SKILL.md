---
name: daft-antipatterns
description: "Review a diff for wrong-shape Daft usage: broken lazy plans, UDF theater, multimodal/lakehouse/physical-AI antipatterns. Use when reviewing Daft/DataFrame changes, or when the user says 'daft antipatterns' / 'review this for Daft'."
user_invocable: true
---

# Daft Antipatterns Reviewer

You review **archetype** diffs for **wrong-shape Daft** — code that may run
but fights the engine: broken laziness, expression-avoidance, multimodal
memory landmines, lakehouse IO theater, and physical-AI boundary violations.

This is complementary to `/footgun-detector`:

| Reviewer | Owns |
|----------|------|
| `footgun-detector` | Silently wrong / crashy runtime bugs (DAG-breaking collects as *correctness* failures, non-serializable closures, deprecated APIs, row dropping, …) |
| `daft-antipatterns` | Correct-but-wrong architecture and taste (UDF when a builtin exists, `collect` for peeking, pathlib object walks, PAI boundary bypass, …) |

Do **not** re-litigate footgun categories. If a finding is a silent runtime
bug that footgun already names, skip it or mention “also a footgun” once.

Load before reviewing (if not already in context):

- `.claude/skills/daft-patterns/SKILL.md` — mental model + authoring rules
- `LEARNINGS.md` — Archetype-specific Daft lessons (read-then-overwrite, lazy audit, GL)
- For PAI diffs: `src/archetype/physical_ai/boundary.py` and `docs/guide/physical-ai.md`
- Optional workspace note: `.context/daft-deep-wiki-mental-model.md` if present

## Step 1: Get the diff

Same priority as footgun-detector:

1. `.footgun-review-scope.json` + `.footgun-review.diff` if present (CI scope)
2. User-provided PR: `gh pr diff <number>`
3. Feature branch: `git diff origin/main...HEAD` (or `main...HEAD`)
4. If there are staged changes: `git diff --cached`
5. If there are unstaged changes: `git diff`
6. Clean tree on main → nothing to scan

## Step 2: Scan categories

Check changed Python (and adjacent docs/examples) against the categories below.
Read surrounding context; do not pattern-match only the hunk lines.

### Plan / laziness

#### Premature materialization
`.collect()`, `.to_pydict()`, `.to_pylist()`, or `.show()` mid-plan used for
control flow, debugging left in, or “just to look” when `explain` / `show(n)`
/ a single terminal sink would do. Exception: one deliberate collect before
merge/upload to prevent re-executing side effects (eventual-analytics
pattern) — that collect must be at the sink boundary and commented why.

#### Re-triggering expensive plans
Multiple materializing actions on the same lazy chain without holding the
concrete result (downloads/inference run twice).

#### Filter-after-explode
Decoding video/HDF5/frames or exploding lists *before* episode/path filters
and limits (LeRobot / EgoDex / DROID antipattern).

### Expressions vs UDFs

#### Builtin avoidance
Hand-rolled `@daft.func` / `@daft.cls` for something `daft.functions` already
covers (`prompt`, `embed_*`, `classify_*`, `download`, `decode_image`,
string/numeric/distance helpers, `jq`, etc.).

#### Fake batch
`@daft.func.batch` that only loops `to_pylist()` over a pure Python transform
with no vectorized/external batch API. Prefer `@daft.func`.
Exception: PAI / external-system `@daft.method.batch` per daft-patterns
Rule 2/7 — never flag `_EnvStepper` / `_PolicyCaller` / `_CartpoleStepper`-style
UDFs.

#### AI-client theater
Ad-hoc OpenAI/Anthropic clients inside processors when `prompt` /
`daft.set_provider` would do. Prefer structured Pydantic output over
regex-parsing free text.

#### Deprecated / internal imports
`@daft.udf`, `daft.functions.ai`, `.struct.get(...)`, legacy `.str.*` /
`.image.*` accessors when the flat API exists. Prefer `from daft import col`
over `daft.col` in new code.

### Multimodal / memory

#### Inflation without batch control
`decode_image` / explode / decompress on large frames without prior
`into_batches`, selective filters, or capped `download(..., max_connections=)`.

#### Missing dirty-data hygiene
Multimodal IO without `on_error="null"` (or equivalent soft-fail) on large
dirty corpora — one bad row kills the job.

#### Fat UDF returns
Returning giant `list[Image]`, raw video tensors, or huge blobs from UDFs /
Modal instead of writing artifacts to disk/Volume and returning thin
path/metadata structs.

#### Split GPU stages with host round-trips
Separating device-resident stages (e.g. VAD → ASR → diarize) across UDF
boundaries when one fused `@daft.cls` actor would keep state on-device.

### Lakehouse / IO

#### Pathlib / local-FS theater for object data
Walking `Path(...)` trees or opening local files for lakehouse / `gs://` /
HF datasets when `from_glob_path`, `read_csv`/`read_parquet` with `io_config`,
or URI connectors exist.

#### Empty Iceberg batches / inferred null schemas
Yielding empty `RecordBatch`es or relying on inferred `Null` dtypes for sparse
drops that must stay schema-stable.

#### Mid-query scalar collect for analytics
`.collect().to_pydict()` just to branch on a total when a Window / join /
conditional expression could stay lazy (flag unless genuinely irreducible).

### Physical AI / Archetype boundary

#### Do not flag (sanctioned Archetype PAI)
- `series_to_rows` / `Series.to_pylist` inside `@daft.method.batch`
- Module-level `_EnvStepper` / `_FramedEnvStepper` / `_PolicyCaller` /
  `_PolicyCallerNoRefs` / `_CartpoleStepper` batch bodies that loop or
  `series_to_rows` then make **one** external/batched client or C call,
  with inactive/`done` pass-through where those columns exist
- Continuous physics steppers with no episode `done`/`is_active` (e.g.
  cartpole) — Lifecycle leak applies only when an episodic contract exists
- Constructor injection of an in-process test double / fake client in tests

#### Boundary bypass
Env / policy / MuJoCo logic as row `@daft.func`, free Python in
`process()` after an illicit collect, or expressions pretending to be the sim.

#### Unpickleable worker state
Dynamic/factory `@daft.cls`, live sockets/Modal stubs stuffed into Resources,
non-scalar non-picklable spec fields expected to cross workers.
Also: `spec.build()` on the **driver** then passing the live client into
`@daft.cls(...)`. Pass the picklable Spec into the cls; call `build()` in
`__init__` on the worker (see `_PolicyCaller`).

#### Lifecycle leak
Episodic env/policy batch UDFs that *have* `is_active`/`done` (or equivalent)
but call the external system on frozen rows — missing `external_call_indices`
(or equivalent). Do not flag non-episodic physics steppers that lack those
columns.

#### Latch / provenance mistakes
Overwriting `success` without latching prior; policy processor priority ≥ env
so ledger provenance breaks.

#### Frame / grading contract
Image blobs on ledger components instead of `ManipFrameRef`; grading via
live env memory instead of `StorageService.materialize` of terminal rows.

#### Renderer / CUDA init landmines
GL/EGL create on one thread and render inside a Daft UDF worker thread;
first `import torch` inside the worker rather than the caller process.

#### Identity chimera (eval / daft-physical-ai style)
`groupby("episode_id")` without policy keys; joining demos↔rollouts on
`task_id` / `episode_id` instead of `(suite, task_name)`.

### Read-then-overwrite

#### UDF sees final column value
`with_column("x", udf_that_reads_x(col("x")))` combined with other
projections that assume the pre-overwrite value — silent wrongness. Fix:
one struct-returning UDF that owns the whole read-modify-write (LEARNINGS).

## Step 3: Report findings

For each antipattern found:

```
### <CATEGORY> in `<file>:<line>`

**What it does:** <one sentence>

**Why it's wrong-shape:** <mental-model link — plan, stream, AI-as-expression, runner, or PAI boundary>

**Fix:**
<concrete alternative>
```

If none:

```
No Daft antipatterns detected in this diff.
```

## Rules for this skill

- **No style nits** unrelated to Daft shape (import order, naming taste).
- **No false positives** on sanctioned PAI batch loops / `series_to_rows` /
  lazy_audit-exempt `Series.to_pylist` inside `@daft.method.batch`.
- **Diff-scoped.** Changed lines + enough context; sibling sweep only when the
  diff establishes a new invariant twins must share.
- **Concrete fixes.** Prefer the builtin / boundary helper / sink pattern.
- **Defer to footgun** for pure runtime footguns already in its category list.
- **Advisory.** This reviewer is not the deterministic merge gate unless CI
  is later wired to it; still be high-signal.

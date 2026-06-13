# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Package the LIBERO×VLA-JEPA rollout ledger into a reproducible dataset.

This is the hackathon "packaged reproducible ledger" deliverable: a
data-preparation / packaging tool that reads our rollout ledger **through the
Archetype query service** and exports a tidy, self-describing dataset anyone can
pick up and use.

What it does
------------
The baseline sweep (``baseline_sweep.py``) fans one Modal Function per LIBERO
task and, when each finishes, copies that task's local Archetype store to the
``libero-eval-results`` Modal Volume under ``{run_id}:task{task_id}``.  Each
such *cell* contains:

- a **lab** world (``StorageConfig(uri=.../lab, namespace="eval_{suite}")``)
  whose rows are ``EvalTrialResult`` components — one per completed trial,
  carrying ``suite / task_id / trial_idx / seed / success / episode_length /
  run_id``; and
- an **episodes** world (``namespace="episodes_{suite}"``) holding the full
  per-tick trajectories.

This tool runs *inside Modal* with the volume mounted at ``/results``, walks the
cells for a set of ``run_id`` s, and for each lab world reads the
``EvalTrialResult`` rows **via the Archetype query service** — the exact same
read path as ``report.py`` (``AsyncStore.get_archetype_df`` /
``AsyncQueryManager``), never a hand-rolled ``daft.read_lance``.  The rows are
aggregated into a tidy per-rollout table and shipped back to the caller, which
writes the artifacts into the working tree (not the volume):

- ``bench/libero/out/dataset/rollouts.parquet`` + ``rollouts.csv`` — one row per
  rollout: ``run_id, suite, task_id, trial_idx, seed, success, episode_length``.
- ``bench/libero/out/dataset/summary.csv`` — per ``(run_id, suite, task_id)``:
  ``rollouts, successes, success_rate, mean_steps``.
- ``bench/libero/out/dataset/splits/*.json`` — the local ``*_split.json`` files
  copied in as a convenience (val/test difficulty splits).
- ``bench/libero/out/dataset/DATASHEET.md`` — what the data is, the schema, the
  ``(world_id, run_id)`` provenance per cell, how ``success`` is sim ground
  truth, and a snippet for re-querying any rollout's full trajectory through the
  Archetype query service.

Run it
------
    modal run bench/libero/prepare_dataset.py

    # custom run_ids (comma-separated)
    modal run bench/libero/prepare_dataset.py --run-ids my-run-a,my-run-b

If a ``run_id`` has no committed cells yet (its batched job is still running),
it is skipped gracefully; if *nothing* is committed yet the tool still exits 0
and reports "0 cells committed yet" rather than crashing.  Individual cells that
error (uncommitted / partial / mid-flush) are caught and skipped.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Where the results Volume is mounted inside the Modal Function, and where the
# repo is copied (mirrors colocated_runner.py / baseline_sweep.py).
ROOT = "/repo"
RESULTS_DIR = "/results"

# The two batched runs we package by default.  Either may be absent on the
# volume while its job is still running — both are skipped gracefully.
DEFAULT_RUN_IDS: list[str] = [
    "libero10-longhorizon-batched",
    "goal-headroom-batched",
]

# Tidy column order for the per-rollout table.
ROLLOUT_COLUMNS: list[str] = [
    "run_id",
    "suite",
    "task_id",
    "trial_idx",
    "seed",
    "success",
    "episode_length",
]


# ---------------------------------------------------------------------------
# Pure helpers (Modal-free → importable + unit-testable in CI)
# ---------------------------------------------------------------------------


@dataclass
class CellProvenance:
    """Where one ``{run_id}:task{task_id}`` cell's rows came from.

    Captures the lab-world coordinates the rows were queried under so the
    DATASHEET can show callers exactly how to re-open the same world.
    """

    cell: str  # the volume dir name, e.g. "libero10-longhorizon-batched:task3"
    run_id: str  # the sweep run label (the {run_id} prefix of the cell)
    suite: str  # LIBERO suite (→ lab namespace "eval_{suite}")
    namespace: str  # the Archetype storage namespace, "eval_{suite}"
    world_ids: list[str] = field(default_factory=list)  # persisted ledger world_id(s)
    ledger_run_ids: list[str] = field(default_factory=list)  # persisted ledger run_id(s)
    n_rollouts: int = 0


def summarize_rollouts(rollouts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse per-rollout rows into a per-(run_id, suite, task_id) summary.

    Returns rows with ``rollouts, successes, success_rate, mean_steps`` — the
    numbers a reader wants before drilling into individual trials.  Pure: takes
    the tidy table and returns the summary table, both as plain dicts.
    """
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for row in rollouts:
        key = (str(row["run_id"]), str(row["suite"]), int(row["task_id"]))
        groups.setdefault(key, []).append(row)

    summary: list[dict[str, Any]] = []
    for (run_id, suite, task_id), rows in sorted(groups.items()):
        n = len(rows)
        successes = sum(1 for r in rows if bool(r["success"]))
        total_steps = sum(int(r["episode_length"]) for r in rows)
        summary.append(
            {
                "run_id": run_id,
                "suite": suite,
                "task_id": task_id,
                "rollouts": n,
                "successes": successes,
                "success_rate": round(successes / n, 4) if n else 0.0,
                "mean_steps": round(total_steps / n, 2) if n else 0.0,
            }
        )
    return summary


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    """Write ``rows`` to ``path`` as CSV with a fixed column order."""
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})


# ---------------------------------------------------------------------------
# The query-service read path (runs inside Modal, next to the volume).
# ---------------------------------------------------------------------------


async def _read_cell_via_query_service(
    cell_dir: Path,
    run_id: str,
) -> tuple[list[dict[str, Any]], CellProvenance | None]:
    """Read one cell's ``EvalTrialResult`` rows through the Archetype query service.

    Mirrors ``report.py`` / the eval driver: open the lab world's Archetype store
    and pull rows via ``AsyncQueryManager`` — **not** a raw ``daft.read_lance``.

    The lab world lives at ``{cell_dir}/lab/eval_{suite}/`` (the
    ``StorageConfig(uri=.../lab, namespace="eval_{suite}")`` written by the
    sweep).  We:

    1. discover the ``eval_{suite}`` namespace by listing ``lab/``;
    2. build an ``AsyncLancedbStore`` over it (the default Archetype backend);
    3. resolve the persisted ``(world_id, run_id)`` ledger coordinates (the lab
       world's own UUIDs — distinct from the human ``run_id`` label, which is a
       *field* on each ``EvalTrialResult``); and
    4. query the ``EvalTrialResult`` component via ``query_components`` scoped to
       each ``(world_id, run_id)``, exactly the contract a downstream consumer
       would use.

    Returns ``(rollout_rows, provenance)``.  On any error — an uncommitted or
    half-flushed cell, a missing namespace, a partial Lance manifest — returns
    ``([], None)`` so the caller can skip the cell and keep going.
    """
    import daft

    from archetype.core.aio import AsyncLancedbStore, AsyncQueryManager
    from archetype.experiments.components import EvalTrialResult

    lab_dir = cell_dir / "lab"
    if not lab_dir.is_dir():
        return [], None

    # The sweep writes exactly one "eval_{suite}" namespace per cell; pick the
    # first that looks like one (be permissive: any subdir is a candidate).
    namespaces = [p.name for p in lab_dir.iterdir() if p.is_dir()]
    eval_namespaces = [n for n in namespaces if n.startswith("eval_")] or namespaces
    if not eval_namespaces:
        return [], None
    namespace = eval_namespaces[0]
    suite = namespace[len("eval_") :] if namespace.startswith("eval_") else namespace

    # Default Archetype backend is LanceDB; the store roots at {uri}/{namespace}.
    store = AsyncLancedbStore(str(lab_dir), namespace)
    sig = (EvalTrialResult,)
    prov = CellProvenance(
        cell=cell_dir.name,
        run_id=run_id,
        suite=suite,
        namespace=namespace,
    )

    try:
        # Resolve the EvalTrialResult table; absent → this cell has no lab rows
        # (e.g. a half-written cell). ``_ensure_table`` raises if not found.
        table = await store._ensure_table(sig)

        # Discover the persisted ledger coordinates. ``get_archetype_df`` always
        # scopes by (world_id, run_id), so we first read the raw table once to
        # enumerate the distinct pairs, then re-query each pair *through the
        # query manager* (the public service contract).
        keys_arrow = await table.query().select(["world_id", "run_id"]).to_arrow()
        keys = daft.from_arrow(keys_arrow).distinct().to_pylist()
        if not keys:
            await store.shutdown()
            return [], prov

        querier = AsyncQueryManager(store=store)
        rows: list[dict[str, Any]] = []
        for key in keys:
            world_id = str(key["world_id"])
            ledger_run_id = str(key["run_id"])
            prov.world_ids.append(world_id)
            prov.ledger_run_ids.append(ledger_run_id)

            df = await querier.query_components(
                [EvalTrialResult],
                world_id=world_id,
                run_id=ledger_run_id,
            )
            for r in df.to_pylist():
                rows.append(
                    {
                        # The human run label (the cell prefix); stable and
                        # readable, unlike the ledger run UUID.
                        "run_id": run_id,
                        "suite": str(r.get("evaltrialresult__suite", suite)),
                        "task_id": int(r.get("evaltrialresult__task_id", 0)),
                        "trial_idx": int(r.get("evaltrialresult__trial_idx", 0)),
                        "seed": int(r.get("evaltrialresult__seed", 0)),
                        "success": bool(r.get("evaltrialresult__success", False)),
                        "episode_length": int(r.get("evaltrialresult__episode_length", 0)),
                    }
                )

        prov.n_rollouts = len(rows)
        await store.shutdown()
        return rows, prov
    except Exception:
        # Uncommitted / partial / mid-flush cell — skip it, never abort the run.
        try:
            await store.shutdown()
        except Exception:
            pass
        return [], None


async def _prepare_async(run_ids: list[str]) -> dict[str, Any]:
    """Enumerate cells for ``run_ids`` and read each via the query service.

    Returns a JSON-serializable payload: the tidy rollout rows, per-cell
    provenance, and a small log of which cells were found / read / skipped.
    Runs inside the Modal Function where ``/results`` is the mounted volume.
    """
    results_root = Path(RESULTS_DIR)

    all_rollouts: list[dict[str, Any]] = []
    provenances: list[dict[str, Any]] = []
    log: list[str] = []

    for run_id in run_ids:
        # Cells for this run are dirs named "{run_id}:task{task_id}".
        prefix = f"{run_id}:task"
        cell_dirs = sorted(
            p for p in results_root.iterdir() if p.is_dir() and p.name.startswith(prefix)
        )
        if not cell_dirs:
            log.append(f"run_id={run_id!r}: 0 cells committed yet (skipped)")
            continue

        run_rows = 0
        ok_cells = 0
        for cell_dir in cell_dirs:
            rows, prov = await _read_cell_via_query_service(cell_dir, run_id)
            if prov is None:
                log.append(f"  {cell_dir.name}: skipped (uncommitted/partial)")
                continue
            ok_cells += 1
            run_rows += len(rows)
            all_rollouts.extend(rows)
            provenances.append(
                {
                    "cell": prov.cell,
                    "run_id": prov.run_id,
                    "suite": prov.suite,
                    "namespace": prov.namespace,
                    "world_ids": prov.world_ids,
                    "ledger_run_ids": prov.ledger_run_ids,
                    "n_rollouts": prov.n_rollouts,
                }
            )
        log.append(
            f"run_id={run_id!r}: {len(cell_dirs)} cell(s) found, "
            f"{ok_cells} read, {run_rows} rollout(s)"
        )

    # Deterministic order for reproducible artifacts.
    all_rollouts.sort(
        key=lambda r: (r["run_id"], r["suite"], r["task_id"], r["trial_idx"], r["seed"])
    )

    # Build the parquet here (the remote env has Daft, the framework's
    # DataFrame); the local entrypoint runs under bare ``modal`` and need not
    # import Daft. Return the bytes so the caller writes them verbatim. We round-
    # trip through a temp file because Daft writes a parquet *dataset* dir; we
    # collapse it to the single file's bytes for a portable single-file artifact.
    parquet_bytes: bytes | None = None
    if all_rollouts:
        import tempfile

        import daft

        df = daft.from_pylist(all_rollouts).select(*ROLLOUT_COLUMNS)
        with tempfile.TemporaryDirectory() as tmp:
            df.write_parquet(tmp)
            parts = sorted(Path(tmp).rglob("*.parquet"))
            if len(parts) == 1:
                parquet_bytes = parts[0].read_bytes()
            else:
                # Coalesce multiple part files into one with pyarrow so the
                # exported artifact is a single readable parquet file.
                import pyarrow.parquet as pq

                table = pq.read_table(tmp)
                buf = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
                pq.write_table(table, buf.name)
                parquet_bytes = Path(buf.name).read_bytes()
                Path(buf.name).unlink(missing_ok=True)

    return {
        "rollouts": all_rollouts,
        "provenance": provenances,
        "log": log,
        "n_cells": len(provenances),
        "n_rollouts": len(all_rollouts),
        "parquet_bytes": parquet_bytes,
    }


# ---------------------------------------------------------------------------
# Local-side writers (run on the caller after the Modal Function returns).
# ---------------------------------------------------------------------------


def _datasheet(
    rollouts: list[dict[str, Any]],
    summary: list[dict[str, Any]],
    provenance: list[dict[str, Any]],
    run_ids: list[str],
) -> str:
    """Render the DATASHEET.md documenting the packaged dataset."""
    suites = sorted({str(r["suite"]) for r in rollouts})
    present_run_ids = sorted({str(r["run_id"]) for r in rollouts})

    lines: list[str] = []
    lines.append("# LIBERO×VLA-JEPA Rollout Dataset")
    lines.append("")
    lines.append(
        "A tidy, reproducible export of LIBERO benchmark rollouts produced by the "
        "VLA-JEPA policy and recorded on the **Archetype append-only ledger**. "
        "Every row is one environment episode (a *rollout*); `success` is "
        "simulator ground truth, not a model self-report."
    )
    lines.append("")

    # --- Provenance ---
    lines.append("## Provenance")
    lines.append("")
    lines.append(
        "Rows were read **through the Archetype query service** "
        "(`AsyncStore.get_archetype_df` / `AsyncQueryManager.query_components`) — "
        "the same read path as `bench/libero/report.py` — from per-task ledger "
        "cells on the `libero-eval-results` Modal Volume. Each cell is a "
        "directory `{run_id}:task{task_id}` holding a **lab** world "
        '(`StorageConfig(uri=.../lab, namespace="eval_{suite}")`) of '
        "`EvalTrialResult` rows and an **episodes** world "
        '(`namespace="episodes_{suite}"`) of full per-tick trajectories.'
    )
    lines.append("")
    lines.append(f"- **Requested run_ids:** {', '.join(repr(r) for r in run_ids)}")
    lines.append(
        f"- **Run_ids with committed cells:** "
        f"{', '.join(repr(r) for r in present_run_ids) or '(none yet)'}"
    )
    lines.append(f"- **Suites:** {', '.join(suites) or '(none)'}")
    lines.append(f"- **Cells packaged:** {len(provenance)}")
    lines.append(f"- **Total rollouts:** {len(rollouts)}")
    lines.append("")
    lines.append("Each packaged cell, with the ledger coordinates its rows were queried under:")
    lines.append("")
    lines.append("| cell | suite | namespace | world_id(s) | ledger run_id(s) | rollouts |")
    lines.append("|------|-------|-----------|-------------|------------------|----------|")
    for p in provenance:
        world_ids = "<br>".join(p["world_ids"]) or "—"
        run_id_cells = "<br>".join(p["ledger_run_ids"]) or "—"
        lines.append(
            f"| `{p['cell']}` | {p['suite']} | `{p['namespace']}` "
            f"| {world_ids} | {run_id_cells} | {p['n_rollouts']} |"
        )
    lines.append("")

    # --- Files ---
    lines.append("## Files")
    lines.append("")
    lines.append("| file | description |")
    lines.append("|------|-------------|")
    lines.append("| `rollouts.parquet` | One row per rollout (tidy). Canonical artifact. |")
    lines.append("| `rollouts.csv` | Same rows as CSV, for spreadsheet / quick-look use. |")
    lines.append(
        "| `summary.csv` | Per `(run_id, suite, task_id)`: "
        "`rollouts, successes, success_rate, mean_steps`. |"
    )
    lines.append(
        "| `splits/*.json` | Local val/test difficulty splits "
        "(copied from `bench/libero/out/*_split.json`). |"
    )
    lines.append("| `provenance.json` | Machine-readable copy of the provenance table above. |")
    lines.append("")

    # --- Schema ---
    lines.append("## Schema (`rollouts.parquet` / `rollouts.csv`)")
    lines.append("")
    lines.append("| column | type | meaning |")
    lines.append("|--------|------|---------|")
    lines.append("| `run_id` | str | Sweep run label (the `{run_id}` cell prefix). |")
    lines.append("| `suite` | str | LIBERO suite, e.g. `libero_10`, `libero_spatial`. |")
    lines.append("| `task_id` | int | Task index within the suite (0-based). |")
    lines.append("| `trial_idx` | int | Trial index within `(suite, task_id)` (0-based). |")
    lines.append("| `seed` | int | Env reset seed (`task_id * 1000 + trial_idx`). |")
    lines.append("| `success` | bool | **Sim ground truth** — see below. |")
    lines.append("| `episode_length` | int | Env steps executed before done/timeout. |")
    lines.append("")

    # --- Success semantics ---
    lines.append("## How `success` is measured")
    lines.append("")
    lines.append(
        "`success` is the LIBERO simulator's own `env.check_success()` verdict, "
        "evaluated inside the MuJoCo environment at the end of the episode. It is "
        "**not** a model confidence or a self-report: the policy proposes actions, "
        "the simulator decides whether the task goal was physically achieved. The "
        "verdict latches on the ledger (once true, it stays true), so the recorded "
        "value is the per-episode terminal outcome. An episode that never reaches "
        "the goal within `max_steps` is recorded as `success = false`."
    )
    lines.append("")

    # --- Re-query snippet ---
    lines.append("## Re-querying a rollout's full trajectory")
    lines.append("")
    lines.append(
        "The summary rows above are derived from the lab world. The **full "
        "per-tick trajectory** (proprio, actions, frame refs) for any rollout "
        "lives in the matching *episodes* world on the same volume and is "
        "queryable by `(world_id, run_id)` through the same Archetype query "
        "service. Mount the `libero-eval-results` volume and:"
    )
    lines.append("")
    lines.append("```python")
    lines.append("import asyncio")
    lines.append("from pathlib import Path")
    lines.append("from archetype.core.aio import AsyncLancedbStore, AsyncQueryManager")
    lines.append("from archetype.experiments.components import EvalTrialResult")
    lines.append("from archetype.experiments.manipulation import (")
    lines.append("    ManipProprio, ManipAction, ManipStatus,")
    lines.append(")")
    lines.append("")
    lines.append("")
    lines.append("async def main():")
    lines.append("    # Point at one cell on the mounted volume.")
    lines.append('    cell = Path("/results") / "RUN_ID:taskTASK_ID"')
    lines.append("")
    lines.append("    # --- lab world: the EvalTrialResult summary rows ---")
    lines.append('    lab = AsyncLancedbStore(str(cell / "lab"), "eval_SUITE")')
    lines.append("    lab_q = AsyncQueryManager(store=lab)")
    lines.append("    # Discover the persisted ledger coordinates for this world,")
    lines.append("    # then query the trial results scoped to (world_id, run_id):")
    lines.append("    tbl = await lab._ensure_table((EvalTrialResult,))")
    lines.append('    key = (await tbl.query().select(["world_id", "run_id"])')
    lines.append("           .to_arrow()).to_pylist()[0]")
    lines.append("    trials = await lab_q.query_components(")
    lines.append('        [EvalTrialResult], world_id=str(key["world_id"]),')
    lines.append('        run_id=str(key["run_id"]),')
    lines.append("    )")
    lines.append("    print(trials.to_pandas())")
    lines.append("")
    lines.append("    # --- episodes world: the full per-tick trajectory ---")
    lines.append('    eps = AsyncLancedbStore(str(cell / "episodes"), "episodes_SUITE")')
    lines.append("    eps_q = AsyncQueryManager(store=eps)")
    lines.append("    etbl = await eps._ensure_table((ManipProprio, ManipAction, ManipStatus))")
    lines.append('    ekey = (await etbl.query().select(["world_id", "run_id"])')
    lines.append("            .to_arrow()).to_pylist()[0]")
    lines.append("    traj = await eps_q.query_components(")
    lines.append("        [ManipProprio, ManipAction, ManipStatus],")
    lines.append('        world_id=str(ekey["world_id"]), run_id=str(ekey["run_id"]),')
    lines.append("    )")
    lines.append('    print(traj.sort("tick").to_pandas())  # one row per env step')
    lines.append("")
    lines.append("")
    lines.append("asyncio.run(main())")
    lines.append("```")
    lines.append("")
    lines.append(
        "Replace `RUN_ID`, `TASK_ID`, and `SUITE` with values from "
        "`provenance.json` / `summary.csv`. (The episodes archetype above is the "
        "one the sweep spawns; adjust the component tuple to match the world you "
        "are reading.)"
    )
    lines.append("")
    return "\n".join(lines)


def write_dataset(payload: dict[str, Any], out_dir: Path, run_ids: list[str]) -> dict[str, Any]:
    """Write all dataset artifacts from a Modal payload into ``out_dir``.

    Pure-ish (filesystem only, no Modal): takes the rollout rows + provenance
    returned by the Modal Function and emits parquet/csv/summary/splits/datasheet
    into the working tree.  Returns a small manifest of what was written.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    rollouts: list[dict[str, Any]] = payload["rollouts"]
    provenance: list[dict[str, Any]] = payload["provenance"]
    parquet_bytes: bytes | None = payload.get("parquet_bytes")

    # --- rollouts.parquet + rollouts.csv ---
    # The parquet is built remotely (where Daft lives) and shipped back as bytes;
    # the local entrypoint runs under bare ``modal`` and writes them verbatim.
    parquet_path = out_dir / "rollouts.parquet"
    csv_path = out_dir / "rollouts.csv"
    wrote_parquet = False
    if parquet_bytes:
        parquet_path.write_bytes(parquet_bytes)
        wrote_parquet = True
    _write_csv(csv_path, ROLLOUT_COLUMNS, rollouts)

    # --- summary.csv ---
    summary = summarize_rollouts(rollouts)
    summary_path = out_dir / "summary.csv"
    _write_csv(
        summary_path,
        ["run_id", "suite", "task_id", "rollouts", "successes", "success_rate", "mean_steps"],
        summary,
    )

    # --- provenance.json ---
    provenance_path = out_dir / "provenance.json"
    provenance_path.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")

    # --- splits/*.json (fold in the local difficulty splits as a convenience) ---
    splits_dir = out_dir / "splits"
    splits_dir.mkdir(parents=True, exist_ok=True)
    split_srcs = sorted((Path(__file__).parent / "out").glob("*_split.json"))
    copied_splits: list[str] = []
    for src in split_srcs:
        dst = splits_dir / src.name
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        copied_splits.append(src.name)

    # --- DATASHEET.md ---
    datasheet_path = out_dir / "DATASHEET.md"
    datasheet_path.write_text(_datasheet(rollouts, summary, provenance, run_ids), encoding="utf-8")

    return {
        "out_dir": str(out_dir),
        "rollouts_parquet": str(parquet_path) if wrote_parquet else None,
        "rollouts_csv": str(csv_path),
        "summary_csv": str(summary_path),
        "provenance_json": str(provenance_path),
        "datasheet_md": str(datasheet_path),
        "splits": copied_splits,
        "n_rollouts": len(rollouts),
        "n_cells": len(provenance),
        "n_summary_rows": len(summary),
    }


# ---------------------------------------------------------------------------
# Modal app — defined only when ``modal`` is importable (runner-time).
# ---------------------------------------------------------------------------

# ``modal`` is a runner-time tool, absent in the CI test venv. Import it lazily
# so the pure helpers above stay importable for unit tests, while the Modal app
# (image, function, entrypoint) is only defined under ``modal run``.
try:
    import modal  # type: ignore[import-not-found]

    _HAS_MODAL = True
except ImportError:  # pragma: no cover - exercised only in the CI test venv
    modal = None  # type: ignore[assignment]
    _HAS_MODAL = False


if _HAS_MODAL:
    # Reuse the lean colocated image: py3.12 + the archetype package only (the
    # env / policy stacks are not needed to read the ledger). The ignore list
    # keeps run logs and the live ``out/`` dir out of the build so an active
    # writer cannot race the image build.
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("uv")
        .add_local_dir(
            ".",
            ROOT,
            copy=True,
            ignore=[
                ".git",
                "**/__pycache__",
                ".claude",
                "target",
                "*.mp4",
                ".venv",
                "**/*.parquet",
                "**/*.log",
                "**/out/**",
            ],
        )
        .run_commands(
            f"cd {ROOT} && uv pip install --system -e . && uv pip install --system pillow numpy"
        )
    )

    app = modal.App("archetype-libero-prepare-dataset", image=image)
    results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)

    @app.function(volumes={RESULTS_DIR: results_volume}, timeout=3600)
    def prepare_remote(run_ids: list[str]) -> dict[str, Any]:
        """Read every committed cell for ``run_ids`` via the Archetype query service.

        Runs in-region with the ``libero-eval-results`` volume mounted at
        ``/results``. Returns the tidy rollout rows + provenance for the caller
        to write into the working tree. Never raises on a partial cell — each is
        caught and skipped inside ``_read_cell_via_query_service``.
        """
        import asyncio
        import os
        import sys

        # Keep logfire quiet/offline and put loose bench scripts on the path
        # (mirrors the eval driver / sweep so imports resolve identically).
        os.environ.setdefault("LOGFIRE_IGNORE_NO_CONFIG", "1")
        os.environ.setdefault("LOGFIRE_SEND_TO_LOGFIRE", "false")
        sys.path.insert(0, f"{ROOT}/bench/libero")

        # Refresh the volume view so cells committed by concurrent sweep jobs
        # since this container started are visible.
        results_volume.reload()

        return asyncio.run(_prepare_async(run_ids))

    @app.local_entrypoint()
    def main(run_ids: str = "") -> None:
        """Package the ledger into ``bench/libero/out/dataset/``.

        ``run_ids`` is a comma-separated list; empty → the two default batched
        runs. The Modal Function does the query-service reads in-region; this
        entrypoint writes the artifacts locally so they land in the working tree.
        """
        ids = [x.strip() for x in run_ids.split(",") if x.strip()] or list(DEFAULT_RUN_IDS)

        print(f"=== prepare_dataset === run_ids={ids}")
        payload = prepare_remote.remote(ids)

        # Surface the per-run / per-cell log from the remote read.
        for line in payload.get("log", []):
            print(f"  {line}")

        out_dir = Path(__file__).parent / "out" / "dataset"
        if payload["n_rollouts"] == 0:
            # Nothing committed yet (or all cells partial): still write a
            # DATASHEET + empty CSVs so the deliverable exists, and exit clean.
            manifest = write_dataset(payload, out_dir, ids)
            print(
                "\n0 cells committed yet (or all partial) — wrote an empty "
                f"dataset scaffold to {manifest['out_dir']!r}."
            )
            print("Re-run once the batched jobs commit their first cells.")
            return

        manifest = write_dataset(payload, out_dir, ids)
        print("\n=== dataset written ===")
        for key, value in manifest.items():
            print(f"  {key}: {value}")

        # Show a sample of the tidy table so the run is self-verifying.
        print("\n=== sample rollouts (first 10) ===")
        header = " | ".join(ROLLOUT_COLUMNS)
        print("  " + header)
        for row in payload["rollouts"][:10]:
            print("  " + " | ".join(str(row[c]) for c in ROLLOUT_COLUMNS))

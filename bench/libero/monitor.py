# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Live progress monitor — reads the ledger through the Archetype query service.

Per the runtime contract, ledger reads are scoped by (world_id, run_id), so we
use ``AsyncQueryManager.query_archetype`` per cell — never raw file reads. The
cell inventory (which (world_id, run_id) exist, with their run_name/suite/task)
comes from the runner's results manifest on the canonical volume.

    uv run --with modal --with pandas python bench/libero/monitor.py
    uv run --with modal --with pandas python bench/libero/monitor.py --watch 15
    uv run --with modal --with pandas python bench/libero/monitor.py --html out.html

Prints per-arm × task success — the number that says whether we're winning.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

import modal

sys.path.insert(0, str(Path(__file__).parent))
from colocated_runner import image  # noqa: E402

app = modal.App("archetype-monitor", image=image)
results_volume = modal.Volume.from_name("libero-eval-results", create_if_missing=True)
RESULTS_DIR = "/results"

CANONICAL_URI = f"{RESULTS_DIR}/canonical"
CANONICAL_NS = "libero_para"
# The runner appends one JSONL line per launched cell: {world_id, run_id, run_name, suite, task_id}.
CELLS_MANIFEST = f"{RESULTS_DIR}/canonical/_cells.jsonl"


@app.function(volumes={RESULTS_DIR: results_volume}, timeout=300)
def aggregate() -> list[dict[str, Any]]:
    """Query each cell via the Archetype service; aggregate by (run_name, suite, task)."""
    import asyncio
    import json

    async def _run() -> list[dict[str, Any]]:
        import daft
        from daft import col

        from archetype.core.aio.async_querier import AsyncQueryManager
        from archetype.core.aio.async_store import AsyncStore
        from archetype.core.config import StorageConfig
        from archetype.experiments.manipulation import (
            ManipAction,
            ManipFrameRef,
            ManipProprio,
            ManipStatus,
            ManipTask,
        )
        from archetype.runtime.session import configure_session

        results_volume.reload()
        manifest = Path(CELLS_MANIFEST)
        if not manifest.exists():
            return []
        cells = [json.loads(ln) for ln in manifest.read_text().splitlines() if ln.strip()]
        if not cells:
            return []

        # One store/querier over the canonical store — the service layer, not file IO.
        store = AsyncStore(
            configure_session(StorageConfig(uri=CANONICAL_URI, namespace=CANONICAL_NS))
        )
        q = AsyncQueryManager(store)
        SIG = (ManipProprio, ManipAction, ManipStatus, ManipTask, ManipFrameRef)

        per_entity: list[dict[str, Any]] = []
        for c in cells:
            try:
                df = await q.query_archetype(SIG, world_id=c["world_id"], run_id=c["run_id"])
            except Exception:  # noqa: BLE001 — cell not yet committed; skip in live view
                continue
            # Terminal state per rollout (entity): success latches → max() is the verdict.
            pe = df.groupby("entity_id").agg(
                col("manipstatus__success").cast(daft.DataType.int64()).max().alias("success"),
                col("manipstatus__env_step").max().alias("steps"),
            )
            for r in pe.to_pylist():
                per_entity.append(
                    {"run_name": c["run_name"], "suite": c["suite"], "task_id": c["task_id"], **r}
                )

        if not per_entity:
            return []
        agg = (
            daft.from_pylist(per_entity)
            .groupby("run_name", "suite", "task_id")
            .agg(
                col("success").count().alias("rollouts"),
                col("success").sum().alias("successes"),
                col("steps").mean().alias("mean_steps"),
            )
            .with_column("success_rate", col("successes") / col("rollouts"))
            .sort(["run_name", "task_id"])
        )
        return agg.to_pylist()

    return asyncio.run(_run())


def _render(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "no committed rollouts on the ledger yet — WF1 baseline pending."
    import daft

    return daft.from_pylist(rows).to_pandas().to_string(index=False)


def _write_html(rows: list[dict[str, Any]], path: str, refresh: int) -> None:
    head = (
        f"<head><meta charset='utf-8'><meta http-equiv='refresh' content='{refresh}'>"
        "<title>LIBERO-Para progress</title><style>"
        "body{font:14px ui-monospace,monospace;background:#0b0e14;color:#cdd6f4;padding:24px}"
        "h1{font-size:16px}table{border-collapse:collapse;margin-top:12px}"
        "td,th{border:1px solid #313244;padding:4px 10px;text-align:right}th{color:#89b4fa}"
        ".arm{text-align:left;color:#a6e3a1}.win{color:#a6e3a1}.low{color:#f38ba8}</style></head>"
    )
    rowsh = ["<p>no committed rollouts yet — WF1 baseline pending.</p>"]
    if rows:
        rowsh = [
            "<table><tr><th class='arm'>arm</th><th>suite</th><th>task</th><th>rollouts</th>"
            "<th>successes</th><th>success_rate</th><th>mean_steps</th></tr>"
        ]
        for r in rows:
            cls = "win" if r["success_rate"] >= 0.5 else "low"
            rowsh.append(
                f"<tr><td class='arm'>{r['run_name']}</td><td>{r['suite']}</td><td>{r['task_id']}</td>"
                f"<td>{r['rollouts']}</td><td>{r['successes']}</td>"
                f"<td class='{cls}'>{r['success_rate']:.2f}</td><td>{r['mean_steps']:.0f}</td></tr>"
            )
        rowsh.append("</table>")
    Path(path).write_text(
        f"<!doctype html><html>{head}<body><h1>LIBERO-Para × VLA-JEPA — auto-refresh {refresh}s</h1>"
        f"{''.join(rowsh)}</body></html>"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch", type=int, default=0, help="refresh every N seconds")
    parser.add_argument("--html", default="", help="write a self-refreshing dashboard here")
    args = parser.parse_args()

    def tick() -> None:
        with app.run():
            rows = aggregate.remote()
        if args.html:
            _write_html(rows, args.html, max(args.watch, 10))
            print(f"[{time.strftime('%H:%M:%S')}] wrote {args.html} ({len(rows)} arm×task rows)")
        else:
            print(f"\n=== ledger @ {time.strftime('%H:%M:%S')} (via query service) ===")
            print(_render(rows))

    if args.watch:
        while True:
            tick()
            time.sleep(args.watch)
    else:
        tick()


if __name__ == "__main__":
    main()

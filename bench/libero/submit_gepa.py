# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fire-and-forget GEPA runs — submit to the cloud, no persistent connection.

`modal run --detach` keeps your local CLI as the driver (it blocks streaming the
result), so a reaped session cancels the job. `spawn()` against a DEPLOYED function
queues one input and returns in ~1s; Modal runs it server-side to completion. Close
your laptop — the result lands in the ledger.

    # 1) deploy the function once (and after any gepa_daft.py edit):
    modal deploy bench/libero/gepa_daft.py

    # 2) submit a job (returns immediately with a call id):
    python bench/libero/submit_gepa.py --suite libero_goal --n-tasks 8 \\
        --n-seeds 5 --max-steps 300 --budget 60 --minibatch 3

    # 3) check on it later (or just `make status` / read the ledger):
    python bench/libero/submit_gepa.py --fetch <call_id>
"""

from __future__ import annotations

import argparse
import json

import modal

APP, FN = "archetype-gepa-daft", "run"
LAST_CALL_FILE = "/tmp/gepa_last_call.txt"  # status.sh reads this


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fetch", metavar="CALL_ID", help="poll a previously-submitted job and exit")
    p.add_argument("--suite", default="libero_goal")
    p.add_argument("--n-tasks", type=int, default=8)
    p.add_argument("--n-seeds", type=int, default=5)
    p.add_argument("--max-steps", type=int, default=300)
    p.add_argument("--budget", type=int, default=60)
    p.add_argument("--minibatch", type=int, default=3)
    p.add_argument("--no-gepa", action="store_true", help="Tier-1 arms only (skip GEPA loop)")
    a = p.parse_args()

    if a.fetch:
        call = modal.FunctionCall.from_id(a.fetch)
        try:
            res = call.get(timeout=5)
            print("STATUS: DONE ✓")
            print(json.dumps(res, indent=2))
        except TimeoutError:
            print("STATUS: RUNNING (server-side, decoupled — safe to close your laptop)")
        except Exception as exc:  # noqa: BLE001 — result may have expired; ledger is the durable copy
            print(f"STATUS: UNKNOWN — {type(exc).__name__}: {exc} (durable result is in the ledger)")
        return

    run = modal.Function.from_name(APP, FN)
    call = run.spawn(
        suite=a.suite,
        n_tasks=a.n_tasks,
        n_seeds=a.n_seeds,
        max_steps=a.max_steps,
        budget=a.budget,
        minibatch=a.minibatch,
        do_gepa=not a.no_gepa,
    )
    with open(LAST_CALL_FILE, "w") as fh:
        fh.write(call.object_id)
    print(f"submitted: {call.object_id}")
    print("running server-side; this process can exit. fetch with:")
    print(f"    python bench/libero/submit_gepa.py --fetch {call.object_id}")


if __name__ == "__main__":
    main()

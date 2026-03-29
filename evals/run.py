# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run all evals and report results.

Usage:
    python -m evals.run [--out results.json]

Exit code 0 if all evals pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

from evals.eval_command_flow import eval_command_flow
from evals.eval_storage_roundtrip import eval_storage_roundtrip
from evals.eval_world_lifecycle import eval_world_lifecycle

EVALS = [
    ("world_lifecycle", eval_world_lifecycle),
    ("storage_roundtrip", eval_storage_roundtrip),
    ("command_flow", eval_command_flow),
]


async def run_all() -> list[dict]:
    results = []
    for name, fn in EVALS:
        t0 = time.perf_counter()
        try:
            code = await fn()
            status = "pass" if code == 0 else "fail"
            error = None
        except Exception as exc:
            status = "error"
            error = str(exc)
            code = 1
        elapsed = time.perf_counter() - t0
        results.append(
            {"name": name, "status": status, "elapsed_s": round(elapsed, 3), "error": error}
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run archetype evals")
    parser.add_argument("--out", default=None, help="Write JSON results to this file")
    args = parser.parse_args()

    results = asyncio.run(run_all())

    # Print summary
    print("\n" + "=" * 60)
    print("EVAL RESULTS")
    print("=" * 60)
    for r in results:
        icon = "PASS" if r["status"] == "pass" else "FAIL"
        print(f"  [{icon}] {r['name']} ({r['elapsed_s']}s)")
        if r["error"]:
            print(f"         error: {r['error']}")
    print("=" * 60)

    passed = sum(1 for r in results if r["status"] == "pass")
    total = len(results)
    print(f"\n{passed}/{total} evals passed")

    if args.out:
        import os

        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results written to {args.out}")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())

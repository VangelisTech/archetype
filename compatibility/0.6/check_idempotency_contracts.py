#!/usr/bin/env python3
"""Fail when the normative idempotency matrix and eval manifest drift."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from evals.suites.idempotency import traceability_checks  # noqa: E402


def main() -> int:
    failed = [name for name, passed in traceability_checks().items() if not passed]
    if failed:
        print("Idempotency contract audit failed:")
        for name in failed:
            print(f"  - {name}")
        print("Update docs/guide/specification.md and evals/suites/idempotency/ together.")
        return 1

    print("Idempotency contract audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

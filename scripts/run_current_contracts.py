#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run the current 0.7 source contracts; never count absent/empty suites as success."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULES = (
    "test_native_runtime_contracts",
    "test_remote_runtime_config",
    "test_native_runtime_binding",
    "test_native_cli_contracts",
    "test_native_server_contracts",
    "test_binding",
    "test_ingress",
    "test_logical_binding",
    "test_logical_ingress",
    "test_live_values",
    "test_remote_config",
    "test_live_binding",
    "test_transports",
    "test_context_attachments_native",
    "test_cut_attachments_native",
)


RELIABILITY = (
    "test_native_runtime_contracts",
    "test_native_server_contracts",
    "test_binding.BindingTests.test_persistence_failure_repair_close",
    "test_binding.BindingTests.test_close_during_restore",
    "test_binding.BindingTests.test_close_during_compile",
    "test_binding.BindingTests.test_held_native_does_not_block_sibling_or_stop",
    "test_binding.BindingTests.test_cancelled_publication_waiter_and_two_closes",
    "test_binding.BindingTests.test_recovery_contract",
    "test_logical_binding",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("current", "reliability"), default="current")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    library = os.environ.get("DDLOG_PYTHON_LIBRARY")
    if not library or not Path(library).is_file():
        raise SystemExit(
            "Set DDLOG_PYTHON_LIBRARY to the matched contract-4 C ABI; no native suite may silently disappear"
        )
    for package in ("archetype-ecs", "archetype-native", "archetype-transports"):
        sys.path.insert(0, str(ROOT / "packages" / package / "src"))
        sys.path.insert(0, str(ROOT / "packages" / package / "tests"))
    selected = MODULES if args.profile == "current" else RELIABILITY
    sys.path.insert(0, str(ROOT / "tests/artifacts"))
    suite = unittest.defaultTestLoader.loadTestsFromNames(selected)
    if suite.countTestCases() < (80 if args.profile == "current" else 20):
        raise SystemExit("Incomplete current contract inventory")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    passed = result.wasSuccessful() and (args.profile != "reliability" or not result.skipped)
    if args.out:
        args.out.write_text(
            json.dumps(
                {
                    "schema": "archetype.current-contracts/v1",
                    "profile": args.profile,
                    "mode": "source contracts; simulated compiler with real C ABI/Iceberg; actual installed gate is separate",
                    "library": str(Path(library).resolve()),
                    "library_sha256": hashlib.sha256(Path(library).read_bytes()).hexdigest(),
                    "selected": selected,
                    "tests_run": result.testsRun,
                    "failures": len(result.failures),
                    "errors": len(result.errors),
                    "skips": [
                        {"test": str(test), "reason": reason} for test, reason in result.skipped
                    ],
                    "result": "pass" if passed else "fail",
                },
                indent=2,
            )
        )
    return int(not passed)


if __name__ == "__main__":
    raise SystemExit(main())

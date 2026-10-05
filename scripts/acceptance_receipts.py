# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fail-closed current child admission and retained parent execution outcomes."""

from __future__ import annotations

import json
from pathlib import Path


def require_child(path: Path, *, schema: str, mode: str, counts=None):
    value = json.loads(path.read_text())
    if (
        type(value) is not dict
        or value.get("schema") != schema
        or value.get("mode") != mode
        or value.get("result") != "pass"
        or value.get("validation_errors") != []
        or type(value.get("origins")) is not int
        or value["origins"] <= 0
        or value.get("failure") is not None
    ):
        raise RuntimeError("Child did not provide complete passing installed evidence")
    for key, expected in (counts or {}).items():
        if type(value.get(key)) is not type(expected) or value[key] != expected:
            raise RuntimeError("Child execution counts differ from required evidence")
    return value


def run_with_receipt(stage: Path, procedure, *, mode: str):
    stage = stage.resolve()
    existed = stage.exists()
    passed = False
    failure = None
    try:
        result = procedure()
        passed = type(result) is int and result == 0
        return result
    except BaseException as error:
        # Raw exception text can include provider details. Stage logs retain
        # masked bounded diagnostics; the parent states only the failure type.
        failure = type(error).__name__
        raise
    finally:
        if not existed and stage.is_dir():
            path = stage / "parent-result.json"
            with path.open("x") as output:
                json.dump(
                    dict(
                        schema="archetype.acceptance-parent/v1",
                        mode=mode,
                        result="pass" if passed else "fail",
                        failure_type=failure,
                        stage=str(stage),
                        logs=sorted(p.name for p in stage.glob("*.log")),
                    ),
                    output,
                    indent=2,
                )

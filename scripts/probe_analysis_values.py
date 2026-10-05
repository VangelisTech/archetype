#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Verify retained declaration/grading values in an installed analysis environment."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import zipfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheels", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if sys.flags.optimize:
        raise RuntimeError("Optimized value proof is unsupported")
    files = {}
    for wheel in args.wheels.glob("*.whl"):
        with zipfile.ZipFile(wheel) as archive:
            for name in archive.namelist():
                if name.endswith(".py") and name.startswith(
                    ("archetype/", "archetype_native/", "archetype_transports/")
                ):
                    files[name] = hashlib.sha256(archive.read(name)).hexdigest()

    def audit(event, values):
        if (
            event == "ctypes.dlopen"
            and values
            and values[0] is not None
            and "archetype_ddlog_python" in str(values[0])
        ):
            raise RuntimeError("Analysis values must not open the native execution library")

    sys.addaudithook(audit)
    success = False
    origins = []
    errors = []
    try:
        from archetype import Component, GraderContract, Outcome
        from archetype.core.component import Component as CanonicalComponent
        from archetype.evaluation.contracts import (
            GraderContract as CanonicalGraderContract,
        )
        from archetype.evaluation.contracts import (
            Outcome as CanonicalOutcome,
        )

        if (Component, Outcome, GraderContract) != (
            CanonicalComponent,
            CanonicalOutcome,
            CanonicalGraderContract,
        ):
            raise RuntimeError("Retained root export identity differs")

        class DeclarationProbe(Component):
            count: int
            ratio: float
            flag: bool

        value = DeclarationProbe(count=3, ratio=0.5, flag=True)
        if value.to_row_dict() != {
            "declarationprobe__count": 3,
            "declarationprobe__ratio": 0.5,
            "declarationprobe__flag": True,
        }:
            raise RuntimeError("Component declaration/prefix contract differs")
        import pyarrow as pa

        schema = DeclarationProbe.get_prefixed_schema()
        if {field.name: field.type for field in schema} != {
            "declarationprobe__count": pa.int64(),
            "declarationprobe__ratio": pa.float64(),
            "declarationprobe__flag": pa.bool_(),
        }:
            raise RuntimeError("Component Arrow schema differs")
        for status in ("pass", "fail", "invalid", "inconclusive"):
            if Outcome(status, score=0.5).status != status:
                raise RuntimeError("Outcome vocabulary differs")
        for kwargs in (
            {"status": "unknown"},
            {"status": "pass", "score": float("nan")},
            {"status": "pass", "score": float("inf")},
        ):
            try:
                Outcome(**kwargs)
            except ValueError:
                pass
            else:
                raise RuntimeError("Invalid outcome accepted")
        contract = GraderContract(
            grader_id="mean-reading-v1",
            implementation_version="2026.07.15",
            config={"prompt": "grade the mean", "temperature": 0.0},
            thresholds={"min": 0.5},
            seed=7,
        )
        if contract.digest() != "1a564400f48bb599ae183c9a06edcfcbd6336cc60b6801a8acef8cb875619b6f":
            raise RuntimeError("Deterministic grading digest differs")
        success = True
    finally:
        prefix = Path(sys.prefix).resolve()
        for name, module in sorted(sys.modules.items()):
            if not (
                name == "archetype"
                or name.startswith(("archetype.", "archetype_native", "archetype_transports"))
            ):
                continue
            filename = getattr(module, "__file__", None)
            if filename is None:
                continue
            item = {"module": name, "file": filename, "validated": False}
            try:
                path = Path(filename).resolve()
                relative = path.relative_to(prefix / "lib/python3.12/site-packages").as_posix()
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                package = (
                    "archetype-native"
                    if relative.startswith("archetype_native/")
                    else "archetype-transports"
                    if relative.startswith("archetype_transports/")
                    else "archetype-smol"
                    if relative.startswith("archetype/smol/")
                    else "archetype-ecs"
                )
                if (
                    files.get(relative) != digest
                    or hashlib.sha256(
                        (args.source / "packages" / package / "src" / relative).read_bytes()
                    ).hexdigest()
                    != digest
                ):
                    raise RuntimeError("Installed/source/wheel content differs")
                item.update(sha256=digest, validated=True)
            except Exception as error:
                item["error"] = type(error).__name__
                errors.append(item)
            origins.append(item)
        if not origins:
            errors.append({"error": "No installed module proof"})
        args.out.write_text(
            json.dumps(
                {
                    "schema": "archetype.analysis-values/v1",
                    "result": "pass" if success and not errors else "fail",
                    "mode": "installed analysis-extra declarations and grading values outside live execution",
                    "origins": origins,
                    "errors": errors,
                },
                indent=2,
            )
        )
    if not success or errors:
        raise SystemExit("Installed retained values failed")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Require exact-wheel public acceptance using actual DDlog and cold HTTP/MCP.

The ordinary simulated source profile is separate. This runner refuses missing
actual-driver configuration, checkout product imports and insufficient capacity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            result.update(chunk)
    return result.hexdigest()


def execute(command, *, environment, cwd, log):
    with log.open("wb") as output:
        subprocess.run(
            command, env=environment, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, check=True
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", required=True, type=Path)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Local candidate evidence only; never release acceptance",
    )
    parser.add_argument(
        "--candidate-dir", type=Path, help="Consume sealed artifacts without rebuilding"
    )
    parser.add_argument(
        "--candidate-manifest", type=Path, help="Exact eight-artifact release manifest"
    )
    args = parser.parse_args()
    if (args.candidate_dir is None) != (args.candidate_manifest is None):
        raise SystemExit("Candidate directory and manifest must be supplied together")
    if args.candidate_dir is not None and args.allow_dirty:
        raise SystemExit("Sealed candidate acceptance requires clean source")
    stage, library, driver = args.stage.resolve(), args.library.resolve(), args.driver.resolve()
    if not library.is_file() or not driver.is_file() or not os.access(driver, os.X_OK):
        raise SystemExit("Actual native library and executable real DDlog driver are mandatory")
    if stage.exists():
        raise SystemExit("Use a fresh stage; previous evidence is never overwritten")
    if shutil.disk_usage(stage.parent).free < 2 << 30:
        raise SystemExit(
            "At least 2 GiB measured free capacity is required before actual compilation"
        )
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    if dirty and not args.allow_dirty:
        raise SystemExit("Release acceptance requires a clean exact source checkout")
    stage.mkdir(parents=True)
    revision = re.search(
        r'pub const DDLOG_REVISION: &str = "([0-9a-f]{40})"',
        (ROOT / "crates/archetype-ddlog/src/lib.rs").read_text(),
    ).group(1)
    source = {
        str(path.relative_to(ROOT)): digest(path)
        for package in (
            "archetype-ecs",
            "archetype-native",
            "archetype-transports",
            "archetype-smol",
        )
        for path in (ROOT / "packages" / package / "src").rglob("*.py")
        if "__pycache__" not in path.parts
    }
    (stage / "source-identity.json").write_text(
        json.dumps(
            {
                "head": subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
                ).strip(),
                "clean": not bool(dirty),
                "files": source,
                "library_sha256": digest(library),
                "ddlog_revision": revision,
                "rust_sources": {
                    str(p.relative_to(ROOT)): digest(p)
                    for name in ("archetype-ddlog", "archetype-ddlog-python")
                    for p in (ROOT / "crates" / name).rglob("*.rs")
                    if "target" not in p.parts
                },
                "driver_sha256": digest(driver),
            },
            indent=2,
        )
    )
    candidate_manifest = None
    if args.candidate_dir is not None:
        from release_artifact import copy_candidate

        candidate_manifest = json.loads(args.candidate_manifest.resolve().read_text())
        copy_candidate(
            candidate_manifest,
            args.candidate_dir.resolve(),
            stage / "wheels",
            expected_commit=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
        )
        (stage / "candidate-manifest.json").write_text(json.dumps(candidate_manifest, indent=2))
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE", "ARCHETYPE_ACCEPTANCE_EXAMPLE"}
    }
    environment.update(
        PYTHONOPTIMIZE="0",
        DO_NOT_TRACK="1",
        PYTHONDONTWRITEBYTECODE="1",
        UV_PROJECT_ENVIRONMENT=str(stage / "env"),
        UV_LINK_MODE="hardlink",
    )
    execute(
        [
            "uv",
            "sync",
            "--frozen",
            "--python",
            "3.12",
            "--all-packages",
            "--all-extras",
            "--group",
            "dev",
        ],
        environment=environment,
        cwd=ROOT,
        log=stage / "dependencies.log",
    )
    if candidate_manifest is None:
        execute(
            ["uv", "build", "--all-packages", "--out-dir", str(stage / "wheels")],
            environment=environment,
            cwd=ROOT,
            log=stage / "wheels.log",
        )
    wheels = sorted((stage / "wheels").glob("*.whl"))
    if len(wheels) != 4:
        raise SystemExit("Expected exactly ECS/native/transports/Smol wheels")
    python = stage / "env/bin/python"
    execute(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--no-deps",
            "--reinstall",
            *map(str, wheels),
        ],
        environment=environment,
        cwd=stage,
        log=stage / "install.log",
    )
    fixture = stage / "fixture"
    for relative in (
        "packages/archetype-ecs/tests",
        "packages/archetype-native/tests",
        "packages/archetype-transports/tests",
        "crates/archetype-ddlog/tests/fixtures",
    ):
        shutil.copytree(
            ROOT / relative,
            fixture / relative,
            ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"),
        )
    ingress = fixture / "packages/archetype-native/tests/test_ingress.py"
    ingress.write_text(
        ingress.read_text().replace(
            'sys.path.insert(0, str(REPO / "packages/archetype-ecs/src"))', ""
        )
    )
    shutil.copyfile(ROOT / "examples/native_simulation.py", fixture / "native_simulation.py")
    wheels_identity = {
        path.name: digest(path) for path in (stage / "wheels").iterdir() if path.is_file()
    }
    (stage / "wheel-identity.json").write_text(json.dumps(wheels_identity, indent=2))
    runner = stage / "run-installed.py"
    runner.write_text(
        'import hashlib, importlib, importlib.metadata, json, os, pathlib, runpy, sys, unittest, zipfile\nif sys.flags.optimize: raise RuntimeError("Optimized execution cannot verify acceptance assertions")\nstage=pathlib.Path(__file__).resolve().parent\nrepo=pathlib.Path(os.environ["ARCHETYPE_ACCEPTANCE_SOURCE"])\nexample=os.environ.get("ARCHETYPE_ACCEPTANCE_EXAMPLE")\nlabel="example-" if example else ""\nexpected_library=pathlib.Path(os.environ["ARCHETYPE_ACCEPTANCE_LIBRARY"]).resolve()\nexpected_sha=os.environ["ARCHETYPE_ACCEPTANCE_LIBRARY_SHA256"]\nlibraries=[]\nrevisions=[]\nexpected_revision=os.environ["ARCHETYPE_ACCEPTANCE_DDLOG_REVISION"]\norigins=[]\nvalidation_errors=[]\nfiles={}\nresult=None\nfailure=None\nsuccessful=False\ndef audit(event,args):\n    if event!="ctypes.dlopen" or not args or args[0] is None:return\n    selected=pathlib.Path(args[0])\n    if "archetype_ddlog_python" not in selected.name:return\n    resolved=selected.resolve()\n    actual=hashlib.sha256(resolved.read_bytes()).hexdigest()\n    valid=resolved==expected_library and actual==expected_sha\n    libraries.append({"selected":str(selected),"resolved":str(resolved),"sha256":actual,"validated":valid})\n    assert valid,"Unexpected native library selected in child"\nsys.addaudithook(audit)\ntry:\n    assert os.environ.get("ARCHETYPE_ACCEPTANCE_DRIVER"),"Actual driver is mandatory"\n    for name in ("archetype-ecs","archetype-native","archetype-transports"):\n        sys.path.insert(0,str(stage/"fixture/packages"/name/"tests"))\n    for name in ("archetype","archetype.runtime","archetype_native","archetype_transports"):\n        module=importlib.import_module(name)\n        assert str(pathlib.Path(module.__file__).resolve()).startswith(str(stage/"env")),(name,module.__file__)\n    assert not any(name=="daft" or name.startswith(("daft.","archetype.core.")) for name in sys.modules)\n    for wheel in (stage/"wheels").glob("*.whl"):\n        with zipfile.ZipFile(wheel) as archive:\n            for name in archive.namelist():\n                if name.endswith(".py") and name.startswith(("archetype/","archetype_native/","archetype_transports/")):\n                    files[name]=hashlib.sha256(archive.read(name)).hexdigest()\n    from archetype_native import Host\n    original_init=Host.__init__\n    def checked_init(self,*args,**kwargs):\n        original_init(self,*args,**kwargs)\n        revisions.append(self.ddlog_revision)\n        assert self.ddlog_revision==expected_revision,"Native upstream revision differs from pinned source"\n    Host.__init__=checked_init\n    if example:\n        runpy.run_path(example,run_name="__main__")\n        successful=True\n    else:\n        result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromNames(["test_native_runtime_binding"]))\n        successful=result.wasSuccessful() and not result.skipped and result.testsRun==1\nexcept BaseException as error:\n    failure=f"{type(error).__name__}: {error}"\n    raise\nfinally:\n    for name,module in sorted(sys.modules.items()):\n        if not (name=="archetype" or name.startswith(("archetype.","archetype_native","archetype_transports"))):continue\n        path=getattr(module,"__file__",None)\n        if path is None:continue\n        entry={"module":name,"file":str(path),"validated":False}\n        try:\n            path=pathlib.Path(path).resolve()\n            assert path.is_relative_to(stage/"env"),"checkout/foreign product import"\n            relative=path.relative_to(stage/"env/lib/python3.12/site-packages").as_posix()\n            actual=hashlib.sha256(path.read_bytes()).hexdigest()\n            entry["sha256"]=actual\n            assert actual==files[relative],"wheel/install drift"\n            package="archetype-native" if relative.startswith("archetype_native/") else "archetype-transports" if relative.startswith("archetype_transports/") else "archetype-ecs"\n            assert actual==hashlib.sha256((repo/"packages"/package/"src"/relative).read_bytes()).hexdigest(),"source/wheel drift"\n            entry["validated"]=True\n        except Exception as error:\n            entry["error"]=f"{type(error).__name__}: {error}"\n            validation_errors.append({"module":name,"error":entry["error"]})\n        origins.append(entry)\n    if not libraries or not all(entry["validated"] for entry in libraries):\n        validation_errors.append({"error":"No validated actual C ABI library loaded in child"})\n    if not revisions or any(revision!=expected_revision for revision in revisions):\n        validation_errors.append({"error":"No matching native upstream revision observed"})\n    (stage/(label+"native-revisions.json")).write_text(json.dumps({"expected":expected_revision,"observed":revisions},indent=2))\n    (stage/(label+"installed-origins.json")).write_text(json.dumps(origins,indent=2))\n    (stage/(label+"loaded-libraries.json")).write_text(json.dumps(libraries,indent=2))\n    receipt={"schema":"archetype.installed-native/v1","mode":"installed actual-DDlog documented example" if example else "actual-DDlog/Iceberg/public-Python-HTTP-MCP","tests_run":None if result is None else result.testsRun,"failures":None if result is None else len(result.failures),"errors":None if result is None else len(result.errors),"skipped":None if result is None else len(result.skipped),"failure":failure,"validation_errors":validation_errors,"result":"pass" if successful and not validation_errors else "fail","origins":len(origins),"versions":{name:importlib.metadata.version(name) for name in ("archetype-ecs","archetype-native","archetype-transports","archetype-smol","mcp","starlette","httpx2")}}\n    (stage/(label+"result.json")).write_text(json.dumps(receipt,indent=2))\nraise SystemExit(not successful or bool(validation_errors))\n'
    )
    environment.update(
        DDLOG_PYTHON_LIBRARY=str(library),
        ARCHETYPE_ACCEPTANCE_DRIVER=str(driver),
        ARCHETYPE_ACCEPTANCE_SOURCE=str(ROOT),
        ARCHETYPE_ACCEPTANCE_LIBRARY=str(library),
        ARCHETYPE_ACCEPTANCE_LIBRARY_SHA256=digest(library),
        ARCHETYPE_ACCEPTANCE_DDLOG_REVISION=revision,
    )
    if shutil.disk_usage(stage).free < 2 << 30:
        raise SystemExit(
            "At least 2 GiB free capacity is required after environment and wheel setup"
        )
    task_temp = stage / "temporary"
    contract_temp = task_temp / "contract"
    example_temp = task_temp / "example"
    contract_temp.mkdir(parents=True)
    example_temp.mkdir()
    environment.update(TMPDIR=str(contract_temp), TEMP=str(contract_temp), TMP=str(contract_temp))
    try:
        execute(
            [str(python), str(runner)],
            environment=environment,
            cwd=stage,
            log=stage / "actual-public-contract.log",
        )
        contract = json.loads((stage / "result.json").read_text())
        if (
            contract.get("mode") != "actual-DDlog/Iceberg/public-Python-HTTP-MCP"
            or contract.get("result") != "pass"
            or any(
                contract.get(key) != value
                for key, value in {"tests_run": 1, "failures": 0, "errors": 0, "skipped": 0}.items()
            )
        ):
            raise RuntimeError("Actual contract child did not produce complete passing evidence")
        example_store = stage / "example"
        environment.update(
            ARCHETYPE_NATIVE_LIBRARY=str(library),
            ARCHETYPE_STORE=str(example_store / "storage"),
            ARCHETYPE_REGISTRY=str(example_store / "registry"),
            ARCHETYPE_BUILDS=str(example_store / "builds"),
            ARCHETYPE_NATIVE_DRIVER=str(driver),
            ARCHETYPE_ACCEPTANCE_EXAMPLE=str(fixture / "native_simulation.py"),
            TMPDIR=str(example_temp),
            TEMP=str(example_temp),
            TMP=str(example_temp),
        )
        execute(
            [str(python), str(runner)],
            environment=environment,
            cwd=stage,
            log=stage / "installed-example.log",
        )
        example = json.loads((stage / "example-result.json").read_text())
        if (
            example.get("mode") != "installed actual-DDlog documented example"
            or example.get("result") != "pass"
            or example.get("failure") is not None
            or example.get("validation_errors")
            or not example.get("origins")
        ):
            raise RuntimeError("Documented example child did not produce complete passing evidence")
        execute(
            [
                str(python),
                str(ROOT / "scripts/run_consumer_acceptance.py"),
                "--stage",
                str(stage / "consumers"),
                "--product-wheels",
                str(stage / "wheels"),
                "--library",
                str(library),
            ],
            environment=environment,
            cwd=stage,
            log=stage / "consumer-acceptance.log",
        )
        roots = sorted(contract_temp.glob("ddlog-python-*"))
        if len(roots) != 1:
            raise RuntimeError("Expected one retained public-contract store")
        execute(
            [
                str(python),
                str(ROOT / "scripts/run_operator_acceptance.py"),
                "--stage",
                str(stage / "operator"),
                "--python",
                str(python),
                "--product-wheels",
                str(stage / "wheels"),
                "--library",
                str(library),
                "--store",
                str(roots[0] / "storage"),
            ],
            environment=environment,
            cwd=stage,
            log=stage / "operator-acceptance.log",
        )
    finally:
        roots = sorted(contract_temp.glob("ddlog-python-*"))
        (stage / "retained-native-roots.json").write_text(
            json.dumps([str(path) for path in roots], indent=2)
        )
        # Child temp roots and the example store already live inside the retained
        # stage. Map external source links without copying shared toolchain caches.
        links = []
        for directory in (task_temp, stage / "example"):
            for path in directory.rglob("*"):
                if not path.is_symlink():
                    continue
                target = path.resolve()
                entry = {"path": str(path.relative_to(stage)), "target": str(target)}
                if target.is_file():
                    entry.update(size_bytes=target.stat().st_size, sha256=digest(target))
                elif target.is_dir():
                    entry["source_files"] = {
                        str(source.relative_to(target)): digest(source)
                        for pattern in ("*.rs", "*.dl", "Cargo.toml", "Cargo.lock")
                        for source in target.rglob(pattern)
                        if source.is_file()
                    }
                else:
                    entry["status"] = (
                        "unresolved link; inspect explicit intermediate cleanup receipt"
                    )
                links.append(entry)
        (stage / "native-source-links.json").write_text(json.dumps(links, indent=2))
    if candidate_manifest is not None:
        from release_artifact import verify

        expected_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        verify(candidate_manifest, args.candidate_dir.resolve(), expected_commit=expected_commit)
        verify(candidate_manifest, stage / "wheels", expected_commit=expected_commit)
    print("Installed actual native acceptance:", stage)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

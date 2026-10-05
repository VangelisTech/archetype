#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Build selected actual consumer ports and verify installed native migration."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def execute(command, environment, cwd, log):
    with log.open("wb") as output:
        subprocess.run(
            command, env=environment, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, check=True
        )


CHILD = r"""import hashlib, importlib, importlib.metadata, json, os, pathlib, sys, zipfile
if sys.flags.optimize: raise RuntimeError("Optimized execution cannot verify acceptance assertions")
import pytest
stage=pathlib.Path(__file__).resolve().parent
source=pathlib.Path(os.environ["CONSUMER_PRODUCT_SOURCE"])
expected_library=pathlib.Path(os.environ["CONSUMER_NATIVE_LIBRARY"]).resolve()
expected_sha=os.environ["CONSUMER_NATIVE_LIBRARY_SHA256"]
expected_revision=os.environ["CONSUMER_DDLOG_REVISION"]
libraries=[]
revisions=[]
origins=[]
errors=[]
reports=[]
files={}
success=False
exit_code=None
failure=None

def audit(event,args):
    if event!="ctypes.dlopen" or not args or args[0] is None:return
    selected=pathlib.Path(args[0])
    if "archetype_ddlog_python" not in selected.name:return
    resolved=selected.resolve()
    sha=hashlib.sha256(resolved.read_bytes()).hexdigest()
    valid=resolved==expected_library and sha==expected_sha
    libraries.append({"selected":str(selected),"resolved":str(resolved),"sha256":sha,"validated":valid})
    if not valid:raise RuntimeError("Unexpected consumer native library")
sys.addaudithook(audit)
class Reports:
    def pytest_runtest_logreport(self,report):
        if report.when=="call" or report.failed or report.skipped:
            reports.append({"nodeid":report.nodeid,"when":report.when,"outcome":report.outcome})
try:
    from archetype_native import Host
    original=Host.__init__
    def observed(self,*args,**kwargs):
        original(self,*args,**kwargs)
        revisions.append(self.ddlog_revision)
        if self.ddlog_revision!=expected_revision:raise RuntimeError("Consumer DLL does not match required upstream revision")
    Host.__init__=observed
    for name in ("archetype","archetype.runtime","archetype_native","archetype_transports","vangelis_gateway","holocron"):
        module=importlib.import_module(name)
        path=getattr(module,"__file__",None)
        if path is not None and not pathlib.Path(path).resolve().is_relative_to(stage/"env"):
            raise RuntimeError("Consumer imports product checkout: "+name)
    for directory in (stage/"product-wheels",stage/"consumer-wheels"):
        for wheel in directory.glob("*.whl"):
            with zipfile.ZipFile(wheel) as archive:
                for name in archive.namelist():
                    if name.endswith(".py") and name.startswith(("archetype/","archetype_native/","archetype_transports/","vangelis_gateway/","holocron/")):
                        files[name]=hashlib.sha256(archive.read(name)).hexdigest()
    exit_code=int(pytest.main(["-q","-p","pytest_asyncio.plugin","-c",str(stage/"fixture/pytest.ini"),"--import-mode=importlib",str(stage/"fixture/gateway_cases"),str(stage/"fixture/holocron_cases")],plugins=[Reports()]))
    success=exit_code==0 and len([r for r in reports if r["when"]=="call" and r["outcome"]=="passed"])==81 and all(r["outcome"]=="passed" for r in reports)
except BaseException as error:
    failure=f"{type(error).__name__}: {error}"
    raise
finally:
    for name,module in sorted(sys.modules.items()):
        if not (name in {"archetype","archetype_native","archetype_transports","vangelis_gateway","holocron"} or name.startswith(("archetype.","archetype_native.","archetype_transports.","vangelis_gateway.","holocron."))):continue
        filename=getattr(module,"__file__",None)
        if filename is None:continue
        item={"module":name,"file":str(filename),"validated":False}
        try:
            path=pathlib.Path(filename).resolve()
            if not path.is_relative_to(stage/"env"):raise RuntimeError("checkout/foreign module origin")
            relative=path.relative_to(stage/"env/lib/python3.12/site-packages").as_posix()
            sha=hashlib.sha256(path.read_bytes()).hexdigest()
            if files.get(relative)!=sha:raise RuntimeError("wheel/install content differs")
            if relative.startswith("vangelis_gateway/"): original=stage/"inputs/gateway/src"/relative
            elif relative.startswith("holocron/"): original=stage/"inputs/holocron/src"/relative
            else:
                package="archetype-native" if relative.startswith("archetype_native/") else "archetype-transports" if relative.startswith("archetype_transports/") else "archetype-ecs"
                original=source/"packages"/package/"src"/relative
            if digest:=hashlib.sha256(original.read_bytes()).hexdigest():
                if digest!=sha:raise RuntimeError("source/wheel content differs")
            item.update(sha256=sha,validated=True)
        except Exception as error:
            item["error"]=f"{type(error).__name__}: {error}"
            errors.append(item)
        origins.append(item)
    if not libraries or not all(item["validated"] for item in libraries):errors.append({"error":"No verified consumer C ABI load"})
    if not revisions or any(value!=expected_revision for value in revisions):errors.append({"error":"No matching consumer native revision"})
    (stage/"installed-origins.json").write_text(json.dumps(origins,indent=2))
    (stage/"loaded-libraries.json").write_text(json.dumps(libraries,indent=2))
    (stage/"native-revisions.json").write_text(json.dumps({"expected":expected_revision,"observed":revisions},indent=2))
    (stage/"result.json").write_text(json.dumps({"schema":"archetype.consumer-migration/v1","mode":os.environ["CONSUMER_ACCEPTANCE_MODE"],"result":"pass" if success and not errors else "fail","exit_code":exit_code,"failure":failure,"validation_errors":errors,"tests":reports,"origins":len(origins),"versions":{name:importlib.metadata.version(name) for name in ("archetype-ecs","archetype-native","archetype-transports","vangelis-gateway","holocron","pyjwt","cryptography")}},indent=2))
raise SystemExit(not success or bool(errors))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=Path, required=True)
    parser.add_argument("--product-wheels", type=Path, required=True)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument(
        "--prior-revision",
        help="Explicit partial local proof only; never current release acceptance",
    )
    args = parser.parse_args()
    stage = args.stage.resolve()
    if stage.exists():
        raise SystemExit("Use a fresh stage to preserve previous evidence")
    library = args.library.resolve()
    if not library.is_file():
        raise SystemExit("A matched native library is mandatory")
    stage.mkdir(parents=True)
    inputs = stage / "inputs"
    inputs.mkdir()
    fixture_archive = ROOT / "tests/consumers/migration-inputs.tar.gz"
    with tarfile.open(fixture_archive) as archive:
        archive.extractall(inputs, filter="data")
    manifest = json.loads((inputs / "migration-inputs.json").read_text())
    for name, entry in manifest["files"].items():
        if digest(inputs / name) != entry["sha256"]:
            raise RuntimeError("Consumer input differs from recorded snapshot")
    compile(CHILD, "run-installed-consumers.py", "exec")
    product = stage / "product-wheels"
    product.mkdir()
    wheels = list(args.product_wheels.resolve().glob("*.whl"))
    if len(wheels) != 4:
        raise SystemExit("Four exact product wheels required")
    for wheel in wheels:
        shutil.copy2(wheel, product / wheel.name)
    environment = {
        k: v
        for k, v in os.environ.items()
        if k not in {"PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE", "PYTEST_ADDOPTS"}
    }
    environment.update(
        DO_NOT_TRACK="1",
        PYTHONDONTWRITEBYTECODE="1",
        UV_PROJECT_ENVIRONMENT=str(stage / "env"),
        UV_LINK_MODE="hardlink",
        PYTHONOPTIMIZE="0",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
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
        environment,
        ROOT,
        stage / "dependencies.log",
    )
    for family in ("gateway", "holocron"):
        execute(
            ["uv", "build", "--wheel", "--out-dir", str(stage / "consumer-wheels")],
            environment,
            inputs / family,
            stage / (family + "-wheel.log"),
        )
    all_wheels = [*product.glob("*.whl"), *(stage / "consumer-wheels").glob("*.whl")]
    execute(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(stage / "env/bin/python"),
            "--no-deps",
            "--reinstall",
            *map(str, all_wheels),
        ],
        environment,
        stage,
        stage / "install.log",
    )
    fixture = stage / "fixture"
    for family in ("gateway", "holocron"):
        (fixture / (family + "_cases")).mkdir(parents=True)
        for test in (inputs / family / "tests").glob("test_*.py"):
            shutil.copy2(test, fixture / (family + "_cases") / test.name)
    (fixture / "pytest.ini").write_text(
        "[pytest]\nasyncio_mode = auto\nasyncio_default_fixture_loop_scope = function\n"
    )
    revision = re.search(
        r'pub const DDLOG_REVISION: &str = "([0-9a-f]{40})"',
        (ROOT / "crates/archetype-ddlog/src/lib.rs").read_text(),
    ).group(1)
    mode = (
        "installed consumer migration on matched current native"
        if args.prior_revision is None
        else "partial installed consumer migration on explicitly prior native"
    )
    environment.update(
        ARCHETYPE_NATIVE_LIBRARY=str(library),
        DDLOG_PYTHON_LIBRARY=str(library),
        CONSUMER_PRODUCT_SOURCE=str(ROOT),
        CONSUMER_NATIVE_LIBRARY=str(library),
        CONSUMER_NATIVE_LIBRARY_SHA256=digest(library),
        CONSUMER_DDLOG_REVISION=args.prior_revision or revision,
        CONSUMER_ACCEPTANCE_MODE=mode,
    )
    (stage / "input-identity.json").write_text(
        json.dumps(
            {
                "archive_sha256": digest(fixture_archive),
                "manifest": manifest,
                "library_sha256": digest(library),
                "current_revision": revision,
                "required_revision": args.prior_revision or revision,
                "wheels": {wheel.name: digest(wheel) for wheel in all_wheels},
            },
            indent=2,
        )
    )
    runner = stage / "run-installed-consumers.py"
    runner.write_text(CHILD)
    execute(
        [str(stage / "env/bin/python"), str(runner)],
        environment,
        stage,
        stage / "consumer-contracts.log",
    )
    result = json.loads((stage / "result.json").read_text())
    if (
        result.get("mode") != mode
        or result.get("result") != "pass"
        or result.get("validation_errors")
        or len(result.get("tests", [])) != 81
    ):
        raise RuntimeError("Consumer child did not provide complete passing installed evidence")
    print(mode + ": " + str(stage))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

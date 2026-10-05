#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise installed CLI against an owned real loopback cold-storage server."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "operator-fixture-" + "A" * 32
OTHER = "operator-fixture-" + "B" * 32

CHILD = r"""import hashlib, importlib.metadata, json, os, pathlib, signal, socket, sys, zipfile
if sys.flags.optimize: raise RuntimeError("Optimized acceptance is unsupported")
stage=pathlib.Path(os.environ["OPERATOR_STAGE"])
source=pathlib.Path(os.environ["OPERATOR_SOURCE"])
env=pathlib.Path(sys.prefix).resolve()
mode=sys.argv.pop(1)
label=sys.argv.pop(1)
files={}
loads=[]
revisions=[]
errors=[]
origins=[]
for wheel in pathlib.Path(os.environ["OPERATOR_WHEELS"]).glob("*.whl"):
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if name.endswith(".py") and name.startswith(("archetype/","archetype_native/","archetype_transports/")):
                files[name]=hashlib.sha256(archive.read(name)).hexdigest()
def audit(event,args):
    if event!="ctypes.dlopen" or not args or args[0] is None:return
    path=pathlib.Path(args[0]).resolve()
    if "archetype_ddlog_python" not in path.name:return
    sha=hashlib.sha256(path.read_bytes()).hexdigest()
    valid=str(path)==os.environ["OPERATOR_LIBRARY"] and sha==os.environ["OPERATOR_LIBRARY_SHA256"]
    loads.append({"file":str(path),"sha256":sha,"validated":valid})
    if not valid:raise RuntimeError("Unexpected native library")
sys.addaudithook(audit)
try:
    if mode=="server":
        import uvicorn
        from archetype_native import Host
        original=Host.__init__
        def observed(self,*args,**kwargs):
            original(self,*args,**kwargs)
            revisions.append(self.ddlog_revision)
            if self.ddlog_revision!=os.environ["OPERATOR_REVISION"]:raise RuntimeError("Unexpected native revision")
        Host.__init__=observed
        from archetype.api import ServerConfig, create_app
        from archetype.api.config import ContextResource, Grant
        from archetype.api.principals import PrincipalDirectory
        directory=PrincipalDirectory.from_provisioning(tuple({"id":name,"credential_sha256":hashlib.sha256(token.encode()).hexdigest(),"capabilities":["artifacts:read"]} for name,token in (("agent",os.environ["OPERATOR_TOKEN"]),("other",os.environ["OPERATOR_OTHER"]))),{})
        config=ServerConfig(tuple((key,value) for key,value in (("library",os.environ["OPERATOR_LIBRARY"]),("store",os.environ["OPERATOR_STORE"]),("registry",None),("builds",None),("driver",None))), (ContextResource("collection","collection","main"),), (Grant("agent","collection",frozenset({"artifacts:read"})),))
        app=create_app(config=config,verifier=directory)
        server=uvicorn.Server(uvicorn.Config(app,host="127.0.0.1",log_level="warning"))
        sock=socket.socket()
        sock.bind(("127.0.0.1",0))
        sock.listen(128)
        (stage/"endpoint.json").write_text(json.dumps({"url":"http://127.0.0.1:"+str(sock.getsockname()[1])}))
        signal.signal(signal.SIGTERM, lambda *_: setattr(server,"should_exit",True))
        try:server.run(sockets=[sock])
        finally:sock.close()
    else:
        entry=next(item for item in importlib.metadata.distribution("archetype-ecs").entry_points if item.name=="archetype" and item.group=="console_scripts")
        sys.argv=["archetype",*sys.argv[1:]]
        entry.load()()
finally:
    for name,module in sorted(sys.modules.items()):
        if not (name in {"archetype","archetype_native","archetype_transports"} or name.startswith(("archetype.","archetype_native.","archetype_transports."))):continue
        filename=getattr(module,"__file__",None)
        if filename is None:continue
        item={"module":name,"file":str(filename),"validated":False}
        try:
            path=pathlib.Path(filename).resolve()
            if not path.is_relative_to(env):raise RuntimeError("Foreign product import")
            relative=path.relative_to(env/"lib/python3.12/site-packages").as_posix()
            sha=hashlib.sha256(path.read_bytes()).hexdigest()
            package="archetype-native" if relative.startswith("archetype_native/") else "archetype-transports" if relative.startswith("archetype_transports/") else "archetype-ecs"
            if files.get(relative)!=sha or hashlib.sha256((source/"packages"/package/"src"/relative).read_bytes()).hexdigest()!=sha:raise RuntimeError("Source/wheel/install drift")
            item.update(sha256=sha,validated=True)
        except Exception as error:
            item["error"]=str(error)
            errors.append(item)
        origins.append(item)
    if not origins:errors.append({"error":"No installed product imports"})
    if any(name=="daft" or name.startswith("daft.") for name in sys.modules):errors.append({"error":"Unexpected Daft import"})
    if mode=="server" and (not loads or not revisions):errors.append({"error":"No native cold-storage read observed"})
    if mode!="server" and loads:errors.append({"error":"HTTP client loaded native library"})
    (stage/(label+"-identity.json")).write_text(json.dumps({"origins":origins,"libraries":loads,"revisions":revisions,"errors":errors},indent=2))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("stage", "python", "product-wheels", "library", "store"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--prior-revision", help="Explicit partial evidence only")
    args = parser.parse_args()
    stage = args.stage.resolve()
    if stage.exists():
        raise SystemExit("Use a fresh evidence stage")
    stage.mkdir(parents=True)
    library = args.library.resolve()
    revision = re.search(
        r'pub const DDLOG_REVISION: &str = "([0-9a-f]{40})"',
        (ROOT / "crates/archetype-ddlog/src/lib.rs").read_text(),
    ).group(1)
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME", "PYTHONOPTIMIZE"}
    }
    environment.update(
        PYTHONOPTIMIZE="0",
        OPERATOR_STAGE=str(stage),
        OPERATOR_SOURCE=str(ROOT),
        OPERATOR_WHEELS=str(args.product_wheels.resolve()),
        OPERATOR_LIBRARY=str(library),
        OPERATOR_LIBRARY_SHA256=hashlib.sha256(library.read_bytes()).hexdigest(),
        OPERATOR_STORE=str(args.store.resolve()),
        OPERATOR_REVISION=args.prior_revision or revision,
        OPERATOR_TOKEN=TOKEN,
        OPERATOR_OTHER=OTHER,
    )
    compile(CHILD, "installed-operator.py", "exec")
    runner = stage / "installed-operator.py"
    runner.write_text(CHILD)
    python = str(args.python.absolute())
    cases = []
    success = False
    failure = None
    with (stage / "server.log").open("wb") as server_log:
        server = subprocess.Popen(
            [python, str(runner), "server", "server"],
            cwd=stage,
            env=environment,
            stdout=server_log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while not (stage / "endpoint.json").exists():
                if server.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Owned loopback server failed to bind")
                time.sleep(0.05)
            url = json.loads((stage / "endpoint.json").read_text())["url"]
            import urllib.error
            import urllib.request

            while True:
                try:
                    urllib.request.urlopen(url + "/invoke", timeout=1).close()
                    break
                except urllib.error.HTTPError:
                    break
                except OSError:
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("Owned server failed to become ready") from None
                    time.sleep(0.05)
            document = {
                "version": 1,
                "resource": "collection",
                "operation": "read_context",
                "arguments": {},
            }
            request = stage / "request.json"
            request.write_text(json.dumps(document))
            for label, token, code, expected in (
                ("success", TOKEN, 0, None),
                ("ungranted", OTHER, 1, "forbidden"),
                ("unauthenticated", "operator-fixture-" + "C" * 32, 1, "unauthenticated"),
            ):
                completed = subprocess.run(
                    [
                        python,
                        str(runner),
                        "cli",
                        label,
                        "invoke",
                        str(request),
                        "--url",
                        url,
                        "--token",
                        token,
                    ],
                    cwd=stage,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=35,
                )
                (stage / (label + ".stdout")).write_text(completed.stdout)
                (stage / (label + ".stderr")).write_text(completed.stderr)
                if label == "unauthenticated":
                    if (
                        completed.returncode != 1
                        or completed.stdout
                        or completed.stderr.strip()
                        != "Invalid server response; dispatched outcome is unknown"
                    ):
                        raise RuntimeError(
                            "Installed CLI did not fail closed on SDK authentication refusal"
                        )
                    cases.append(
                        {
                            "case": "SDK authentication refusal",
                            "returncode": 1,
                            "diagnostic": completed.stderr.strip(),
                        }
                    )
                    continue
                response = json.loads(completed.stdout)
                if completed.returncode != code or (
                    expected is None
                    and (
                        response.get("resource") != "collection"
                        or response.get("operation") != "read_context"
                    )
                ):
                    raise RuntimeError("Installed CLI response identity/exit code differs")
                if expected is None:
                    if (
                        not response["ok"]
                        or response["value"]["origin"] != "artifact_collection"
                        or not response["value"]["context_id"]
                    ):
                        raise RuntimeError("Installed CLI did not read retained artifact context")
                elif response != {
                    "version": 1,
                    "ok": False,
                    "error": {"code": expected, "outcome": "not_dispatched"},
                }:
                    raise RuntimeError("Installed CLI authorization failed open")
                cases.append(
                    {"case": label, "returncode": completed.returncode, "response": response}
                )
            success = True
        except Exception as error:
            failure = f"Operator acceptance failed: {type(error).__name__}"
        finally:
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
            identities = {
                path.stem: json.loads(path.read_text()) for path in stage.glob("*-identity.json")
            }
            if (
                len(identities) != 4
                or any(value["errors"] for value in identities.values())
                or server.returncode != 0
            ):
                success = False
                failure = failure or "Incomplete installed process identity or server shutdown"
            (stage / "result.json").write_text(
                json.dumps(
                    {
                        "schema": "archetype.installed-operator/v1",
                        "mode": "current installed loopback CLI"
                        if args.prior_revision is None
                        else "partial installed loopback CLI on prior native",
                        "result": "pass" if success else "fail",
                        "failure": failure,
                        "cases": cases,
                        "identities": identities,
                        "server_returncode": server.returncode,
                    },
                    indent=2,
                )
            )
    if not success:
        raise SystemExit(failure)
    print("Installed operator acceptance:", stage)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# Adapted from ddlog-runtime MIT-licensed transaction fixture at 3451df0.
"""SIMULATED transaction transport only; not a Datalog evaluator or evidence."""
import json
import os
from pathlib import Path
import re
import sys
import time

control = Path(__CONTROL__)
source = Path(__file__).with_name("program.dl").read_text()
arity = {name: len(re.findall(r"f\d+:", fields)) for name, fields in
         re.findall(r"(?:input|output) relation R_(\w+)\(([^\n]*)\)", source)}
inputs = re.findall(r"input relation R_(\w+)\(", source)
facts, staged = {}, {}
for raw in sys.stdin:
    command = raw.strip()
    if command.startswith(("commit", "dump")):
        with (control / "commands").open("a") as log:
            log.write(command + "\n")
    if command == "start;":
        staged = {name: set(rows) for name, rows in facts.items()}
    elif command.startswith(("insert R_", "delete R_")):
        match = re.fullmatch(r"(insert|delete) R_(\w+)\((.*)\);", command)
        operation, name, args = match.groups()
        value = json.dumps(json.loads("[" + args + "]"), ensure_ascii=False)
        relation = staged.setdefault(name, set())
        (relation.add if operation == "insert" else relation.discard)(value)
    elif command.startswith("commit"):
        facts = staged
        if (control / f"hold_commit_{os.getpid()}").exists():
            (control / f"commit_held_{os.getpid()}").touch()
            while (control / f"hold_commit_{os.getpid()}").exists():
                time.sleep(0.005)
        if (control / "die_on_commit").exists():
            os._exit(42)
        if (control / "fail_replay").exists() and command == "commit;":
            print("error: simulated replay rejection", flush=True)
        if (control / "large_deltas").exists() and command == "commit dump_changes;":
            for i in range(100):
                print('R_unrelated{.f0 = "' + "x" * 65536 + '"}: +1', flush=True)
    elif command.startswith("dump R_"):
        name = command[len("dump R_"):-1]
        if (control / "malformed_query").exists():
            print("unexpected native output", flush=True)
        elif (control / "oversized_query").exists():
            print('R_' + name + '{.f0 = 1, .f1 = "' + "x" * (4 * 1024 * 1024) + '"}', flush=True)
        else:
            selected = sorted(facts.get("unused" if name == "unrelated" else inputs[0], set()))
            if (control / "expanded_output").exists():
                selected = [json.dumps([i, "x" * 4000]) for i in range(1200)]
            for row in selected:
                values = json.loads(row)[:arity[name]]
                fields = ", ".join(f".f{i} = {json.dumps(v, ensure_ascii=False)}" for i, v in enumerate(values))
                print("R_" + name + "{" + fields + "}", flush=True)
    elif command.startswith("echo "):
        print(command[5:-1], flush=True)

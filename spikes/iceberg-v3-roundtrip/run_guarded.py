#!/usr/bin/env python3
"""Run only this spike's command, stopping its process group before disk fills."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--timeout", type=int, default=900)
parser.add_argument("--minimum-free-mib", type=int, default=900)
parser.add_argument("--log", type=Path, required=True)
parser.add_argument("command", nargs=argparse.REMAINDER)
args = parser.parse_args()
command = args.command[1:] if args.command[:1] == ["--"] else args.command
if not command:
    parser.error("a command is required")
directory = Path(__file__).resolve().parent
minimum = args.minimum_free_mib * 1024**2
initial_free = shutil.disk_usage(directory).free
if initial_free < minimum:
    raise SystemExit("Insufficient free space before launch")
started = time.monotonic()
lowest_free = initial_free
reason = None
with args.log.open("w") as log:
    process = subprocess.Popen(command, cwd=directory, stdout=log,
                               stderr=subprocess.STDOUT, start_new_session=True)
    while process.poll() is None:
        free = shutil.disk_usage(directory).free
        lowest_free = min(lowest_free, free)
        if free < minimum:
            reason = "disk floor"
        elif time.monotonic() - started > args.timeout:
            reason = "timeout"
        if reason:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            break
        time.sleep(1)
result = dict(command=command, returncode=process.returncode, stopped_for=reason,
              seconds=round(time.monotonic() - started, 2),
              initial_free_mib=initial_free // 1024**2,
              minimum_free_mib=lowest_free // 1024**2)
args.log.with_suffix(args.log.suffix + ".json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
raise SystemExit(process.returncode if process.returncode is not None and not reason else 1)

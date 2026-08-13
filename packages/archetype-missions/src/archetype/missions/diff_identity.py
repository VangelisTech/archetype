# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical Git diff identity shared by Mission authors and critics."""

from typing import Final

GIT_DIFF_IDENTITY_FLAGS: Final = (
    "--no-ext-diff",
    "--no-textconv",
    "--binary",
)

FILE_MEASUREMENT_SCRIPT: Final = """\
import hashlib
import json
import sys

digest = hashlib.sha256()
size = 0
with open(sys.argv[1], "rb") as source:
    for chunk in iter(lambda: source.read(1 << 20), b""):
        digest.update(chunk)
        size += len(chunk)
print(json.dumps({"digest": digest.hexdigest(), "size_bytes": size}, sort_keys=True))
"""

GIT_DIFF_MEASUREMENT_SCRIPT: Final = """\
import hashlib
import json
import subprocess
import sys

process = subprocess.Popen(
    ["git", "diff", *sys.argv[3:], sys.argv[1], sys.argv[2]],
    stdout=subprocess.PIPE,
)
assert process.stdout is not None
digest = hashlib.sha256()
size = 0
for chunk in iter(lambda: process.stdout.read(1 << 20), b""):
    digest.update(chunk)
    size += len(chunk)
returncode = process.wait()
if returncode:
    raise SystemExit(returncode)
print(json.dumps({"digest": digest.hexdigest(), "size_bytes": size}, sort_keys=True))
"""

__all__ = [
    "FILE_MEASUREMENT_SCRIPT",
    "GIT_DIFF_IDENTITY_FLAGS",
    "GIT_DIFF_MEASUREMENT_SCRIPT",
]

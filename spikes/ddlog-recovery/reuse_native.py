#!/usr/bin/env python3
"""Fixture-owned verified reuse. This never invokes a compiler."""
from pathlib import Path
import hashlib
import json
import os
import sys

config = json.loads(Path(os.environ['DDLOG_RECOVERY_ARTIFACT']).read_text())
source, destination = map(Path, sys.argv[1:])
for name, expected in config['generated_files'].items():
    actual = hashlib.sha256((source.parent / name).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f'Native reuse rejected: generated {name} changed: {actual}')
artifact = Path(config['native_artifact'])
with artifact.open('rb') as stream:
    actual = hashlib.file_digest(stream, 'sha256').hexdigest()
if actual != config['native_sha256']:
    raise SystemExit('Native reuse rejected: executable hash changed')
# Do not chmod or modify either hardlink: they share the existing immutable bytes.
try:
    os.link(artifact, destination)
    storage = 'hardlink'
except OSError as error:
    import errno
    import shutil
    if error.errno != errno.EXDEV:
        raise
    with artifact.open('rb') as incoming, destination.open('xb') as outgoing:
        shutil.copyfileobj(incoming, outgoing)
    destination.chmod(artifact.stat().st_mode & 0o777)
    storage = 'cross-filesystem copy'

print(json.dumps({'artifact_mode':'verified-reuse','native_builds':0,
                  'native_sha256':actual,'storage':storage}))

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical storage URI handling shared by stores and control catalogs."""

from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")


def local_storage_path(uri: str) -> Path | None:
    """Return the canonical local path for a path or file URI."""
    parsed = urlparse(uri)
    windows_drive = bool(_WINDOWS_DRIVE.match(uri))
    if parsed.scheme.lower() not in ("", "file") and not windows_drive:
        return None

    if parsed.scheme.lower() == "file":
        path = unquote(parsed.path)
        if parsed.netloc and parsed.netloc.lower() != "localhost":
            path = f"//{parsed.netloc}{path}"
        if os.name == "nt" and re.match(r"^/[A-Za-z]:[\\/]", path):
            path = path[1:]
    else:
        path = uri

    return Path(path).expanduser().resolve(strict=False)


def normalized_storage_uri(uri: str) -> str:
    """Return a credential-free canonical value for storage identity."""
    local_path = local_storage_path(uri)
    return str(local_path) if local_path is not None else uri.rstrip("/")

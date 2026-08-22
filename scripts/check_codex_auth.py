#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail fast when the Codex device auth in the broker Volume is missing or stale.

The Modal Agent Mission release lane depends on ``auth.json`` in the Codex auth
Volume. Mission sandboxes never write refreshed tokens back to the Volume, so
the stored refresh token ages until Codex startup fails inside a paid sandbox
mid-release. This preflight reads only the credential's ``last_refresh``
timestamp and fails with an actionable message instead, without printing any
token material.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys


def auth_age_days(payload: dict[str, object], *, now: dt.datetime) -> float:
    """Return the age in days of the credential's last refresh."""

    raw = payload.get("last_refresh")
    if not isinstance(raw, str) or not raw:
        raise ValueError("auth.json has no last_refresh timestamp")
    refreshed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if refreshed.tzinfo is None:
        raise ValueError("auth.json last_refresh must carry a timezone")
    return (now - refreshed).total_seconds() / 86400.0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-age-days", type=float, default=14.0)
    args = parser.parse_args(argv)

    import modal

    volume_name = os.environ.get("CODEX_AUTH_VOLUME", "archetype-codex-auth")
    environment = os.environ.get("CODING_AGENT_MODAL_ENVIRONMENT") or None
    remedy = (
        "run `make codex-login` (one-hour device-auth window) to refresh"
        f" auth.json in Volume {volume_name!r}"
    )
    try:
        volume = modal.Volume.from_name(volume_name, environment_name=environment)
        data = b"".join(volume.read_file("auth.json"))
    except Exception as error:  # noqa: BLE001 - every failure has the same remedy
        print(f"Codex auth preflight failed: {error}; {remedy}", file=sys.stderr)
        return 1

    payload = json.loads(data)
    if not isinstance(payload, dict):
        print(f"Codex auth preflight failed: auth.json is not an object; {remedy}", file=sys.stderr)
        return 1
    try:
        age = auth_age_days(payload, now=dt.datetime.now(dt.UTC))
    except ValueError as error:
        print(f"Codex auth preflight failed: {error}; {remedy}", file=sys.stderr)
        return 1
    if age > args.max_age_days:
        print(
            f"Codex auth preflight failed: auth.json was last refreshed {age:.1f} days ago"
            f" (limit {args.max_age_days:g}); {remedy}",
            file=sys.stderr,
        )
        return 1
    print(f"Codex auth preflight passed: auth.json refreshed {age:.1f} days ago")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the Codex device-auth freshness preflight."""

from __future__ import annotations

import datetime as dt

import pytest

from scripts.check_codex_auth import auth_age_days

_NOW = dt.datetime(2026, 8, 21, 12, 0, tzinfo=dt.UTC)


def test_auth_age_reads_zulu_last_refresh() -> None:
    age = auth_age_days({"last_refresh": "2026-08-14T12:00:00Z"}, now=_NOW)
    assert age == pytest.approx(7.0)


def test_auth_age_reads_offset_last_refresh() -> None:
    age = auth_age_days({"last_refresh": "2026-08-21T05:00:00-07:00"}, now=_NOW)
    assert age == pytest.approx(0.0)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"last_refresh": ""},
        {"last_refresh": 12345},
    ],
)
def test_auth_age_rejects_missing_or_untyped_last_refresh(payload: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="last_refresh"):
        auth_age_days(payload, now=_NOW)


def test_auth_age_rejects_naive_last_refresh() -> None:
    with pytest.raises(ValueError, match="timezone"):
        auth_age_days({"last_refresh": "2026-08-14T12:00:00"}, now=_NOW)

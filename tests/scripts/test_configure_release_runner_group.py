# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for the operator-only release runner-group configurator."""

from __future__ import annotations

from typing import Any

import pytest

from scripts import configure_release_runner_group as target


def test_runner_group_payload_is_exact_ref_and_deny_by_default() -> None:
    payload = target._group_payload(957180054, "v0.5.0")

    assert payload == {
        "name": "archetype-release-macos",
        "visibility": "selected",
        "allows_public_repositories": True,
        "restricted_to_workflows": True,
        "selected_repository_ids": [957180054],
        "selected_workflows": [
            "VangelisTech/archetype/.github/workflows/release.yml@refs/tags/v0.5.0"
        ],
    }


@pytest.mark.parametrize("tag", ["0.5.0", "v0.5.0/other", "v 0.5.0", "v"])
def test_runner_group_payload_rejects_ambiguous_refs(tag: str) -> None:
    with pytest.raises(ValueError, match="release tag"):
        target._group_payload(957180054, tag)


def test_configure_refuses_to_retarget_a_group_with_a_registered_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(target, "_require_release_ref", lambda tag: (957180054, "main"))
    calls: list[tuple[str, str]] = []

    def fake_api(
        endpoint: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> Any:
        del payload
        calls.append((method, endpoint))
        if endpoint == "orgs/VangelisTech/actions/runner-groups":
            return {"runner_groups": [{"id": 3, "name": target.RUNNER_GROUP}]}
        if endpoint.endswith("/3/runners"):
            return {"total_count": 1}
        raise AssertionError(f"unexpected API call: {method} {endpoint}")

    monkeypatch.setattr(target, "_gh_api", fake_api)

    with pytest.raises(RuntimeError, match="while a runner is registered"):
        target.configure("v0.5.0")

    assert all(method != "PATCH" for method, _ in calls)


def test_configure_restricts_existing_group_before_runner_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(target, "_require_release_ref", lambda tag: (957180054, "main"))
    calls: list[tuple[str, str, dict[str, Any] | None]] = []
    expected_workflow = target._selected_workflow("v0.5.0")

    def fake_api(
        endpoint: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> Any:
        calls.append((method, endpoint, payload))
        if endpoint == "orgs/VangelisTech/actions/runner-groups":
            return {"runner_groups": [{"id": 3, "name": target.RUNNER_GROUP}]}
        if endpoint.endswith("/3/runners"):
            return {"total_count": 0}
        if method == "PATCH":
            assert payload is not None
            return {
                "id": 3,
                **payload,
                "selected_workflows": [expected_workflow],
            }
        if method == "PUT" and endpoint.endswith("/repositories/957180054"):
            return None
        raise AssertionError(f"unexpected API call: {method} {endpoint}")

    monkeypatch.setattr(target, "_gh_api", fake_api)

    group = target.configure("v0.5.0")

    assert group["selected_workflows"] == [expected_workflow]
    patch = next(payload for method, _, payload in calls if method == "PATCH")
    assert patch is not None
    assert "selected_repository_ids" not in patch
    assert patch["restricted_to_workflows"] is True

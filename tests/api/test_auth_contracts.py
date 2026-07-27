# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fail-closed HTTP authentication contracts."""

from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from uuid_utils import uuid7

from archetype.api.app import create_app
from archetype.api.deps import get_dispatcher
from archetype.api.errors import raise_api_error
from archetype.commands.models import ActorCtx


class _ListDispatcher:
    def __init__(self) -> None:
        self.calls = 0

    async def apply_as(self, _actor: ActorCtx, _operation: object) -> list[object]:
        self.calls += 1
        return []


def _client(app, dispatcher: _ListDispatcher) -> TestClient:
    app.dependency_overrides[get_dispatcher] = lambda: dispatcher
    return TestClient(app)


def test_development_auth_is_explicit_and_loopback_only() -> None:
    with pytest.raises(ValueError, match="explicit loopback"):
        create_app(dev_auth=True)
    with pytest.raises(ValueError, match="explicit loopback"):
        create_app(dev_auth=True, bind_host="0.0.0.0")

    dispatcher = _ListDispatcher()
    app = create_app(dev_auth=True, bind_host="127.0.0.1")
    response = _client(app, dispatcher).get(
        "/worlds",
        headers={"Authorization": "Bearer admin"},
    )

    assert response.status_code == 200
    assert dispatcher.calls == 1


@pytest.mark.parametrize(
    "headers",
    ({}, {"Authorization": "Bearer admin"}),
)
def test_absent_injected_authenticator_fails_closed(headers: dict[str, str]) -> None:
    dispatcher = _ListDispatcher()
    response = _client(create_app(), dispatcher).get("/worlds", headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication failed"}
    assert response.headers["www-authenticate"] == "Bearer"
    assert dispatcher.calls == 0


def test_injected_authenticator_is_the_only_non_development_auth_path() -> None:
    calls: list[str] = []
    actor = ActorCtx(id=uuid7(), roles={"viewer"})

    async def authenticate(token: str) -> ActorCtx:
        calls.append(token)
        return actor

    dispatcher = _ListDispatcher()
    response = _client(create_app(authenticator=authenticate), dispatcher).get(
        "/worlds",
        headers={"Authorization": "Bearer opaque-token"},
    )

    assert response.status_code == 200
    assert calls == ["opaque-token"]
    assert dispatcher.calls == 1


def test_authentication_and_authorization_errors_are_generic_publicly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "opaque-secret-never-log"
    dispatcher = _ListDispatcher()
    app = create_app(dev_auth=True, bind_host="localhost")

    with caplog.at_level(logging.INFO, logger="archetype.api.deps"):
        response = _client(app, dispatcher).get(
            "/worlds",
            headers={"Authorization": f"Bearer {secret}"},
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication failed"}
    assert secret not in caplog.text
    assert dispatcher.calls == 0

    internal_detail = "actor private-principal expected role admin"
    with caplog.at_level(logging.INFO, logger="archetype.api.errors"):
        with pytest.raises(HTTPException) as raised:
            raise_api_error(PermissionError(internal_detail))

    assert raised.value.status_code == 403
    assert raised.value.detail == "Forbidden"
    assert internal_detail in caplog.text

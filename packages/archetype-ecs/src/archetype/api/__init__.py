# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Supported HTTP/MCP operator factory and immutable server configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from archetype_native.ingress import PrincipalVerifier

from archetype.api.config import ServerConfig as ServerConfig


def create_app(*, config: ServerConfig | None = None, verifier: PrincipalVerifier | None = None):
    """Construct an inert configured server; its lifespan owns native drain/close."""
    from archetype.api.app import create_app as factory

    return factory(config=config, verifier=verifier)


__all__ = ["ServerConfig", "create_app"]

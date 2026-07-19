# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Persistent artifact ECS schemas: publication identity and asset references.

These Components are historical evidence rows, not authority: no field on
them may name an authority decision (accepted/promote/approved/...), ever.
The content-addressing helpers that build and digest them live in
``archetype.artifacts.contracts``.
"""

from __future__ import annotations

from archetype.core.component import Component


class ArtifactMeta(Component):
    """Publication identity carried by claim-backed artifact rows."""

    producer: str = ""
    external_id: str = ""
    payload_digest: str = ""
    commit_id: str = ""


class AssetRef(Component):
    """Content-addressed reference to an external artifact.

    The digest is the identity; the uri is a hint that may rot. Artifact
    components embed these fields (or this component) to reference sidecar
    artifacts durably.
    """

    digest: str = ""
    uri: str = ""
    media_type: str = ""
    size_bytes: int = 0
    created_at_ms: int = 0

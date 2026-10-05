# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Private version 0.7 composition. No legacy Daft live owner is constructed."""

from __future__ import annotations

from typing import Any

from archetype.api.config import ServerConfig as RuntimeBootstrapConfig


def build_runtime_resources(config: RuntimeBootstrapConfig):
    """Construct one inert supported runtime; the server borrows its native owner."""
    from archetype.runtime.runtime import ArchetypeRuntime

    values = dict(config.native)
    runtime = ArchetypeRuntime(
        library=values["library"],
        store=values["store"],
        registry=values["registry"],
        builds=values["builds"],
        driver=values["driver"],
        storage_only=not any(values[key] for key in ("registry", "builds", "driver")),
    )
    for resource in config.resources:
        runtime._configure(resource)
    return runtime


def build_native_owner(configuration: dict[str, Any]):
    """Private native loader transaction; ABI and contract precede open effects."""
    from archetype_native import Host

    return Host(**configuration)


class _ArtifactWorkflow:
    def __init__(self, host):
        self._host = host

    def upload(self, resource, operation):
        from archetype.artifacts.uploads import publish_upload
        from archetype.storage.context_artifacts import (
            ArtifactTarget,
            ContextArtifactStorage,
            ExactContextCut,
            PublishedContextRef,
        )

        context = PublishedContextRef(resource.world, resource.run, operation.context_id)
        exact = None if operation.exact_cut is None else ExactContextCut(*operation.exact_cut)
        return publish_upload(
            ContextArtifactStorage(self._host, context),
            ArtifactTarget(context, exact),
            artifact_id=operation.artifact_id,
            logical_path=operation.logical_path,
            content=operation.content,
        )

    def prepare_files(self, world, run, context_id, exact_cut, sources):
        from archetype.artifacts.context_attachments import prepare_context_attachments
        from archetype.storage.context_artifacts import (
            ArtifactTarget,
            ContextArtifactStorage,
            ExactContextCut,
            PublishedContextRef,
        )

        context = PublishedContextRef(world, run, context_id)
        target = ArtifactTarget(context, None if exact_cut is None else ExactContextCut(*exact_cut))
        return prepare_context_attachments(
            ContextArtifactStorage(self._host, context), target, sources
        )

    def publish_files(self, prepared):
        from archetype.artifacts.context_attachments import publish_context_attachments
        from archetype.storage.context_artifacts import ContextArtifactStorage

        return publish_context_attachments(
            ContextArtifactStorage(self._host, prepared.target.context), prepared
        )


def artifact_workflow(host):
    return _ArtifactWorkflow(host)

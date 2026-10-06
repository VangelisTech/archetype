# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Common locations promote only after verified publication; scanners stay local."""

from unittest.mock import Mock

import daft
import pytest

from archetype.artifacts._ingestion import publish_objects
from archetype.storage.context_artifacts import (
    ArtifactTarget,
    ContextArtifactStorage,
    PublishedContextRef,
)


def test_native_publication_promotes_common_refs_and_keeps_staged_frame_local():
    digest = "a" * 64
    values = {
        "sha256": [digest, digest],
        "size_bytes": [4, 4],
        "object_uri": ["file:///staged/a", "file:///staged/a"],
    }
    stored = daft.from_pydict(values)
    storage = Mock()
    storage.publish_object.return_value = (
        "s3://synthetic-bucket/task/case/artifact_objects/objects/sha256/aa/" + digest
    )
    target = object()
    common, refs = publish_objects(storage, target, stored, values)
    storage.publish_object.assert_called_once_with(target, sha256=digest, size_bytes=4)
    assert (
        ContextArtifactStorage.materialize(common).to_pydict()["object_uri"] == refs["object_uri"]
    )
    assert (
        ContextArtifactStorage.materialize(stored).to_pydict()["object_uri"] == values["object_uri"]
    )
    assert all(uri.startswith("s3://") for uri in refs["object_uri"])


def test_failed_original_publication_cannot_produce_common_metadata():
    values = {"sha256": ["a" * 64], "size_bytes": [4], "object_uri": ["file:///staged/a"]}
    storage = Mock()
    storage.publish_object.side_effect = RuntimeError("unknown outcome")
    with pytest.raises(RuntimeError):
        publish_objects(storage, object(), daft.from_pydict(values), values)
    storage.encode.assert_not_called()
    storage.publish.assert_not_called()


def test_storage_port_rejects_wrong_target_and_changed_provider_facts_before_index():
    context = PublishedContextRef("files", "main", "a" * 64)
    host = Mock()
    storage = ContextArtifactStorage(host, context)
    other = ArtifactTarget(PublishedContextRef("other", "main", "b" * 64))
    with pytest.raises(ValueError):
        storage.publish_object(other, sha256="c" * 64, size_bytes=4)
    host.request.assert_not_called()
    host.request.return_value = dict(
        object_uri="s3://synthetic-bucket/task/case/object", sha256="d" * 64, size_bytes=4
    )
    with pytest.raises(ValueError):
        storage.publish_object(ArtifactTarget(context), sha256="c" * 64, size_bytes=4)
    assert host.request.call_args.args == ("publish_context_object",)

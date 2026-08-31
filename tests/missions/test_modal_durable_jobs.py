# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Behavior contracts for provider-native durable Mission jobs."""

from __future__ import annotations

from dataclasses import replace

import pytest

from archetype.missions.modal_jobs import (
    ModalMissionJobNamespace,
    ModalMissionJobRef,
    ModalMissionJobUnknown,
    modal_mission_call_record,
    modal_mission_job_key,
    parse_modal_mission_call_record,
)


def _namespace() -> ModalMissionJobNamespace:
    return ModalMissionJobNamespace(
        deployment_digest="a" * 64,
        image_id="im-123",
        result_dict_name="mission-results",
        redaction_policy_id="redaction-v1",
    )


def _ref() -> ModalMissionJobRef:
    return ModalMissionJobRef(
        family="author",
        operation_id="mission:author:dispatch-1",
        request_digest="b" * 64,
        namespace_digest=_namespace().digest,
        call_id="fc-123",
    )


def test_call_record_round_trips_exact_job_identity() -> None:
    ref = _ref()
    encoded = modal_mission_call_record(ref)

    assert parse_modal_mission_call_record(encoded) == ref
    assert modal_mission_job_key("author", ref.operation_id, "call").startswith("author:call:")


def test_call_record_rejects_every_conflicting_identity_coordinate() -> None:
    record = modal_mission_call_record(_ref())
    changes = {
        "family": "critic",
        "operation_id": "mission:author:dispatch-2",
        "request_digest": "c" * 64,
        "namespace_digest": "d" * 64,
        "call_id": "fc-456",
    }
    for field, value in changes.items():
        conflicting = dict(record)
        conflicting[field] = value
        assert parse_modal_mission_call_record(conflicting) != _ref()

    with pytest.raises(ValueError, match="incompatible"):
        parse_modal_mission_call_record({**record, "extra": "field"})


def test_namespace_digest_binds_deployment_image_result_store_and_policy() -> None:
    namespace = _namespace()
    for field, value in {
        "deployment_digest": "e" * 64,
        "image_id": "im-456",
        "result_dict_name": "other-results",
        "redaction_policy_id": "redaction-v2",
    }.items():
        assert replace(namespace, **{field: value}).digest != namespace.digest

    marker = namespace.start_record(
        family="author",
        operation_id=_ref().operation_id,
        request_digest=_ref().request_digest,
    )
    assert marker["namespace_digest"] == namespace.digest
    assert marker["redaction_policy_id"] == namespace.redaction_policy_id


def test_unknown_is_bounded_and_requires_a_reason() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        ModalMissionJobUnknown(_ref(), "")
    with pytest.raises(ValueError, match="4096"):
        ModalMissionJobUnknown(_ref(), "x" * 4097)

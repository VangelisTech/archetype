# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Exact contracts for the canonical whole hosted physical-episode boundary."""

from __future__ import annotations

import copy
import hashlib
import math

import pyarrow as pa
import pytest

from archetype.physical_ai.hosted_episode import (
    HOSTED_EPISODE_CONTRACT_VERSION,
    HOSTED_EPISODE_MANIFEST_SCHEMA,
    HOSTED_EPISODE_REQUEST_SCHEMA,
    HOSTED_EPISODE_RESULT_SCHEMA,
    HOSTED_EPISODE_TERMINATION_REASONS,
    HOSTED_EPISODE_TRAJECTORY_SCHEMA,
    build_hosted_episode_manifest,
    build_hosted_episode_results,
    canonical_hosted_episode_config,
    decode_hosted_episode_manifest,
    decode_hosted_episode_requests,
    decode_hosted_episode_results,
    decode_hosted_episode_trajectory,
    direct_max_steps_from_max_transitions,
    encode_hosted_episode_manifest,
    encode_hosted_episode_requests,
    encode_hosted_episode_results,
    encode_hosted_episode_trajectory,
    hosted_episode_config_digest,
    hosted_episode_id,
    hosted_episode_manifest_digest,
    hosted_episode_request_digest,
    hosted_episode_result_id,
    hosted_episode_results_digest,
    hosted_episode_step_id,
    hosted_episode_trajectory_digest,
    max_transitions_from_direct_max_steps,
    validate_hosted_episode_result,
)


def _request_rows() -> list[dict]:
    # Intentionally reverse trial order and config-key order. The codec owns
    # deterministic ordering and JSON normalization.
    return [
        {
            "operation_id": "physical-eval:42",
            "trial_id": 1,
            "suite": "libero_spatial",
            "task_id": 3,
            "seed": 101,
            "instruction": "put the bowl on the plate",
            "max_transitions": 1,
            "environment_id": "libero@sha256:environment",
            "policy_id": "vla-jepa@sha256:policy",
            "config_json": {"use_sdpa": False, "camera": {"height": 256, "width": 256}},
        },
        {
            "operation_id": "physical-eval:42",
            "trial_id": 0,
            "suite": "libero_spatial",
            "task_id": 3,
            "seed": 100,
            "instruction": "put the bowl on the plate",
            "max_transitions": 2,
            "environment_id": "libero@sha256:environment",
            "policy_id": "vla-jepa@sha256:policy",
            "config_json": {"camera": {"width": 256, "height": 256}, "use_sdpa": False},
        },
    ]


def _proprio(step: int) -> dict:
    return {
        "eef_pos": [0.1 * step, 0.0, 0.5],
        "eef_quat": [1.0, 0.0, 0.0, 0.0],
        "gripper": 0.25,
        "gripper_qpos": [0.25, 0.25],
    }


def _frame(byte: str) -> dict:
    digest = hashlib.sha256(byte.encode()).hexdigest()
    return {
        "content_id": f"sha256:{digest}",
        "sha256": digest,
        "media_type": "image/png",
        "size_bytes": len(byte),
    }


def _trajectory_row(
    request: dict,
    request_digest: str,
    step_index: int,
    *,
    reward: float = 0.0,
    environment_done: bool = False,
    success: bool = False,
    terminal: bool = False,
    termination_reason: str | None = None,
) -> dict:
    operation_id = request["operation_id"]
    episode_id = request["episode_id"]
    return {
        **{
            field: request[field]
            for field in (
                "operation_id",
                "episode_id",
                "trial_id",
                "suite",
                "task_id",
                "seed",
                "instruction",
                "max_transitions",
                "environment_id",
                "policy_id",
                "config_digest",
            )
        },
        "episode_result_id": hosted_episode_result_id(operation_id, episode_id),
        "step_id": hosted_episode_step_id(operation_id, episode_id, step_index),
        "request_digest": request_digest,
        "step_index": step_index,
        "action": None if step_index == 0 else [0.01 * step_index] * 7,
        "proprio": _proprio(step_index),
        "reward": reward,
        "environment_done": environment_done,
        "success": success,
        "terminal": terminal,
        "termination_reason": termination_reason,
        "agentview_frame": _frame(f"agent-{request['trial_id']}-{step_index}"),
        "wrist_frame": None,
    }


def _payloads() -> tuple[bytes, bytes, bytes, bytes]:
    request_ipc = encode_hosted_episode_requests(_request_rows())
    requests = decode_hosted_episode_requests(request_ipc)
    request_digest = hosted_episode_request_digest(request_ipc)
    by_trial = {row["trial_id"]: row for row in requests}
    trajectory_rows = [
        _trajectory_row(
            by_trial[1], request_digest, 1, terminal=True, termination_reason="transition_budget"
        ),
        _trajectory_row(
            by_trial[0],
            request_digest,
            2,
            reward=1.5,
            success=True,
            terminal=True,
            termination_reason="success",
        ),
        _trajectory_row(by_trial[0], request_digest, 0),
        _trajectory_row(by_trial[1], request_digest, 0),
        _trajectory_row(by_trial[0], request_digest, 1, reward=0.5),
    ]
    trajectory_ipc = encode_hosted_episode_trajectory(trajectory_rows)
    results_ipc = build_hosted_episode_results(request_ipc, trajectory_ipc)
    manifest_ipc = build_hosted_episode_manifest(
        request_ipc,
        trajectory_ipc,
        results_ipc,
    )
    return request_ipc, trajectory_ipc, results_ipc, manifest_ipc


def test_canonical_request_is_one_trial_seed_and_episode_per_row() -> None:
    request_ipc = encode_hosted_episode_requests(_request_rows())
    reordered_ipc = encode_hosted_episode_requests(list(reversed(_request_rows())))
    requests = decode_hosted_episode_requests(request_ipc)

    assert request_ipc == reordered_ipc
    assert [row["trial_id"] for row in requests] == [0, 1]
    assert len({row["episode_id"] for row in requests}) == 2
    assert {row["episode_id"] for row in requests} == {
        hosted_episode_id("physical-eval:42", 0),
        hosted_episode_id("physical-eval:42", 1),
    }
    assert requests[0]["config_json"] == ('{"camera":{"height":256,"width":256},"use_sdpa":false}')
    assert requests[0]["config_digest"] == hosted_episode_config_digest(requests[0]["config_json"])


def test_request_digest_is_contract_and_payload_domain_separated() -> None:
    request_ipc = encode_hosted_episode_requests(_request_rows())
    request_digest = hosted_episode_request_digest(request_ipc)

    assert request_digest != hashlib.sha256(request_ipc).hexdigest()
    assert hosted_episode_trajectory_digest(_payloads()[1]) != request_digest
    assert HOSTED_EPISODE_REQUEST_SCHEMA.metadata == {
        b"archetype.contract": HOSTED_EPISODE_CONTRACT_VERSION.encode(),
        b"archetype.payload": b"request",
    }


@pytest.mark.parametrize(
    "config,path",
    [
        ({"provider": {"container_id": "ctr-1"}}, "config_json.provider.container_id"),
        ({"nested": [{"GPU-UUID": "gpu-1"}]}, "config_json.nested[0].GPU-UUID"),
        ({"auth": {"api_key": "secret"}}, "config_json.auth.api_key"),
        ({"frames": {"host_path": "/tmp/frame.png"}}, "config_json.frames.host_path"),
        ({"facts": {"Created-At": "later"}}, "config_json.facts.Created-At"),
        ({"metrics": {"requestTimestampNs": 3}}, "config_json.metrics.requestTimestampNs"),
        ({"metrics": {"LATENCY-MS": 4}}, "config_json.metrics.LATENCY-MS"),
        ({"placement": {"device": "cuda:0"}}, "config_json.placement"),
        ({"runtime": {"Cuda-Device": 0}}, "config_json.runtime.Cuda-Device"),
        ({"runtime": {"gpu": "L40S"}}, "config_json.runtime.gpu"),
        ({"auth": {"Secret-Name": "provider"}}, "config_json.auth.Secret-Name"),
        ({"metrics": {"request-latency-ms": 8}}, "config_json.metrics.request-latency-ms"),
        ({"runtime": {"device-id": "cuda:0"}}, "config_json.runtime.device-id"),
        ({"runtime": {"gpu-type": "L40S"}}, "config_json.runtime.gpu-type"),
        ({"runtime": {"cloud-region": "us-east"}}, "config_json.runtime.cloud-region"),
        ({"frames": {"hostPath": "/tmp/frame.png"}}, "config_json.frames.hostPath"),
        ({"metrics": {"gpuDurationMs": 42}}, "config_json.metrics.gpuDurationMs"),
        ({"auth": {"apiKey": "secret"}}, "config_json.auth.apiKey"),
        ({"auth": {"APIKey": "secret"}}, "config_json.auth.APIKey"),
        ({"runtime": {"GPUUuid": "gpu-1"}}, "config_json.runtime.GPUUuid"),
        ({"auth": {"access_token": "secret"}}, "config_json.auth.access_token"),
        ({"auth": {"idToken": "secret"}}, "config_json.auth.idToken"),
        ({"auth": {"session_token": "secret"}}, "config_json.auth.session_token"),
        ({"auth": {"accessKey": "secret"}}, "config_json.auth.accessKey"),
        ({"auth": {"secretKeyId": "secret"}}, "config_json.auth.secretKeyId"),
    ],
)
def test_config_quarantine_is_recursive_and_case_insensitive(config: dict, path: str) -> None:
    with pytest.raises(ValueError, match=path.replace("[", r"\[").replace("]", r"\]")):
        canonical_hosted_episode_config(config)


def test_config_rejects_nonfinite_json_and_non_string_keys() -> None:
    with pytest.raises(ValueError, match="finite JSON"):
        canonical_hosted_episode_config({"temperature": math.nan})
    with pytest.raises(ValueError, match="keys must be strings"):
        canonical_hosted_episode_config({"nested": {1: "ambiguous"}})


def test_config_parser_accepts_only_canonical_json_objects() -> None:
    assert canonical_hosted_episode_config(None) == "{}"
    assert canonical_hosted_episode_config('{"z": 1, "a": 2}') == '{"a":2,"z":1}'

    with pytest.raises(ValueError, match="valid JSON"):
        canonical_hosted_episode_config("{")
    with pytest.raises(ValueError, match="JSON object"):
        canonical_hosted_episode_config("[]")
    with pytest.raises(TypeError, match="JSON object"):
        canonical_hosted_episode_config(())  # type: ignore[arg-type]


def test_config_quarantine_preserves_deterministic_provider_configuration() -> None:
    assert canonical_hosted_episode_config(
        {
            "camera_size": 256,
            "env_seed": 7,
            "max_tokens": 2048,
            "settle_steps": 10,
            "use_bf16": True,
            "use_sdpa": False,
        }
    ) == (
        '{"camera_size":256,"env_seed":7,"max_tokens":2048,'
        '"settle_steps":10,"use_bf16":true,"use_sdpa":false}'
    )


def test_trajectory_uses_fixed_vectors_content_refs_and_two_idempotency_levels() -> None:
    request_ipc, trajectory_ipc, _, _ = _payloads()
    request_digest = hosted_episode_request_digest(request_ipc)
    rows = decode_hosted_episode_trajectory(trajectory_ipc)
    first, terminal = rows[0], rows[2]

    assert first["step_index"] == 0 and first["action"] is None
    assert terminal["step_index"] == 2 and len(terminal["action"]) == 7
    assert terminal["termination_reason"] == "success"
    assert terminal["request_digest"] == request_digest
    assert terminal["step_id"] != rows[1]["step_id"]
    assert terminal["episode_result_id"] == first["episode_result_id"]
    assert rows[-1]["episode_result_id"] != first["episode_result_id"]
    assert terminal["agentview_frame"]["content_id"].startswith("sha256:")
    assert pa.types.is_fixed_size_list(HOSTED_EPISODE_TRAJECTORY_SCHEMA.field("action").type)
    assert HOSTED_EPISODE_TRAJECTORY_SCHEMA.field("action").type.list_size == 7
    proprio = HOSTED_EPISODE_TRAJECTORY_SCHEMA.field("proprio").type
    assert pa.types.is_fixed_size_list(proprio.field("eef_pos").type)
    assert pa.types.is_fixed_size_list(proprio.field("eef_quat").type)
    assert pa.types.is_fixed_size_list(proprio.field("gripper_qpos").type)


def test_exact_results_and_manifest_prove_batch_completeness() -> None:
    request_ipc, trajectory_ipc, results_ipc, manifest_ipc = _payloads()

    manifest = validate_hosted_episode_result(
        request_ipc,
        trajectory_ipc,
        results_ipc,
        manifest_ipc,
    )
    results = decode_hosted_episode_results(results_ipc)

    assert manifest == decode_hosted_episode_manifest(manifest_ipc)
    assert manifest["episode_count"] == 2
    assert manifest["trajectory_row_count"] == 5
    assert manifest["transition_count"] == 3
    assert manifest["success_count"] == 1
    assert manifest["request_digest"] == hosted_episode_request_digest(request_ipc)
    assert manifest["trajectory_digest"] == hosted_episode_trajectory_digest(trajectory_ipc)
    assert manifest["episode_results_digest"] == hosted_episode_results_digest(results_ipc)
    assert hosted_episode_manifest_digest(manifest_ipc) != hashlib.sha256(manifest_ipc).hexdigest()
    assert [row["transition_count"] for row in results] == [2, 1]
    assert [row["trajectory_row_count"] for row in results] == [3, 2]
    assert [row["total_reward"] for row in results] == [2.0, 0.0]
    assert {row["termination_reason"] for row in results} == {
        "success",
        "transition_budget",
    }


def test_missing_admitted_episode_cannot_build_results_or_manifest() -> None:
    request_ipc = encode_hosted_episode_requests(_request_rows())
    requests = decode_hosted_episode_requests(request_ipc)
    request_digest = hosted_episode_request_digest(request_ipc)
    only_first = requests[0]
    partial_trajectory = encode_hosted_episode_trajectory(
        [
            _trajectory_row(only_first, request_digest, 0),
            _trajectory_row(
                only_first,
                request_digest,
                1,
                environment_done=True,
                terminal=True,
                termination_reason="environment_done",
            ),
        ]
    )

    with pytest.raises(ValueError, match="exactly every admitted episode"):
        build_hosted_episode_results(request_ipc, partial_trajectory)


def test_trajectory_bound_to_a_different_request_cannot_publish() -> None:
    request_ipc, trajectory_ipc, _, _ = _payloads()
    rows = decode_hosted_episode_trajectory(trajectory_ipc)
    for row in rows:
        row["request_digest"] = "0" * 64
    mismatched_trajectory = encode_hosted_episode_trajectory(rows)

    with pytest.raises(ValueError, match="exact canonical request digest"):
        build_hosted_episode_results(request_ipc, mismatched_trajectory)


def test_independently_valid_but_tampered_result_cannot_build_manifest() -> None:
    request_ipc, trajectory_ipc, results_ipc, _ = _payloads()
    rows = decode_hosted_episode_results(results_ipc)
    rows[0]["total_reward"] += 1.0
    tampered_results = encode_hosted_episode_results(rows)

    with pytest.raises(ValueError, match="not the exact derivation"):
        build_hosted_episode_manifest(
            request_ipc,
            trajectory_ipc,
            tampered_results,
        )


def test_independently_valid_but_tampered_manifest_cannot_settle() -> None:
    request_ipc, trajectory_ipc, results_ipc, manifest_ipc = _payloads()
    row = decode_hosted_episode_manifest(manifest_ipc)
    row["success_count"] = 0
    # Counts remain structurally plausible and the ID still binds the three
    # payload digests, but the manifest is not their exact derivation.
    tampered_manifest = encode_hosted_episode_manifest(row)

    with pytest.raises(ValueError, match="exact canonical completeness record"):
        validate_hosted_episode_result(
            request_ipc,
            trajectory_ipc,
            results_ipc,
            tampered_manifest,
        )


@pytest.mark.parametrize(
    ("success", "environment_done", "step", "budget", "reason"),
    [
        (True, False, 1, 3, "success"),
        (False, True, 1, 3, "environment_done"),
        (False, False, 3, 3, "transition_budget"),
    ],
)
def test_closed_terminal_vocabulary_distinguishes_provider_and_budget(
    success: bool,
    environment_done: bool,
    step: int,
    budget: int,
    reason: str,
) -> None:
    request = _request_rows()[0]
    request["trial_id"] = 9
    request["max_transitions"] = budget
    request_ipc = encode_hosted_episode_requests([request])
    normalized = decode_hosted_episode_requests(request_ipc)[0]
    request_digest = hosted_episode_request_digest(request_ipc)
    rows = [_trajectory_row(normalized, request_digest, 0)]
    rows.extend(
        _trajectory_row(
            normalized,
            request_digest,
            index,
            success=success and index == step,
            environment_done=environment_done and index == step,
            terminal=index == step,
            termination_reason=reason if index == step else None,
        )
        for index in range(1, step + 1)
    )

    trajectory_ipc = encode_hosted_episode_trajectory(rows)
    final = decode_hosted_episode_trajectory(trajectory_ipc)[-1]

    assert final["termination_reason"] == reason
    assert set(HOSTED_EPISODE_TERMINATION_REASONS) == {
        "success",
        "environment_done",
        "transition_budget",
    }


def test_reset_only_episode_consumes_zero_transitions() -> None:
    request = _request_rows()[0]
    request["trial_id"] = 10
    request["max_transitions"] = 0
    request_ipc = encode_hosted_episode_requests([request])
    normalized = decode_hosted_episode_requests(request_ipc)[0]
    request_digest = hosted_episode_request_digest(request_ipc)
    trajectory_ipc = encode_hosted_episode_trajectory(
        [
            _trajectory_row(
                normalized,
                request_digest,
                0,
                terminal=True,
                termination_reason="transition_budget",
            )
        ]
    )
    results = decode_hosted_episode_results(
        build_hosted_episode_results(request_ipc, trajectory_ipc)
    )

    assert results[0]["trajectory_row_count"] == 1
    assert results[0]["transition_count"] == 0


def test_old_proof_field_names_fail_instead_of_silently_changing_meaning() -> None:
    request = _request_rows()[0]
    request["max_steps"] = request.pop("max_transitions")

    with pytest.raises(ValueError, match="unknown fields: max_steps"):
        encode_hosted_episode_requests([request])


def test_request_batch_rejects_empty_duplicate_and_cross_operation_identity() -> None:
    with pytest.raises(ValueError, match="at least one episode"):
        encode_hosted_episode_requests([])

    duplicate = _request_rows()[0]
    with pytest.raises(ValueError, match="trial_id must be unique"):
        encode_hosted_episode_requests([duplicate, copy.deepcopy(duplicate)])

    mixed = _request_rows()
    mixed[1]["operation_id"] = "physical-eval:other"
    with pytest.raises(ValueError, match="exactly one operation_id"):
        encode_hosted_episode_requests(mixed)

    wrong_episode = _request_rows()[0]
    wrong_episode["episode_id"] = "physical-episode-wrong"
    with pytest.raises(ValueError, match="episode_id does not match"):
        encode_hosted_episode_requests([wrong_episode])

    wrong_config = _request_rows()[0]
    wrong_config["config_digest"] = "0" * 64
    with pytest.raises(ValueError, match="config_digest does not match"):
        encode_hosted_episode_requests([wrong_config])


def test_content_ref_rejects_non_content_addressed_identity() -> None:
    request_ipc = encode_hosted_episode_requests([_request_rows()[0]])
    request = decode_hosted_episode_requests(request_ipc)[0]
    request_digest = hosted_episode_request_digest(request_ipc)
    row = _trajectory_row(
        request,
        request_digest,
        0,
        terminal=True,
        termination_reason="transition_budget",
    )
    row["agentview_frame"]["content_id"] = "/tmp/frame.png"

    with pytest.raises(ValueError, match="content-addressed"):
        encode_hosted_episode_trajectory([row])


def test_trajectory_row_rejects_malformed_provider_values() -> None:
    request_ipc = encode_hosted_episode_requests([_request_rows()[0]])
    request = decode_hosted_episode_requests(request_ipc)[0]
    request_digest = hosted_episode_request_digest(request_ipc)
    reset = _trajectory_row(
        request,
        request_digest,
        0,
        terminal=True,
        termination_reason="transition_budget",
    )

    malformed = copy.deepcopy(reset)
    malformed["episode_result_id"] = "physical-episode-result-wrong"
    with pytest.raises(ValueError, match="episode_result_id is invalid"):
        encode_hosted_episode_trajectory([malformed])

    malformed = copy.deepcopy(reset)
    malformed["step_id"] = "physical-step-wrong"
    with pytest.raises(ValueError, match="step_id is invalid"):
        encode_hosted_episode_trajectory([malformed])

    malformed = copy.deepcopy(reset)
    malformed["action"] = [0.0] * 7
    with pytest.raises(ValueError, match="reset row action must be null"):
        encode_hosted_episode_trajectory([malformed])

    malformed = copy.deepcopy(reset)
    malformed["termination_reason"] = "provider_timeout"
    with pytest.raises(ValueError, match="closed hosted-episode vocabulary"):
        encode_hosted_episode_trajectory([malformed])

    malformed = copy.deepcopy(reset)
    malformed["proprio"] = None
    with pytest.raises(ValueError, match="proprio must be an object"):
        encode_hosted_episode_trajectory([malformed])

    malformed = copy.deepcopy(reset)
    malformed["agentview_frame"] = []
    with pytest.raises(ValueError, match="content reference or null"):
        encode_hosted_episode_trajectory([malformed])

    transition = _trajectory_row(
        request,
        request_digest,
        1,
        terminal=True,
        termination_reason="transition_budget",
    )
    transition["action"] = None
    with pytest.raises(ValueError, match="transition row action must not be null"):
        encode_hosted_episode_trajectory([_trajectory_row(request, request_digest, 0), transition])


def test_trajectory_group_rejects_noncontiguous_and_contradictory_history() -> None:
    request = _request_rows()[0]
    request["max_transitions"] = 3
    request_ipc = encode_hosted_episode_requests([request])
    normalized = decode_hosted_episode_requests(request_ipc)[0]
    request_digest = hosted_episode_request_digest(request_ipc)

    reset = _trajectory_row(normalized, request_digest, 0)
    second = _trajectory_row(
        normalized,
        request_digest,
        2,
        terminal=True,
        termination_reason="transition_budget",
    )
    with pytest.raises(ValueError, match="contiguous from reset"):
        encode_hosted_episode_trajectory([reset, second])

    first = _trajectory_row(
        normalized,
        request_digest,
        1,
        terminal=True,
        termination_reason="transition_budget",
    )
    with pytest.raises(ValueError, match="invalid terminal reason"):
        encode_hosted_episode_trajectory([reset, first])

    early_reason = copy.deepcopy(reset)
    early_reason["termination_reason"] = "success"
    terminal = _trajectory_row(
        normalized,
        request_digest,
        1,
        success=True,
        terminal=True,
        termination_reason="success",
    )
    with pytest.raises(ValueError, match="early termination reason"):
        encode_hosted_episode_trajectory([early_reason, terminal])

    early_success = copy.deepcopy(reset)
    early_success["success"] = True
    with pytest.raises(ValueError, match="unacknowledged provider terminal"):
        encode_hosted_episode_trajectory([early_success, terminal])

    changed_identity = copy.deepcopy(terminal)
    changed_identity["instruction"] = "a different admitted task"
    with pytest.raises(ValueError, match="changes request identity"):
        encode_hosted_episode_trajectory([reset, changed_identity])

    duplicate = _trajectory_row(
        normalized,
        request_digest,
        0,
        terminal=True,
        termination_reason="transition_budget",
    )
    with pytest.raises(ValueError, match="step_id values must be unique"):
        encode_hosted_episode_trajectory([duplicate, copy.deepcopy(duplicate)])


def test_result_and_manifest_rows_reject_internally_plausible_drift() -> None:
    request_ipc, trajectory_ipc, results_ipc, manifest_ipc = _payloads()
    del request_ipc, trajectory_ipc
    result = decode_hosted_episode_results(results_ipc)[0]

    malformed = copy.deepcopy(result)
    malformed["episode_result_id"] = "physical-episode-result-wrong"
    with pytest.raises(ValueError, match="episode_result_id is invalid"):
        encode_hosted_episode_results([malformed])

    malformed = copy.deepcopy(result)
    malformed["termination_reason"] = "provider_timeout"
    with pytest.raises(ValueError, match="closed vocabulary"):
        encode_hosted_episode_results([malformed])

    malformed = copy.deepcopy(result)
    malformed["trajectory_row_count"] += 1
    with pytest.raises(ValueError, match="reset plus contiguous transitions"):
        encode_hosted_episode_results([malformed])

    malformed = copy.deepcopy(result)
    malformed["success"] = not result["success"]
    with pytest.raises(ValueError, match="terminal flags"):
        encode_hosted_episode_results([malformed])

    with pytest.raises(ValueError, match="at least one row"):
        encode_hosted_episode_results([])
    with pytest.raises(ValueError, match="episode_result_id values must be unique"):
        encode_hosted_episode_results([result, copy.deepcopy(result)])

    manifest = decode_hosted_episode_manifest(manifest_ipc)
    malformed_manifest = copy.deepcopy(manifest)
    malformed_manifest["contract_version"] = "unsupported"
    with pytest.raises(ValueError, match="contract_version is not supported"):
        encode_hosted_episode_manifest(malformed_manifest)

    malformed_manifest = copy.deepcopy(manifest)
    malformed_manifest["manifest_id"] = "physical-manifest-wrong"
    with pytest.raises(ValueError, match="manifest_id does not match"):
        encode_hosted_episode_manifest(malformed_manifest)

    malformed_manifest = copy.deepcopy(manifest)
    malformed_manifest["success_count"] = manifest["episode_count"] + 1
    with pytest.raises(ValueError, match="success_count exceeds"):
        encode_hosted_episode_manifest(malformed_manifest)

    malformed_manifest = copy.deepcopy(manifest)
    malformed_manifest["trajectory_row_count"] += 1
    with pytest.raises(ValueError, match="reset rows plus transitions"):
        encode_hosted_episode_manifest(malformed_manifest)


def test_decoders_reject_empty_invalid_and_wrong_schema_streams() -> None:
    with pytest.raises(ValueError, match="non-empty bytes"):
        decode_hosted_episode_requests(b"")
    with pytest.raises(ValueError, match="valid Arrow stream"):
        decode_hosted_episode_requests(b"not-arrow")

    request_ipc = encode_hosted_episode_requests(_request_rows())
    with pytest.raises(ValueError, match="wrong schema"):
        decode_hosted_episode_results(request_ipc)


def test_noncanonical_multibatch_stream_is_rejected() -> None:
    canonical = encode_hosted_episode_requests(_request_rows())
    rows = decode_hosted_episode_requests(canonical)
    first = pa.RecordBatch.from_pylist(rows[:1], schema=HOSTED_EPISODE_REQUEST_SCHEMA)
    second = pa.RecordBatch.from_pylist(rows[1:], schema=HOSTED_EPISODE_REQUEST_SCHEMA)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, HOSTED_EPISODE_REQUEST_SCHEMA) as writer:
        writer.write_batch(first)
        writer.write_batch(second)

    with pytest.raises(ValueError, match="exactly one canonical record batch"):
        decode_hosted_episode_requests(sink.getvalue().to_pybytes())


def test_direct_and_hosted_budgets_have_an_exact_reset_bridge() -> None:
    assert max_transitions_from_direct_max_steps(1) == 0
    assert max_transitions_from_direct_max_steps(40) == 39
    assert direct_max_steps_from_max_transitions(0) == 1
    assert direct_max_steps_from_max_transitions(39) == 40


def test_all_four_schemas_are_versioned_and_have_distinct_payload_domains() -> None:
    schemas = (
        HOSTED_EPISODE_REQUEST_SCHEMA,
        HOSTED_EPISODE_TRAJECTORY_SCHEMA,
        HOSTED_EPISODE_RESULT_SCHEMA,
        HOSTED_EPISODE_MANIFEST_SCHEMA,
    )

    assert {schema.metadata[b"archetype.payload"] for schema in schemas} == {
        b"request",
        b"trajectory",
        b"episode-result",
        b"manifest",
    }
    assert all(
        schema.metadata[b"archetype.contract"] == HOSTED_EPISODE_CONTRACT_VERSION.encode()
        for schema in schemas
    )

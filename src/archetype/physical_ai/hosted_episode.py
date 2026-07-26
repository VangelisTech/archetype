# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Canonical data contract for complete hosted physical-AI episodes.

The boundary is provider-neutral and data-only. One request row is one logical
trial, one seed, and one physical episode. A batch may contain many episodes,
but every row shares one caller-stable ``operation_id``. Reset is trajectory
row zero and consumes no transition; ``max_transitions`` counts only actions
applied after reset.

Providers return a complete trajectory, one derived result row per episode,
and one batch manifest. The manifest is valid only when all four canonical
Arrow IPC payloads agree exactly. Live clients, activation identifiers,
placement, credentials, timings, and host-local frame paths are deliberately
outside this contract.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, Final

import pyarrow as pa

HOSTED_EPISODE_CONTRACT_VERSION: Final = "archetype.physical-ai.hosted-episode/v1"
HOSTED_EPISODE_TERMINATION_REASONS: Final = frozenset(
    {"success", "environment_done", "transition_budget"}
)

_SCHEMA_METADATA: Final = {
    b"archetype.contract": HOSTED_EPISODE_CONTRACT_VERSION.encode(),
}
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_TEXT_LIMIT: Final = 4096

# Attempt facts, placement, credentials, and host paths cannot participate in
# replay identity. Matching is case-insensitive and treats '-' like '_'.
_QUARANTINED_CONFIG_KEYS: Final = frozenset(
    {
        "accelerator",
        "activation_id",
        "activation_metadata",
        "api_key",
        "attempt_id",
        "auth_token",
        "availability_zone",
        "bearer_token",
        "created_at",
        "cuda_device",
        "container_id",
        "container_name",
        "credential",
        "credentials",
        "device",
        "duration_ms",
        "elapsed_ms",
        "finished_at",
        "frame_path",
        "function_call_id",
        "gpu",
        "gpu_id",
        "gpu_uuid",
        "host_path",
        "hostname",
        "instance_id",
        "latency",
        "latency_ms",
        "machine_type",
        "modal_app_id",
        "modal_function_call_id",
        "modal_task_id",
        "node",
        "node_id",
        "password",
        "pid",
        "placement",
        "process_id",
        "refresh_token",
        "region",
        "secret",
        "secrets",
        "started_at",
        "timestamp",
        "token",
        "updated_at",
        "volume_path",
        "wall_time",
        "wall_seconds",
        "worker_id",
        "zone",
    }
)
_QUARANTINED_CONFIG_PREFIXES: Final = (
    "accelerator_",
    "activation_",
    "attempt_",
    "container_",
    "credential_",
    "cuda_",
    "device_",
    "gpu_",
    "host_",
    "instance_",
    "modal_",
    "node_",
    "placement_",
    "region_",
    "secret_",
    "worker_",
    "zone_",
)
_QUARANTINED_CONFIG_SUFFIXES: Final = (
    "_accelerator",
    "_api_key",
    "_credential",
    "_device",
    "_gpu",
    "_gpu_uuid",
    "_hostname",
    "_instance",
    "_password",
    "_region",
    "_secret",
    "_zone",
)

_VEC2: Final = pa.list_(pa.float64(), 2)
_VEC3: Final = pa.list_(pa.float64(), 3)
_VEC4: Final = pa.list_(pa.float64(), 4)
_ACTION7: Final = pa.list_(pa.float64(), 7)
_PROPRIO: Final = pa.struct(
    [
        pa.field("eef_pos", _VEC3, nullable=False),
        pa.field("eef_quat", _VEC4, nullable=False),
        pa.field("gripper", pa.float64(), nullable=False),
        pa.field("gripper_qpos", _VEC2, nullable=False),
    ]
)
_FRAME_CONTENT_REF: Final = pa.struct(
    [
        pa.field("content_id", pa.string(), nullable=False),
        pa.field("sha256", pa.string(), nullable=False),
        pa.field("media_type", pa.string(), nullable=False),
        pa.field("size_bytes", pa.int64(), nullable=False),
    ]
)


def _schema(payload: str, fields: Sequence[pa.Field]) -> pa.Schema:
    return pa.schema(
        fields,
        metadata={
            **_SCHEMA_METADATA,
            b"archetype.payload": payload.encode(),
        },
    )


HOSTED_EPISODE_REQUEST_SCHEMA: Final[pa.Schema] = _schema(
    "request",
    [
        pa.field("operation_id", pa.string(), nullable=False),
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("trial_id", pa.int64(), nullable=False),
        pa.field("suite", pa.string(), nullable=False),
        pa.field("task_id", pa.int64(), nullable=False),
        pa.field("seed", pa.int64(), nullable=False),
        pa.field("instruction", pa.string(), nullable=False),
        pa.field("max_transitions", pa.int64(), nullable=False),
        pa.field("environment_id", pa.string(), nullable=False),
        pa.field("policy_id", pa.string(), nullable=False),
        pa.field("config_json", pa.string(), nullable=False),
        pa.field("config_digest", pa.string(), nullable=False),
    ],
)

HOSTED_EPISODE_TRAJECTORY_SCHEMA: Final[pa.Schema] = _schema(
    "trajectory",
    [
        pa.field("operation_id", pa.string(), nullable=False),
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("trial_id", pa.int64(), nullable=False),
        pa.field("episode_result_id", pa.string(), nullable=False),
        pa.field("step_id", pa.string(), nullable=False),
        pa.field("request_digest", pa.string(), nullable=False),
        pa.field("suite", pa.string(), nullable=False),
        pa.field("task_id", pa.int64(), nullable=False),
        pa.field("seed", pa.int64(), nullable=False),
        pa.field("instruction", pa.string(), nullable=False),
        pa.field("max_transitions", pa.int64(), nullable=False),
        pa.field("environment_id", pa.string(), nullable=False),
        pa.field("policy_id", pa.string(), nullable=False),
        pa.field("config_digest", pa.string(), nullable=False),
        pa.field("step_index", pa.int64(), nullable=False),
        pa.field("action", _ACTION7, nullable=True),
        pa.field("proprio", _PROPRIO, nullable=False),
        pa.field("reward", pa.float64(), nullable=False),
        pa.field("environment_done", pa.bool_(), nullable=False),
        pa.field("success", pa.bool_(), nullable=False),
        pa.field("terminal", pa.bool_(), nullable=False),
        pa.field("termination_reason", pa.string(), nullable=True),
        pa.field("agentview_frame", _FRAME_CONTENT_REF, nullable=True),
        pa.field("wrist_frame", _FRAME_CONTENT_REF, nullable=True),
    ],
)

HOSTED_EPISODE_RESULT_SCHEMA: Final[pa.Schema] = _schema(
    "episode-result",
    [
        pa.field("operation_id", pa.string(), nullable=False),
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("trial_id", pa.int64(), nullable=False),
        pa.field("episode_result_id", pa.string(), nullable=False),
        pa.field("request_digest", pa.string(), nullable=False),
        pa.field("trajectory_digest", pa.string(), nullable=False),
        pa.field("episode_trajectory_digest", pa.string(), nullable=False),
        pa.field("terminal_step_index", pa.int64(), nullable=False),
        pa.field("trajectory_row_count", pa.int64(), nullable=False),
        pa.field("transition_count", pa.int64(), nullable=False),
        pa.field("environment_done", pa.bool_(), nullable=False),
        pa.field("success", pa.bool_(), nullable=False),
        pa.field("termination_reason", pa.string(), nullable=False),
        pa.field("total_reward", pa.float64(), nullable=False),
    ],
)

HOSTED_EPISODE_MANIFEST_SCHEMA: Final[pa.Schema] = _schema(
    "manifest",
    [
        pa.field("contract_version", pa.string(), nullable=False),
        pa.field("operation_id", pa.string(), nullable=False),
        pa.field("manifest_id", pa.string(), nullable=False),
        pa.field("request_digest", pa.string(), nullable=False),
        pa.field("trajectory_digest", pa.string(), nullable=False),
        pa.field("episode_results_digest", pa.string(), nullable=False),
        pa.field("episode_count", pa.int64(), nullable=False),
        pa.field("trajectory_row_count", pa.int64(), nullable=False),
        pa.field("transition_count", pa.int64(), nullable=False),
        pa.field("success_count", pa.int64(), nullable=False),
    ],
)


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("hosted episode configuration must be finite JSON data") from exc


def _quarantined_config_path(value: Any, path: str = "config_json") -> str | None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            normalized_key = key.casefold().replace("-", "_")
            nested_path = f"{path}.{key}"
            if (
                normalized_key in _QUARANTINED_CONFIG_KEYS
                or normalized_key.startswith(_QUARANTINED_CONFIG_PREFIXES)
                or normalized_key.endswith(_QUARANTINED_CONFIG_SUFFIXES)
                or "timestamp" in normalized_key
                or any(
                    token in {"latency", "elapsed", "duration"}
                    for token in normalized_key.split("_")
                )
                or normalized_key == "path"
                or normalized_key.startswith("path_")
                or normalized_key.endswith("_path")
            ):
                return nested_path
            found = _quarantined_config_path(nested, nested_path)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            found = _quarantined_config_path(nested, f"{path}[{index}]")
            if found is not None:
                return found
    return None


def canonical_hosted_episode_config(
    value: str | Mapping[str, Any] | None,
) -> str:
    """Return canonical replay configuration after recursive quarantine checks."""

    if value is None:
        parsed: Any = {}
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("config_json must contain valid JSON") from exc
    elif isinstance(value, Mapping):
        parsed = dict(value)
    else:
        raise TypeError("config_json must be a JSON object, JSON string, or None")
    if not isinstance(parsed, dict):
        raise ValueError("config_json must encode a JSON object")
    quarantined_path = _quarantined_config_path(parsed)
    if quarantined_path is not None:
        raise ValueError(
            f"hosted episode configuration contains quarantined metadata at {quarantined_path}"
        )
    return _canonical_json(parsed)


def _payload_digest(kind: str, payload: bytes) -> str:
    if type(payload) is not bytes or not payload:
        raise ValueError(f"{kind} payload must be non-empty bytes")
    digest = hashlib.sha256()
    digest.update(HOSTED_EPISODE_CONTRACT_VERSION.encode())
    digest.update(b"\0")
    digest.update(kind.encode())
    digest.update(b"\0")
    digest.update(payload)
    return digest.hexdigest()


def hosted_episode_config_digest(config_json: str | Mapping[str, Any] | None) -> str:
    """Return the version-domain-separated digest of canonical provider config."""

    canonical = canonical_hosted_episode_config(config_json)
    return _payload_digest("config", canonical.encode())


def _identity(kind: str, *parts: object) -> str:
    encoded = _canonical_json([HOSTED_EPISODE_CONTRACT_VERSION, kind, *parts]).encode()
    return f"physical-{kind}-{hashlib.sha256(encoded).hexdigest()}"


def hosted_episode_id(operation_id: str, trial_id: int) -> str:
    """Derive one episode identity beneath a stable provider operation."""

    return _identity(
        "episode",
        _text(operation_id, "operation_id", maximum=256),
        _integer(trial_id, "trial_id"),
    )


def hosted_episode_result_id(operation_id: str, episode_id: str) -> str:
    """Derive the idempotency identity for one complete episode result."""

    return _identity(
        "episode-result",
        _text(operation_id, "operation_id", maximum=256),
        _text(episode_id, "episode_id", maximum=256),
    )


def hosted_episode_step_id(operation_id: str, episode_id: str, step_index: int) -> str:
    """Derive the idempotency identity for one trajectory row."""

    return _identity(
        "step",
        _text(operation_id, "operation_id", maximum=256),
        _text(episode_id, "episode_id", maximum=256),
        _integer(step_index, "step_index"),
    )


def direct_max_steps_from_max_transitions(max_transitions: int) -> int:
    """Translate a hosted transition budget to the direct ledger-row budget."""

    return _integer(max_transitions, "max_transitions") + 1


def max_transitions_from_direct_max_steps(max_steps: int) -> int:
    """Translate the direct reset-inclusive budget to hosted transitions."""

    return _integer(max_steps, "max_steps", minimum=1) - 1


def _manifest_id(
    operation_id: str,
    request_digest: str,
    trajectory_digest: str,
    episode_results_digest: str,
) -> str:
    return _identity(
        "manifest",
        operation_id,
        request_digest,
        trajectory_digest,
        episode_results_digest,
    )


def _text(value: Any, field: str, *, maximum: int = _TEXT_LIMIT) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field} must be a non-empty, trimmed string")
    if len(value) > maximum:
        raise ValueError(f"{field} must be at most {maximum} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field} must not contain control characters")
    return value


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite number") from exc
    if not math.isfinite(normalized):
        raise ValueError(f"{field} must be a finite number")
    return normalized


def _vector(value: Any, size: int, field: str) -> list[float]:
    if not isinstance(value, (list, tuple)) or len(value) != size:
        raise ValueError(f"{field} must contain exactly {size} values")
    return [_finite(item, field) for item in value]


def _digest_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _reject_unknown_fields(row: Mapping[str, Any], schema: pa.Schema, label: str) -> None:
    unknown = sorted(set(row) - set(schema.names))
    if unknown:
        raise ValueError(f"{label} contains unknown fields: {', '.join(unknown)}")


def _normalize_request_row(row: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(row, HOSTED_EPISODE_REQUEST_SCHEMA, "request row")
    operation_id = _text(row.get("operation_id"), "operation_id", maximum=256)
    trial_id = _integer(row.get("trial_id"), "trial_id")
    episode_id = hosted_episode_id(operation_id, trial_id)
    supplied_episode_id = row.get("episode_id")
    if supplied_episode_id is not None and supplied_episode_id != episode_id:
        raise ValueError("episode_id does not match operation_id and trial_id")
    config_json = canonical_hosted_episode_config(row.get("config_json"))
    config_digest = hosted_episode_config_digest(config_json)
    supplied_config_digest = row.get("config_digest")
    if supplied_config_digest is not None and supplied_config_digest != config_digest:
        raise ValueError("config_digest does not match canonical config_json")
    return {
        "operation_id": operation_id,
        "episode_id": episode_id,
        "trial_id": trial_id,
        "suite": _text(row.get("suite"), "suite"),
        "task_id": _integer(row.get("task_id"), "task_id"),
        "seed": _integer(row.get("seed"), "seed"),
        "instruction": _text(row.get("instruction"), "instruction"),
        "max_transitions": _integer(row.get("max_transitions"), "max_transitions"),
        "environment_id": _text(row.get("environment_id"), "environment_id"),
        "policy_id": _text(row.get("policy_id"), "policy_id"),
        "config_json": config_json,
        "config_digest": config_digest,
    }


def _request_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str]:
    return (str(row["operation_id"]), int(row["trial_id"]), str(row["episode_id"]))


def _validate_request_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        raise ValueError("hosted episode request must contain at least one episode")
    normalized = sorted((_normalize_request_row(row) for row in rows), key=_request_sort_key)
    operation_ids = {row["operation_id"] for row in normalized}
    if len(operation_ids) != 1:
        raise ValueError("one hosted request batch must contain exactly one operation_id")
    trial_ids = [row["trial_id"] for row in normalized]
    episode_ids = [row["episode_id"] for row in normalized]
    if len(trial_ids) != len(set(trial_ids)):
        raise ValueError("trial_id must be unique within one hosted request")
    if len(episode_ids) != len(set(episode_ids)):
        raise ValueError("episode_id must be unique within one hosted request")
    return normalized


def _normalize_frame(value: Any, field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a content reference or null")
    required = {"content_id", "sha256", "media_type", "size_bytes"}
    if set(value) != required:
        raise ValueError(f"{field} fields must be exactly {', '.join(sorted(required))}")
    digest = _digest_text(value["sha256"], f"{field}.sha256")
    content_id = _text(value["content_id"], f"{field}.content_id")
    if content_id != f"sha256:{digest}":
        raise ValueError(f"{field}.content_id must be the content-addressed SHA-256 identity")
    return {
        "content_id": content_id,
        "sha256": digest,
        "media_type": _text(value["media_type"], f"{field}.media_type", maximum=255),
        "size_bytes": _integer(value["size_bytes"], f"{field}.size_bytes"),
    }


def _normalize_proprio(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("proprio must be an object")
    required = {"eef_pos", "eef_quat", "gripper", "gripper_qpos"}
    if set(value) != required:
        raise ValueError(f"proprio fields must be exactly {', '.join(sorted(required))}")
    return {
        "eef_pos": _vector(value["eef_pos"], 3, "proprio.eef_pos"),
        "eef_quat": _vector(value["eef_quat"], 4, "proprio.eef_quat"),
        "gripper": _finite(value["gripper"], "proprio.gripper"),
        "gripper_qpos": _vector(value["gripper_qpos"], 2, "proprio.gripper_qpos"),
    }


def _normalize_trajectory_row(row: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(row, HOSTED_EPISODE_TRAJECTORY_SCHEMA, "trajectory row")
    operation_id = _text(row.get("operation_id"), "operation_id", maximum=256)
    trial_id = _integer(row.get("trial_id"), "trial_id")
    episode_id = _text(row.get("episode_id"), "episode_id", maximum=256)
    if episode_id != hosted_episode_id(operation_id, trial_id):
        raise ValueError("trajectory episode_id does not match operation_id and trial_id")
    step_index = _integer(row.get("step_index"), "step_index")
    episode_result_id = hosted_episode_result_id(operation_id, episode_id)
    if row.get("episode_result_id") != episode_result_id:
        raise ValueError("trajectory episode_result_id is invalid")
    step_id = hosted_episode_step_id(operation_id, episode_id, step_index)
    if row.get("step_id") != step_id:
        raise ValueError("trajectory step_id is invalid")
    action = row.get("action")
    if step_index == 0:
        if action is not None:
            raise ValueError("reset row action must be null")
    elif action is None:
        raise ValueError("transition row action must not be null")
    else:
        action = _vector(action, 7, "action")
    reason = row.get("termination_reason")
    if reason is not None and (
        not isinstance(reason, str) or reason not in HOSTED_EPISODE_TERMINATION_REASONS
    ):
        raise ValueError("termination_reason is not in the closed hosted-episode vocabulary")
    return {
        "operation_id": operation_id,
        "episode_id": episode_id,
        "trial_id": trial_id,
        "episode_result_id": episode_result_id,
        "step_id": step_id,
        "request_digest": _digest_text(row.get("request_digest"), "request_digest"),
        "suite": _text(row.get("suite"), "suite"),
        "task_id": _integer(row.get("task_id"), "task_id"),
        "seed": _integer(row.get("seed"), "seed"),
        "instruction": _text(row.get("instruction"), "instruction"),
        "max_transitions": _integer(row.get("max_transitions"), "max_transitions"),
        "environment_id": _text(row.get("environment_id"), "environment_id"),
        "policy_id": _text(row.get("policy_id"), "policy_id"),
        "config_digest": _digest_text(row.get("config_digest"), "config_digest"),
        "step_index": step_index,
        "action": action,
        "proprio": _normalize_proprio(row.get("proprio")),
        "reward": _finite(row.get("reward"), "reward"),
        "environment_done": _boolean(row.get("environment_done"), "environment_done"),
        "success": _boolean(row.get("success"), "success"),
        "terminal": _boolean(row.get("terminal"), "terminal"),
        "termination_reason": reason,
        "agentview_frame": _normalize_frame(row.get("agentview_frame"), "agentview_frame"),
        "wrist_frame": _normalize_frame(row.get("wrist_frame"), "wrist_frame"),
    }


def _trajectory_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str, int]:
    return (
        str(row["operation_id"]),
        int(row["trial_id"]),
        str(row["episode_id"]),
        int(row["step_index"]),
    )


def _expected_terminal_reason(row: Mapping[str, Any]) -> str | None:
    if row["success"]:
        return "success"
    if row["environment_done"]:
        return "environment_done"
    if row["step_index"] == row["max_transitions"]:
        return "transition_budget"
    return None


def _validate_trajectory_group(episode: Sequence[Mapping[str, Any]]) -> None:
    episode_id = str(episode[0]["episode_id"])
    actual_indices = [int(row["step_index"]) for row in episode]
    if actual_indices != list(range(len(episode))):
        raise ValueError(f"episode {episode_id!r} step_index must be contiguous from reset")
    if actual_indices[-1] > int(episode[0]["max_transitions"]):
        raise ValueError(f"episode {episode_id!r} exceeds max_transitions")
    if any(row["terminal"] for row in episode[:-1]) or not episode[-1]["terminal"]:
        raise ValueError(f"episode {episode_id!r} must terminate exactly on its final row")
    if any(row["termination_reason"] is not None for row in episode[:-1]):
        raise ValueError(f"episode {episode_id!r} has an early termination reason")
    if any(row["success"] or row["environment_done"] for row in episode[:-1]):
        raise ValueError(f"episode {episode_id!r} has an unacknowledged provider terminal row")
    final = episode[-1]
    expected_reason = _expected_terminal_reason(final)
    if expected_reason is None or final["termination_reason"] != expected_reason:
        raise ValueError(f"episode {episode_id!r} has an invalid terminal reason")
    identity_fields = (
        "operation_id",
        "episode_id",
        "trial_id",
        "episode_result_id",
        "request_digest",
        "suite",
        "task_id",
        "seed",
        "instruction",
        "max_transitions",
        "environment_id",
        "policy_id",
        "config_digest",
    )
    identity = tuple(episode[0][field] for field in identity_fields)
    if any(tuple(row[field] for field in identity_fields) != identity for row in episode[1:]):
        raise ValueError(f"episode {episode_id!r} changes request identity between steps")


def _validate_trajectory_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        raise ValueError("hosted episode trajectory must contain at least one row")
    normalized = sorted(
        (_normalize_trajectory_row(row) for row in rows),
        key=_trajectory_sort_key,
    )
    operation_ids = {row["operation_id"] for row in normalized}
    request_digests = {row["request_digest"] for row in normalized}
    if len(operation_ids) != 1 or len(request_digests) != 1:
        raise ValueError("one trajectory must bind one operation_id and one request_digest")
    step_ids = [row["step_id"] for row in normalized]
    if len(step_ids) != len(set(step_ids)):
        raise ValueError("trajectory step_id values must be unique")
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in normalized:
        groups[row["episode_id"]].append(row)
    for episode in groups.values():
        _validate_trajectory_group(episode)
    return normalized


def _normalize_result_row(row: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(row, HOSTED_EPISODE_RESULT_SCHEMA, "episode result row")
    operation_id = _text(row.get("operation_id"), "operation_id", maximum=256)
    trial_id = _integer(row.get("trial_id"), "trial_id")
    episode_id = _text(row.get("episode_id"), "episode_id", maximum=256)
    if episode_id != hosted_episode_id(operation_id, trial_id):
        raise ValueError("result episode_id does not match operation_id and trial_id")
    episode_result_id = hosted_episode_result_id(operation_id, episode_id)
    if row.get("episode_result_id") != episode_result_id:
        raise ValueError("episode_result_id is invalid")
    reason = row.get("termination_reason")
    if not isinstance(reason, str) or reason not in HOSTED_EPISODE_TERMINATION_REASONS:
        raise ValueError("result termination_reason is not in the closed vocabulary")
    terminal_step_index = _integer(row.get("terminal_step_index"), "terminal_step_index")
    transition_count = _integer(row.get("transition_count"), "transition_count")
    trajectory_row_count = _integer(
        row.get("trajectory_row_count"),
        "trajectory_row_count",
        minimum=1,
    )
    if terminal_step_index != transition_count or trajectory_row_count != transition_count + 1:
        raise ValueError("result counts do not describe reset plus contiguous transitions")
    environment_done = _boolean(row.get("environment_done"), "environment_done")
    success = _boolean(row.get("success"), "success")
    if (
        reason == "success"
        and not success
        or reason == "environment_done"
        and (success or not environment_done)
        or reason == "transition_budget"
        and (success or environment_done)
    ):
        raise ValueError("result terminal flags do not match termination_reason")
    return {
        "operation_id": operation_id,
        "episode_id": episode_id,
        "trial_id": trial_id,
        "episode_result_id": episode_result_id,
        "request_digest": _digest_text(row.get("request_digest"), "request_digest"),
        "trajectory_digest": _digest_text(row.get("trajectory_digest"), "trajectory_digest"),
        "episode_trajectory_digest": _digest_text(
            row.get("episode_trajectory_digest"),
            "episode_trajectory_digest",
        ),
        "terminal_step_index": terminal_step_index,
        "trajectory_row_count": trajectory_row_count,
        "transition_count": transition_count,
        "environment_done": environment_done,
        "success": success,
        "termination_reason": reason,
        "total_reward": _finite(row.get("total_reward"), "total_reward"),
    }


def _result_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str]:
    return (str(row["operation_id"]), int(row["trial_id"]), str(row["episode_id"]))


def _validate_result_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        raise ValueError("hosted episode results must contain at least one row")
    normalized = sorted((_normalize_result_row(row) for row in rows), key=_result_sort_key)
    if len({row["operation_id"] for row in normalized}) != 1:
        raise ValueError("episode results must contain exactly one operation_id")
    if len({row["request_digest"] for row in normalized}) != 1:
        raise ValueError("episode results must bind exactly one request_digest")
    if len({row["trajectory_digest"] for row in normalized}) != 1:
        raise ValueError("episode results must bind exactly one trajectory_digest")
    result_ids = [row["episode_result_id"] for row in normalized]
    if len(result_ids) != len(set(result_ids)):
        raise ValueError("episode_result_id values must be unique")
    return normalized


def _normalize_manifest_row(row: Mapping[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(row, HOSTED_EPISODE_MANIFEST_SCHEMA, "manifest row")
    contract_version = _text(row.get("contract_version"), "contract_version")
    if contract_version != HOSTED_EPISODE_CONTRACT_VERSION:
        raise ValueError("manifest contract_version is not supported")
    operation_id = _text(row.get("operation_id"), "operation_id", maximum=256)
    request_digest = _digest_text(row.get("request_digest"), "request_digest")
    trajectory_digest = _digest_text(row.get("trajectory_digest"), "trajectory_digest")
    results_digest = _digest_text(
        row.get("episode_results_digest"),
        "episode_results_digest",
    )
    manifest_id = _manifest_id(
        operation_id,
        request_digest,
        trajectory_digest,
        results_digest,
    )
    if row.get("manifest_id") != manifest_id:
        raise ValueError("manifest_id does not match its bound payload digests")
    episode_count = _integer(row.get("episode_count"), "episode_count", minimum=1)
    trajectory_row_count = _integer(
        row.get("trajectory_row_count"),
        "trajectory_row_count",
        minimum=1,
    )
    transition_count = _integer(row.get("transition_count"), "transition_count")
    success_count = _integer(row.get("success_count"), "success_count")
    if success_count > episode_count:
        raise ValueError("manifest success_count exceeds episode_count")
    if trajectory_row_count != episode_count + transition_count:
        raise ValueError("manifest row count is not reset rows plus transitions")
    return {
        "contract_version": contract_version,
        "operation_id": operation_id,
        "manifest_id": manifest_id,
        "request_digest": request_digest,
        "trajectory_digest": trajectory_digest,
        "episode_results_digest": results_digest,
        "episode_count": episode_count,
        "trajectory_row_count": trajectory_row_count,
        "transition_count": transition_count,
        "success_count": success_count,
    }


def _encode_rows(rows: Sequence[Mapping[str, Any]], schema: pa.Schema) -> bytes:
    batch = pa.RecordBatch.from_pylist(list(rows), schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _decode_rows(payload: bytes, schema: pa.Schema, label: str) -> list[dict[str, Any]]:
    if type(payload) is not bytes or not payload:
        raise ValueError(f"{label} IPC payload must be non-empty bytes")
    try:
        reader = pa.ipc.open_stream(pa.py_buffer(payload))
        if not reader.schema.equals(schema, check_metadata=True):
            raise ValueError(f"{label} IPC has the wrong schema or contract version")
        batches = list(reader)
    except (pa.ArrowException, OSError) as exc:
        raise ValueError(f"{label} IPC is not a valid Arrow stream") from exc
    if len(batches) != 1:
        raise ValueError(f"{label} IPC must contain exactly one canonical record batch")
    return batches[0].to_pylist()


def encode_hosted_episode_requests(rows: Sequence[Mapping[str, Any]]) -> bytes:
    """Normalize and encode one deterministic hosted request batch."""

    return _encode_rows(_validate_request_rows(rows), HOSTED_EPISODE_REQUEST_SCHEMA)


def decode_hosted_episode_requests(payload: bytes) -> list[dict[str, Any]]:
    """Decode only the canonical v1 request representation."""

    rows = _decode_rows(payload, HOSTED_EPISODE_REQUEST_SCHEMA, "request")
    normalized = _validate_request_rows(rows)
    if rows != normalized or payload != _encode_rows(normalized, HOSTED_EPISODE_REQUEST_SCHEMA):
        raise ValueError("request IPC is valid Arrow but is not canonically encoded")
    return normalized


def hosted_episode_request_digest(payload: bytes) -> str:
    """Validate and version-domain-separate the request payload digest."""

    decode_hosted_episode_requests(payload)
    return _payload_digest("request", payload)


def encode_hosted_episode_trajectory(rows: Sequence[Mapping[str, Any]]) -> bytes:
    """Normalize and encode complete per-step evidence."""

    return _encode_rows(_validate_trajectory_rows(rows), HOSTED_EPISODE_TRAJECTORY_SCHEMA)


def decode_hosted_episode_trajectory(payload: bytes) -> list[dict[str, Any]]:
    """Decode only complete, canonical hosted trajectory evidence."""

    rows = _decode_rows(payload, HOSTED_EPISODE_TRAJECTORY_SCHEMA, "trajectory")
    normalized = _validate_trajectory_rows(rows)
    if rows != normalized or payload != _encode_rows(normalized, HOSTED_EPISODE_TRAJECTORY_SCHEMA):
        raise ValueError("trajectory IPC is valid Arrow but is not canonically encoded")
    return normalized


def hosted_episode_trajectory_digest(payload: bytes) -> str:
    """Validate and version-domain-separate the full trajectory digest."""

    decode_hosted_episode_trajectory(payload)
    return _payload_digest("trajectory", payload)


def encode_hosted_episode_results(rows: Sequence[Mapping[str, Any]]) -> bytes:
    """Normalize and encode one derived terminal row per episode."""

    return _encode_rows(_validate_result_rows(rows), HOSTED_EPISODE_RESULT_SCHEMA)


def decode_hosted_episode_results(payload: bytes) -> list[dict[str, Any]]:
    """Decode only canonical per-episode terminal result rows."""

    rows = _decode_rows(payload, HOSTED_EPISODE_RESULT_SCHEMA, "episode result")
    normalized = _validate_result_rows(rows)
    if rows != normalized or payload != _encode_rows(normalized, HOSTED_EPISODE_RESULT_SCHEMA):
        raise ValueError("episode result IPC is valid Arrow but is not canonically encoded")
    return normalized


def hosted_episode_results_digest(payload: bytes) -> str:
    """Validate and version-domain-separate the episode-results digest."""

    decode_hosted_episode_results(payload)
    return _payload_digest("episode-results", payload)


def encode_hosted_episode_manifest(row: Mapping[str, Any]) -> bytes:
    """Encode the one canonical batch manifest row."""

    normalized = _normalize_manifest_row(row)
    return _encode_rows([normalized], HOSTED_EPISODE_MANIFEST_SCHEMA)


def decode_hosted_episode_manifest(payload: bytes) -> dict[str, Any]:
    """Decode the one canonical batch manifest row."""

    rows = _decode_rows(payload, HOSTED_EPISODE_MANIFEST_SCHEMA, "manifest")
    if len(rows) != 1:
        raise ValueError("hosted episode manifest must contain exactly one row")
    normalized = _normalize_manifest_row(rows[0])
    if rows[0] != normalized or payload != _encode_rows(
        [normalized], HOSTED_EPISODE_MANIFEST_SCHEMA
    ):
        raise ValueError("manifest IPC is valid Arrow but is not canonically encoded")
    return normalized


def hosted_episode_manifest_digest(payload: bytes) -> str:
    """Validate and version-domain-separate the manifest payload digest."""

    decode_hosted_episode_manifest(payload)
    return _payload_digest("manifest", payload)


_ECHOED_REQUEST_FIELDS: Final = (
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


def _bind_request_to_trajectory(
    request_rows: Sequence[Mapping[str, Any]],
    trajectory_rows: Sequence[Mapping[str, Any]],
    request_digest: str,
) -> dict[str, list[dict[str, Any]]]:
    requests = {str(row["episode_id"]): row for row in request_rows}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trajectory_rows:
        if row["request_digest"] != request_digest:
            raise ValueError("trajectory does not bind the exact canonical request digest")
        request = requests.get(str(row["episode_id"]))
        if request is None:
            raise ValueError("trajectory contains an episode absent from the request")
        if any(row[field] != request[field] for field in _ECHOED_REQUEST_FIELDS):
            raise ValueError("trajectory identity does not match its admitted request row")
        grouped[str(row["episode_id"])].append(dict(row))
    if set(grouped) != set(requests):
        raise ValueError("trajectory does not contain exactly every admitted episode")
    return grouped


def build_hosted_episode_results(
    request_ipc: bytes,
    trajectory_ipc: bytes,
) -> bytes:
    """Derive the exact terminal result rows from a complete trajectory."""

    requests = decode_hosted_episode_requests(request_ipc)
    trajectory = decode_hosted_episode_trajectory(trajectory_ipc)
    request_digest = hosted_episode_request_digest(request_ipc)
    trajectory_digest = hosted_episode_trajectory_digest(trajectory_ipc)
    grouped = _bind_request_to_trajectory(requests, trajectory, request_digest)
    rows: list[dict[str, Any]] = []
    for request in requests:
        episode = grouped[str(request["episode_id"])]
        final = episode[-1]
        episode_ipc = encode_hosted_episode_trajectory(episode)
        rows.append(
            {
                "operation_id": request["operation_id"],
                "episode_id": request["episode_id"],
                "trial_id": request["trial_id"],
                "episode_result_id": final["episode_result_id"],
                "request_digest": request_digest,
                "trajectory_digest": trajectory_digest,
                "episode_trajectory_digest": _payload_digest(
                    "episode-trajectory",
                    episode_ipc,
                ),
                "terminal_step_index": final["step_index"],
                "trajectory_row_count": len(episode),
                "transition_count": final["step_index"],
                "environment_done": final["environment_done"],
                "success": final["success"],
                "termination_reason": final["termination_reason"],
                "total_reward": math.fsum(float(row["reward"]) for row in episode[1:]),
            }
        )
    return encode_hosted_episode_results(rows)


def build_hosted_episode_manifest(
    request_ipc: bytes,
    trajectory_ipc: bytes,
    episode_results_ipc: bytes,
) -> bytes:
    """Build the one manifest only when request, trajectory, and results agree."""

    requests = decode_hosted_episode_requests(request_ipc)
    trajectory = decode_hosted_episode_trajectory(trajectory_ipc)
    results = decode_hosted_episode_results(episode_results_ipc)
    expected_results = build_hosted_episode_results(request_ipc, trajectory_ipc)
    if episode_results_ipc != expected_results:
        raise ValueError("episode results are not the exact derivation of this trajectory")
    request_digest = hosted_episode_request_digest(request_ipc)
    trajectory_digest = hosted_episode_trajectory_digest(trajectory_ipc)
    results_digest = hosted_episode_results_digest(episode_results_ipc)
    operation_id = str(requests[0]["operation_id"])
    manifest_id = _manifest_id(
        operation_id,
        request_digest,
        trajectory_digest,
        results_digest,
    )
    return encode_hosted_episode_manifest(
        {
            "contract_version": HOSTED_EPISODE_CONTRACT_VERSION,
            "operation_id": operation_id,
            "manifest_id": manifest_id,
            "request_digest": request_digest,
            "trajectory_digest": trajectory_digest,
            "episode_results_digest": results_digest,
            "episode_count": len(requests),
            "trajectory_row_count": len(trajectory),
            "transition_count": sum(int(row["transition_count"]) for row in results),
            "success_count": sum(bool(row["success"]) for row in results),
        }
    )


def validate_hosted_episode_result(
    request_ipc: bytes,
    trajectory_ipc: bytes,
    episode_results_ipc: bytes,
    manifest_ipc: bytes,
) -> dict[str, Any]:
    """Validate exact four-payload completeness and return the manifest row."""

    expected_results = build_hosted_episode_results(request_ipc, trajectory_ipc)
    if episode_results_ipc != expected_results:
        raise ValueError("episode results differ from the canonical trajectory derivation")
    expected_manifest = build_hosted_episode_manifest(
        request_ipc,
        trajectory_ipc,
        episode_results_ipc,
    )
    if manifest_ipc != expected_manifest:
        raise ValueError("manifest differs from the exact canonical completeness record")
    return decode_hosted_episode_manifest(manifest_ipc)


__all__ = [
    "HOSTED_EPISODE_CONTRACT_VERSION",
    "HOSTED_EPISODE_MANIFEST_SCHEMA",
    "HOSTED_EPISODE_REQUEST_SCHEMA",
    "HOSTED_EPISODE_RESULT_SCHEMA",
    "HOSTED_EPISODE_TERMINATION_REASONS",
    "HOSTED_EPISODE_TRAJECTORY_SCHEMA",
    "build_hosted_episode_manifest",
    "build_hosted_episode_results",
    "canonical_hosted_episode_config",
    "decode_hosted_episode_manifest",
    "decode_hosted_episode_requests",
    "decode_hosted_episode_results",
    "decode_hosted_episode_trajectory",
    "direct_max_steps_from_max_transitions",
    "encode_hosted_episode_manifest",
    "encode_hosted_episode_requests",
    "encode_hosted_episode_results",
    "encode_hosted_episode_trajectory",
    "hosted_episode_config_digest",
    "hosted_episode_id",
    "hosted_episode_manifest_digest",
    "hosted_episode_request_digest",
    "hosted_episode_result_id",
    "hosted_episode_results_digest",
    "hosted_episode_step_id",
    "hosted_episode_trajectory_digest",
    "max_transitions_from_direct_max_steps",
    "validate_hosted_episode_result",
]

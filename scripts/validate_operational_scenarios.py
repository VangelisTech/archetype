#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Validate the static operational-scenario inventory and ownership policy."""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from pathlib import Path, PurePosixPath
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "quality" / "operational_scenarios.toml"
CONTRACT_REGISTRY = ROOT / "quality" / "contracts.toml"

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
_REFERENCE_RE = re.compile(r"^[a-z0-9][a-z0-9._/-]*/v[1-9][0-9]*$")
_NUMBERED_EXAMPLE_RE = re.compile(r"^[0-9]{2}_.+\.py$")
_TIERS = set(range(7))
_KINDS = {"example", "dogfood"}
_APPLICABILITY = {"source", "wheel"}
_ORACLE_KINDS = {"pytest", "eval"}
_CLEANUP_POLICIES = {"isolated", "provider", "remote_prefix"}
_ARTIFACT_POLICIES = {"receipt", "redacted_receipt"}
_CADENCES = {"demand", "main", "pr", "release"}
_PREREQUISITE_KINDS = {"binary", "credential", "infrastructure", "platform", "service"}
_PREREQUISITE_POLICIES = {"fail", "not_run"}
_RECEIPT_OUTCOMES = {"failed", "not_run", "passed"}
_REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
_TRACKED_RECEIPT_FIELDS = {
    "expected_kind",
    "expected_outcome",
    "expected_profile",
    "expected_revision",
    "id",
    "invocation_entrypoint",
    "path",
    "require_clean",
    "required_graders",
    "required_task_id",
    "scenario_id",
}


def _load_registry(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    if type(payload.get("version")) is not int or payload["version"] != 1:
        raise ValueError(f"{path}: unsupported operational scenario registry version")
    unknown = set(payload) - {"version", "scenario", "tracked_receipt"}
    if unknown:
        raise ValueError(f"{path}: unknown top-level fields {sorted(unknown)}")
    rows = payload.get("scenario")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{path}: one or more [[scenario]] rows are required")
    tracked_receipts = payload.get("tracked_receipt", [])
    if not isinstance(tracked_receipts, list):
        raise ValueError(f"{path}: tracked_receipt must use [[tracked_receipt]] rows")
    return payload


def load_scenarios(path: Path = REGISTRY) -> list[dict[str, Any]]:
    """Load version-one ``[[scenario]]`` rows from *path*."""

    rows = _load_registry(path)["scenario"]
    return rows


def load_contract_ids(path: Path = CONTRACT_REGISTRY) -> set[str]:
    """Load the registered contract IDs that scenarios may claim as evidence."""

    with path.open("rb") as stream:
        payload = tomllib.load(stream)
    if type(payload.get("version")) is not int or payload["version"] != 1:
        raise ValueError(f"{path}: unsupported contract registry version")
    rows = payload.get("contract")
    if not isinstance(rows, list):
        raise ValueError(f"{path}: [[contract]] rows are required")

    contract_ids: set[str] = set()
    for index, row in enumerate(rows):
        contract_id = row.get("id") if isinstance(row, dict) else None
        if not isinstance(contract_id, str) or not contract_id:
            raise ValueError(f"{path}: contract[{index}] has no stable id")
        contract_ids.add(contract_id)
    return contract_ids


def _string_list(
    row: dict[str, Any],
    field: str,
    *,
    label: str,
    errors: list[str],
    allow_empty: bool,
    unique: bool = True,
) -> list[str] | None:
    value = row.get(field)
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        qualifier = "" if allow_empty else " non-empty"
        errors.append(f"{label}: {field} must be a{qualifier} list of non-empty strings")
        return None
    if not allow_empty and not value:
        errors.append(f"{label}: {field} must not be empty")
    if unique and len(value) != len(set(value)):
        errors.append(f"{label}: {field} contains duplicate values")
    return value


def _repo_path(value: str) -> PurePosixPath | None:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        return None
    return path


def _validate_existing_path(
    value: object,
    *,
    root: Path,
    label: str,
    field: str,
    errors: list[str],
    file_only: bool,
) -> PurePosixPath | None:
    if not isinstance(value, str) or not value:
        errors.append(f"{label}: {field} must be a non-empty repository-relative path")
        return None
    relative = _repo_path(value)
    if relative is None:
        errors.append(f"{label}: {field} must stay within the repository: {value!r}")
        return None
    resolved = root.joinpath(*relative.parts)
    exists = resolved.is_file() if file_only else resolved.exists()
    if not exists:
        noun = "file" if file_only else "path"
        errors.append(f"{label}: {field} names a missing {noun}: {value}")
    return relative


def _strict_json_object(path: Path, *, label: str, errors: list[str]) -> dict[str, Any] | None:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-standard JSON constant {value}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        errors.append(f"{label}: receipt is not strict JSON: {exc}")
        return None
    if not isinstance(payload, dict) or not payload:
        errors.append(f"{label}: receipt must be a non-empty JSON object")
        return None
    return payload


def _validate_tracked_receipts(
    rows: list[object],
    *,
    root: Path,
    scenario_ids: set[str],
    errors: list[str],
) -> None:
    seen: set[str] = set()
    root_resolved = root.resolve()

    for index, candidate in enumerate(rows):
        fallback_label = f"tracked_receipt[{index}]"
        if not isinstance(candidate, dict):
            errors.append(f"{fallback_label}: tracked receipt row must be a table")
            continue
        row = cast(dict[str, Any], candidate)
        receipt_id = row.get("id")
        if not isinstance(receipt_id, str) or not _ID_RE.fullmatch(receipt_id):
            errors.append(f"{fallback_label}: id must be a stable lowercase identifier")
            label = fallback_label
        else:
            label = receipt_id
            if receipt_id in seen:
                errors.append(f"{label}: duplicate tracked receipt id")
            seen.add(receipt_id)

        unknown_fields = set(row) - _TRACKED_RECEIPT_FIELDS
        if unknown_fields:
            errors.append(f"{label}: unknown fields {sorted(unknown_fields)}")
        missing_fields = _TRACKED_RECEIPT_FIELDS - set(row)
        if missing_fields:
            errors.append(f"{label}: missing required fields {sorted(missing_fields)}")

        receipt_relative = _validate_existing_path(
            row.get("path"),
            root=root,
            label=label,
            field="path",
            errors=errors,
            file_only=True,
        )
        receipt_path: Path | None = None
        if receipt_relative is not None:
            receipt_path = (root / receipt_relative).resolve()
            if not receipt_path.is_relative_to(root_resolved):
                errors.append(f"{label}: path resolves outside the active checkout")
                receipt_path = None
            elif receipt_path.suffix != ".json":
                errors.append(f"{label}: path must name a JSON file")
                receipt_path = None

        scenario_id = row.get("scenario_id")
        if not isinstance(scenario_id, str) or not _ID_RE.fullmatch(scenario_id):
            errors.append(f"{label}: scenario_id must be a stable lowercase identifier")
        elif scenario_id not in scenario_ids:
            errors.append(f"{label}: scenario_id references unknown scenario {scenario_id!r}")

        expected_revision = row.get("expected_revision")
        if not isinstance(expected_revision, str) or not _REVISION_RE.fullmatch(expected_revision):
            errors.append(f"{label}: expected_revision must be a full lowercase Git revision")

        require_clean = row.get("require_clean")
        if type(require_clean) is not bool:
            errors.append(f"{label}: require_clean must be a boolean")

        expected_values: dict[str, str] = {}
        for field in ("expected_kind", "expected_profile", "expected_outcome"):
            value = row.get(field)
            if not isinstance(value, str) or not _ID_RE.fullmatch(value):
                errors.append(f"{label}: {field} must be a stable lowercase identifier")
            else:
                expected_values[field] = value
        if (
            isinstance(row.get("expected_outcome"), str)
            and row["expected_outcome"] not in _RECEIPT_OUTCOMES
        ):
            errors.append(f"{label}: expected_outcome must be one of {sorted(_RECEIPT_OUTCOMES)}")

        invocation_relative = _validate_existing_path(
            row.get("invocation_entrypoint"),
            root=root,
            label=label,
            field="invocation_entrypoint",
            errors=errors,
            file_only=True,
        )
        invocation_entrypoint: Path | None = None
        if invocation_relative is not None:
            invocation_entrypoint = (root / invocation_relative).resolve()
            if not invocation_entrypoint.is_relative_to(root_resolved):
                errors.append(
                    f"{label}: invocation_entrypoint resolves outside the active checkout"
                )
                invocation_entrypoint = None

        required_task_id = row.get("required_task_id")
        if not isinstance(required_task_id, str) or not _ID_RE.fullmatch(required_task_id):
            errors.append(f"{label}: required_task_id must be a stable lowercase identifier")

        required_graders = _string_list(
            row,
            "required_graders",
            label=label,
            errors=errors,
            allow_empty=False,
        )
        if required_graders is not None:
            invalid_graders = sorted(
                grader for grader in required_graders if not _ID_RE.fullmatch(grader)
            )
            if invalid_graders:
                errors.append(
                    f"{label}: required_graders must contain stable lowercase identifiers; "
                    f"invalid values {invalid_graders}"
                )

        if receipt_path is None or not receipt_path.is_file():
            continue
        payload = _strict_json_object(receipt_path, label=label, errors=errors)
        if payload is None:
            continue
        if payload.get("schema_version") != 1:
            errors.append(f"{label}: receipt schema_version must be 1")

        expected_to_payload = {
            "expected_kind": "kind",
            "expected_profile": "profile",
            "expected_outcome": "outcome",
        }
        for expected_field, payload_field in expected_to_payload.items():
            expected = expected_values.get(expected_field)
            if expected is not None and payload.get(payload_field) != expected:
                errors.append(
                    f"{label}: receipt {payload_field} {payload.get(payload_field)!r} "
                    f"does not match {expected_field} {expected!r}"
                )

        revision = payload.get("revision")
        if not isinstance(revision, dict):
            errors.append(f"{label}: receipt revision must be an object")
        else:
            commit = revision.get("commit")
            dirty = revision.get("dirty")
            if isinstance(expected_revision, str) and commit != expected_revision:
                errors.append(
                    f"{label}: receipt revision {commit!r} does not match "
                    f"expected_revision {expected_revision!r}"
                )
            if type(dirty) is not bool:
                errors.append(f"{label}: receipt revision.dirty must be a boolean")
            elif require_clean is True and dirty:
                errors.append(f"{label}: receipt was produced from a dirty checkout")

        invocation = payload.get("invocation")
        if (
            not isinstance(invocation, list)
            or not invocation
            or any(not isinstance(item, str) or not item for item in invocation)
        ):
            errors.append(f"{label}: receipt invocation must be a non-empty list of strings")
        else:
            actual_entrypoint = Path(invocation[0])
            if actual_entrypoint.is_absolute():
                errors.append(
                    f"{label}: tracked receipt invocation entrypoint must be repository-relative"
                )
            else:
                actual_entrypoint = (root / actual_entrypoint).resolve()
            if not actual_entrypoint.is_relative_to(root_resolved):
                errors.append(f"{label}: receipt invocation is outside the active checkout")
            elif invocation_entrypoint is not None and actual_entrypoint != invocation_entrypoint:
                errors.append(
                    f"{label}: receipt invocation entrypoint does not match {invocation_relative}"
                )

        results = payload.get("results")
        if not isinstance(results, list):
            errors.append(f"{label}: receipt results must be a list")
            continue
        if not isinstance(required_task_id, str):
            continue
        matching_tasks = [
            result
            for result in results
            if isinstance(result, dict) and result.get("task_id") == required_task_id
        ]
        if len(matching_tasks) != 1:
            errors.append(
                f"{label}: receipt must contain exactly one result for task {required_task_id!r}"
            )
            continue

        task = matching_tasks[0]
        trials = task.get("trials")
        if not isinstance(trials, list) or not trials:
            errors.append(
                f"{label}: task {required_task_id!r} must contain non-empty trial evidence"
            )
            continue
        if row.get("expected_outcome") == "passed" and task.get("all_passed") is not True:
            errors.append(f"{label}: passed receipt task {required_task_id!r} is not all_passed")

        for trial_index, trial in enumerate(trials):
            trial_label = f"{label}: task {required_task_id!r} trial {trial_index}"
            if not isinstance(trial, dict):
                errors.append(f"{trial_label} must be an object")
                continue
            if row.get("expected_outcome") == "passed" and trial.get("passed") is not True:
                errors.append(f"{trial_label} is not passed")
            graders = trial.get("graders")
            if not isinstance(graders, list) or not graders:
                errors.append(f"{trial_label} must contain non-empty grader evidence")
                continue
            grader_rows: dict[str, dict[str, Any]] = {}
            for grader in graders:
                if not isinstance(grader, dict):
                    continue
                grader_record = cast(dict[str, Any], grader)
                grader_name = grader_record.get("name")
                if isinstance(grader_name, str) and grader_name:
                    grader_rows[grader_name] = grader_record
            missing_graders = sorted(set(required_graders or []) - set(grader_rows))
            if missing_graders:
                errors.append(f"{trial_label} is missing required graders {missing_graders}")
            if row.get("expected_outcome") == "passed":
                failed_graders = sorted(
                    grader
                    for grader in set(required_graders or []) & set(grader_rows)
                    if grader_rows[grader].get("passed") is not True
                )
                if failed_graders:
                    errors.append(
                        f"{trial_label} has non-passing required graders {failed_graders}"
                    )


def _oracle_path(reference: str) -> str:
    return reference.split("::", 1)[0]


def _yaml_scalar(value: str) -> str:
    scalar = value.split("#", 1)[0].strip()
    if len(scalar) >= 2 and scalar[0] == scalar[-1] and scalar[0] in {'"', "'"}:
        return scalar[1:-1]
    return scalar


def _pull_request_paths(workflow: Path) -> tuple[bool, set[str] | None]:
    """Return ``(has_pull_request, paths)``; ``None`` means unfiltered."""

    lines = workflow.read_text(encoding="utf-8").splitlines()
    on_index = next((i for i, line in enumerate(lines) if line == "on:"), None)
    if on_index is None:
        return False, set()

    pull_index: int | None = None
    for index in range(on_index + 1, len(lines)):
        line = lines[index]
        if line and not line.startswith(" "):
            break
        if re.match(r"^  pull_request\s*:", line):
            pull_index = index
            suffix = line.split(":", 1)[1].strip()
            if suffix and suffix not in {"{}", "null", "~"}:
                return True, None
            break
    if pull_index is None:
        return False, set()

    paths_index: int | None = None
    for index in range(pull_index + 1, len(lines)):
        line = lines[index]
        if line and len(line) - len(line.lstrip()) <= 2:
            break
        if re.match(r"^    paths\s*:", line):
            paths_index = index
            break
    if paths_index is None:
        return True, None

    paths: set[str] = set()
    for line in lines[paths_index + 1 :]:
        if line and len(line) - len(line.lstrip()) <= 4:
            break
        match = re.match(r"^\s{6,}-\s+(.+?)\s*$", line)
        if match:
            value = _yaml_scalar(match.group(1))
            if value:
                paths.add(value)
    return True, paths


def _trigger_covers_owner(trigger: str, owner: str) -> bool:
    if trigger in {"**", "**/*"}:
        return True
    prefix = trigger
    for suffix in ("/**", "/*"):
        if prefix.endswith(suffix):
            prefix = prefix[: -len(suffix)]
            return owner == prefix or owner.startswith(f"{prefix}/")
    return owner == trigger


def _validate_workflow(
    workflow: object,
    *,
    root: Path,
    label: str,
    owner_paths: list[str],
    cadence: list[str],
    errors: list[str],
) -> None:
    if not isinstance(workflow, dict):
        errors.append(f"{label}: workflow must be a table")
        return
    extra = set(workflow) - {"path", "trigger_paths"}
    if extra:
        errors.append(f"{label}: workflow has unknown fields {sorted(extra)}")

    relative = _validate_existing_path(
        workflow.get("path"),
        root=root,
        label=label,
        field="workflow.path",
        errors=errors,
        file_only=True,
    )
    expected = workflow.get("trigger_paths", [])
    if not isinstance(expected, list) or any(
        not isinstance(item, str) or not item for item in expected
    ):
        errors.append(f"{label}: workflow.trigger_paths must be a list of non-empty strings")
        return
    expected_paths = cast(list[str], expected)
    if len(expected_paths) != len(set(expected_paths)):
        errors.append(f"{label}: workflow.trigger_paths contains duplicate values")

    if relative is not None and (root / relative).is_file():
        has_pull_request, actual = _pull_request_paths(root / relative)
        if "pr" in cadence and not has_pull_request:
            errors.append(f"{label}: PR-required workflow has no pull_request trigger")
        if has_pull_request and actual is not None:
            missing = sorted(set(expected_paths) - actual)
            if missing:
                errors.append(
                    f"{label}: workflow pull_request paths omit declared triggers {missing}"
                )
            for owner in owner_paths:
                if not any(_trigger_covers_owner(trigger, owner) for trigger in expected_paths):
                    errors.append(
                        f"{label}: workflow trigger_paths do not cover owner path {owner}"
                    )


def validate_operational_scenarios(
    *,
    root: Path = ROOT,
    registry_path: Path = REGISTRY,
    contract_registry_path: Path | None = None,
) -> list[str]:
    """Return every static operational-scenario registry violation."""

    errors: list[str] = []
    try:
        registry = _load_registry(registry_path)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        return [str(exc)]
    rows = registry["scenario"]
    tracked_receipts = registry.get("tracked_receipt", [])
    if contract_registry_path is None:
        contract_registry_path = root / "quality" / "contracts.toml"
    try:
        registered_contracts = load_contract_ids(contract_registry_path)
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        return [str(exc)]

    seen: set[str] = set()
    covered_examples: set[str] = set()
    for index, row in enumerate(rows):
        fallback_label = f"scenario[{index}]"
        scenario_id = row.get("id")
        if not isinstance(scenario_id, str) or not _ID_RE.fullmatch(scenario_id):
            errors.append(f"{fallback_label}: id must be a stable lowercase identifier")
            label = fallback_label
        else:
            label = scenario_id
            if scenario_id in seen:
                errors.append(f"{label}: duplicate scenario id")
            seen.add(scenario_id)

        kind = row.get("kind")
        if kind not in _KINDS:
            errors.append(f"{label}: kind must be one of {sorted(_KINDS)}")
        owner = row.get("owner")
        if not isinstance(owner, str) or not owner:
            errors.append(f"{label}: owner must be a non-empty string")

        owner_paths = _string_list(
            row,
            "owner_paths",
            label=label,
            errors=errors,
            allow_empty=False,
        )
        if owner_paths is not None:
            for path in owner_paths:
                _validate_existing_path(
                    path,
                    root=root,
                    label=label,
                    field="owner_paths",
                    errors=errors,
                    file_only=False,
                )

        source = _validate_existing_path(
            row.get("source_path"),
            root=root,
            label=label,
            field="source_path",
            errors=errors,
            file_only=True,
        )
        if source is not None and source.parent == PurePosixPath("examples"):
            if _NUMBERED_EXAMPLE_RE.fullmatch(source.name):
                covered_examples.add(source.as_posix())

        _string_list(
            row,
            "source_command",
            label=label,
            errors=errors,
            allow_empty=False,
            unique=False,
        )
        _string_list(
            row,
            "required_extras",
            label=label,
            errors=errors,
            allow_empty=True,
        )

        tier = row.get("tier")
        if type(tier) is not int or tier not in _TIERS:
            errors.append(f"{label}: tier must be an integer from 0 through 6")

        applicability = _string_list(
            row,
            "applicability",
            label=label,
            errors=errors,
            allow_empty=False,
        )
        if applicability is not None:
            unknown = set(applicability) - _APPLICABILITY
            if unknown:
                errors.append(f"{label}: unknown applicability values {sorted(unknown)}")

        timeout = row.get("timeout_seconds")
        if type(timeout) is not int or timeout <= 0:
            errors.append(f"{label}: timeout_seconds must be a positive integer")

        prerequisites = _string_list(
            row,
            "prerequisites",
            label=label,
            errors=errors,
            allow_empty=True,
        )
        if prerequisites is not None:
            for prerequisite in prerequisites:
                prerequisite_kind, separator, name = prerequisite.partition(":")
                if not separator or prerequisite_kind not in _PREREQUISITE_KINDS or not name:
                    errors.append(
                        f"{label}: invalid prerequisite {prerequisite!r}; expected "
                        f"<{','.join(sorted(_PREREQUISITE_KINDS))}>:<name>"
                    )

        prerequisite_policy = row.get("missing_prerequisite")
        if (
            prerequisite_policy == "pass"
            and prerequisites
            and any(value.startswith("credential:") for value in prerequisites)
        ):
            errors.append(
                f"{label}: credentialed scenario cannot silently pass when credentials are absent"
            )
        if prerequisite_policy not in _PREREQUISITE_POLICIES:
            errors.append(
                f"{label}: missing_prerequisite must be one of {sorted(_PREREQUISITE_POLICIES)}"
            )
        if prerequisites == [] and prerequisite_policy != "fail":
            errors.append(f"{label}: prerequisite-free scenario must use fail policy")

        oracle = row.get("semantic_oracle")
        if not isinstance(oracle, dict):
            errors.append(f"{label}: semantic_oracle must be a table")
        else:
            if set(oracle) != {"kind", "ref"}:
                errors.append(f"{label}: semantic_oracle must contain exactly kind and ref")
            oracle_kind = oracle.get("kind")
            oracle_ref = oracle.get("ref")
            if oracle_kind not in _ORACLE_KINDS:
                errors.append(
                    f"{label}: semantic_oracle.kind must be one of {sorted(_ORACLE_KINDS)}"
                )
            if not isinstance(oracle_ref, str) or not oracle_ref:
                errors.append(f"{label}: semantic_oracle.ref must be a non-empty string")
            elif oracle_kind in {"pytest", "eval"}:
                _validate_existing_path(
                    _oracle_path(oracle_ref),
                    root=root,
                    label=label,
                    field="semantic_oracle.ref",
                    errors=errors,
                    file_only=True,
                )

        contracts = _string_list(
            row,
            "contracts",
            label=label,
            errors=errors,
            allow_empty=False,
        )
        if contracts is not None:
            unknown_contracts = sorted(set(contracts) - registered_contracts)
            if unknown_contracts:
                errors.append(f"{label}: references unknown contract IDs {unknown_contracts}")

        cleanup_policy = row.get("cleanup_policy")
        if cleanup_policy not in _CLEANUP_POLICIES:
            errors.append(f"{label}: cleanup_policy must be one of {sorted(_CLEANUP_POLICIES)}")
        artifact_policy = row.get("artifact_policy")
        if artifact_policy not in _ARTIFACT_POLICIES:
            errors.append(f"{label}: artifact_policy must be one of {sorted(_ARTIFACT_POLICIES)}")
        artifact_schema = row.get("artifact_schema")
        if not isinstance(artifact_schema, str) or not _REFERENCE_RE.fullmatch(artifact_schema):
            errors.append(f"{label}: artifact_schema must be a versioned stable identifier")

        cadence = _string_list(
            row,
            "required_cadence",
            label=label,
            errors=errors,
            allow_empty=False,
        )
        if cadence is not None:
            unknown = set(cadence) - _CADENCES
            if unknown:
                errors.append(f"{label}: unknown required_cadence values {sorted(unknown)}")
            if "release" in cadence and artifact_policy not in _ARTIFACT_POLICIES:
                errors.append(f"{label}: release-required scenario needs an artifact policy")

        workflow = row.get("workflow")
        if workflow is not None:
            _validate_workflow(
                workflow,
                root=root,
                label=label,
                owner_paths=owner_paths or [],
                cadence=cadence or [],
                errors=errors,
            )

        known_fields = {
            "id",
            "kind",
            "owner",
            "owner_paths",
            "source_path",
            "source_command",
            "required_extras",
            "tier",
            "applicability",
            "timeout_seconds",
            "prerequisites",
            "missing_prerequisite",
            "semantic_oracle",
            "contracts",
            "cleanup_policy",
            "artifact_policy",
            "artifact_schema",
            "required_cadence",
            "workflow",
        }
        unknown_fields = set(row) - known_fields
        if unknown_fields:
            errors.append(f"{label}: unknown fields {sorted(unknown_fields)}")

    examples = root / "examples"
    numbered_examples = {
        path.relative_to(root).as_posix()
        for path in examples.glob("[0-9][0-9]_*.py")
        if path.is_file()
    }
    missing_examples = sorted(numbered_examples - covered_examples)
    if missing_examples:
        errors.append(f"numbered examples missing manifest scenarios: {missing_examples}")

    _validate_tracked_receipts(
        tracked_receipts,
        root=root,
        scenario_ids=seen,
        errors=errors,
    )

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--contracts", type=Path, default=CONTRACT_REGISTRY)
    args = parser.parse_args(argv)
    errors = validate_operational_scenarios(
        registry_path=args.registry,
        contract_registry_path=args.contracts,
    )
    if errors:
        for error in errors:
            print(f"operational scenario registry error: {error}")
        return 1
    print("Operational scenario registry audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

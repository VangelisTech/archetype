# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Versioned benchmark reports with explicit comparability provenance."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
_PACKAGES = ("archetype-ecs", "daft", "lancedb", "pyiceberg")


class ReportFormatError(ValueError):
    """A benchmark report does not satisfy the versioned wire contract."""


def capture_environment(*, runner_id: str | None = None) -> dict[str, Any]:
    """Capture fields that determine whether two timing runs are comparable."""
    packages: dict[str, str] = {}
    for package in _PACKAGES:
        try:
            packages[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            packages[package] = "not-installed"

    return {
        "runner_id": (
            runner_id or os.environ.get("ARCHETYPE_BENCH_RUNNER") or platform.node() or "unknown"
        ),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "packages": packages,
    }


def capture_revision() -> dict[str, Any]:
    """Capture the measured Git revision without making Git a hard dependency."""
    commit = os.environ.get("GITHUB_SHA")
    if not commit:
        commit = _git_output("rev-parse", "HEAD") or "unknown"

    dirty_output = _git_output("status", "--short")
    return {
        "commit": commit,
        "dirty": None if dirty_output is None else bool(dirty_output),
    }


def build_report(
    results: Sequence[Mapping[str, Any]],
    *,
    suite: str,
    config: Mapping[str, Any],
    environment: Mapping[str, Any] | None = None,
    revision: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Normalize raw benchmark rows into the stable report schema."""
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "suite": suite,
        "created_at": created_at or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "revision": dict(revision or capture_revision()),
        "environment": dict(environment or capture_environment()),
        "config": dict(config),
        "benchmarks": [_normalize_result(result) for result in results],
    }
    _assert_json_value(report, "report")
    report["report_id"] = _report_id(report)
    validate_report(report)
    return report


def validate_report(report: Any) -> dict[str, Any]:
    """Validate and return one schema-v1 report.

    Historical files are inputs to regression decisions, so malformed or
    hand-edited data fails closed instead of being silently ignored.
    """
    if not isinstance(report, dict):
        raise ReportFormatError("report must be a JSON object")
    _validate_report_header(report)
    _validate_benchmarks(report.get("benchmarks"))

    expected_id = _report_id({key: value for key, value in report.items() if key != "report_id"})
    if report["report_id"] != expected_id:
        raise ReportFormatError("report_id does not match report contents")
    _assert_json_value(report, "report")
    return report


def _validate_report_header(report: Mapping[str, Any]) -> None:
    if report.get("schema_version") != SCHEMA_VERSION:
        raise ReportFormatError(
            f"unsupported benchmark report schema: {report.get('schema_version')!r}"
        )

    for field in ("suite", "created_at", "report_id"):
        if not isinstance(report.get(field), str) or not report[field]:
            raise ReportFormatError(f"{field} must be a non-empty string")
    _parse_created_at(report["created_at"])
    for field in ("revision", "environment", "config"):
        if not isinstance(report.get(field), dict):
            raise ReportFormatError(f"{field} must be an object")
    _validate_revision(report["revision"])
    _validate_environment(report["environment"])


def _validate_benchmarks(benchmarks: Any) -> None:
    if not isinstance(benchmarks, list) or not benchmarks:
        raise ReportFormatError("benchmarks must be a non-empty array")
    identities: set[str] = set()
    for index, benchmark in enumerate(benchmarks):
        _validate_benchmark(benchmark, index)
        identity = benchmark_identity(benchmark)
        if identity in identities:
            raise ReportFormatError(f"duplicate benchmark identity at benchmarks[{index}]")
        identities.add(identity)


def load_report(path: str | Path) -> dict[str, Any]:
    """Load one report from disk and validate its content hash and schema."""
    report_path = Path(path)
    try:
        payload = json.loads(report_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ReportFormatError(f"cannot read {report_path}: {exc}") from exc
    try:
        return validate_report(payload)
    except ReportFormatError as exc:
        raise ReportFormatError(f"invalid report {report_path}: {exc}") from exc


def write_report(
    report: Mapping[str, Any],
    *,
    current_path: str | Path | None = None,
    history_dir: str | Path | None = None,
) -> Path | None:
    """Atomically write the current report and an optional immutable history copy."""
    validated = validate_report(dict(report))
    if current_path is None and history_dir is None:
        raise ValueError("current_path or history_dir is required")

    if current_path is not None:
        _atomic_json_write(Path(current_path), validated)

    if history_dir is None:
        return None

    history_path = Path(history_dir) / f"{validated['report_id']}.json"
    if history_path.exists():
        existing = load_report(history_path)
        if existing != validated:
            raise ReportFormatError(f"history collision at {history_path}")
        return history_path
    _atomic_json_write(history_path, validated)
    return history_path


def benchmark_identity(benchmark: Mapping[str, Any]) -> str:
    """Return the stable identity used to align one benchmark across runs."""
    payload = {
        "name": benchmark["name"],
        "dimensions": benchmark["dimensions"],
    }
    return _canonical_json(payload)


def compatibility_identity(report: Mapping[str, Any]) -> str:
    """Return the fields that must match before timing values are compared."""
    payload = {
        "suite": report["suite"],
        "environment": report["environment"],
        "config": report["config"],
    }
    return _canonical_json(payload)


def report_timestamp(report: Mapping[str, Any]) -> datetime:
    """Return a validated report timestamp for chronological windowing."""
    value = report.get("created_at")
    if not isinstance(value, str):
        raise ReportFormatError("created_at must be a string")
    return _parse_created_at(value)


def _normalize_result(result: Mapping[str, Any]) -> dict[str, Any]:
    name = _required_string(result, "name")
    bench_name = result.get("bench_name")
    if bench_name is not None and bench_name != name:
        raise ReportFormatError(f"bench_name {bench_name!r} does not match name {name!r}")

    entities = _required_number(result, "entities", integer=True)
    steps = _required_number(result, "steps", integer=True)
    elapsed = _required_number(result, "elapsed_s")
    if entities < 0 or steps <= 0 or elapsed <= 0:
        raise ReportFormatError(
            "entities must be non-negative; steps and elapsed_s must be positive"
        )

    extras = result.get("extras", {})
    if not isinstance(extras, dict):
        raise ReportFormatError(f"{name}: extras must be an object")

    normalized: dict[str, Any] = {
        "name": name,
        "dimensions": {
            "entities": entities,
            "steps": steps,
            "extras": extras,
        },
        "metrics": {
            "elapsed_s": elapsed,
            "steps_per_sec": steps / elapsed,
            "entities_per_sec": (entities * steps) / elapsed,
        },
    }

    provenance = {
        field: str(result[field])
        for field in ("world_id", "run_id")
        if result.get(field) is not None
    }
    if provenance:
        normalized["provenance"] = provenance
    return normalized


def _validate_benchmark(benchmark: Any, index: int) -> None:
    prefix = f"benchmarks[{index}]"
    if not isinstance(benchmark, dict):
        raise ReportFormatError(f"{prefix} must be an object")
    if not isinstance(benchmark.get("name"), str) or not benchmark["name"]:
        raise ReportFormatError(f"{prefix}.name must be a non-empty string")
    if not isinstance(benchmark.get("dimensions"), dict):
        raise ReportFormatError(f"{prefix}.dimensions must be an object")
    _assert_json_value(benchmark["dimensions"], f"{prefix}.dimensions")
    _validate_metrics(benchmark.get("metrics"), prefix)
    _validate_provenance(benchmark.get("provenance"), prefix)


def _validate_metrics(metrics: Any, prefix: str) -> None:
    if not isinstance(metrics, dict) or not metrics:
        raise ReportFormatError(f"{prefix}.metrics must be a non-empty object")
    for name, value in metrics.items():
        if not isinstance(name, str) or not _is_finite_number(value) or value < 0:
            raise ReportFormatError(f"{prefix}.metrics contains an invalid value")


def _validate_provenance(provenance: Any, prefix: str) -> None:
    if provenance is not None and (
        not isinstance(provenance, dict)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in provenance.items()
        )
    ):
        raise ReportFormatError(f"{prefix}.provenance must map strings to strings")


def _parse_created_at(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ReportFormatError("created_at must be an RFC 3339 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ReportFormatError("created_at must be an RFC 3339 UTC timestamp") from exc
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ReportFormatError("created_at must use UTC")
    return parsed


def _validate_revision(revision: Mapping[str, Any]) -> None:
    if not isinstance(revision.get("commit"), str) or not revision["commit"]:
        raise ReportFormatError("revision.commit must be a non-empty string")
    if revision.get("dirty") is not None and not isinstance(revision["dirty"], bool):
        raise ReportFormatError("revision.dirty must be a boolean or null")


def _validate_environment(environment: Mapping[str, Any]) -> None:
    fields = (
        "runner_id",
        "system",
        "release",
        "machine",
        "processor",
        "python_implementation",
        "python_version",
    )
    for field in fields:
        if not isinstance(environment.get(field), str):
            raise ReportFormatError(f"environment.{field} must be a string")
    if not environment["runner_id"]:
        raise ReportFormatError("environment.runner_id must not be empty")
    packages = environment.get("packages")
    if not isinstance(packages, dict) or not packages:
        raise ReportFormatError("environment.packages must be a non-empty object")
    if any(
        not isinstance(name, str) or not isinstance(version, str)
        for name, version in packages.items()
    ):
        raise ReportFormatError("environment.packages must map strings to strings")


def _required_string(result: Mapping[str, Any], field: str) -> str:
    value = result.get(field)
    if not isinstance(value, str) or not value:
        raise ReportFormatError(f"raw benchmark {field} must be a non-empty string")
    return value


def _required_number(
    result: Mapping[str, Any], field: str, *, integer: bool = False
) -> int | float:
    value = result.get(field)
    if not _is_finite_number(value) or (integer and not isinstance(value, int)):
        expected = "integer" if integer else "finite number"
        raise ReportFormatError(f"raw benchmark {field} must be a {expected}")
    return value


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _assert_json_value(value: Any, path: str) -> None:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ReportFormatError(f"{path} is not finite JSON data: {exc}") from exc


def _report_id(report: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(report).encode()).hexdigest()


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ReportFormatError(f"value is not canonical JSON data: {exc}") from exc


def _git_output(*args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _atomic_json_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(payload, stream, allow_nan=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise

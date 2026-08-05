#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Execute one example's ``run_demo`` function and emit its semantic result."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.util
import inspect
import json
import os
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from types import ModuleType
from typing import Any, cast

RECEIPT_PREFIX = "ARCHETYPE_OPERATIONAL_RECEIPT="
CAPTURED_RECEIPT_ENV = "ARCHETYPE_OPERATIONAL_CAPTURED_RECEIPT"
MAX_RECEIPT_BYTES = 1024 * 1024
MAX_RECEIPT_DEPTH = 32


def _load_example(path: Path) -> ModuleType:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"example source does not exist: {resolved}")
    sys.path.insert(0, str(resolved.parent))
    identity = hashlib.sha256(str(resolved).encode()).hexdigest()[:12]
    spec = importlib.util.spec_from_file_location(
        f"_archetype_operational_{resolved.stem}_{identity}",
        resolved,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load example module {resolved}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _json_receipt(value: object, *, label: str) -> str:
    """Encode one portable, standards-compliant JSON receipt."""

    _validate_receipt_shape(value, label=label)
    try:
        encoded = json.dumps(value, allow_nan=False, sort_keys=True)
    except (RecursionError, TypeError, ValueError) as exc:
        raise TypeError(f"{label} must contain only portable JSON values") from exc
    if len(encoded.encode("utf-8")) > MAX_RECEIPT_BYTES:
        raise ValueError(f"{label} exceeds the {MAX_RECEIPT_BYTES}-byte receipt budget")
    return encoded


def _validate_receipt_shape(
    value: object,
    *,
    label: str,
    depth: int = 0,
    ancestors: frozenset[int] = frozenset(),
) -> None:
    if depth > MAX_RECEIPT_DEPTH:
        raise ValueError(f"{label} exceeds the maximum JSON depth of {MAX_RECEIPT_DEPTH}")
    if not isinstance(value, (dict, list, tuple)):
        return
    identity = id(value)
    if identity in ancestors:
        raise TypeError(f"{label} contains a recursive collection")
    nested_ancestors = ancestors | {identity}
    values = (
        (entry for pair in value.items() for entry in pair)
        if isinstance(value, dict)
        else iter(value)
    )
    for item in values:
        _validate_receipt_shape(
            item,
            label=label,
            depth=depth + 1,
            ancestors=nested_ancestors,
        )


def _decode_receipt(encoded: str, *, label: str) -> dict[str, Any]:
    """Decode one strict, non-empty receipt object."""

    if len(encoded.encode("utf-8")) > MAX_RECEIPT_BYTES:
        raise ValueError(f"{label} exceeds the {MAX_RECEIPT_BYTES}-byte receipt budget")

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-standard JSON constant {value}")

    try:
        value = json.loads(encoded, parse_constant=reject_constant)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} is not strict JSON: {exc}") from exc
    if not isinstance(value, dict) or not value:
        raise TypeError(f"{label} must be a non-empty JSON object")
    _validate_receipt_shape(value, label=label)
    return cast(dict[str, Any], value)


async def captured_receipt_or_run(
    run_demo: Callable[..., Awaitable[dict[str, Any]]],
    storage_uri: str,
) -> dict[str, Any]:
    """Validate the runner-captured receipt, or execute a standalone test.

    The operational runner sets ``CAPTURED_RECEIPT_ENV`` so a semantic pytest
    oracle checks the exact result emitted by the source process. Normal
    focused tests leave it unset and exercise ``run_demo`` directly.
    """

    captured = os.environ.get(CAPTURED_RECEIPT_ENV)
    if captured is None:
        return await run_demo(storage_uri=storage_uri)
    path = Path(captured)
    return _decode_receipt(
        path.read_text(encoding="utf-8"),
        label=f"captured example receipt {path}",
    )


def _contains_storage_location(value: object, storage_locations: set[str]) -> bool:
    """Return whether semantic output retained its isolated storage location."""

    if isinstance(value, str):
        return any(location and location in value for location in storage_locations)
    if isinstance(value, dict):
        return any(
            _contains_storage_location(key, storage_locations)
            or _contains_storage_location(item, storage_locations)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_storage_location(item, storage_locations) for item in value)
    return False


async def run_example(path: Path, storage_uri: str) -> dict[str, object]:
    """Call a deterministic example through its structured authoring seam."""

    if not storage_uri:
        raise ValueError("storage_uri must be non-empty")
    module = _load_example(path)
    run_demo = getattr(module, "run_demo", None)
    if run_demo is None or not inspect.iscoroutinefunction(run_demo):
        raise TypeError(f"{path} must expose async run_demo(storage_uri: str)")
    signature = inspect.signature(run_demo)
    storage_parameter = signature.parameters.get("storage_uri")
    if storage_parameter is None:
        raise TypeError(f"{path}: run_demo must accept storage_uri")
    if storage_parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
        raise TypeError(f"{path}: run_demo storage_uri must be keyword-callable")
    required_other = [
        parameter.name
        for parameter in signature.parameters.values()
        if parameter.name != "storage_uri"
        and parameter.default is inspect.Parameter.empty
        and parameter.kind
        in {
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }
    ]
    if required_other:
        raise TypeError(
            f"{path}: operational run_demo has unsupported required parameters {required_other}"
        )
    result = await run_demo(storage_uri=storage_uri)
    if not isinstance(result, dict) or not result:
        raise TypeError(f"{path}: run_demo must return a non-empty dict")
    storage_locations = {storage_uri}
    if "://" not in storage_uri:
        storage_locations.add(str(Path(storage_uri).resolve()))
    if _contains_storage_location(result, storage_locations):
        raise ValueError(f"{path}: run_demo receipt leaked its isolated storage location")
    # Fail here if an example leaks live objects, NaN/Infinity, or other
    # non-portable values. The runner deliberately does not stringify them.
    _json_receipt(result, label=f"{path}: run_demo receipt")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--storage-uri", required=True)
    args = parser.parse_args(argv)
    result = asyncio.run(run_example(args.source, args.storage_uri))
    print(f"{RECEIPT_PREFIX}{_json_receipt(result, label='example receipt')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify that an index contains only the attested release artifacts."""

from __future__ import annotations

import argparse
import base64
import importlib.metadata
import json
import re
import subprocess
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import urlopen

if __package__:
    from .release_artifact import DISTRIBUTIONS, SCHEMA, artifact_records, manifest_sha256
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import DISTRIBUTIONS, SCHEMA, artifact_records, manifest_sha256

DEFAULT_API_TEMPLATE = "https://pypi.org/pypi/{distribution}/{version}/json"
DEFAULT_INTEGRITY_TEMPLATE = (
    "https://pypi.org/integrity/{distribution}/{version}/{filename}/provenance"
)
Fetch = Callable[[str], dict[str, Any] | None]
Sleep = Callable[[float], None]
VerifyAttestation = Callable[[str, str, bool], None]
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_ATTESTATION_TOOL_VERSION = "0.0.30"


class IncompleteIndexError(ValueError):
    """The index has not yet made all required release evidence visible."""


class CryptographicVerificationError(RuntimeError):
    """The registry artifact could not yet be cryptographically verified."""


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError("release artifact manifest must be an object")
    return value


def _fetch(url: str) -> dict[str, Any] | None:
    try:
        with urlopen(url, timeout=30) as response:  # noqa: S310 - validated HTTPS URL
            payload = json.load(response)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise RuntimeError(f"release index request failed for {url}: HTTP {error.code}") from error
    except URLError as error:
        raise RuntimeError(f"release index request failed for {url}: {error.reason}") from error
    if not isinstance(payload, dict):
        raise TypeError(f"release index response for {url} must be an object")
    return payload


def _expected_by_filename(
    manifest: dict[str, Any],
    *,
    expected_commit: str,
) -> dict[str, dict[str, Any]]:
    if manifest.get("schema") != SCHEMA:
        raise ValueError("release index verification requires the current artifact schema")
    if manifest.get("clean_checkout") is not True:
        raise ValueError("release index verification requires a clean-checkout manifest")
    version = manifest.get("version")
    if not isinstance(version, str) or not version:
        raise ValueError("release artifact manifest has no version")
    if _COMMIT.fullmatch(expected_commit) is None:
        raise ValueError("release index verification requires one full expected commit")
    commit = manifest.get("commit")
    if commit != expected_commit:
        raise ValueError(
            f"release artifact manifest commit mismatch: {commit!r} != {expected_commit!r}"
        )
    return {str(record["name"]): record for record in artifact_records(manifest).values()}


def _template_url(template: str, **coordinates: str) -> str:
    try:
        value = template.format(
            distribution=quote(coordinates.get("distribution", ""), safe=""),
            version=quote(coordinates.get("version", ""), safe=""),
            filename=quote(coordinates.get("filename", ""), safe=""),
        )
    except (KeyError, ValueError) as error:
        raise ValueError(f"invalid release index URL template {template!r}") from error
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise ValueError("release index verification requires an HTTPS URL template")
    return value


def _repository_url(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or parsed.username
        or parsed.password
        or parsed.params
        or parsed.query
        or parsed.fragment
        or len([part for part in parsed.path.split("/") if part]) != 2
    ):
        raise ValueError("attestation repository must be an exact HTTPS GitHub repository URL")
    return value.rstrip("/")


def verify_payloads(
    manifest: dict[str, Any],
    payloads: Mapping[str, dict[str, Any] | None],
    *,
    require_complete: bool,
    expected_commit: str,
) -> dict[str, Any]:
    """Compare index JSON payloads with the exact release manifest."""

    expected = _expected_by_filename(manifest, expected_commit=expected_commit)
    version = str(manifest["version"])
    observed: set[str] = set()
    observed_records: list[dict[str, Any]] = []
    project_counts: dict[str, int] = {}
    for distribution in DISTRIBUTIONS:
        payload = payloads.get(distribution)
        if payload is None:
            project_counts[distribution] = 0
            continue
        info = payload.get("info")
        if not isinstance(info, dict) or info.get("version") != version:
            raise ValueError(f"index metadata for {distribution} does not identify {version}")
        urls = payload.get("urls")
        if not isinstance(urls, list):
            raise TypeError(f"index metadata for {distribution} has no artifact list")
        count = 0
        for value in urls:
            if not isinstance(value, dict):
                raise TypeError(f"index artifact for {distribution} must be an object")
            filename = value.get("filename")
            if not isinstance(filename, str) or filename in observed:
                raise ValueError(
                    f"index contains an invalid or duplicate artifact filename {filename!r}"
                )
            record = expected.get(filename)
            if record is None or record.get("distribution") != distribution:
                raise ValueError(f"index contains unattested artifact {filename!r}")
            digests = value.get("digests")
            sha256 = digests.get("sha256") if isinstance(digests, dict) else None
            size = value.get("size")
            if sha256 != record.get("sha256") or size != record.get("size_bytes"):
                raise ValueError(f"index artifact {filename!r} does not match attested bytes")
            expected_kind = "bdist_wheel" if record.get("kind") == "wheel" else "sdist"
            if value.get("packagetype") != expected_kind:
                raise ValueError(f"index artifact {filename!r} has the wrong package type")
            if value.get("yanked") is not False:
                raise ValueError(f"index artifact {filename!r} must be explicitly unyanked")
            observed.add(filename)
            observed_records.append(
                {
                    "distribution": distribution,
                    "kind": record["kind"],
                    "name": filename,
                    "sha256": record["sha256"],
                    "size_bytes": record["size_bytes"],
                }
            )
            count += 1
        project_counts[distribution] = count

    expected_names = set(expected)
    if require_complete and observed != expected_names:
        missing = sorted(expected_names - observed)
        raise IncompleteIndexError(
            "release index is missing attested artifacts: " + ", ".join(missing)
        )
    return {
        "schema": "archetype.release-index-evidence/v2",
        "version": version,
        "manifest_commit": manifest["commit"],
        "manifest_sha256": manifest_sha256(manifest),
        "complete": observed == expected_names,
        "artifact_count": len(observed),
        "artifacts": sorted(observed_records, key=lambda value: str(value["name"])),
        "projects": project_counts,
    }


def _statement(value: object, *, filename: str) -> dict[str, Any]:
    if not isinstance(value, str):
        raise TypeError(f"release provenance for {filename!r} has no statement")
    try:
        decoded = base64.b64decode(value, validate=True)
        statement = json.loads(decoded)
    except (ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"release provenance for {filename!r} has an invalid statement") from error
    if not isinstance(statement, dict):
        raise TypeError(f"release provenance statement for {filename!r} must be an object")
    return statement


def verify_provenance(
    artifacts: list[dict[str, Any]],
    provenances: Mapping[str, dict[str, Any] | None],
    *,
    publisher: Mapping[str, str],
) -> dict[str, Any]:
    """Verify PyPI-reported publish provenance for every observed file."""

    expected_publisher = dict(publisher)
    required_publisher_keys = {"kind", "repository", "workflow", "environment"}
    if set(expected_publisher) != required_publisher_keys or any(
        not isinstance(value, str) or not value for value in expected_publisher.values()
    ):
        raise ValueError("release provenance requires one exact publisher identity")

    verified: list[str] = []
    for artifact in artifacts:
        filename = str(artifact["name"])
        provenance = provenances.get(filename)
        if provenance is None:
            raise IncompleteIndexError(f"release index is missing provenance for {filename!r}")
        if provenance.get("version") != 1:
            raise ValueError(f"release provenance for {filename!r} has an unsupported version")
        bundles = provenance.get("attestation_bundles")
        if not isinstance(bundles, list) or not bundles:
            raise ValueError(f"release provenance for {filename!r} has no attestation bundles")

        found_publish = False
        for bundle in bundles:
            if not isinstance(bundle, dict) or bundle.get("publisher") != expected_publisher:
                raise ValueError(f"release provenance for {filename!r} has an unexpected publisher")
            attestations = bundle.get("attestations")
            if not isinstance(attestations, list) or not attestations:
                raise ValueError(f"release provenance for {filename!r} has no attestations")
            for attestation in attestations:
                if not isinstance(attestation, dict):
                    raise TypeError(f"release attestation for {filename!r} must be an object")
                envelope = attestation.get("envelope")
                if not isinstance(envelope, dict):
                    raise TypeError(f"release attestation for {filename!r} has no envelope")
                statement = _statement(envelope.get("statement"), filename=filename)
                if statement.get("predicateType") != (
                    "https://docs.pypi.org/attestations/publish/v1"
                ):
                    continue
                expected_subject = [
                    {
                        "name": filename,
                        "digest": {"sha256": artifact["sha256"]},
                    }
                ]
                if (
                    statement.get("_type") != "https://in-toto.io/Statement/v1"
                    or statement.get("subject") != expected_subject
                    or statement.get("predicate") not in (None, {})
                ):
                    raise ValueError(
                        f"release publish attestation for {filename!r} is not digest-bound"
                    )
                found_publish = True
        if not found_publish:
            raise ValueError(f"release provenance for {filename!r} has no publish attestation")
        verified.append(filename)

    return {
        "publisher": expected_publisher,
        "artifact_count": len(verified),
        "artifacts": sorted(verified),
    }


def verify_attestation(
    filename: str,
    repository: str,
    staging: bool,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> None:
    """Cryptographically verify one registry artifact with the pinned PyPI tool."""

    repository = _repository_url(repository)
    try:
        observed_version = importlib.metadata.version("pypi-attestations")
    except importlib.metadata.PackageNotFoundError as error:
        raise RuntimeError(
            f"release verification requires pypi-attestations=={_ATTESTATION_TOOL_VERSION}"
        ) from error
    if observed_version != _ATTESTATION_TOOL_VERSION:
        raise RuntimeError(
            "release verification requires "
            f"pypi-attestations=={_ATTESTATION_TOOL_VERSION}, found {observed_version}"
        )
    command = [
        "pypi-attestations",
        "verify",
        "pypi",
        "--repository",
        repository,
    ]
    if staging:
        command.append("--staging")
    command.append(f"pypi:{filename}")
    process = run(command, check=False, capture_output=True, text=True)
    if process.returncode:
        raise CryptographicVerificationError(
            f"cryptographic provenance verification failed for {filename!r}\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )


def verify_index(
    manifest: dict[str, Any],
    *,
    api_template: str,
    require_complete: bool,
    expected_commit: str,
    integrity_template: str | None = None,
    publisher: Mapping[str, str] | None = None,
    attestation_repository: str | None = None,
    attestation_staging: bool = False,
    fetch: Fetch = _fetch,
    verify_cryptographic_attestation: VerifyAttestation = verify_attestation,
    attempts: int = 1,
    interval_seconds: float = 0,
    sleep: Sleep = time.sleep,
) -> dict[str, Any]:
    if attempts < 1:
        raise ValueError("release index verification attempts must be positive")
    version = manifest.get("version")
    if not isinstance(version, str) or not version:
        raise ValueError("release artifact manifest has no version")
    if (integrity_template is None) is not (publisher is None):
        raise ValueError("integrity template and publisher identity must be configured together")
    if (integrity_template is None) is not (attestation_repository is None):
        raise ValueError(
            "integrity verification and cryptographic attestation repository "
            "must be configured together"
        )
    if attestation_staging and attestation_repository is None:
        raise ValueError("staging attestation verification requires a repository")
    if attestation_repository is not None:
        attestation_repository = _repository_url(attestation_repository)
        assert publisher is not None
        if publisher.get("kind") != "GitHub" or publisher.get("repository") != urlparse(
            attestation_repository
        ).path.strip("/"):
            raise ValueError("attestation repository does not match the publisher identity")
    for attempt in range(attempts):
        payloads = {
            distribution: fetch(
                _template_url(
                    api_template,
                    distribution=distribution,
                    version=version,
                )
            )
            for distribution in DISTRIBUTIONS
        }
        try:
            result = verify_payloads(
                manifest,
                payloads,
                require_complete=require_complete,
                expected_commit=expected_commit,
            )
            result["index_api_template"] = api_template
            if integrity_template is not None and publisher is not None:
                provenances = {
                    str(artifact["name"]): fetch(
                        _template_url(
                            integrity_template,
                            distribution=str(artifact["distribution"]),
                            version=version,
                            filename=str(artifact["name"]),
                        )
                    )
                    for artifact in result["artifacts"]
                }
                result["integrity_api_template"] = integrity_template
                result["provenance"] = verify_provenance(
                    result["artifacts"],
                    provenances,
                    publisher=publisher,
                )
                assert attestation_repository is not None
                for artifact in result["artifacts"]:
                    verify_cryptographic_attestation(
                        str(artifact["name"]),
                        attestation_repository,
                        attestation_staging,
                    )
                result["cryptographic_provenance"] = {
                    "tool": "pypi-attestations",
                    "tool_version": _ATTESTATION_TOOL_VERSION,
                    "repository": attestation_repository,
                    "staging": attestation_staging,
                    "artifact_count": len(result["artifacts"]),
                    "artifacts": sorted(str(artifact["name"]) for artifact in result["artifacts"]),
                }
            return result
        except CryptographicVerificationError as error:
            if attempt + 1 == attempts:
                raise CryptographicVerificationError(
                    "cryptographic provenance verification exhausted "
                    f"{attempts} attempt(s)\n{error}"
                ) from error
            sleep(interval_seconds)
        except IncompleteIndexError:
            if not require_complete:
                raise
            if attempt + 1 == attempts:
                raise
            sleep(interval_seconds)
    raise AssertionError("release index retry loop did not return")  # pragma: no cover


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "complete"))
    parser.add_argument("--manifest", type=Path, default=Path("release-artifact.json"))
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--api-template", default=DEFAULT_API_TEMPLATE)
    parser.add_argument("--integrity-template")
    parser.add_argument("--publisher-kind", default="GitHub")
    parser.add_argument("--publisher-repository")
    parser.add_argument("--publisher-workflow")
    parser.add_argument("--publisher-environment")
    parser.add_argument("--attestation-repository")
    parser.add_argument("--attestation-staging", action="store_true")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=5)
    args = parser.parse_args(argv)
    publisher_values = (
        args.publisher_repository,
        args.publisher_workflow,
        args.publisher_environment,
    )
    if args.integrity_template is None and any(publisher_values):
        parser.error("publisher identity requires --integrity-template")
    if args.integrity_template is not None and not all(publisher_values):
        parser.error("integrity verification requires the complete publisher identity")
    if args.integrity_template is not None and args.attestation_repository is None:
        parser.error("integrity verification requires --attestation-repository")
    if args.attestation_repository is not None and args.integrity_template is None:
        parser.error("attestation verification requires --integrity-template")
    publisher = (
        {
            "kind": args.publisher_kind,
            "repository": args.publisher_repository,
            "workflow": args.publisher_workflow,
            "environment": args.publisher_environment,
        }
        if args.integrity_template is not None
        else None
    )
    result = verify_index(
        _load(args.manifest),
        api_template=args.api_template,
        require_complete=args.command == "complete",
        expected_commit=args.expected_commit,
        integrity_template=args.integrity_template,
        publisher=publisher,
        attestation_repository=args.attestation_repository,
        attestation_staging=args.attestation_staging,
        attempts=args.attempts,
        interval_seconds=args.interval_seconds,
    )
    if args.out is not None:
        args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    state = "complete" if result["complete"] else "safe partial/empty"
    print(
        f"Release index {state}: {result['artifact_count']} attested artifacts "
        f"for {result['version']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

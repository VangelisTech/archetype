#!/usr/bin/env python3
# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Verify that an index contains only the attested release artifacts."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import json
import re
import subprocess
import time
from collections.abc import Callable, Mapping
from http.client import HTTPException, HTTPMessage
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import IO, Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

if __package__:
    from .release_artifact import DISTRIBUTIONS, SCHEMA, artifact_records, manifest_sha256
else:  # pragma: no cover - exercised by the command-line entry point
    from release_artifact import DISTRIBUTIONS, SCHEMA, artifact_records, manifest_sha256

DEFAULT_API_TEMPLATE = "https://pypi.org/pypi/{distribution}/{version}/json"
DEFAULT_INTEGRITY_TEMPLATE = (
    "https://pypi.org/integrity/{distribution}/{version}/{filename}/provenance"
)
Fetch = Callable[[str], dict[str, Any] | None]
FetchBytes = Callable[[str, int], bytes]
Sleep = Callable[[float], None]
VerifyAttestation = Callable[[Mapping[str, Any], Mapping[str, Any], str], None]
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_ATTESTATION_TOOL_VERSION = "0.0.30"
_ATTESTATION_TIMEOUT_SECONDS = 180
_ARTIFACT_HOSTS = {"files.pythonhosted.org", "test-files.pythonhosted.org"}
_DOWNLOAD_CHUNK_SIZE = 64 * 1024


class IncompleteIndexError(ValueError):
    """The index has not yet made all required release evidence visible."""


class CryptographicVerificationError(RuntimeError):
    """The registry artifact could not yet be cryptographically verified."""


class RegistryTransportError(RuntimeError):
    """The package registry could not yet return a complete response."""


class _RejectRedirects(HTTPRedirectHandler):
    """Keep registry artifact downloads on the already validated host."""

    def redirect_request(
        self,
        req: Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> Request | None:
        del req, fp, code, msg, headers, newurl
        return None


_ARTIFACT_OPENER = build_opener(_RejectRedirects())


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
        raise RegistryTransportError(
            f"release index request failed for {url}: HTTP {error.code}"
        ) from error
    except (HTTPException, OSError, URLError) as error:
        reason = getattr(error, "reason", error)
        raise RegistryTransportError(f"release index request failed for {url}: {reason}") from error
    if not isinstance(payload, dict):
        raise TypeError(f"release index response for {url} must be an object")
    return payload


def _fetch_bytes(url: str, expected_size: int) -> bytes:
    if isinstance(expected_size, bool) or expected_size < 0:
        raise ValueError("release artifact size must be a non-negative integer")
    try:
        with _ARTIFACT_OPENER.open(
            url,
            timeout=30,
        ) as response:  # noqa: S310 - validated PyPI host; redirects disabled
            content_length = response.headers.get("Content-Length")
            if content_length is not None:
                try:
                    observed_length = int(content_length)
                except ValueError as error:
                    raise RegistryTransportError(
                        f"release artifact response for {url} has invalid Content-Length"
                    ) from error
                if observed_length != expected_size:
                    raise RegistryTransportError(
                        f"release artifact response for {url} has unexpected Content-Length"
                    )

            content = bytearray()
            remaining = expected_size + 1
            while remaining:
                chunk = response.read(min(_DOWNLOAD_CHUNK_SIZE, remaining))
                if not chunk:
                    break
                content.extend(chunk)
                remaining -= len(chunk)
            if len(content) != expected_size:
                raise RegistryTransportError(
                    f"release artifact response for {url} has unexpected size"
                )
            return bytes(content)
    except HTTPError as error:
        raise RegistryTransportError(
            f"release artifact request failed for {url}: HTTP {error.code}"
        ) from error
    except (HTTPException, OSError, URLError) as error:
        reason = getattr(error, "reason", error)
        raise RegistryTransportError(
            f"release artifact request failed for {url}: {reason}"
        ) from error


def _artifact_url(
    value: object,
    *,
    filename: str,
    expected_host: str | None = None,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"index artifact {filename!r} has no download URL")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or parsed.netloc not in _ARTIFACT_HOSTS
        or (expected_host is not None and parsed.netloc != expected_host)
        or parsed.username
        or parsed.password
        or parsed.params
        or parsed.query
        or parsed.fragment
        or unquote(parsed.path.rsplit("/", 1)[-1]) != filename
    ):
        raise ValueError(f"index artifact {filename!r} has an unexpected download URL")
    return value


def _registry_artifact_host(value: str) -> str:
    if value not in _ARTIFACT_HOSTS:
        raise ValueError(
            "registry artifact host must be files.pythonhosted.org or test-files.pythonhosted.org"
        )
    return value


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
    expected_artifact_host: str | None = None,
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
            artifact_url = _artifact_url(
                value.get("url"),
                filename=filename,
                expected_host=expected_artifact_host,
            )
            observed.add(filename)
            observed_records.append(
                {
                    "distribution": distribution,
                    "kind": record["kind"],
                    "name": filename,
                    "sha256": record["sha256"],
                    "size_bytes": record["size_bytes"],
                    "url": artifact_url,
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
    artifact: Mapping[str, Any],
    provenance: Mapping[str, Any],
    repository: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    fetch_bytes: FetchBytes = _fetch_bytes,
) -> None:
    """Cryptographically verify one registry artifact with the pinned PyPI tool."""

    repository = _repository_url(repository)
    filename = artifact.get("name")
    if not isinstance(filename, str):
        raise TypeError("cryptographic verification requires an artifact filename")
    artifact_url = _artifact_url(artifact.get("url"), filename=filename)
    expected_sha256 = artifact.get("sha256")
    expected_size = artifact.get("size_bytes")
    if (
        not isinstance(expected_sha256, str)
        or not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
    ):
        raise TypeError(f"cryptographic verification metadata is incomplete for {filename!r}")
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
    try:
        distribution_bytes = fetch_bytes(artifact_url, expected_size)
    except (HTTPException, OSError, RuntimeError, TypeError, ValueError) as error:
        raise CryptographicVerificationError(
            f"cryptographic verification could not fetch {filename!r}: {error}"
        ) from error
    if (
        len(distribution_bytes) != expected_size
        or hashlib.sha256(distribution_bytes).hexdigest() != expected_sha256
    ):
        raise CryptographicVerificationError(
            f"cryptographic verification downloaded unexpected bytes for {filename!r}"
        )

    with TemporaryDirectory(prefix="archetype-pypi-attestation-") as directory:
        artifact_path = Path(directory, filename)
        provenance_path = Path(directory, f"{filename}.provenance.json")
        artifact_path.write_bytes(distribution_bytes)
        provenance_path.write_text(
            json.dumps(dict(provenance), separators=(",", ":")),
            encoding="utf-8",
        )
        command = [
            "pypi-attestations",
            "verify",
            "pypi",
            "--repository",
            repository,
            "--provenance-file",
            str(provenance_path),
        ]
        command.append(str(artifact_path))
        try:
            process = run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=_ATTESTATION_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as error:
            raise CryptographicVerificationError(
                "cryptographic provenance verification timed out for "
                f"{filename!r} after {_ATTESTATION_TIMEOUT_SECONDS} seconds"
            ) from error
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
    registry_artifact_host: str | None = None,
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
    if (attestation_repository is None) is not (registry_artifact_host is None):
        raise ValueError(
            "cryptographic attestation verification requires one registry artifact host"
        )
    if attestation_repository is not None:
        attestation_repository = _repository_url(attestation_repository)
        assert registry_artifact_host is not None
        registry_artifact_host = _registry_artifact_host(registry_artifact_host)
        assert publisher is not None
        if publisher.get("kind") != "GitHub" or publisher.get("repository") != urlparse(
            attestation_repository
        ).path.strip("/"):
            raise ValueError("attestation repository does not match the publisher identity")
    for attempt in range(attempts):
        try:
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
            result = verify_payloads(
                manifest,
                payloads,
                require_complete=require_complete,
                expected_commit=expected_commit,
                expected_artifact_host=registry_artifact_host,
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
                    filename = str(artifact["name"])
                    provenance = provenances[filename]
                    assert provenance is not None
                    verify_cryptographic_attestation(
                        artifact,
                        provenance,
                        attestation_repository,
                    )
                result["cryptographic_provenance"] = {
                    "tool": "pypi-attestations",
                    "tool_version": _ATTESTATION_TOOL_VERSION,
                    "repository": attestation_repository,
                    "registry_artifact_host": registry_artifact_host,
                    "sigstore_environment": "production",
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
        except RegistryTransportError as error:
            if attempt + 1 == attempts:
                raise RegistryTransportError(
                    f"release registry transport exhausted {attempts} attempt(s)\n{error}"
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
    parser.add_argument("--registry-artifact-host")
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
    if (args.attestation_repository is None) is not (args.registry_artifact_host is None):
        parser.error(
            "attestation verification requires --attestation-repository and "
            "--registry-artifact-host"
        )
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
        registry_artifact_host=args.registry_artifact_host,
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

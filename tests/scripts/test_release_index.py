# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for idempotent, exact-byte package-index publication."""

from __future__ import annotations

import base64
import json
import subprocess
from typing import Any, cast

import pytest

from scripts.release_artifact import DISTRIBUTIONS, SCHEMA
from scripts.verify_release_index import (
    CryptographicVerificationError,
    IncompleteIndexError,
    verify_attestation,
    verify_index,
    verify_payloads,
    verify_provenance,
)

COMMIT = "a" * 40

_PREFIXES = {
    "archetype-ecs": "archetype_ecs",
    "archetype-missions": "archetype_missions",
    "archetype-physical-ai": "archetype_physical_ai",
    "archetype-research": "archetype_research",
}


def _manifest() -> dict[str, Any]:
    records = []
    for index, distribution in enumerate(DISTRIBUTIONS):
        prefix = _PREFIXES[distribution]
        records.extend(
            (
                {
                    "distribution": distribution,
                    "kind": "wheel",
                    "name": f"{prefix}-0.6.0-py3-none-any.whl",
                    "sha256": f"{index + 1:064x}",
                    "size_bytes": 100 + index,
                },
                {
                    "distribution": distribution,
                    "kind": "sdist",
                    "name": f"{prefix}-0.6.0.tar.gz",
                    "sha256": f"{index + 11:064x}",
                    "size_bytes": 200 + index,
                },
            )
        )
    return {
        "schema": SCHEMA,
        "version": "0.6.0",
        "commit": "a" * 40,
        "clean_checkout": True,
        "artifacts": records,
    }


def _payloads(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    payloads = {
        distribution: {"info": {"version": manifest["version"]}, "urls": []}
        for distribution in DISTRIBUTIONS
    }
    for record in manifest["artifacts"]:
        urls = cast(list[dict[str, Any]], payloads[record["distribution"]]["urls"])
        urls.append(
            {
                "filename": record["name"],
                "digests": {"sha256": record["sha256"]},
                "size": record["size_bytes"],
                "packagetype": "bdist_wheel" if record["kind"] == "wheel" else "sdist",
                "yanked": False,
            }
        )
    return payloads


def _publisher(environment: str = "release-pypi") -> dict[str, str]:
    return {
        "kind": "GitHub",
        "repository": "VangelisTech/archetype",
        "workflow": "release.yml",
        "environment": environment,
    }


def _provenance(record: dict[str, Any], publisher: dict[str, str]) -> dict[str, Any]:
    statement = {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [
            {
                "name": record["name"],
                "digest": {"sha256": record["sha256"]},
            }
        ],
        "predicateType": "https://docs.pypi.org/attestations/publish/v1",
        "predicate": None,
    }
    encoded = base64.b64encode(json.dumps(statement, separators=(",", ":")).encode()).decode()
    return {
        "version": 1,
        "attestation_bundles": [
            {
                "publisher": dict(publisher),
                "attestations": [{"envelope": {"statement": encoded}}],
            }
        ],
    }


def test_preflight_accepts_empty_or_exact_partial_publication() -> None:
    manifest = _manifest()
    complete = _payloads(manifest)
    payloads: dict[str, dict[str, Any] | None] = {
        distribution: None for distribution in DISTRIBUTIONS
    }
    payloads["archetype-ecs"] = {
        "info": complete["archetype-ecs"]["info"],
        "urls": complete["archetype-ecs"]["urls"][:1],
    }

    result = verify_payloads(
        manifest,
        payloads,
        require_complete=False,
        expected_commit=COMMIT,
    )

    assert result["complete"] is False
    assert result["artifact_count"] == 1
    assert result["projects"]["archetype-ecs"] == 1


def test_complete_index_requires_all_eight_attested_artifacts() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)

    result = verify_payloads(
        manifest,
        payloads,
        require_complete=True,
        expected_commit=COMMIT,
    )

    assert result["complete"] is True
    assert result["artifact_count"] == 8
    assert result["manifest_commit"] == "a" * 40
    assert len(result["manifest_sha256"]) == 64
    assert [row["name"] for row in result["artifacts"]] == sorted(
        record["name"] for record in manifest["artifacts"]
    )
    payloads["archetype-research"]["urls"].pop()
    with pytest.raises(ValueError, match="missing attested artifacts"):
        verify_payloads(
            manifest,
            payloads,
            require_complete=True,
            expected_commit=COMMIT,
        )


@pytest.mark.parametrize("field", ["sha256", "size", "packagetype", "yanked"])
def test_index_rejects_non_attested_artifact_metadata(field: str) -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    record = payloads["archetype-missions"]["urls"][0]
    if field == "sha256":
        record["digests"]["sha256"] = "f" * 64
    elif field == "size":
        record["size"] = -1
    elif field == "packagetype":
        record["packagetype"] = "sdist"
    else:
        record["yanked"] = True

    with pytest.raises(ValueError, match="does not match|wrong package type|unyanked"):
        verify_payloads(
            manifest,
            payloads,
            require_complete=False,
            expected_commit=COMMIT,
        )


def test_index_rejects_unattested_filename() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    payloads["archetype-research"]["urls"][0]["filename"] = "surprise.whl"

    with pytest.raises(ValueError, match="unattested artifact"):
        verify_payloads(
            manifest,
            payloads,
            require_complete=False,
            expected_commit=COMMIT,
        )


def test_remote_verifier_requests_each_exact_project_version() -> None:
    manifest = _manifest()
    requested: list[str] = []

    def fetch(url: str) -> None:
        requested.append(url)
        return None

    result = verify_index(
        manifest,
        api_template="https://index.invalid/{distribution}/{version}",
        require_complete=False,
        expected_commit=COMMIT,
        fetch=fetch,
    )

    assert requested == [
        f"https://index.invalid/{distribution}/0.6.0" for distribution in DISTRIBUTIONS
    ]
    assert result["artifact_count"] == 0


def test_complete_index_retries_only_missing_attested_files() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    incomplete = {**payloads, "archetype-research": None}
    rounds = iter((incomplete, payloads))
    current: dict[str, dict[str, Any] | None] = {}
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any] | None:
        distribution = url.split("/")[-2]
        if distribution == DISTRIBUTIONS[0]:
            current.clear()
            current.update(next(rounds))
        return current[distribution]

    result = verify_index(
        manifest,
        api_template="https://index.invalid/{distribution}/{version}",
        require_complete=True,
        expected_commit=COMMIT,
        fetch=fetch,
        attempts=2,
        interval_seconds=7,
        sleep=sleeps.append,
    )

    assert result["complete"] is True
    assert sleeps == [7]


def test_provenance_binds_every_file_to_exact_publisher_and_digest() -> None:
    manifest = _manifest()
    result = verify_payloads(
        manifest,
        _payloads(manifest),
        require_complete=True,
        expected_commit=COMMIT,
    )
    publisher = _publisher()
    provenances = {
        record["name"]: _provenance(record, publisher) for record in manifest["artifacts"]
    }

    evidence = verify_provenance(result["artifacts"], provenances, publisher=publisher)

    assert evidence == {
        "publisher": publisher,
        "artifact_count": 8,
        "artifacts": sorted(provenances),
    }


def test_provenance_rejects_wrong_publisher_or_subject_digest() -> None:
    manifest = _manifest()
    result = verify_payloads(
        manifest,
        _payloads(manifest),
        require_complete=True,
        expected_commit=COMMIT,
    )
    publisher = _publisher()
    record = manifest["artifacts"][0]
    observed = next(value for value in result["artifacts"] if value["name"] == record["name"])
    provenance = _provenance(record, publisher)

    provenance["attestation_bundles"][0]["publisher"]["workflow"] = "other.yml"
    with pytest.raises(ValueError, match="unexpected publisher"):
        verify_provenance([observed], {record["name"]: provenance}, publisher=publisher)

    provenance = _provenance(record, publisher)
    envelope = provenance["attestation_bundles"][0]["attestations"][0]["envelope"]
    statement = json.loads(base64.b64decode(envelope["statement"]))
    statement["subject"][0]["digest"]["sha256"] = "f" * 64
    envelope["statement"] = base64.b64encode(json.dumps(statement).encode()).decode()
    with pytest.raises(ValueError, match="not digest-bound"):
        verify_provenance([observed], {record["name"]: provenance}, publisher=publisher)


def test_complete_index_retries_provenance_propagation() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    publisher = _publisher()
    records = {record["name"]: record for record in manifest["artifacts"]}
    missing_once = {next(iter(records))}
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any] | None:
        if "/pypi/" in url:
            return payloads[url.split("/")[-3]]
        filename = url.split("/")[-2]
        if filename in missing_once:
            missing_once.remove(filename)
            return None
        return _provenance(records[filename], publisher)

    result = verify_index(
        manifest,
        api_template="https://index.invalid/pypi/{distribution}/{version}/json",
        integrity_template=(
            "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
        ),
        publisher=publisher,
        attestation_repository="https://github.com/VangelisTech/archetype",
        require_complete=True,
        expected_commit=COMMIT,
        fetch=fetch,
        verify_cryptographic_attestation=lambda *_args: None,
        attempts=2,
        interval_seconds=3,
        sleep=sleeps.append,
    )

    assert result["provenance"]["artifact_count"] == 8
    assert result["index_api_template"].startswith("https://index.invalid/")
    assert sleeps == [3]


def test_complete_index_retries_transient_staging_crypto_propagation() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    publisher = _publisher("release-testpypi")
    records = {record["name"]: record for record in manifest["artifacts"]}
    calls: list[tuple[str, str, bool]] = []
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any]:
        if "/pypi/" in url:
            return payloads[url.split("/")[-3]]
        filename = url.split("/")[-2]
        return _provenance(records[filename], publisher)

    def verify_crypto(filename: str, repository: str, staging: bool) -> None:
        calls.append((filename, repository, staging))
        if len(calls) == 1:
            raise CryptographicVerificationError("staging provenance is still propagating")

    result = verify_index(
        manifest,
        api_template="https://index.invalid/pypi/{distribution}/{version}/json",
        integrity_template=(
            "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
        ),
        publisher=publisher,
        attestation_repository="https://github.com/VangelisTech/archetype",
        attestation_staging=True,
        require_complete=True,
        expected_commit=COMMIT,
        fetch=fetch,
        verify_cryptographic_attestation=verify_crypto,
        attempts=2,
        interval_seconds=4,
        sleep=sleeps.append,
    )

    assert result["cryptographic_provenance"]["staging"] is True
    assert result["cryptographic_provenance"]["artifact_count"] == 8
    assert len(calls) == 9
    assert all(
        repository == "https://github.com/VangelisTech/archetype" and staging is True
        for _filename, repository, staging in calls
    )
    assert sleeps == [4]


def test_complete_index_exhausts_persistent_crypto_failure_with_diagnostics() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    publisher = _publisher()
    records = {record["name"]: record for record in manifest["artifacts"]}
    calls: list[tuple[str, str, bool]] = []
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any]:
        if "/pypi/" in url:
            return payloads[url.split("/")[-3]]
        filename = url.split("/")[-2]
        return _provenance(records[filename], publisher)

    def verify_crypto(filename: str, repository: str, staging: bool) -> None:
        calls.append((filename, repository, staging))
        raise CryptographicVerificationError(
            f"cryptographic provenance verification failed for {filename!r}\n"
            "stdout:\nregistry response\nstderr:\nbad signature"
        )

    with pytest.raises(
        CryptographicVerificationError,
        match=(
            r"(?s)exhausted 3 attempt\(s\).*cryptographic provenance verification failed"
            r".*registry response.*bad signature"
        ),
    ):
        verify_index(
            manifest,
            api_template="https://index.invalid/pypi/{distribution}/{version}/json",
            integrity_template=(
                "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
            ),
            publisher=publisher,
            attestation_repository="https://github.com/VangelisTech/archetype",
            require_complete=True,
            expected_commit=COMMIT,
            fetch=fetch,
            verify_cryptographic_attestation=verify_crypto,
            attempts=3,
            interval_seconds=2,
            sleep=sleeps.append,
        )

    assert len(calls) == 3
    assert len({filename for filename, _repository, _staging in calls}) == 1
    assert all(staging is False for _filename, _repository, staging in calls)
    assert sleeps == [2, 2]


def test_remote_verifier_rejects_non_https_templates() -> None:
    with pytest.raises(ValueError, match="HTTPS URL template"):
        verify_index(
            _manifest(),
            api_template="http://index.invalid/{distribution}/{version}",
            require_complete=False,
            expected_commit=COMMIT,
            fetch=lambda _url: None,
        )


def test_preflight_requires_provenance_and_crypto_for_exact_partial_upload() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    publisher = _publisher()
    first = cast(dict[str, Any], manifest["artifacts"][0])
    partial = {distribution: None for distribution in DISTRIBUTIONS}
    partial[first["distribution"]] = {
        "info": payloads[first["distribution"]]["info"],
        "urls": payloads[first["distribution"]]["urls"][:1],
    }
    crypto: list[tuple[str, str, bool]] = []

    def fetch(url: str) -> dict[str, Any] | None:
        if "/integrity/" in url:
            return _provenance(first, publisher)
        distribution = url.split("/")[-2]
        return partial[distribution]

    result = verify_index(
        manifest,
        api_template="https://index.invalid/{distribution}/{version}",
        integrity_template=(
            "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
        ),
        publisher=publisher,
        attestation_repository="https://github.com/VangelisTech/archetype",
        require_complete=False,
        expected_commit=COMMIT,
        fetch=fetch,
        verify_cryptographic_attestation=lambda *values: crypto.append(values),
    )

    assert result["artifact_count"] == 1
    assert crypto == [
        (
            first["name"],
            "https://github.com/VangelisTech/archetype",
            False,
        )
    ]

    def token_upload(url: str) -> dict[str, Any] | None:
        if "/integrity/" in url:
            return None
        return fetch(url)

    with pytest.raises(IncompleteIndexError, match="missing provenance"):
        verify_index(
            manifest,
            api_template="https://index.invalid/{distribution}/{version}",
            integrity_template=(
                "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
            ),
            publisher=publisher,
            attestation_repository="https://github.com/VangelisTech/archetype",
            require_complete=False,
            expected_commit=COMMIT,
            fetch=token_upload,
            verify_cryptographic_attestation=lambda *_args: None,
        )


def test_cryptographic_verifier_is_pinned_and_preserves_failure_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "scripts.verify_release_index.importlib.metadata.version",
        lambda _name: "0.0.30",
    )
    calls: list[list[str]] = []

    def fail(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 1, "verification output", "bad signature")

    with pytest.raises(RuntimeError, match=r"(?s)verification output.*bad signature"):
        verify_attestation(
            "archetype_ecs-0.6.0-py3-none-any.whl",
            "https://github.com/VangelisTech/archetype",
            True,
            run=fail,
        )

    assert calls[0][-2:] == ["--staging", "pypi:archetype_ecs-0.6.0-py3-none-any.whl"]

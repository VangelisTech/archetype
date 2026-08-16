# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Contracts for idempotent, exact-byte package-index publication."""

from __future__ import annotations

import base64
import hashlib
import json
import subprocess
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast

import pytest

from scripts import verify_release_index as release_index
from scripts.release_artifact import DISTRIBUTIONS, PUBLISHER_WORKFLOWS, SCHEMA
from scripts.verify_release_index import (
    CryptographicVerificationError,
    IncompleteIndexError,
    RegistryTransportError,
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


def _payloads(
    manifest: dict[str, Any],
    *,
    artifact_host: str = "files.pythonhosted.org",
) -> dict[str, dict[str, Any]]:
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
                "url": f"https://{artifact_host}/packages/{record['name']}",
            }
        )
    return payloads


def _publisher(environment: str = "release-pypi") -> dict[str, str]:
    return {
        "kind": "GitHub",
        "repository": "VangelisTech/archetype",
        "environment": environment,
    }


def _provenance(record: dict[str, Any], publisher: dict[str, str]) -> dict[str, Any]:
    exact_publisher = {
        **publisher,
        "workflow": PUBLISHER_WORKFLOWS[record["distribution"]],
    }
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
                "publisher": exact_publisher,
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


@pytest.mark.parametrize(
    "url",
    [
        "https://example.invalid/packages/archetype_ecs-0.6.0-py3-none-any.whl",
        "https://files.pythonhosted.org/packages/a-different-file.whl",
        "http://files.pythonhosted.org/packages/archetype_ecs-0.6.0-py3-none-any.whl",
        "https://user@files.pythonhosted.org/packages/archetype_ecs-0.6.0-py3-none-any.whl",
        "https://files.pythonhosted.org/packages/archetype_ecs-0.6.0-py3-none-any.whl?download=1",
        "https://files.pythonhosted.org/packages/archetype_ecs-0.6.0-py3-none-any.whl#fragment",
        "https://files.pythonhosted.org/packages/%2Farchetype_ecs-0.6.0-py3-none-any.whl",
    ],
)
def test_index_rejects_untrusted_or_mismatched_artifact_url(url: str) -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    payloads["archetype-ecs"]["urls"][0]["url"] = url

    with pytest.raises(ValueError, match="unexpected download URL"):
        verify_payloads(
            manifest,
            payloads,
            require_complete=False,
            expected_commit=COMMIT,
        )


def test_index_requires_an_artifact_download_url() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    del payloads["archetype-ecs"]["urls"][0]["url"]

    with pytest.raises(TypeError, match="has no download URL"):
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
        "publishers": {
            distribution: {
                **publisher,
                "workflow": PUBLISHER_WORKFLOWS[distribution],
            }
            for distribution in DISTRIBUTIONS
        },
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
        registry_artifact_host="files.pythonhosted.org",
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


@pytest.mark.parametrize("failure_phase", ["index", "integrity"])
def test_complete_index_retries_transient_registry_transport(failure_phase: str) -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    publisher = _publisher()
    records = {record["name"]: record for record in manifest["artifacts"]}
    failed = False
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any]:
        nonlocal failed
        is_index = "/pypi/" in url
        if not failed and (
            (failure_phase == "index" and is_index)
            or (failure_phase == "integrity" and not is_index)
        ):
            failed = True
            raise RegistryTransportError(f"transient {failure_phase} transport failure")
        if is_index:
            return payloads[url.split("/")[-3]]
        filename = url.split("/")[-2]
        return _provenance(records[filename], publisher)

    result = verify_index(
        manifest,
        api_template="https://index.invalid/pypi/{distribution}/{version}/json",
        integrity_template=(
            "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
        ),
        publisher=publisher,
        attestation_repository="https://github.com/VangelisTech/archetype",
        registry_artifact_host="files.pythonhosted.org",
        require_complete=True,
        expected_commit=COMMIT,
        fetch=fetch,
        verify_cryptographic_attestation=lambda *_args: None,
        attempts=2,
        interval_seconds=2,
        sleep=sleeps.append,
    )

    assert result["complete"] is True
    assert failed is True
    assert sleeps == [2]


def test_complete_index_retries_transient_testpypi_crypto_propagation() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest, artifact_host="test-files.pythonhosted.org")
    publisher = _publisher("release-testpypi")
    records = {record["name"]: record for record in manifest["artifacts"]}
    calls: list[tuple[str, str, str]] = []
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any]:
        if "/pypi/" in url:
            return payloads[url.split("/")[-3]]
        filename = url.split("/")[-2]
        return _provenance(records[filename], publisher)

    def verify_crypto(
        artifact: Mapping[str, Any],
        provenance: Mapping[str, Any],
        repository: str,
    ) -> None:
        filename = cast(str, artifact["name"])
        assert provenance == _provenance(records[filename], publisher)
        calls.append((filename, cast(str, artifact["url"]), repository))
        if len(calls) == 1:
            raise CryptographicVerificationError("TestPyPI provenance is still propagating")

    result = verify_index(
        manifest,
        api_template="https://index.invalid/pypi/{distribution}/{version}/json",
        integrity_template=(
            "https://index.invalid/integrity/{distribution}/{version}/{filename}/provenance"
        ),
        publisher=publisher,
        attestation_repository="https://github.com/VangelisTech/archetype",
        registry_artifact_host="test-files.pythonhosted.org",
        require_complete=True,
        expected_commit=COMMIT,
        fetch=fetch,
        verify_cryptographic_attestation=verify_crypto,
        attempts=2,
        interval_seconds=4,
        sleep=sleeps.append,
    )

    assert result["cryptographic_provenance"]["sigstore_environment"] == "production"
    assert result["cryptographic_provenance"]["artifact_count"] == 8
    assert len(calls) == 9
    assert all(
        artifact_url.startswith("https://test-files.pythonhosted.org/")
        and repository == "https://github.com/VangelisTech/archetype"
        for _filename, artifact_url, repository in calls
    )
    assert sleeps == [4]


def test_complete_index_exhausts_persistent_crypto_failure_with_diagnostics() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)
    publisher = _publisher()
    records = {record["name"]: record for record in manifest["artifacts"]}
    calls: list[tuple[str, str]] = []
    sleeps: list[float] = []

    def fetch(url: str) -> dict[str, Any]:
        if "/pypi/" in url:
            return payloads[url.split("/")[-3]]
        filename = url.split("/")[-2]
        return _provenance(records[filename], publisher)

    def verify_crypto(
        artifact: Mapping[str, Any],
        _provenance: Mapping[str, Any],
        repository: str,
    ) -> None:
        filename = cast(str, artifact["name"])
        calls.append((filename, repository))
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
            registry_artifact_host="files.pythonhosted.org",
            require_complete=True,
            expected_commit=COMMIT,
            fetch=fetch,
            verify_cryptographic_attestation=verify_crypto,
            attempts=3,
            interval_seconds=2,
            sleep=sleeps.append,
        )

    assert len(calls) == 3
    assert len({filename for filename, _repository in calls}) == 1
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
    crypto: list[tuple[str, str, str]] = []

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
        registry_artifact_host="files.pythonhosted.org",
        require_complete=False,
        expected_commit=COMMIT,
        fetch=fetch,
        verify_cryptographic_attestation=lambda artifact, provenance, repository: crypto.append(
            (
                cast(str, artifact["name"]),
                cast(str, provenance["attestation_bundles"][0]["publisher"]["environment"]),
                repository,
            )
        ),
    )

    assert result["artifact_count"] == 1
    assert crypto == [
        (
            first["name"],
            "release-pypi",
            "https://github.com/VangelisTech/archetype",
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
            registry_artifact_host="files.pythonhosted.org",
            require_complete=False,
            expected_commit=COMMIT,
            fetch=token_upload,
            verify_cryptographic_attestation=lambda *_args: None,
        )


def test_testpypi_verifier_uses_local_files_and_production_sigstore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "scripts.verify_release_index.importlib.metadata.version",
        lambda _name: "0.0.30",
    )
    filename = "archetype_ecs-0.6.0-py3-none-any.whl"
    distribution_bytes = b"exact TestPyPI artifact bytes"
    provenance = {"version": 1, "attestation_bundles": []}
    artifact = {
        "name": filename,
        "sha256": hashlib.sha256(distribution_bytes).hexdigest(),
        "size_bytes": len(distribution_bytes),
        "url": f"https://test-files.pythonhosted.org/packages/{filename}",
    }
    calls: list[list[str]] = []
    observed_files: list[tuple[bytes, dict[str, Any]]] = []

    def fail(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        provenance_path = Path(command[command.index("--provenance-file") + 1])
        observed_files.append(
            (
                Path(command[-1]).read_bytes(),
                json.loads(provenance_path.read_text(encoding="utf-8")),
            )
        )
        return subprocess.CompletedProcess(command, 1, "verification output", "bad signature")

    with pytest.raises(RuntimeError, match=r"(?s)verification output.*bad signature"):
        verify_attestation(
            artifact,
            provenance,
            "https://github.com/VangelisTech/archetype",
            run=fail,
            fetch_bytes=lambda url, expected_size: (
                distribution_bytes
                if url == artifact["url"] and expected_size == len(distribution_bytes)
                else pytest.fail(f"unexpected artifact download: url={url}, size={expected_size}")
            ),
        )

    assert calls[0][-1].endswith(filename)
    assert "--staging" not in calls[0]
    assert not any(value.startswith("pypi:") for value in calls[0])
    assert observed_files == [(distribution_bytes, provenance)]


@pytest.mark.parametrize(
    "downloaded",
    [
        b"different registry bytes",
        b"EXPECTED REGISTRY BYTES",
    ],
)
def test_cryptographic_verifier_rejects_downloaded_byte_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    downloaded: bytes,
) -> None:
    monkeypatch.setattr(
        "scripts.verify_release_index.importlib.metadata.version",
        lambda _name: "0.0.30",
    )
    filename = "archetype_ecs-0.6.0.tar.gz"
    expected = b"expected registry bytes"
    artifact = {
        "name": filename,
        "sha256": hashlib.sha256(expected).hexdigest(),
        "size_bytes": len(expected),
        "url": f"https://files.pythonhosted.org/packages/{filename}",
    }

    with pytest.raises(CryptographicVerificationError, match="unexpected bytes"):
        verify_attestation(
            artifact,
            {"version": 1},
            "https://github.com/VangelisTech/archetype",
            run=lambda *_args, **_kwargs: pytest.fail("verifier must not run"),
            fetch_bytes=lambda _url, _expected_size: downloaded,
        )


def test_index_rejects_cross_registry_artifact_url() -> None:
    manifest = _manifest()
    payloads = _payloads(manifest)

    with pytest.raises(ValueError, match="unexpected download URL"):
        verify_payloads(
            manifest,
            payloads,
            require_complete=True,
            expected_commit=COMMIT,
            expected_artifact_host="test-files.pythonhosted.org",
        )


def test_cryptographic_verifier_wraps_transient_download_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "scripts.verify_release_index.importlib.metadata.version",
        lambda _name: "0.0.30",
    )
    filename = "archetype_ecs-0.6.0.tar.gz"
    artifact = {
        "name": filename,
        "sha256": hashlib.sha256(b"artifact").hexdigest(),
        "size_bytes": len(b"artifact"),
        "url": f"https://files.pythonhosted.org/packages/{filename}",
    }

    def fail_download(_url: str, _expected_size: int) -> bytes:
        raise TimeoutError("registry read timed out")

    with pytest.raises(
        CryptographicVerificationError,
        match=r"could not fetch.*registry read timed out",
    ):
        verify_attestation(
            artifact,
            {"version": 1},
            "https://github.com/VangelisTech/archetype",
            run=lambda *_args, **_kwargs: pytest.fail("verifier must not run"),
            fetch_bytes=fail_download,
        )


def test_cryptographic_verifier_bounds_subprocess_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "scripts.verify_release_index.importlib.metadata.version",
        lambda _name: "0.0.30",
    )
    filename = "archetype_ecs-0.6.0.tar.gz"
    distribution_bytes = b"artifact"
    artifact = {
        "name": filename,
        "sha256": hashlib.sha256(distribution_bytes).hexdigest(),
        "size_bytes": len(distribution_bytes),
        "url": f"https://files.pythonhosted.org/packages/{filename}",
    }

    def timeout(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        timeout_seconds = cast(float, kwargs["timeout"])
        raise subprocess.TimeoutExpired(command, timeout_seconds)

    with pytest.raises(CryptographicVerificationError, match="timed out.*180 seconds"):
        verify_attestation(
            artifact,
            {"version": 1},
            "https://github.com/VangelisTech/archetype",
            run=timeout,
            fetch_bytes=lambda _url, _expected_size: distribution_bytes,
        )


def test_artifact_downloader_rejects_redirects() -> None:
    target_requests: list[str] = []

    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler protocol
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/target")
                self.end_headers()
                return
            target_requests.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", "8")
            self.end_headers()
            self.wfile.write(b"artifact")

        def log_message(self, format: str, *args: Any) -> None:
            del format, args
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = cast(tuple[str, int], server.server_address)
        with pytest.raises(RuntimeError, match="HTTP 302"):
            release_index._fetch_bytes(f"http://{host}:{port}/redirect", 8)
        assert target_requests == []
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert not thread.is_alive()


def test_artifact_downloader_caps_response_at_expected_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class OversizedResponse:
        headers: dict[str, str] = {}

        def __init__(self) -> None:
            self.read_sizes: list[int] = []

        def __enter__(self) -> OversizedResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, size: int) -> bytes:
            self.read_sizes.append(size)
            return b"x" * size

    class OversizedOpener:
        def __init__(self, response: OversizedResponse) -> None:
            self.response = response

        def open(self, _url: str, *, timeout: int) -> OversizedResponse:
            assert timeout == 30
            return self.response

    response = OversizedResponse()
    monkeypatch.setattr(release_index, "_ARTIFACT_OPENER", OversizedOpener(response))

    with pytest.raises(RuntimeError, match="unexpected size"):
        release_index._fetch_bytes(
            "https://files.pythonhosted.org/packages/artifact.whl",
            8,
        )

    assert response.read_sizes == [9]

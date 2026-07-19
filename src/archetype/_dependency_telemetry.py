# Copyright 2026 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Process-host isolation for dependency-owned native telemetry.

Daft 0.7.19 snapshots the standard OTLP environment when its compiled module
is imported.  A generic endpoint enables Daft logs, metrics, and traces; its
UDF error log can contain exception text, tracebacks, and argument values.
Archetype therefore consumes content-bearing endpoint configuration before
any Archetype submodule can import Daft.  Only the explicit metrics endpoint
remains visible to Daft and to child workers.

This module configures no provider and emits no signal.  It only routes process
host configuration to the provider that owns the corresponding safe schema.
"""

from __future__ import annotations

import os
import re
import sys
from importlib.metadata import version
from ipaddress import ip_address
from threading import RLock
from urllib.parse import urlsplit, urlunsplit

_ARCHETYPE_TRACES_ENDPOINT = "ARCHETYPE_OTLP_TRACES_ENDPOINT"
_GENERIC_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
_TRACES_ENDPOINT = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
_LOGS_ENDPOINT = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
_METRICS_ENDPOINT = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
_DEPRECATED_DAFT_ENDPOINT = "DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"
_PROTOCOL = "OTEL_EXPORTER_OTLP_PROTOCOL"
_RESOURCE_CONFIGURATION = ("OTEL_RESOURCE_ATTRIBUTES", "OTEL_SERVICE_NAME")
_CONTENT_BEARING_ENDPOINTS = (
    _GENERIC_ENDPOINT,
    _LOGS_ENDPOINT,
    _TRACES_ENDPOINT,
    _DEPRECATED_DAFT_ENDPOINT,
)

_LATE_DAFT_DIAGNOSTIC = (
    "Daft native telemetry initialized before Archetype's host isolation boundary; "
    "dependency signal export may be unsafe."
)
_METRICS_DISABLED_DIAGNOSTIC = "Unsupported Daft metrics telemetry configuration was disabled."
_UNVALIDATED_DAFT_DIAGNOSTIC = (
    "Daft native metrics were disabled for an unvalidated dependency version."
)
_TRACES_DISABLED_DIAGNOSTIC = "Malformed generic OTLP trace configuration was disabled."

_lock = RLock()
_pending_diagnostics: set[str] = set()

_ENDPOINT_RE = re.compile(r"[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]{1,2048}")
_HOST_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")
_INVALID_PERCENT_ESCAPE_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_VALIDATED_DAFT_NATIVE_OTEL_VERSIONS = frozenset({"0.7.19"})


def _generic_traces_endpoint(endpoint: str) -> str | None:
    try:
        parsed = urlsplit(endpoint)
    except (TypeError, ValueError):
        return None
    path = parsed.path + ("v1/traces" if parsed.path.endswith("/") else "/v1/traces")
    return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))


def _daft_native_metrics_are_validated() -> bool:
    try:
        installed_version = version("daft")
    except BaseException:
        return False
    return installed_version in _VALIDATED_DAFT_NATIVE_OTEL_VERSIONS


def _valid_metrics_endpoint(endpoint: str) -> bool:
    if (
        _ENDPOINT_RE.fullmatch(endpoint) is None
        or _INVALID_PERCENT_ESCAPE_RE.search(endpoint) is not None
    ):
        return False
    try:
        parsed = urlsplit(endpoint)
        parsed_port = parsed.port
    except (TypeError, ValueError):
        return False
    hostname = parsed.hostname
    if (
        parsed.scheme not in {"http", "https"}
        or hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.netloc.endswith(":")
        or bool(parsed.fragment)
        or not (parsed_port is None or 0 < parsed_port <= 65535)
    ):
        return False
    if ":" not in hostname:
        return _HOST_RE.fullmatch(hostname) is not None
    try:
        ip_address(hostname)
    except ValueError:
        return False
    return True


def prepare_dependency_telemetry() -> None:
    """Keep content-bearing OTLP configuration out of Daft and its workers.

    The operation is idempotent and deliberately does not import Daft.  It is
    safe to repeat at explicit host configuration points so endpoint values set
    after package import are consumed before later Archetype imports.
    """
    try:
        with _lock:
            unsafe_present = any(name in os.environ for name in _CONTENT_BEARING_ENDPOINTS)
            daft_already_loaded = "daft.daft" in sys.modules

            generic_endpoint = os.environ.get(_GENERIC_ENDPOINT)
            traces_endpoint = os.environ.get(_TRACES_ENDPOINT)
            if not os.environ.get(_ARCHETYPE_TRACES_ENDPOINT):
                if traces_endpoint:
                    os.environ[_ARCHETYPE_TRACES_ENDPOINT] = traces_endpoint
                elif generic_endpoint:
                    isolated_endpoint = _generic_traces_endpoint(generic_endpoint)
                    if isolated_endpoint is None:
                        _pending_diagnostics.add(_TRACES_DISABLED_DIAGNOSTIC)
                    else:
                        os.environ[_ARCHETYPE_TRACES_ENDPOINT] = isolated_endpoint

            for name in _CONTENT_BEARING_ENDPOINTS:
                os.environ.pop(name, None)

            metrics_endpoint = os.environ.get(_METRICS_ENDPOINT)
            dependency_telemetry_requested = unsafe_present or metrics_endpoint is not None
            if dependency_telemetry_requested:
                for name in _RESOURCE_CONFIGURATION:
                    os.environ.pop(name, None)

            protocol = os.environ.get(_PROTOCOL, "grpc").strip().lower()
            if metrics_endpoint is not None:
                if not _daft_native_metrics_are_validated():
                    os.environ.pop(_METRICS_ENDPOINT, None)
                    _pending_diagnostics.add(_UNVALIDATED_DAFT_DIAGNOSTIC)
                elif not _valid_metrics_endpoint(metrics_endpoint) or protocol not in {
                    "grpc",
                    "http/protobuf",
                }:
                    os.environ.pop(_METRICS_ENDPOINT, None)
                    _pending_diagnostics.add(_METRICS_DISABLED_DIAGNOSTIC)

            if unsafe_present and daft_already_loaded:
                _pending_diagnostics.add(_LATE_DAFT_DIAGNOSTIC)
    except BaseException:
        # Process-host telemetry is advisory.  Even a hostile environment
        # mapping must not become application control flow.
        return


def archetype_traces_endpoint() -> str | None:
    """Return the isolated endpoint for Archetype's filtered trace exporter."""
    prepare_dependency_telemetry()
    try:
        return os.environ.get(_ARCHETYPE_TRACES_ENDPOINT) or None
    except BaseException:
        return None


def take_diagnostics() -> tuple[str, ...]:
    """Return each fixed host diagnostic once, without configuration values."""
    try:
        with _lock:
            diagnostics = tuple(sorted(_pending_diagnostics))
            _pending_diagnostics.clear()
            return diagnostics
    except BaseException:
        return ()

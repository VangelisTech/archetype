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
_GENERIC_COMPRESSION = "OTEL_EXPORTER_OTLP_COMPRESSION"
_METRICS_COMPRESSION = "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION"
_METRICS_INTERVAL = "OTEL_METRIC_EXPORT_INTERVAL"
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
_METRICS_DISABLED_DIAGNOSTIC = "Unsupported Daft metrics telemetry configuration was removed."
_UNVALIDATED_DAFT_DIAGNOSTIC = (
    "Daft native metrics configuration was removed for an unvalidated dependency version."
)
_LATE_METRICS_DIAGNOSTIC = (
    "Rejected Daft metrics configuration may already have initialized a dependency "
    "provider before Archetype isolation."
)
_METRICS_COMPRESSION_DIAGNOSTIC = "Unsupported Daft metrics compression configuration was removed."
_METRICS_INTERVAL_DIAGNOSTIC = "Unsupported Daft metrics export interval configuration was removed."
_TRACES_DISABLED_DIAGNOSTIC = "Unsupported Archetype OTLP trace configuration was removed."

_lock = RLock()
_pending_diagnostics: set[str] = set()

_ENDPOINT_RE = re.compile(r"[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]{1,2048}")
_HOST_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?")
_INVALID_PERCENT_ESCAPE_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_VALIDATED_DAFT_NATIVE_OTEL_VERSIONS = frozenset({"0.7.19"})


def _generic_traces_endpoint(endpoint: str) -> str | None:
    if not _valid_otlp_endpoint(endpoint):
        return None
    try:
        parsed = urlsplit(endpoint)
    except (TypeError, ValueError):
        return None
    path = parsed.path + ("v1/traces" if parsed.path.endswith("/") else "/v1/traces")
    isolated = urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return isolated if _valid_otlp_endpoint(isolated) else None


def _daft_native_metrics_are_validated() -> bool:
    try:
        installed_version = version("daft")
    except BaseException:
        return False
    return installed_version in _VALIDATED_DAFT_NATIVE_OTEL_VERSIONS


def _valid_otlp_endpoint(endpoint: str) -> bool:
    if (
        type(endpoint) is not str
        or _ENDPOINT_RE.fullmatch(endpoint) is None
        or _INVALID_PERCENT_ESCAPE_RE.search(endpoint) is not None
        or "?" in endpoint
        or "#" in endpoint
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


def _valid_metrics_endpoint(endpoint: str) -> bool:
    return _valid_otlp_endpoint(endpoint)


def _valid_metrics_interval(interval: str) -> bool:
    if (
        type(interval) is not str
        or len(interval) > 20
        or not interval.isascii()
        or not interval.isdigit()
    ):
        return False
    try:
        parsed = int(interval)
    except (TypeError, ValueError):
        return False
    return 0 < parsed <= (2**64 - 1)


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

            private_endpoint_present = _ARCHETYPE_TRACES_ENDPOINT in os.environ
            generic_endpoint_present = _GENERIC_ENDPOINT in os.environ
            traces_endpoint_present = _TRACES_ENDPOINT in os.environ
            generic_endpoint = os.environ.get(_GENERIC_ENDPOINT)
            traces_endpoint = os.environ.get(_TRACES_ENDPOINT)
            trace_configuration_present = (
                private_endpoint_present or generic_endpoint_present or traces_endpoint_present
            )
            candidate_endpoint = os.environ.get(_ARCHETYPE_TRACES_ENDPOINT)
            if not private_endpoint_present:
                if traces_endpoint_present:
                    candidate_endpoint = traces_endpoint
                elif generic_endpoint_present:
                    candidate_endpoint = _generic_traces_endpoint(generic_endpoint or "")

            if trace_configuration_present:
                if candidate_endpoint and _valid_otlp_endpoint(candidate_endpoint):
                    os.environ[_ARCHETYPE_TRACES_ENDPOINT] = candidate_endpoint
                else:
                    os.environ.pop(_ARCHETYPE_TRACES_ENDPOINT, None)
                    _pending_diagnostics.add(_TRACES_DISABLED_DIAGNOSTIC)

            for name in _CONTENT_BEARING_ENDPOINTS:
                os.environ.pop(name, None)

            metrics_endpoint = os.environ.get(_METRICS_ENDPOINT)
            metrics_compression_present = metrics_endpoint is not None and (
                _METRICS_COMPRESSION in os.environ or _GENERIC_COMPRESSION in os.environ
            )
            metrics_rejected = metrics_compression_present
            if metrics_endpoint is not None:
                os.environ.pop(_METRICS_COMPRESSION, None)
                os.environ.pop(_GENERIC_COMPRESSION, None)
            if metrics_compression_present:
                _pending_diagnostics.add(_METRICS_COMPRESSION_DIAGNOSTIC)
            metrics_interval = os.environ.get(_METRICS_INTERVAL)
            if (
                metrics_endpoint is not None
                and metrics_interval is not None
                and not _valid_metrics_interval(metrics_interval)
            ):
                os.environ.pop(_METRICS_INTERVAL, None)
                _pending_diagnostics.add(_METRICS_INTERVAL_DIAGNOSTIC)
                metrics_rejected = True
            dependency_telemetry_requested = (
                trace_configuration_present or unsafe_present or metrics_endpoint is not None
            )
            if dependency_telemetry_requested:
                for name in _RESOURCE_CONFIGURATION:
                    os.environ.pop(name, None)

            protocol = os.environ.get(_PROTOCOL, "grpc")
            if metrics_endpoint is not None:
                if not _daft_native_metrics_are_validated():
                    os.environ.pop(_METRICS_ENDPOINT, None)
                    _pending_diagnostics.add(_UNVALIDATED_DAFT_DIAGNOSTIC)
                    metrics_rejected = True
                elif not _valid_metrics_endpoint(metrics_endpoint) or protocol not in {
                    "grpc",
                    "http/protobuf",
                }:
                    os.environ.pop(_METRICS_ENDPOINT, None)
                    _pending_diagnostics.add(_METRICS_DISABLED_DIAGNOSTIC)
                    metrics_rejected = True

            if unsafe_present and daft_already_loaded:
                _pending_diagnostics.add(_LATE_DAFT_DIAGNOSTIC)
            if metrics_rejected and daft_already_loaded:
                _pending_diagnostics.add(_LATE_METRICS_DIAGNOSTIC)
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

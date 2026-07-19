# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Fresh-process contracts for Daft's dependency-owned OTLP providers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Protocol, cast

import grpc
import pytest
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2, logs_service_pb2_grpc
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2,
    metrics_service_pb2_grpc,
)
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc

pytestmark = [
    pytest.mark.contract("observability.signals.safe"),
    pytest.mark.integration,
    pytest.mark.process,
]

ROOT = Path(__file__).resolve().parents[2]
_EXCEPTION_CANARY = b"exception-canary"
_ARGUMENT_CANARY = b"argument-canary"
_PROMPT_CANARY = b"prompt-canary"
_PAYLOAD_CANARY = b"payload-canary"
_RESOURCE_CANARY = b"resource-canary"


class _SerializableRequest(Protocol):
    def SerializeToString(self) -> bytes: ...  # noqa: N802


@dataclass
class _CapturedOTLP:
    """Thread-safe serialized requests received from Daft's native exporters."""

    log_requests: list[bytes] = field(default_factory=list)
    metric_requests: list[bytes] = field(default_factory=list)
    trace_requests: list[bytes] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def append_log(self, request: _SerializableRequest) -> None:
        with self.lock:
            self.log_requests.append(request.SerializeToString())

    def append_metrics(self, request: _SerializableRequest) -> None:
        with self.lock:
            self.metric_requests.append(request.SerializeToString())

    def append_trace(self, request: _SerializableRequest) -> None:
        with self.lock:
            self.trace_requests.append(request.SerializeToString())


class _LogsReceiver(logs_service_pb2_grpc.LogsServiceServicer):
    def __init__(self, captured: _CapturedOTLP) -> None:
        self._captured = captured

    def Export(  # noqa: N802
        self, request: _SerializableRequest, context: grpc.ServicerContext
    ) -> object:
        self._captured.append_log(request)
        return logs_service_pb2.ExportLogsServiceResponse()


class _MetricsReceiver(metrics_service_pb2_grpc.MetricsServiceServicer):
    def __init__(self, captured: _CapturedOTLP) -> None:
        self._captured = captured

    def Export(  # noqa: N802
        self, request: _SerializableRequest, context: grpc.ServicerContext
    ) -> object:
        self._captured.append_metrics(request)
        return metrics_service_pb2.ExportMetricsServiceResponse()


class _TraceReceiver(trace_service_pb2_grpc.TraceServiceServicer):
    def __init__(self, captured: _CapturedOTLP) -> None:
        self._captured = captured

    def Export(  # noqa: N802
        self, request: _SerializableRequest, context: grpc.ServicerContext
    ) -> object:
        self._captured.append_trace(request)
        return trace_service_pb2.ExportTraceServiceResponse()


class _OTLPHTTPServer(ThreadingHTTPServer):
    captured: _CapturedOTLP


class _OTLPHTTPHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        server = cast(_OTLPHTTPServer, self.server)
        if self.path == "/v1/metrics":
            server.captured.append_metrics(
                metrics_service_pb2.ExportMetricsServiceRequest.FromString(body)
            )
            response = metrics_service_pb2.ExportMetricsServiceResponse().SerializeToString()
        elif self.path == "/v1/traces":
            server.captured.append_trace(
                trace_service_pb2.ExportTraceServiceRequest.FromString(body)
            )
            response = trace_service_pb2.ExportTraceServiceResponse().SerializeToString()
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


@contextmanager
def _otlp_receiver() -> Iterator[tuple[str, _CapturedOTLP]]:
    captured = _CapturedOTLP()
    executor = ThreadPoolExecutor(max_workers=3)
    server = grpc.server(executor)
    logs_service_pb2_grpc.add_LogsServiceServicer_to_server(_LogsReceiver(captured), server)
    metrics_service_pb2_grpc.add_MetricsServiceServicer_to_server(
        _MetricsReceiver(captured), server
    )
    trace_service_pb2_grpc.add_TraceServiceServicer_to_server(_TraceReceiver(captured), server)
    port = server.add_insecure_port("127.0.0.1:0")
    assert port > 0
    server.start()
    try:
        yield f"http://127.0.0.1:{port}", captured
    finally:
        server.stop(grace=0).wait(timeout=5)
        executor.shutdown(wait=True)


@contextmanager
def _metrics_http_receiver() -> Iterator[tuple[str, _CapturedOTLP]]:
    captured = _CapturedOTLP()
    server = _OTLPHTTPServer(("127.0.0.1", 0), _OTLPHTTPHandler)
    server.captured = captured
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/v1/metrics", captured
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def _otlp_http_receiver() -> Iterator[tuple[str, _CapturedOTLP]]:
    captured = _CapturedOTLP()
    server = _OTLPHTTPServer(("127.0.0.1", 0), _OTLPHTTPHandler)
    server.captured = captured
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", captured
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _child_environment(**values: str) -> dict[str, str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith(("ARCHETYPE_", "DAFT_DEV_OTEL_", "LOGFIRE_", "OTEL_")):
            env.pop(key)
    env.update(values)
    env["DO_NOT_TRACK"] = "1"
    source = str(ROOT / "src")
    env["PYTHONPATH"] = source + os.pathsep + env.get("PYTHONPATH", "")
    return env


def _run_daft_failure(**env_values: str) -> subprocess.CompletedProcess[str]:
    source = """
        import json
        import logging
        import os

        logging.disable(logging.CRITICAL)
        late_generic_endpoint = os.environ.pop("TEST_LATE_OTLP_ENDPOINT", None)
        emit_archetype_trace = os.environ.pop("TEST_EMIT_ARCHETYPE_TRACE", None) == "1"

        # Importing Archetype establishes the dependency telemetry boundary
        # before Daft's compiled extension initializes its native providers.
        import archetype  # noqa: F401
        if emit_archetype_trace:
            from archetype import _obs

            _obs.configure_tracing(service_name="archetype-test")
            with _obs.span(
                "artifact.publish",
                operation="artifact.publish",
            ):
                pass
        if late_generic_endpoint:
            os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = late_generic_endpoint
            from archetype import ArchetypeRuntime  # noqa: F401
        import daft
        from daft import DataType, col

        @daft.func(return_dtype=DataType.int64(), on_error="log", use_process=False)
        def fail(secret: str) -> int:
            raise RuntimeError(f"exception-canary:{secret}")

        out = daft.from_pydict({
            "secret": ["argument-canary:prompt-canary:payload-canary"]
        }).with_column(
            "out", fail(col("secret"))
        ).collect()
        if emit_archetype_trace:
            from opentelemetry import trace

            provider = trace.get_tracer_provider()
            provider.force_flush(timeout_millis=5_000)
            provider.shutdown()
        print(json.dumps({
            "daft_version": daft.__version__,
            "archetype_trace_endpoint_preserved": bool(
                os.environ.get("ARCHETYPE_OTLP_TRACES_ENDPOINT")
            ),
            "generic_endpoint_present": "OTEL_EXPORTER_OTLP_ENDPOINT" in os.environ,
            "resource_attributes_present": "OTEL_RESOURCE_ATTRIBUTES" in os.environ,
            "out": out.to_pydict()["out"],
        }, sort_keys=True))
    """
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        check=False,
        capture_output=True,
        text=True,
        env=_child_environment(**env_values),
        timeout=30,
    )


def _run_inherited_daft_failure(**env_values: str) -> subprocess.CompletedProcess[str]:
    source = """
        import json
        import os
        import subprocess
        import sys
        import textwrap

        import archetype  # noqa: F401

        child_source = '''
            import json
            import os

            import daft
            from daft import DataType, col

            @daft.func(return_dtype=DataType.int64(), on_error="log", use_process=False)
            def fail(secret: str) -> int:
                raise RuntimeError(f"exception-canary:{secret}")

            out = daft.from_pydict({
                "secret": ["argument-canary:prompt-canary:payload-canary"]
            }).with_column(
                "out", fail(col("secret"))
            ).collect()
            print(json.dumps({
                "generic_endpoint_present": (
                    "OTEL_EXPORTER_OTLP_ENDPOINT" in os.environ
                ),
                "out": out.to_pydict()["out"],
            }, sort_keys=True))
        '''
        child = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(child_source)],
            check=False,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            timeout=30,
        )
        assert child.returncode == 0, child.stderr
        print(json.dumps({
            "archetype_trace_endpoint_preserved": bool(
                os.environ.get("ARCHETYPE_OTLP_TRACES_ENDPOINT")
            ),
            "child": json.loads(child.stdout.strip().splitlines()[-1]),
            "generic_endpoint_present": "OTEL_EXPORTER_OTLP_ENDPOINT" in os.environ,
        }, sort_keys=True))
    """
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        check=False,
        capture_output=True,
        text=True,
        env=_child_environment(**env_values),
        timeout=40,
    )


def _result(completed: subprocess.CompletedProcess[str]) -> dict[str, object]:
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _run_daft_import(**env_values: str) -> subprocess.CompletedProcess[str]:
    source = """
        import json
        import os

        import archetype  # noqa: F401
        import daft

        print(json.dumps({
            "daft_version": daft.__version__,
            "generic_compression_present": (
                "OTEL_EXPORTER_OTLP_COMPRESSION" in os.environ
            ),
            "metrics_compression_present": (
                "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION" in os.environ
            ),
            "metrics_endpoint_present": (
                "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" in os.environ
            ),
            "metrics_interval_present": (
                "OTEL_METRIC_EXPORT_INTERVAL" in os.environ
            ),
        }, sort_keys=True))
    """
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        check=False,
        capture_output=True,
        text=True,
        env=_child_environment(**env_values),
        timeout=30,
    )


def test_generic_otlp_cannot_export_daft_udf_error_content() -> None:
    with _otlp_receiver() as (endpoint, captured):
        completed = _run_daft_failure(
            DAFT_LOG="off",
            OTEL_EXPORTER_OTLP_ENDPOINT=endpoint,
            OTEL_LOGS_EXPORTER="none",
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
            OTEL_RESOURCE_ATTRIBUTES="unsafe.label=resource-canary",
            OTEL_SDK_DISABLED="true",
            OTEL_TRACES_EXPORTER="none",
        )

    assert _result(completed) == {
        "archetype_trace_endpoint_preserved": True,
        "daft_version": "0.7.19",
        "generic_endpoint_present": False,
        "out": [None],
        "resource_attributes_present": False,
    }
    exported_requests = b"".join(
        captured.log_requests + captured.metric_requests + captured.trace_requests
    )
    assert captured.log_requests == []
    assert captured.metric_requests == []
    assert captured.trace_requests == []
    assert _EXCEPTION_CANARY not in exported_requests
    assert _ARGUMENT_CANARY not in exported_requests
    assert _PROMPT_CANARY not in exported_requests
    assert _PAYLOAD_CANARY not in exported_requests
    assert b"traceback" not in exported_requests
    assert b"udf_args" not in exported_requests


@pytest.mark.parametrize(
    ("endpoint_variable", "archetype_trace_endpoint_preserved"),
    [
        ("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", False),
        ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", True),
        ("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT", False),
    ],
)
def test_dependency_log_and_trace_endpoints_are_not_visible_to_daft(
    endpoint_variable: str, archetype_trace_endpoint_preserved: bool
) -> None:
    with _otlp_receiver() as (endpoint, captured):
        completed = _run_daft_failure(
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
            **{endpoint_variable: endpoint},
        )

    result = _result(completed)
    assert result["archetype_trace_endpoint_preserved"] is (archetype_trace_endpoint_preserved)
    assert result["out"] == [None]
    assert captured.log_requests == []
    assert captured.metric_requests == []
    assert captured.trace_requests == []


def test_metrics_specific_endpoint_is_explicit_daft_opt_in_without_logs() -> None:
    with _otlp_receiver() as (endpoint, captured):
        completed = _run_daft_failure(
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
            OTEL_METRIC_EXPORT_INTERVAL="10",
            OTEL_RESOURCE_ATTRIBUTES="unsafe.label=resource-canary",
        )

    assert _result(completed) == {
        "archetype_trace_endpoint_preserved": False,
        "daft_version": "0.7.19",
        "generic_endpoint_present": False,
        "out": [None],
        "resource_attributes_present": False,
    }
    assert captured.metric_requests
    assert captured.log_requests == []
    assert captured.trace_requests == []
    exported_metrics = b"".join(captured.metric_requests)
    assert _EXCEPTION_CANARY not in exported_metrics
    assert _ARGUMENT_CANARY not in exported_metrics
    assert _PROMPT_CANARY not in exported_metrics
    assert _PAYLOAD_CANARY not in exported_metrics
    assert _RESOURCE_CANARY not in exported_metrics


@pytest.mark.parametrize("protocol", ["grpc", "http/protobuf"])
@pytest.mark.parametrize(
    ("compression_variable", "compression_value"),
    [
        ("OTEL_EXPORTER_OTLP_COMPRESSION", "invalid-compression-canary"),
        ("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip"),
        ("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION", "invalid-compression-canary"),
        ("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION", "gzip"),
    ],
)
def test_native_metrics_compression_cannot_make_daft_import_fail(
    protocol: str,
    compression_variable: str,
    compression_value: str,
) -> None:
    endpoint = "http://127.0.0.1:1"
    if protocol == "http/protobuf":
        endpoint += "/v1/metrics"
    completed = _run_daft_import(
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=endpoint,
        OTEL_EXPORTER_OTLP_PROTOCOL=protocol,
        **{compression_variable: compression_value},
    )

    assert _result(completed) == {
        "daft_version": "0.7.19",
        "generic_compression_present": False,
        "metrics_compression_present": False,
        "metrics_endpoint_present": True,
        "metrics_interval_present": False,
    }
    assert "invalid-compression-canary" not in completed.stderr


@pytest.mark.parametrize("protocol", ["grpc", "http/protobuf"])
def test_zero_metrics_interval_cannot_busy_spin_native_reader(protocol: str) -> None:
    endpoint = "http://127.0.0.1:1"
    if protocol == "http/protobuf":
        endpoint += "/v1/metrics"
    completed = _run_daft_import(
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=endpoint,
        OTEL_EXPORTER_OTLP_PROTOCOL=protocol,
        OTEL_METRIC_EXPORT_INTERVAL="0",
    )

    result = _result(completed)
    assert result["daft_version"] == "0.7.19"
    assert result["metrics_endpoint_present"] is True
    assert result["metrics_interval_present"] is False


def test_generic_archetype_traces_and_explicit_daft_metrics_stay_separate() -> None:
    with _otlp_http_receiver() as (endpoint, captured):
        completed = _run_daft_failure(
            OTEL_EXPORTER_OTLP_ENDPOINT=endpoint,
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=f"{endpoint}/v1/metrics",
            OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf",
            OTEL_METRIC_EXPORT_INTERVAL="10",
            TEST_EMIT_ARCHETYPE_TRACE="1",
        )

    result = _result(completed)
    assert result["archetype_trace_endpoint_preserved"] is True
    assert result["generic_endpoint_present"] is False
    assert result["out"] == [None]
    assert captured.metric_requests
    assert captured.log_requests == []
    assert captured.trace_requests
    exported_traces = b"".join(captured.trace_requests)
    assert b"artifact.publish" in exported_traces
    assert _EXCEPTION_CANARY not in exported_traces
    assert _ARGUMENT_CANARY not in exported_traces
    assert _PROMPT_CANARY not in exported_traces
    assert _PAYLOAD_CANARY not in exported_traces
    exported_metrics = b"".join(captured.metric_requests)
    assert _EXCEPTION_CANARY not in exported_metrics
    assert _ARGUMENT_CANARY not in exported_metrics
    assert _PROMPT_CANARY not in exported_metrics
    assert _PAYLOAD_CANARY not in exported_metrics


def test_signal_specific_metrics_protocol_is_translated_for_daft() -> None:
    with _metrics_http_receiver() as (endpoint, captured):
        completed = _run_daft_failure(
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
            OTEL_EXPORTER_OTLP_METRICS_PROTOCOL="http/protobuf",
            OTEL_METRIC_EXPORT_INTERVAL="10",
            OTEL_RESOURCE_ATTRIBUTES="unsafe.label=resource-canary",
        )

    assert _result(completed)["out"] == [None]
    assert captured.metric_requests
    exported_metrics = b"".join(captured.metric_requests)
    assert _EXCEPTION_CANARY not in exported_metrics
    assert _ARGUMENT_CANARY not in exported_metrics
    assert _PROMPT_CANARY not in exported_metrics
    assert _PAYLOAD_CANARY not in exported_metrics
    assert _RESOURCE_CANARY not in exported_metrics


def test_generic_otlp_is_not_restored_for_inherited_daft_workers() -> None:
    with _otlp_receiver() as (endpoint, captured):
        completed = _run_inherited_daft_failure(
            OTEL_EXPORTER_OTLP_ENDPOINT=endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
        )

    assert _result(completed) == {
        "archetype_trace_endpoint_preserved": True,
        "child": {"generic_endpoint_present": False, "out": [None]},
        "generic_endpoint_present": False,
    }
    assert captured.log_requests == []
    assert captured.metric_requests == []
    assert captured.trace_requests == []


def test_lazy_runtime_export_rechecks_late_host_configuration_before_daft() -> None:
    with _otlp_receiver() as (endpoint, captured):
        completed = _run_daft_failure(TEST_LATE_OTLP_ENDPOINT=endpoint)

    result = _result(completed)
    assert result["archetype_trace_endpoint_preserved"] is True
    assert result["generic_endpoint_present"] is False
    assert result["out"] == [None]
    assert captured.log_requests == []
    assert captured.metric_requests == []
    assert captured.trace_requests == []


@pytest.mark.parametrize(
    ("endpoint", "protocol"),
    [
        ("http://127.0.0.1:1", "http/json"),
        ("http://bad host:4317", "grpc"),
        ("http://%zz:4317", "grpc"),
        ("http://127.0.0.1:4317/a path", "grpc"),
    ],
)
def test_unsupported_metrics_configuration_cannot_change_result(
    endpoint: str, protocol: str
) -> None:
    baseline = _result(_run_daft_failure())
    unsupported = _result(
        _run_daft_failure(
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=endpoint,
            OTEL_EXPORTER_OTLP_PROTOCOL=protocol,
        )
    )

    assert unsupported["out"] == baseline["out"]
    assert unsupported["daft_version"] == baseline["daft_version"] == "0.7.19"


def test_malformed_generic_endpoint_cannot_reach_daft_initialization() -> None:
    baseline = _result(_run_daft_failure())
    malformed = _result(
        _run_daft_failure(
            OTEL_EXPORTER_OTLP_ENDPOINT="http://[endpoint-canary",
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
        )
    )

    assert malformed["archetype_trace_endpoint_preserved"] is False
    assert malformed["generic_endpoint_present"] is False
    assert malformed["out"] == baseline["out"]
    assert malformed["daft_version"] == baseline["daft_version"] == "0.7.19"


def test_disabled_and_unreachable_otlp_preserve_daft_result() -> None:
    disabled = _result(_run_daft_failure())
    unreachable_generic = _result(
        _run_daft_failure(
            OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:1",
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
        )
    )
    unreachable_metrics = _result(
        _run_daft_failure(
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT="http://127.0.0.1:1",
            OTEL_EXPORTER_OTLP_PROTOCOL="grpc",
            OTEL_EXPORTER_OTLP_TIMEOUT="0.1",
        )
    )

    assert disabled["out"] == [None]
    assert unreachable_generic["out"] == disabled["out"]
    assert unreachable_metrics["out"] == disabled["out"]
    assert (
        unreachable_generic["daft_version"]
        == unreachable_metrics["daft_version"]
        == disabled["daft_version"]
        == "0.7.19"
    )

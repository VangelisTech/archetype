# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""The CLI preserves shared requests and verifies bounded exact reply identity."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from archetype_native import wire
from typer.testing import CliRunner

from archetype.cli.main import app

TOKEN = "public-contract-fixture-" + "a" * 32


class CLIContracts(unittest.TestCase):
    def command(self, body, args=None, status=200):
        captured = []

        def reply(request):
            captured.append((request.content, request.headers["authorization"]))
            return httpx.Response(status, content=body)

        actual = httpx.Client
        with patch(
            "archetype.cli.main.httpx.Client",
            side_effect=lambda **kwargs: actual(transport=httpx.MockTransport(reply), **kwargs),
        ):
            result = CliRunner().invoke(app, args or ["world", "status", "alpha", "--token", TOKEN])
        return result, captured

    def test_exact_request_bytes_and_decimal_cells_survive_http_client(self):
        request = json.dumps(
            {
                "version": 1,
                "resource": "alpha",
                "operation": "admit",
                "arguments": {
                    "generation": "1",
                    "revision": "1",
                    "admission_key": "first",
                    "expected_head": None,
                    "changes": [
                        {
                            "op": "insert",
                            "predicate": "seed",
                            "values": [
                                {"int64": "9007199254741109"},
                                {"bool": True},
                                {"float64": "3ff0000000000001"},
                            ],
                        }
                    ],
                },
            },
            indent=2,
        ).encode()
        response = wire.response(
            {"version": 1, "ok": True, "resource": "alpha", "operation": "admit", "value": {}}
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "request.json"
            path.write_bytes(request)
            result, captured = self.command(response, ["invoke", str(path), "--token", TOKEN])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(captured, [(request, "Bearer " + TOKEN)])
        self.assertNotIn(TOKEN, result.output)

    def test_wrong_identity_version_duplicates_extensions_and_oversize_are_rejected(self):
        valid = {"version": 1, "ok": True, "resource": "alpha", "operation": "status", "value": {}}
        cases = [
            json.dumps(valid | changed).encode()
            for changed in (
                {"version": 2},
                {"version": True},
                {"resource": "other"},
                {"operation": "stop"},
                {"private_path": "/private/diagnostic"},
                {"value": []},
            )
        ]
        cases += [
            b'{"version":1,"version":1,"ok":true,"resource":"alpha","operation":"status","value":{}}',
            b"x" * (wire.MAX_RESPONSE_BYTES + 1),
        ]
        for body in cases:
            with self.subTest(body=body[:80]):
                result, _ = self.command(body)
                self.assertEqual(result.exit_code, 1)
                self.assertIn("Invalid server response", result.output)
                self.assertNotIn("/private/diagnostic", result.output)

    def test_error_envelope_and_unknown_outcome_are_preserved_without_diagnostics(self):
        response = wire.response(
            {"version": 1, "ok": False, "error": {"code": "operation_failed", "outcome": "unknown"}}
        )
        result, _ = self.command(response, status=500)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(json.loads(result.output)["error"]["outcome"], "unknown")
        bad = wire.response(
            {
                "version": 1,
                "ok": False,
                "error": {
                    "code": "operation_failed",
                    "outcome": "unknown",
                    "message": "/private/path",
                },
            }
        )
        result, _ = self.command(bad, status=500)
        self.assertEqual(result.exit_code, 1)
        self.assertNotIn("/private/path", result.output)

    def test_request_bound_is_distinct_from_reply_bound(self):
        request = {
            "version": 1,
            "resource": "program",
            "operation": "program_create",
            "arguments": {
                "request_key": "first",
                "description": "",
                "definition": {
                    "rules": "x" * 20000,
                    "schemas": [{"name": "out", "input": False, "fields": ["int64"]}],
                    "inputs": [],
                    "outputs": ["out"],
                },
            },
        }
        encoded = wire.encode_request(request)
        self.assertGreater(len(encoded), wire.MAX_RESPONSE_BYTES)
        self.assertLess(len(encoded), wire.MAX_REQUEST_BYTES)
        wire.Request.decode(encoded)

    def test_upload_paths_reject_normalization_before_any_effect(self):
        args = {
            "context_id": "a" * 64,
            "exact_cut": None,
            "artifact_id": "019f2b20-1234-7000-8000-000000000001",
            "content_base64": "YWJj",
        }
        for path in (
            " notes.txt",
            "notes.txt ",
            "/notes.txt",
            "a/../notes.txt",
            "a/./notes.txt",
            "a//notes.txt",
            "a\\notes.txt",
        ):
            with self.assertRaises(ValueError):
                wire.Request.decode(
                    wire.encode_request(
                        {
                            "version": 1,
                            "resource": "files",
                            "operation": "artifact_upload",
                            "arguments": args | {"logical_path": path},
                        }
                    )
                )
        decoded = wire.Request.decode(
            wire.encode_request(
                {
                    "version": 1,
                    "resource": "files",
                    "operation": "artifact_upload",
                    "arguments": args | {"logical_path": "notes.txt"},
                }
            )
        )
        self.assertEqual(decoded.operation.logical_path, "notes.txt")

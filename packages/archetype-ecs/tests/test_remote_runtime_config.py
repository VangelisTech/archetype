# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0
"""Remote placement is immutable operator input, inert until native activation."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from archetype import ArchetypeRuntime, RemoteData
from archetype.api import ServerConfig
from archetype.wiring import build_runtime_resources


def profile():
    return RemoteData(
        version=1, uri="s3://synthetic-bucket/task/case", region="auto", path_style_access=True
    )


class RemoteRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_profile_is_inert_then_reaches_single_native_owner(self):
        remote = profile()
        host = Mock()
        with patch("archetype.runtime.runtime.open_host", return_value=host) as opened:
            runtime = ArchetypeRuntime(
                library="/tmp/synthetic-library",
                store="/tmp/synthetic-store",
                storage_only=True,
                remote_data=remote,
            )
            async with runtime:
                opened.assert_not_called()
                await runtime._activate()
                self.assertIs(opened.call_args.args[0]["remote_data"], remote)
                self.assertIs(await runtime._activate(), host)
                self.assertEqual(opened.call_count, 1)
            host.close.assert_called_once_with()

    async def test_closed_operator_toml_reaches_server_owner_without_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            resources = root / "resources.toml"
            resources.write_text("resource = []\ngrant = []\n")
            remote = root / "remote.toml"
            remote.write_text(
                'version = 1\nuri = "s3://synthetic-bucket/task/case"\nregion = "auto"\npath_style_access = true\ncredential_source = "aws_environment"\n'
            )
            with (
                patch.dict(
                    os.environ,
                    {
                        "ARCHETYPE_RESOURCES_PATH": str(resources),
                        "ARCHETYPE_REMOTE_DATA_PATH": str(remote),
                        "ARCHETYPE_NATIVE_LIBRARY": "/tmp/library",
                        "ARCHETYPE_STORE": "/tmp/store",
                    },
                    clear=True,
                ),
                patch("archetype.runtime.runtime.open_host") as opened,
            ):
                config = ServerConfig.from_env()
                runtime = build_runtime_resources(config)
                self.assertEqual(runtime._config["remote_data"], profile())
                opened.assert_not_called()
                await runtime.shutdown()
                remote.write_text(remote.read_text() + 'secret_access_key = "synthetic-secret"\n')
                with self.assertRaises(ValueError):
                    ServerConfig.from_env()
                opened.assert_not_called()

    async def test_artifact_workflow_preserves_bounded_native_failure_codes(self):
        from archetype_native import NativeError

        from archetype import RuntimeOperationError

        for code in (
            "resource_limit",
            "corrupt_data",
            "invalid_request",
            "unsupported_format",
            "conflict",
            "foreign",
        ):
            with self.subTest(code=code):
                host = Mock()
                workflow = Mock()
                workflow.prepare_files.side_effect = NativeError(
                    "operation", "synthetic-provider-secret", "publish_context_object", code=code
                )
                with (
                    patch("archetype.runtime.runtime.open_host", return_value=host),
                    patch("archetype.wiring.artifact_workflow", return_value=workflow),
                ):
                    async with ArchetypeRuntime(
                        library="/tmp/library",
                        store="/tmp/store",
                        storage_only=True,
                        remote_data=profile(),
                    ) as runtime:
                        runtime.artifacts("files")
                        with self.assertRaises(RuntimeOperationError) as captured:
                            await runtime._artifact_files("files", "prepare_files")
                        self.assertEqual(
                            captured.exception.code,
                            code if code != "foreign" else "operation_failed",
                        )
                        self.assertEqual(captured.exception.outcome, "unknown")
                        self.assertNotIn("synthetic-provider-secret", str(captured.exception))

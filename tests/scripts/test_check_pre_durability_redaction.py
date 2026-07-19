# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

CHECKER_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_pre_durability_redaction.py"
SPEC = importlib.util.spec_from_file_location("check_pre_durability_redaction", CHECKER_PATH)
assert SPEC is not None and SPEC.loader is not None
checker = importlib.util.module_from_spec(SPEC)
sys.modules["check_pre_durability_redaction"] = checker
SPEC.loader.exec_module(checker)


def test_negative_fixture_detects_claim_and_upload_bypasses(tmp_path: Path) -> None:
    source = tmp_path / "bundle_service.py"
    source.write_text(
        """
class ArtifactBundleService:
    def prepare(self):
        self.canonical_json()
        self._bind_redaction_policy()

    async def publish(self):
        await self.publish_prepared()
        self.prepare()

    async def publish_prepared(self):
        await self._control_catalog()
        self._request_from_preparation()
        self._safe_failure_detail()
        self.fail_artifact_publication()

    async def reconcile(self):
        self._safe_failure_detail()
        self.fail_artifact_publication()

    async def _resume(self):
        self._upload_bundle()
        self._index_records()
        self._assert_object_root_safe()

    async def _upload_bundle(self):
        self._assert_materialized_metadata_safe()
        self._file_metadata()
        self._sanitize_materialized()
        self._upload_files()
        self._upload_bytes()
        self._redaction_manifest()
        self._assert_records_safe()
""",
        encoding="utf-8",
    )
    errors = checker.audit_path(source)
    assert errors == [
        "prepare must call _bind_redaction_policy() before canonical_json()",
        "publish must call prepare() before publish_prepared()",
        "publish_prepared must call _request_from_preparation() before _control_catalog()",
        "_resume must call _assert_object_root_safe() before _upload_bundle()",
        "_resume must call _assert_object_root_safe() before _index_records()",
        "_upload_bundle must call _sanitize_materialized() before _file_metadata()",
        "_upload_bundle must call _redaction_manifest() before _upload_bytes()",
    ]


def test_negative_fixture_detects_raw_durable_failure_details(tmp_path: Path) -> None:
    source = tmp_path / "bundle_service.py"
    source.write_text(
        """
class ArtifactBundleService:
    def prepare(self):
        self._bind_redaction_policy()
        self.canonical_json()

    async def publish(self):
        self.prepare()
        await self.publish_prepared()

    async def publish_prepared(self):
        self._request_from_preparation()
        await self._control_catalog()
        self.fail_artifact_publication()
        self._safe_failure_detail()

    async def reconcile(self):
        self.fail_artifact_publication()

    async def _resume(self):
        self._assert_object_root_safe()
        self._upload_bundle()
        self._index_records()

    async def _upload_bundle(self):
        self._assert_materialized_metadata_safe()
        self._sanitize_materialized()
        self._file_metadata()
        self._upload_files()
        self._redaction_manifest()
        self._upload_bytes()
        self._assert_records_safe()
""",
        encoding="utf-8",
    )
    assert checker.audit_path(source) == [
        "publish_prepared must call _safe_failure_detail() before fail_artifact_publication()",
        "reconcile must call _safe_failure_detail()",
    ]


def test_repository_redaction_order_passes() -> None:
    completed = subprocess.run(
        [sys.executable, str(CHECKER_PATH)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "Pre-durability redaction audit passed" in completed.stdout

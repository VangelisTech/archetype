# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Identity contract for the promoted canonical redaction family."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_POLICY_ID = (
    "archetype-secret-redaction-v1:29e333086a510698113362b0281b9b4740a9b7871e795304a26af99a0910d0dc"
)
_INPUT = (
    "Authorization: Bearer EXAMPLE_TOKEN_1234567890 and sk-proj-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
)
_EXPECTED_TEXT = (
    "Authorization: Bearer <redacted:authorization-header> and <redacted:openai-api-key>"
)
_EXPECTED_RECEIPT = {
    "policy_id": _POLICY_ID,
    "scope": "pr4-contract",
    "status": "redacted",
    "scanned_bytes": 91,
    "redaction_count": 2,
    "rule_ids": ["authorization-header", "openai-api-key"],
}


def _assert_scanner_contract(service_type: type[Any]) -> None:
    service = service_type()
    result = service.redact_text(_INPUT, scope="pr4-contract")
    assert service.policy_id == _POLICY_ID
    assert result.text == _EXPECTED_TEXT
    assert result.receipt.model_dump(mode="json") == _EXPECTED_RECEIPT


def test_canonical_redaction_import_preserves_policy_digest_and_receipt_behavior() -> None:
    canonical = import_module("archetype.redaction")
    _assert_scanner_contract(canonical.RedactionService)

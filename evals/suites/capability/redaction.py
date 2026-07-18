# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free capability proof for pre-durability redaction."""

from __future__ import annotations

from archetype.app.redaction import RedactionService, SecretQuarantineError
from evals.graders import state_check
from evals.harness import EvalHarness
from evals.types import GraderResult
from quality.secret_corpus import SAFE_REDACTION_CORPUS, SECRET_LEAK_CORPUS

SUITE = "capability"


def task_pre_durability_redaction() -> list[GraderResult]:
    service = RedactionService()
    leak_results = [
        (case, service.redact_text(case.payload, scope=f"eval:{case.name}"))
        for case in SECRET_LEAK_CORPUS
    ]
    safe_results = [
        service.redact_text(value, scope="eval:safe") for value in SAFE_REDACTION_CORPUS
    ]

    errors_are_safe = True
    metadata_fails_closed = True
    for case in SECRET_LEAK_CORPUS:
        try:
            service.assert_safe_metadata(case.payload, field=f"eval:{case.name}")
        except SecretQuarantineError as exc:
            errors_are_safe = errors_are_safe and case.payload not in str(exc)
        else:
            metadata_fails_closed = False

    return [
        state_check(
            {
                "every_provider_format_redacted": all(
                    result.receipt.status == "redacted" and case.rule_id in result.receipt.rule_ids
                    for case, result in leak_results
                ),
                "no_synthetic_secret_survives": all(
                    case.payload not in result.text for case, result in leak_results
                ),
                "safe_placeholders_are_stable": all(
                    result.receipt.status == "clean" and result.text == original
                    for original, result in zip(SAFE_REDACTION_CORPUS, safe_results, strict=True)
                ),
                "metadata_fails_closed": metadata_fails_closed,
                "quarantine_errors_do_not_echo": errors_are_safe,
                "policy_identity_is_versioned": service.policy_id.startswith(
                    "archetype-secret-redaction-v1:"
                ),
            },
            name="pre_durability_secret_redaction",
        )
    ]


def register(harness: EvalHarness) -> None:
    harness.add(
        "security.pre_durability_redaction",
        suite=SUITE,
        fn=task_pre_durability_redaction,
        desc=(
            "Shared scanner redacts provider/cloud credentials, preserves safe placeholders, "
            "and fails metadata closed without echoing secrets"
        ),
    )

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Credential-free capability proof for pre-durability redaction."""

from __future__ import annotations

import tempfile
from pathlib import Path

from archetype.app.artifacts.worktree_archive import (
    capture_worktree_archive,
    restore_worktree_archive,
    sanitize_worktree_archive,
)
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

    with tempfile.TemporaryDirectory(prefix="archetype-archive-eval-") as temporary_value:
        temporary = Path(temporary_value)
        worktree = temporary / "worktree"
        worktree.mkdir()
        corpus = "\n".join(case.payload for case in SECRET_LEAK_CORPUS)
        (worktree / "tracked.txt").write_text(corpus)
        (worktree / "ignored.log").write_text("ignored but portable\n")
        (worktree / ".context").mkdir()
        (worktree / ".context" / "review.md").write_text("portable context\n")
        (worktree / ".env").write_text(SECRET_LEAK_CORPUS[0].payload)
        recovery = temporary / "recovery"
        recovery.mkdir()
        recovery_files = {}
        for name in ("git-status.txt", "worktree.patch", "repository.bundle"):
            path = recovery / name
            path.write_text(f"safe {name}\n")
            recovery_files[name] = path
        raw = temporary / "raw.tar"
        capture_worktree_archive(
            worktree,
            raw,
            baseline_sha="a" * 40,
            head_sha="b" * 40,
            recovery_files=recovery_files,
        )
        approved = temporary / "approved.tar"
        archive_receipt = sanitize_worktree_archive(
            raw,
            approved,
            logical_path="recovery/full-worktree.tar",
            redaction_service=service,
        ).receipt
        restored = temporary / "restored"
        archive_manifest = restore_worktree_archive(approved, restored)
        restored_corpus = (restored / "worktree/tracked.txt").read_text()
        ignored_and_context_restored = (restored / "worktree/ignored.log").is_file() and (
            restored / "worktree/.context/review.md"
        ).is_file()
        credential_path_excluded = not (restored / "worktree/.env").exists() and any(
            item["path"] == ".env" and item["reason"] == "credential-path"
            for item in archive_manifest["exclusions"]
        )

        opaque = temporary / "opaque"
        opaque.mkdir()
        (opaque / "secret.bin").write_bytes(b"\x00" + SECRET_LEAK_CORPUS[0].payload.encode())
        opaque_raw = temporary / "opaque-raw.tar"
        capture_worktree_archive(
            opaque,
            opaque_raw,
            baseline_sha="a" * 40,
            head_sha="b" * 40,
        )
        opaque_quarantined = False
        try:
            sanitize_worktree_archive(
                opaque_raw,
                temporary / "opaque-approved.tar",
                logical_path="recovery/full-worktree.tar",
                redaction_service=service,
            )
        except SecretQuarantineError:
            opaque_quarantined = True

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
                "worktree_archive_redacts_complete_corpus": archive_receipt.status == "redacted"
                and all(case.payload not in restored_corpus for case in SECRET_LEAK_CORPUS),
                "worktree_archive_keeps_ignored_and_context": ignored_and_context_restored,
                "worktree_archive_excludes_credential_paths": credential_path_excluded,
                "worktree_archive_quarantines_opaque_secret": opaque_quarantined,
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
            "fails metadata closed, and sanitizes portable full-worktree archives"
        ),
    )

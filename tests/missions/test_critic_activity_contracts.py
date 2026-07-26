# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Durable value contracts for exact-candidate critic Activities."""

from __future__ import annotations

import hashlib
from dataclasses import fields, replace

import pytest

from archetype.missions import CriticPolicy
from archetype.missions.critics import (
    CRITIC_ACTIVITY_KIND,
    CandidateReviewRequest,
    CriticActivityCodec,
    CriticActivityRequest,
    CriticExecutionResult,
    CriticFindingValue,
    CriticReceiptValue,
    CriticSubjectPolicy,
    CriticSubjectTooLarge,
    CriticSubjectTransport,
    CriticValidationEvidence,
    bind_critic_subject,
)
from archetype.missions.sandboxes import SandboxIdentity, SandboxStatus
from archetype.missions.transitions import (
    CriticConclusion,
    CriticExecutionStatus,
)
from archetype.redaction import RedactionService

_SECRET = "github_pat_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
_DIFF = b"diff --git a/value.txt b/value.txt\n+candidate\n"


def _request(*, max_subject_bytes: int = 8_192) -> CandidateReviewRequest:
    return CandidateReviewRequest(
        candidate_entity_id=11,
        candidate_id=hashlib.sha256(b"candidate").hexdigest(),
        mission_id=1,
        task_id=2,
        task_name="Review candidate",
        task_prompt=f"Preserve the contract. token={_SECRET}",
        dispatch_id=hashlib.sha256(b"dispatch").hexdigest(),
        dispatch_sequence=1,
        author_execution_id=7,
        author_sandbox_id="author-sandbox",
        repository="https://github.com/example/repository.git",
        branch="agent/review",
        base_ref="main",
        base_revision="1" * 40,
        head_revision="2" * 40,
        diff_digest=hashlib.sha256(_DIFF).hexdigest(),
        validator_bundle_digest=hashlib.sha256(b"validators").hexdigest(),
        policy=CriticPolicy(max_subject_bytes=max_subject_bytes),
        validation=(
            CriticValidationEvidence(
                validator_id=4,
                name="focused",
                command=("pytest", "-q", f"--token={_SECRET}"),
                expected_returncode=0,
                actual_returncode=0,
                revision="2" * 40,
                stdout=f"passed token={_SECRET}",
            ),
        ),
        candidate_published_at_ms=100,
        attempt=2,
    )


def _subject(request: CandidateReviewRequest):
    return bind_critic_subject(
        CriticSubjectPolicy(
            digest=request.diff_digest,
            max_bytes=request.policy.max_subject_bytes,
        ),
        metadata=b"bounded prompt metadata",
        content=_DIFF,
        transport=CriticSubjectTransport.SANDBOX_FILE,
        ref="/workspace/review/.archetype/critic-subject.diff",
    )


def _result(request: CandidateReviewRequest) -> CriticExecutionResult:
    subject = _subject(request)
    finding = CriticFindingValue(
        finding_id="finding-1",
        severity="blocking",
        category="correctness",
        confidence=0.95,
        title="Contract regression",
        detail=f"Reproduction used token={_SECRET}",
    )
    receipt = CriticReceiptValue(
        review_id=request.review_id,
        conclusion=CriticConclusion.BLOCKING,
        candidate_digest=request.candidate_digest,
        policy_digest=request.policy.digest,
        evidence_digest=hashlib.sha256(b"evidence").hexdigest(),
        reviewed_base_revision=request.base_revision,
        reviewed_head_revision=request.head_revision,
        reviewed_diff_digest=request.diff_digest,
        validator_bundle_digest=request.validator_bundle_digest,
        subject_metadata_digest=subject.metadata_digest,
        subject_digest=subject.subject_digest,
        subject_content_size_bytes=subject.content_size_bytes,
        subject_metadata_size_bytes=subject.metadata_size_bytes,
        subject_size_bytes=subject.total_size_bytes,
        subject_media_type=subject.media_type,
        subject_transport=subject.transport.value,
        subject_ref=subject.ref,
        reviewed_scope=f"exact diff token={_SECRET}",
        finding_count=1,
        blocking_count=1,
        output_schema_version=request.policy.output_schema_version,
        completed_at_ms=20,
    )
    return CriticExecutionResult(
        request=request,
        status=CriticExecutionStatus.EXITED,
        sandbox=SandboxIdentity("local", "critic-sandbox", "critic"),
        sandbox_status=SandboxStatus.READY,
        sandbox_acquired=True,
        started_at_ms=10,
        ended_at_ms=20,
        raw_output=f"structured output token={_SECRET}",
        findings=(finding,),
        receipt=receipt,
    )


def test_request_codec_is_canonical_secret_safe_and_diff_free() -> None:
    codec = CriticActivityCodec(RedactionService())
    raw = _request()

    request = codec.prepare_request(raw)
    first = codec.encode_request(request)
    second = codec.encode_request(request)
    restored = codec.decode_request(first.payload)

    assert CRITIC_ACTIVITY_KIND == "missions.critic"
    assert request.review_id == raw.review_id
    assert request.domain_review_attempt == 2
    assert request.subject.digest == raw.diff_digest
    assert request.subject.max_bytes == raw.policy.max_subject_bytes
    assert request.author_sandbox_id == "author-sandbox"
    assert request.as_review_request().diff == ""
    assert first == second
    assert restored == request
    assert first.ref.endswith(first.digest)
    assert hashlib.sha256(first.payload).hexdigest() == first.digest
    assert _DIFF not in first.payload
    assert _SECRET.encode() not in first.payload
    assert b"<redacted:" in first.payload


def test_domain_review_attempt_is_not_generic_activity_delivery_attempt() -> None:
    codec = CriticActivityCodec(RedactionService())
    request = codec.prepare_request(_request())
    repeated_delivery_attempts = (1, 2, 9)

    assert "domain_review_attempt" in {field.name for field in fields(CriticActivityRequest)}
    assert "activity_attempt" not in {field.name for field in fields(CriticActivityRequest)}
    assert len({request.review_id for _ in repeated_delivery_attempts}) == 1
    assert replace(request, domain_review_attempt=3).review_id != request.review_id


def test_codec_rejects_bypassed_redaction_and_policy_mismatch() -> None:
    codec = CriticActivityCodec(RedactionService())
    request = codec.prepare_request(_request())

    with pytest.raises(ValueError, match="not sanitized"):
        codec.encode_request(
            replace(
                request,
                task_prompt=f"unsafe token={_SECRET}",
            )
        )
    with pytest.raises(ValueError, match="another redaction policy"):
        codec.encode_request(
            replace(
                request,
                redaction_policy_id="another-policy",
            )
        )


def test_decoder_rejects_noncanonical_bytes_and_value_digest_drift() -> None:
    codec = CriticActivityCodec(RedactionService())
    encoded = codec.encode_request(codec.prepare_request(_request()))
    noncanonical = (
        b" " + encoded.payload,
        b'{"unknown":true,' + encoded.payload[1:],
        encoded.payload.replace(b'"value":{', b'"value":{"unknown":true,', 1),
        b'{"kind":"request",' + encoded.payload[1:],
    )

    for payload in noncanonical:
        with pytest.raises(ValueError, match="canonical JSON"):
            codec.decode_request(payload)

    wrong_digest = "0" * 64
    with pytest.raises(ValueError, match="digest does not match"):
        replace(
            encoded,
            ref=encoded.ref[:-64] + wrong_digest,
            digest=wrong_digest,
        )


def test_subject_binding_is_exact_bounded_and_file_or_stdin_capable() -> None:
    digest = hashlib.sha256(_DIFF).hexdigest()
    policy = CriticSubjectPolicy(digest=digest, max_bytes=1_024)

    file_binding = bind_critic_subject(
        policy,
        metadata=b"metadata",
        content=_DIFF,
        transport=CriticSubjectTransport.SANDBOX_FILE,
        ref="/workspace/review/subject.diff",
    )
    stdin_binding = bind_critic_subject(
        policy,
        metadata=b"metadata",
        content=_DIFF,
        transport=CriticSubjectTransport.STDIN,
        ref="stdin",
    )

    assert file_binding.content_digest == digest
    assert file_binding.total_size_bytes == len(b"metadata") + len(_DIFF)
    assert stdin_binding.subject_digest != file_binding.subject_digest

    with pytest.raises(ValueError, match="do not match"):
        bind_critic_subject(
            policy,
            metadata=b"metadata",
            content=b"another diff",
            transport=CriticSubjectTransport.STDIN,
            ref="stdin",
        )

    with pytest.raises(CriticSubjectTooLarge) as raised:
        bind_critic_subject(
            CriticSubjectPolicy(digest=digest, max_bytes=1),
            metadata=b"metadata",
            content=_DIFF,
            transport=CriticSubjectTransport.SANDBOX_FILE,
            ref="/workspace/review/subject.diff",
        )
    assert raised.value.content_digest == digest
    assert _DIFF.decode() not in str(raised.value)


@pytest.mark.parametrize(
    ("subject_transport", "subject_ref", "subject_digest", "message"),
    (
        ("evil", "/tmp/subject.diff", "0" * 64, "transport is invalid"),
        ("sandbox_file", "relative.diff", "0" * 64, "safe non-root absolute"),
        ("sandbox_file", "/tmp/../subject.diff", "0" * 64, "safe non-root absolute"),
        ("sandbox_file", "/tmp/subject.diff", "not-a-digest", "lowercase SHA-256"),
        ("stdin", "/tmp/subject.diff", "0" * 64, "must be 'stdin'"),
    ),
)
def test_provider_request_rejects_malformed_subject_bindings(
    subject_transport: str,
    subject_ref: str,
    subject_digest: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(
            _request(),
            subject_ref=subject_ref,
            subject_transport=subject_transport,
            subject_size_bytes=1,
            subject_digest=subject_digest,
        )


def test_result_and_receipt_codecs_bind_exact_subject_and_redact_output() -> None:
    codec = CriticActivityCodec(RedactionService())
    raw_request = _request()
    request = codec.prepare_request(raw_request)
    result = codec.prepare_result(_result(raw_request), request)
    encoded = codec.encode_result(result)
    restored = codec.decode_result(encoded.payload)
    assert result.receipt is not None

    receipt_value = codec.encode_receipt(result.receipt)
    restored_receipt = codec.decode_receipt(receipt_value.payload)

    assert restored == result
    assert restored_receipt == result.receipt
    assert result.receipt.reviewed_base_revision == request.base_revision
    assert result.receipt.reviewed_head_revision == request.head_revision
    assert result.receipt.reviewed_diff_digest == request.diff_digest
    assert result.receipt.validator_bundle_digest == request.validator_bundle_digest
    assert result.receipt.policy_digest == request.policy.digest
    assert result.receipt.subject.content_digest == request.diff_digest
    assert result.receipt.author_sandbox_id == request.author_sandbox_id
    assert result.receipt.critic_sandbox == result.sandbox
    assert result.sandbox.sandbox_id != request.author_sandbox_id
    assert _SECRET.encode() not in encoded.payload
    assert _SECRET.encode() not in receipt_value.payload
    assert b"<redacted:" in encoded.payload
    with pytest.raises(ValueError, match="reused the author sandbox"):
        replace(
            result.receipt,
            critic_sandbox=SandboxIdentity(
                "local",
                result.receipt.author_sandbox_id,
                "critic",
            ),
        )


def test_result_rejects_author_sandbox_and_receipt_identity_drift() -> None:
    codec = CriticActivityCodec(RedactionService())
    raw_request = _request()
    request = codec.prepare_request(raw_request)
    raw_result = _result(raw_request)

    with pytest.raises(ValueError, match="author sandbox"):
        codec.prepare_result(
            replace(
                raw_result,
                sandbox=SandboxIdentity("local", "author-sandbox", "critic"),
            ),
            request,
        )

    swapped_author = replace(
        raw_result.request,
        author_sandbox_id=raw_result.sandbox.sandbox_id,
    )
    with pytest.raises(ValueError, match="another Activity request"):
        codec.prepare_result(
            replace(raw_result, request=swapped_author),
            request,
        )

    changed_prompt = replace(
        raw_result.request,
        task_prompt="Review a different contract",
    )
    with pytest.raises(ValueError, match="another Activity request"):
        codec.prepare_result(
            replace(raw_result, request=changed_prompt),
            request,
        )

    changed_observation = replace(
        raw_result.request,
        validation=(
            replace(
                raw_result.request.validation[0],
                actual_returncode=1,
            ),
        ),
    )
    with pytest.raises(ValueError, match="another Activity request"):
        codec.prepare_result(
            replace(raw_result, request=changed_observation),
            request,
        )

    assert raw_result.receipt is not None
    with pytest.raises(ValueError, match="receipt identity"):
        codec.prepare_result(
            replace(
                raw_result,
                receipt=replace(
                    raw_result.receipt,
                    reviewed_head_revision="3" * 40,
                ),
            ),
            request,
        )


@pytest.mark.parametrize(
    "status",
    (
        CriticExecutionStatus.TIMED_OUT,
        CriticExecutionStatus.ERRORED,
        CriticExecutionStatus.MALFORMED,
        CriticExecutionStatus.UNVERIFIABLE,
    ),
)
def test_every_failure_status_rejects_author_sandbox_reuse(
    status: CriticExecutionStatus,
) -> None:
    codec = CriticActivityCodec(RedactionService())
    raw_request = _request()
    request = codec.prepare_request(raw_request)
    raw_failure = replace(
        _result(raw_request),
        status=status,
        sandbox=SandboxIdentity("local", raw_request.author_sandbox_id, "critic"),
        findings=(),
        receipt=None,
    )

    with pytest.raises(ValueError, match="author sandbox"):
        codec.prepare_result(raw_failure, request)

    durable = codec.prepare_result(
        replace(
            raw_failure,
            sandbox=SandboxIdentity("local", "critic-sandbox", "critic"),
        ),
        request,
    )
    with pytest.raises(ValueError, match="author sandbox"):
        replace(
            durable,
            sandbox=SandboxIdentity("local", request.author_sandbox_id, "critic"),
        )


def test_receipt_rejects_unadmitted_subject_media_type() -> None:
    codec = CriticActivityCodec(RedactionService())
    raw_request = _request()
    request = codec.prepare_request(raw_request)
    raw_result = _result(raw_request)
    assert raw_result.receipt is not None
    other_subject = bind_critic_subject(
        CriticSubjectPolicy(
            digest=raw_request.diff_digest,
            max_bytes=raw_request.policy.max_subject_bytes,
            media_type="application/octet-stream",
        ),
        metadata=b"bounded prompt metadata",
        content=_DIFF,
        transport=CriticSubjectTransport.SANDBOX_FILE,
        ref="/workspace/review/.archetype/critic-subject.diff",
    )

    with pytest.raises(ValueError, match="non-admitted subject media type"):
        codec.prepare_result(
            replace(
                raw_result,
                receipt=replace(
                    raw_result.receipt,
                    subject_metadata_digest=other_subject.metadata_digest,
                    subject_digest=other_subject.subject_digest,
                    subject_content_size_bytes=other_subject.content_size_bytes,
                    subject_metadata_size_bytes=other_subject.metadata_size_bytes,
                    subject_size_bytes=other_subject.total_size_bytes,
                    subject_media_type=other_subject.media_type,
                    subject_transport=other_subject.transport.value,
                    subject_ref=other_subject.ref,
                ),
            ),
            request,
        )


def test_receipt_rejects_unadmitted_output_schema_version() -> None:
    codec = CriticActivityCodec(RedactionService())
    raw_request = _request()
    request = codec.prepare_request(raw_request)
    raw_result = _result(raw_request)
    assert raw_result.receipt is not None

    with pytest.raises(ValueError, match="non-admitted output schema version"):
        codec.prepare_result(
            replace(
                raw_result,
                receipt=replace(
                    raw_result.receipt,
                    output_schema_version=request.policy.output_schema_version + 1,
                ),
            ),
            request,
        )


@pytest.mark.parametrize(
    ("conclusion", "blocking_count"),
    (
        (CriticConclusion.APPROVED, 1),
        (CriticConclusion.BLOCKING, 0),
    ),
)
def test_receipt_conclusion_must_match_blocking_findings(
    conclusion: CriticConclusion,
    blocking_count: int,
) -> None:
    codec = CriticActivityCodec(RedactionService())
    raw_request = _request()
    request = codec.prepare_request(raw_request)
    raw_result = _result(raw_request)
    assert raw_result.receipt is not None
    findings = raw_result.findings if blocking_count else ()

    with pytest.raises(ValueError, match="conclusion conflicts"):
        codec.prepare_result(
            replace(
                raw_result,
                findings=findings,
                receipt=replace(
                    raw_result.receipt,
                    conclusion=conclusion,
                    finding_count=len(findings),
                    blocking_count=blocking_count,
                ),
            ),
            request,
        )

# Copyright 2025 Vangelis Technologies Inc.
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

"""End-to-end contracts for deterministic scope, rendering, and workflow wiring."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import footgun_review_gate as gate  # noqa: E402
from footgun_review_gate import (  # noqa: E402
    BOT_LOGIN,
    GateError,
    build_github_scope,
    changed_line_anchors,
    design_review_marker,
    evidence_marker,
    extract_structured_json,
    gating_findings,
    render_evidence,
    render_human_design_brief,
    review_payload,
    validate_result,
    verify_posted_evidence,
)
from review_aggregation import (  # noqa: E402
    assemble_preliminary_bundle,
    finalize_review_bundle,
    make_infra_failure_receipt,
)
from review_contracts import artifact_digest  # noqa: E402
from review_test_support import (  # noqa: E402
    ANCHORS,
    BASE_SHA,
    DIFF,
    FILES,
    HEAD_SHA,
    RENAMED_DIFF,
    design_finding,
    footgun_finding,
    normalized_design_brief,
    preliminary_bundle,
    raw_result,
    reviewer_receipts,
    scope,
)

WORKFLOW = _ROOT / ".github" / "workflows" / "deterministic-review.yml"


def _metadata(*, head_sha: str = HEAD_SHA, changed_files: int = 2) -> dict[str, Any]:
    return {
        "number": 7,
        "state": "open",
        "changed_files": changed_files,
        "base": {"sha": BASE_SHA, "repo": {"full_name": "owner/repo"}},
        "head": {"sha": head_sha},
    }


def _bot_item(**values: Any) -> dict[str, Any]:
    return {"user": {"login": BOT_LOGIN}, **values}


def _final_with(findings_for=None) -> dict[str, Any]:
    preliminary = preliminary_bundle(findings_for)
    return dict(
        finalize_review_bundle(
            preliminary,
            [],
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )
    )


def test_changed_line_anchors_include_only_added_and_removed_lines():
    assert changed_line_anchors(DIFF) == ANCHORS


def test_renamed_file_anchors_use_github_new_path_on_both_sides():
    assert changed_line_anchors(RENAMED_DIFF) == {
        ("new_name.py", "LEFT", 1),
        ("new_name.py", "RIGHT", 1),
    }


def test_github_scope_binds_api_snapshot_to_event_identity():
    result = build_github_scope(
        repository="owner/repo",
        pr_number=7,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        before=_metadata(),
        after=_metadata(),
        file_pages=[[{"filename": "old.py"}, {"filename": "new.py"}]],
        diff=DIFF,
    )

    assert result == scope()


def test_github_scope_fails_closed_when_head_changes_during_fetch():
    with pytest.raises(GateError, match="base/head"):
        build_github_scope(
            repository="owner/repo",
            pr_number=7,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            before=_metadata(),
            after=_metadata(head_sha="c" * 40),
            file_pages=[[{"filename": "old.py"}, {"filename": "new.py"}]],
            diff=DIFF,
        )


def test_lens_validation_uses_exact_model_contract_without_provenance_echoes():
    normalized = validate_result(raw_result(), scope(), DIFF, lens="authority")

    assert normalized["schema_version"] == 2
    assert set(normalized) == {
        "schema_version",
        "head_sha",
        "summary",
        "review_context",
        "findings",
    }


def test_finding_must_anchor_to_a_changed_line():
    with pytest.raises(GateError, match="not anchored"):
        validate_result(
            raw_result(findings=[footgun_finding(line=99)]),
            scope(),
            DIFF,
            lens="authority",
        )


def test_extract_prefers_last_head_bound_json_object():
    raw = (
        'schema example {"title": "placeholder"}\n'
        f'first {{"head_sha": "{HEAD_SHA}", "summary": "old"}}\n'
        f'final {{"head_sha": "{HEAD_SHA}", "summary": "new"}}'
    )

    assert extract_structured_json(raw) == {"head_sha": HEAD_SHA, "summary": "new"}


def test_rendered_receipt_reports_fan_out_and_does_not_inline_model_narration():
    bundle = _final_with()
    digest = artifact_digest(bundle)
    rendered = render_evidence(
        bundle,
        digest,
        run_url="https://example.test/runs/7",
        artifact_name="review-7",
    )

    assert "6 lenses × 2 independent reviewers" in rendered
    assert "no footgun findings" in rendered
    assert evidence_marker(HEAD_SHA, 0, digest) in rendered
    assert "[workflow artifact](https://example.test/runs/7#artifacts)" in rendered
    assert bundle["lenses"][0]["reviewers"][0]["result"]["summary"] not in rendered


def test_review_payload_posts_one_thread_per_footgun_cluster():
    def findings(lens: str, _reviewer: str) -> list[dict[str, Any]]:
        return [footgun_finding()] if lens == "authority" else []

    bundle = _final_with(findings)
    digest = artifact_digest(bundle)
    payload = review_payload(bundle, digest)

    assert payload["commit_id"] == HEAD_SHA
    assert payload["event"] == "COMMENT"
    assert evidence_marker(HEAD_SHA, 1, digest) in payload["body"]
    assert len(payload["comments"]) == 1
    assert payload["comments"][0]["path"] == "old.py"
    assert "**Failing input or sequence:**" in payload["comments"][0]["body"]
    assert "**Corroboration:** corroborated" in payload["comments"][0]["body"]


def test_human_design_review_combines_generated_brief_and_advisory_design_notes():
    def findings(lens: str, _reviewer: str) -> list[dict[str, Any]]:
        return [design_finding()] if lens == "design-coherence" else []

    bundle = _final_with(findings)
    bundle_digest = artifact_digest(bundle)
    brief = normalized_design_brief(bundle)
    brief_digest = artifact_digest(brief)
    rendered = render_human_design_brief(
        brief,
        bundle,
        bundle_digest=bundle_digest,
        brief_digest=brief_digest,
    )

    assert "ready for human review" in rendered
    assert "Suggested reading order" in rendered
    assert "Design-coherence notes (advisory-only)" in rendered
    assert "single-use-abstraction" in rendered
    assert design_review_marker(HEAD_SHA, bundle_digest, brief_digest) in rendered


def test_verification_requires_both_footgun_and_design_evidence():
    bundle = _final_with()
    digest = artifact_digest(bundle)
    brief = normalized_design_brief(bundle)
    brief_digest = artifact_digest(brief)
    footgun = evidence_marker(HEAD_SHA, 0, digest)
    design = design_review_marker(HEAD_SHA, digest, brief_digest)

    verify_posted_evidence(
        issue_comments=[[_bot_item(body=footgun), _bot_item(body=design)]],
        reviews=[[]],
        review_comments=[[]],
        head_sha=HEAD_SHA,
        finding_count=0,
        gating_count=0,
        digest=digest,
        brief_digest=brief_digest,
    )

    with pytest.raises(GateError, match="human design-review"):
        verify_posted_evidence(
            issue_comments=[[_bot_item(body=footgun)]],
            reviews=[[]],
            review_comments=[[]],
            head_sha=HEAD_SHA,
            finding_count=0,
            gating_count=0,
            digest=digest,
            brief_digest=brief_digest,
        )


def test_verification_binds_findings_to_exact_review_and_thread_count():
    digest = "d" * 64
    brief_digest = "e" * 64
    footgun = evidence_marker(HEAD_SHA, 1, digest)
    design = design_review_marker(HEAD_SHA, digest, brief_digest)
    review = _bot_item(id=17, commit_id=HEAD_SHA, body=footgun)
    inline = _bot_item(
        pull_request_review_id=17,
        body=f"finding head={HEAD_SHA}",
    )

    verify_posted_evidence(
        issue_comments=[_bot_item(body=design)],
        reviews=[review],
        review_comments=[inline],
        head_sha=HEAD_SHA,
        finding_count=1,
        gating_count=1,
        digest=digest,
        brief_digest=brief_digest,
    )


def test_cli_review_matrix_is_the_typed_six_by_two_plan(capsys):
    assert gate.main(["review-matrix"]) == 0
    output = json.loads(capsys.readouterr().out)

    assert len(output["include"]) == 12
    assert {item["lens"] for item in output["include"]} == {
        "daft-shape",
        "state-lifecycle",
        "contracts",
        "authority",
        "observability",
        "design-coherence",
    }


def test_cli_normalize_stamps_reviewer_and_prompt_provenance(tmp_path):
    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    result_path = tmp_path / "result.json"
    prompt_path = tmp_path / "prompt.md"
    output_path = tmp_path / "receipt.json"
    scope_path.write_text(json.dumps(scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")
    result_path.write_text(json.dumps(raw_result()), encoding="utf-8")
    prompt_path.write_text("trusted prompt\n", encoding="utf-8")

    assert (
        gate.main(
            [
                "normalize",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--result",
                str(result_path),
                "--prompt",
                str(prompt_path),
                "--output",
                str(output_path),
                "--lens",
                "authority",
                "--reviewer",
                "claude",
            ]
        )
        == 0
    )
    receipt = json.loads(output_path.read_text(encoding="utf-8"))

    assert receipt["reviewer_id"] == "claude"
    assert receipt["categories"] == list(gate.lens_categories("authority"))
    assert re.fullmatch(r"[0-9a-f]{64}", receipt["prompt_digest"])
    assert re.fullmatch(r"[0-9a-f]{64}", receipt["artifact_digest"])
    assert "reviewer_id" not in receipt["result"]


def test_cli_materializes_one_bounded_design_brief_correction(tmp_path):
    prompt_path = tmp_path / "design-brief-prompt.txt"
    result_path = tmp_path / "design-brief-result.json"
    feedback_path = tmp_path / "design-brief-validation.txt"
    output_path = tmp_path / "design-brief-retry-prompt.txt"
    prompt_path.write_text("Original reviewed contract.\n", encoding="utf-8")
    result_path.write_text(json.dumps({"change_cohorts": []}), encoding="utf-8")
    feedback_path.write_text(
        "change_cohorts do not cover every changed file: ['old.py']\n",
        encoding="utf-8",
    )

    assert (
        gate.main(
            [
                "brief-retry-prompt",
                "--prompt",
                str(prompt_path),
                "--result",
                str(result_path),
                "--feedback",
                str(feedback_path),
                "--output",
                str(output_path),
            ]
        )
        == 0
    )
    rendered = output_path.read_text(encoding="utf-8")

    assert rendered.startswith("Original reviewed contract.")
    assert "one bounded correction" in rendered
    assert "change_cohorts do not cover every changed file" in rendered


def test_workflow_uses_reviewed_prompts_and_typed_dynamic_fan_out():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "python3 scripts/footgun_review_gate.py review-matrix" in workflow
    assert "matrix: ${{ fromJSON(needs.review-plan.outputs.matrix) }}" in workflow
    assert "--kind lens-review" in workflow
    assert "--kind lens-retry" in workflow
    assert "--kind adjudication" in workflow
    assert "--kind design-brief" in workflow
    assert "brief-retry-prompt" in workflow
    assert '--prompt "${RUNNER_TEMP}/lens-prompt.txt"' in workflow
    assert '--reviewer "${REVIEWER_ID}"' in workflow
    assert "lists every scoped file exactly once" not in workflow


def test_workflow_has_aggregation_adjudication_and_human_review_jobs():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    for job in (
        "review-plan",
        "lens-review",
        "review-aggregate",
        "review-adjudication",
        "footgun-review",
        "review-complete",
    ):
        assert re.search(rf"^  {job}:$", workflow, re.MULTILINE)
    assert "adjudication-matrix" in workflow
    assert "Finalize evidence-conserving aggregate" in workflow
    assert "Publish human design-review brief" in workflow
    assert "name: footgun-review" in workflow
    assert "name: review-complete" in workflow


def test_every_codex_reviewer_uses_the_pinned_cli():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert workflow.count('CODEX_VERSION: "0.144.0"') == 1
    assert workflow.count('npm install -g "@openai/codex@${CODEX_VERSION}"') == 4


def test_human_design_brief_is_grounded_without_secret_read_capabilities():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    materialize = workflow.split(
        "- name: Materialize human design-review contract",
        1,
    )[1]
    materialize = materialize.split(
        "- name: Generate human design-review brief (Codex)",
        1,
    )[0]
    step = workflow.split("- name: Generate human design-review brief (Codex)", 1)[1]
    step = step.split("- name: Validate initial human design-review brief", 1)[0]

    assert "--disable shell_tool \\" in step
    assert "--disable multi_agent \\" in step
    assert '- < "${RUNNER_TEMP}/design-brief-prompt.txt" \\' in step
    assert '--bundle "${REVIEW_DIR}/validated/review-bundle.json" \\' in materialize
    assert '--scope "${SCOPE_FILE}" \\' in materialize
    assert '--diff "${DIFF_FILE}" \\' in materialize
    assert "<finalized-review-bundle>" not in materialize
    assert "for path in \\" not in materialize
    assert 'wc -m < "${RUNNER_TEMP}/design-brief-prompt.txt"' in materialize
    assert '> "${RUNNER_TEMP}/design-brief-raw.txt" \\' in step
    assert '2> "${RUNNER_TEMP}/design-brief-stderr.txt"' in step
    assert "Codex diagnostic: " not in step
    assert "grep -E" not in step
    assert "tail -c" not in step
    assert workflow.count("one bounded correction") == 1
    correction = workflow.split("- name: Correct human design-review brief (Codex)", 1)[1]
    correction = correction.split(
        "- name: Validate corrected human design-review brief",
        1,
    )[0]
    assert "--disable shell_tool \\" in correction
    assert "--disable multi_agent \\" in correction


def test_human_design_brief_job_budgets_time_for_one_correction():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    footgun_job = workflow.split("  footgun-review:", 1)[1]
    footgun_job = footgun_job.split("  review-complete:", 1)[0]
    timeouts = [int(value) for value in re.findall(r"timeout-minutes: (\d+)", footgun_job)]

    assert timeouts == [60, 25, 20]
    assert timeouts[0] >= sum(timeouts[1:]) + 10


def test_workflow_preserves_inert_candidate_and_fail_closed_publication():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "pull_request_target:" in workflow
    assert "application/vnd.github.diff" not in workflow
    assert workflow.count('git fetch --no-tags origin "refs/pull/${PR_NUMBER}/head"') == 5
    assert workflow.count("fetch-depth: 0") == 5
    assert "if: ${{ !cancelled() && github.event_name != 'merge_group' }}" in workflow
    assert "if: always()" not in workflow
    assert "gh pr merge" not in workflow


def test_reaffirmed_head_skips_spend_and_publication_but_required_checks_run():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "commits/${HEAD_SHA}/check-runs?check_name=review-complete" in workflow
    assert '[.check_runs[] | select(.conclusion == "success")] | length' in workflow
    assert "reaffirmed: ${{ needs.reaffirm.outputs.reaffirmed }}" in workflow
    assert "needs.footgun-review.outputs.reaffirmed != 'true'" in workflow


def test_final_artifact_retains_bundle_and_design_brief_together():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "validated/review-bundle.json" in workflow
    assert "validated/human-design-brief.json" in workflow
    assert f"retention-days: {gate.FINAL_ARTIFACT_RETENTION_DAYS}" in workflow


# ---------------------------------------------------------------------------
# Advisory findings publish inside the receipt, never as threads. Advisory
# threads were the documented arming livelock: queue-ready counts every
# unresolved thread, so a gate rerun on a green head republished its own
# advisory findings and un-queued the PR it had just passed.
# ---------------------------------------------------------------------------


def _advisory_only(lens: str, reviewer: str):
    if lens == "authority":
        return [footgun_finding(severity="advisory")]
    return []


def _blocking_and_advisory(lens: str, reviewer: str):
    if lens == "authority":
        return [
            footgun_finding(severity="blocking"),
            footgun_finding(severity="advisory", path="new.py", line=1),
        ]
    return []


def test_advisory_findings_render_in_evidence_but_never_as_threads():
    bundle = _final_with(_advisory_only)
    digest = artifact_digest(bundle)

    body = render_evidence(bundle, digest)
    assert "### Advisory findings (non-gating)" in body
    assert "do not block the queue" in body

    assert gating_findings(bundle) == []
    with pytest.raises(GateError, match="gating"):
        review_payload(bundle, digest)


def test_gating_findings_keep_blocking_threads_and_drop_advisory_ones():
    bundle = _final_with(_blocking_and_advisory)
    digest = artifact_digest(bundle)

    gating = gating_findings(bundle)
    assert [finding["gate_disposition"] for finding in gating] == ["blocking"]

    payload = review_payload(bundle, digest)
    assert len(payload["comments"]) == 1
    assert "blocking" in payload["comments"][0]["body"]
    # The receipt body inside the same review still carries the advisory
    # finding, so nothing is hidden — it just is not a thread.
    assert "### Advisory findings (non-gating)" in payload["body"]


def test_verification_accepts_the_comment_path_for_advisory_only_heads():
    """finding_count stays in the marker; the surface is chosen by gating."""
    bundle = _final_with(_advisory_only)
    digest = artifact_digest(bundle)
    brief = normalized_design_brief(bundle)
    brief_digest = artifact_digest(brief)
    marker = evidence_marker(HEAD_SHA, 1, digest)
    design = design_review_marker(HEAD_SHA, digest, artifact_digest(brief))

    verify_posted_evidence(
        issue_comments=[
            [
                {"user": {"login": BOT_LOGIN}, "body": f"receipt {marker}"},
                {"user": {"login": BOT_LOGIN}, "body": f"brief {design}"},
            ]
        ],
        reviews=[[]],
        review_comments=[[]],
        head_sha=HEAD_SHA,
        finding_count=1,
        gating_count=0,
        digest=digest,
        brief_digest=brief_digest,
    )

    with pytest.raises(GateError, match="gating findings cannot exceed"):
        verify_posted_evidence(
            issue_comments=[[]],
            reviews=[[]],
            review_comments=[[]],
            head_sha=HEAD_SHA,
            finding_count=0,
            gating_count=1,
            digest=digest,
            brief_digest=brief_digest,
        )


def test_rendered_receipt_names_neutral_seats():
    def findings_for(lens: str, reviewer: str):
        return []

    receipts = reviewer_receipts(findings_for)
    receipts = [
        receipt
        for receipt in receipts
        if not (receipt["lens"] == "observability" and receipt["reviewer_id"] == "claude")
    ]
    receipts.append(
        dict(
            make_infra_failure_receipt(
                lens="observability",
                reviewer_id="claude",
                head_sha=HEAD_SHA,
                failure_class="quota",
                detail="usage limit for this billing cycle",
            )
        )
    )
    preliminary = assemble_preliminary_bundle(
        receipts,
        head_sha=HEAD_SHA,
        scoped_files=FILES,
        anchors=ANCHORS,
    )
    bundle = dict(
        finalize_review_bundle(
            preliminary,
            [],
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )
    )
    digest = artifact_digest(bundle)

    body = render_evidence(bundle, digest)
    assert "`claude` (neutral: quota)" in body
    assert "| 1/2 |" in body


def test_publication_steps_key_on_the_gating_count():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "steps.prepare.outputs.gating_finding_count == '0'" in workflow
    assert "steps.prepare.outputs.gating_finding_count != '0'" in workflow
    assert '--gating-count "${GATING_FINDING_COUNT}"' in workflow
    # The merge block still keys on blocking findings alone.
    assert "steps.prepare.outputs.blocking_finding_count != '0'" in workflow


def test_lens_seat_failure_records_an_infra_receipt_instead_of_failing():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Record infra failure when no validated receipt exists" in workflow
    assert "infra-receipt" in workflow
    # Quota classification must recognize the exact 2026-07-26 error shape.
    assert "usage limit|spend limit|rate.?limit" in workflow
    # The bounded retry no longer hard-fails the job; the receipt decides.
    assert "Require retry completion" not in workflow

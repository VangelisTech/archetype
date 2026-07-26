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

"""Contract tests for the deterministic footgun review gate."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import footgun_review_gate as gate  # noqa: E402
from footgun_review_gate import (  # noqa: E402
    BOT_LOGIN,
    REQUIRED_CATEGORIES,
    GateError,
    artifact_digest,
    build_github_scope,
    changed_line_anchors,
    evidence_marker,
    render_evidence,
    review_bundle_findings,
    review_payload,
    validate_result,
    validate_review_bundle,
    verify_posted_evidence,
)

HEAD_SHA = "a" * 40
BASE_SHA = "b" * 40
DIFF = """\
diff --git a/old.py b/old.py
index 1111111..2222222 100644
--- a/old.py
+++ b/old.py
@@ -2,3 +2,3 @@
 keep
-unsafe_call()
+safe_call()
 tail
diff --git a/new.py b/new.py
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/new.py
@@ -0,0 +1,2 @@
+first = 1
+second = 2
"""

RENAMED_DIFF = """\
diff --git a/old_name.py b/new_name.py
similarity index 80%
rename from old_name.py
rename to new_name.py
index 1111111..2222222 100644
--- a/old_name.py
+++ b/new_name.py
@@ -1 +1 @@
-unsafe_call()
+safe_call()
"""


def _scope() -> dict:
    return {
        "schema_version": 1,
        "base_sha": BASE_SHA,
        "head_sha": HEAD_SHA,
        "files": ["new.py", "old.py"],
        "categories": list(REQUIRED_CATEGORIES),
    }


def _result(*, findings: list[dict] | None = None) -> dict:
    return {
        "head_sha": HEAD_SHA,
        "summary": (
            "The change replaces an unsafe call in old.py and adds a two-line new.py module; "
            "the reviewed paths preserve the surrounding runtime contracts."
        ),
        "reviewed_files": ["old.py", "new.py"],
        "reviewed_categories": list(reversed(REQUIRED_CATEGORIES)),
        "review_context": [
            {
                "area": "Call safety",
                "files": ["old.py"],
                "assessment": "The replacement preserves the call site while removing the unsafe operation.",
            },
            {
                "area": "New module",
                "files": ["new.py"],
                "assessment": "The new assignments are isolated and do not bypass an existing lifecycle.",
            },
        ],
        "findings": findings or [],
    }


def _finding(**overrides) -> dict:
    finding = {
        "category": "fail-open-failure-paths",
        "severity": "blocking",
        "title": "Validation silently fails open",
        "path": "old.py",
        "side": "RIGHT",
        "line": 3,
        "what_it_does": "The replacement catches a validation error and continues execution.",
        "what_goes_wrong": "Unauthorized rows can pass through when the validation dependency fails.",
        "fix": "Propagate the validation error or return an explicitly empty result.",
    }
    finding.update(overrides)
    return finding


def _lens_result(lens: str, *, findings: list[dict] | None = None) -> dict:
    result = _result(findings=findings)
    result["reviewed_categories"] = list(reversed(gate.LENSES[lens]))
    return result


def _bundle(*, lens_results: dict[str, dict] | None = None) -> dict:
    results = lens_results or {lens: _lens_result(lens) for lens in gate.LENSES}
    return gate.merge_lens_results(results, _scope(), DIFF)


def _run_attempt(tmp_path: Path, result: dict | None) -> tuple[Path, Path, Path]:
    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    result_path = tmp_path / "result.json"
    output_path = tmp_path / "validated" / "normalized.json"
    feedback_path = tmp_path / "validation-feedback.txt"
    github_output = tmp_path / "github-output.txt"
    scope_path.write_text(json.dumps(_scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")
    result_path.write_text("" if result is None else json.dumps(result), encoding="utf-8")

    assert (
        gate.main(
            [
                "attempt",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--result",
                str(result_path),
                "--output",
                str(output_path),
                "--feedback",
                str(feedback_path),
                "--github-output",
                str(github_output),
            ]
        )
        == 0
    )
    return output_path, feedback_path, github_output


def _bot_item(**values) -> dict:
    return {"user": {"login": BOT_LOGIN}, **values}


def _pr_metadata(**overrides) -> dict:
    metadata = {
        "number": 299,
        "state": "open",
        "changed_files": 2,
        "base": {
            "sha": BASE_SHA,
            "repo": {"full_name": "VangelisTech/archetype"},
        },
        "head": {"sha": HEAD_SHA},
    }
    metadata.update(overrides)
    return metadata


def test_github_scope_binds_api_snapshot_to_event_identity():
    scope = build_github_scope(
        repository="VangelisTech/archetype",
        pr_number=299,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        before=_pr_metadata(),
        after=_pr_metadata(),
        file_pages=[[{"filename": "old.py"}, {"filename": "new.py"}]],
        diff=DIFF,
    )

    assert scope == _scope()


def test_github_scope_rejects_head_change_during_fetch():
    after = _pr_metadata()
    after["head"] = {"sha": "c" * 40}

    with pytest.raises(GateError, match="after base/head"):
        build_github_scope(
            repository="VangelisTech/archetype",
            pr_number=299,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            before=_pr_metadata(),
            after=after,
            file_pages=[[{"filename": "old.py"}, {"filename": "new.py"}]],
            diff=DIFF,
        )


def test_github_scope_rejects_incomplete_file_manifest():
    with pytest.raises(GateError, match="changed-file count"):
        build_github_scope(
            repository="VangelisTech/archetype",
            pr_number=299,
            base_sha=BASE_SHA,
            head_sha=HEAD_SHA,
            before=_pr_metadata(),
            after=_pr_metadata(),
            file_pages=[[{"filename": "old.py"}]],
            diff=DIFF,
        )


def test_changed_line_anchors_include_only_added_and_removed_lines():
    assert changed_line_anchors(DIFF) == {
        ("old.py", "LEFT", 3),
        ("old.py", "RIGHT", 3),
        ("new.py", "RIGHT", 1),
        ("new.py", "RIGHT", 2),
    }


def test_renamed_file_anchors_use_githubs_new_path_on_both_sides():
    assert changed_line_anchors(RENAMED_DIFF) == {
        ("new_name.py", "LEFT", 1),
        ("new_name.py", "RIGHT", 1),
    }


def test_no_findings_result_requires_exact_file_category_and_context_coverage():
    normalized = validate_result(_result(), _scope(), DIFF)

    assert normalized["reviewed_files"] == ["new.py", "old.py"]
    assert normalized["reviewed_categories"] == sorted(REQUIRED_CATEGORIES)
    assert normalized["findings"] == []


def test_file_spanning_multiple_context_areas_is_accepted():
    result = _result()
    result["review_context"].append(
        {
            "area": "Cross-cutting notes",
            "files": ["old.py"],
            "assessment": "The same module also carries cross-cutting notes that span both reviewed areas.",
        }
    )

    normalized = validate_result(result, _scope(), DIFF)

    assert normalized["review_context"][2]["files"] == ["old.py"]


def test_context_evidence_path_is_rejected_with_actionable_retry_feedback():
    result = _result()
    result["review_context"][0]["files"].append("src/archetype/core/aio/async_system.py")

    with pytest.raises(GateError, match="outside the diff") as caught:
        validate_result(result, _scope(), DIFF)

    feedback = gate.retry_feedback(caught.value)
    assert "src/archetype/core/aio/async_system.py" in feedback
    assert "changed paths only" in feedback
    assert "assessment prose" in feedback


def test_schema_placeholder_result_is_rejected_with_actionable_retry_feedback():
    placeholder = _result()
    placeholder["reviewed_files"] = ["a.md"]
    placeholder["reviewed_categories"] = ["row-dropping"]
    placeholder["review_context"] = [
        {
            "area": "test area name",
            "files": ["a.md"],
            "assessment": "test assessment text that is at least thirty characters long.",
        }
    ]

    with pytest.raises(GateError, match="reviewed_files") as caught:
        validate_result(placeholder, _scope(), DIFF)

    feedback = gate.retry_feedback(caught.value)
    assert "a.md" in feedback
    assert "schema examples or placeholder values" in feedback


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda result: result.update(head_sha="c" * 40), "head_sha"),
        (lambda result: result["reviewed_files"].pop(), "reviewed_files"),
        (lambda result: result["reviewed_categories"].pop(), "reviewed_categories"),
        (lambda result: result["review_context"].pop(), "review_context"),
        (lambda result: result.update(summary="No notes."), "summary"),
    ],
)
def test_result_fails_closed_when_completion_evidence_is_incomplete(mutation, message):
    result = _result()
    mutation(result)

    with pytest.raises(GateError, match=message):
        validate_result(result, _scope(), DIFF)


def test_finding_must_anchor_to_a_changed_line():
    with pytest.raises(GateError, match="not anchored"):
        validate_result(_result(findings=[_finding(line=2)]), _scope(), DIFF)


def test_finding_requires_a_recognized_severity():
    with pytest.raises(GateError, match="severity"):
        validate_result(_result(findings=[_finding(severity="cosmetic")]), _scope(), DIFF)

    missing = _finding()
    del missing["severity"]
    with pytest.raises(GateError, match="severity"):
        validate_result(_result(findings=[missing]), _scope(), DIFF)


def test_schema_requires_severity_on_findings():
    finding_schema = gate.result_schema()["properties"]["findings"]["items"]

    assert finding_schema["properties"]["severity"]["enum"] == list(gate.SEVERITIES)
    assert "severity" in finding_schema["required"]


def test_rendered_finding_carries_its_severity_disposition():
    blocking = validate_result(_result(findings=[_finding()]), _scope(), DIFF)

    assert "**Severity:** blocking — fix before merge" in gate.render_finding(
        blocking["findings"][0], HEAD_SHA
    )

    advisory = validate_result(_result(findings=[_finding(severity="advisory")]), _scope(), DIFF)

    assert (
        "**Severity:** advisory — fix, or resolve this thread with a written disposition"
        in gate.render_finding(advisory["findings"][0], HEAD_SHA)
    )


def test_prepare_reports_blocking_and_total_finding_counts(tmp_path):
    advisory = _finding(
        severity="advisory",
        category="dead-code-contracts",
        title="New assignment is never consumed",
        path="new.py",
        line=1,
        what_it_does="The new module assigns a value that no caller reads.",
        what_goes_wrong="Readers assume the field is wired to behavior when nothing consumes it.",
        fix="Wire the value to its consumer or remove the assignment.",
    )
    lens_results = {lens: _lens_result(lens) for lens in gate.LENSES}
    lens_results["authority"]["findings"] = [_finding()]
    lens_results["contracts"]["findings"] = [advisory]
    bundle = _bundle(lens_results=lens_results)
    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    result_path = tmp_path / "result.json"
    output_dir = tmp_path / "out"
    github_output = tmp_path / "github-output.txt"
    scope_path.write_text(json.dumps(_scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")
    result_path.write_text(json.dumps(bundle), encoding="utf-8")

    assert (
        gate.main(
            [
                "prepare",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--result",
                str(result_path),
                "--output-dir",
                str(output_dir),
                "--github-output",
                str(github_output),
            ]
        )
        == 0
    )

    outputs = github_output.read_text(encoding="utf-8")
    assert "finding_count=2\n" in outputs
    assert "blocking_finding_count=1\n" in outputs
    evidence = (output_dir / "evidence.md").read_text(encoding="utf-8")
    assert "2 finding(s) — 1 blocking, 1 advisory" in evidence
    assert json.loads((output_dir / "review-bundle.json").read_text(encoding="utf-8")) == bundle


def test_block_step_fires_only_on_blocking_findings():
    bodies = dict(_review_workflow_steps())

    assert (
        "steps.prepare.outputs.blocking_finding_count != '0'" in bodies["Block merge on findings"]
    )
    # Advisory findings still publish as resolvable threads: the publish step
    # keys on the total count, only the merge block keys on the blocking count.
    assert (
        "steps.prepare.outputs.finding_count != '0'"
        in bodies["Publish findings as blocking review threads"]
    )


def _skill_severities() -> tuple[str, ...]:
    skill = (
        Path(__file__).resolve().parents[2] / ".claude" / "skills" / "footgun-detector" / "SKILL.md"
    )
    return tuple(
        re.findall(r"^- \*\*([a-z]+)\*\* — ", skill.read_text(encoding="utf-8"), re.MULTILINE)
    )


def test_severities_track_the_skill_rulebook():
    """The skill file is the single source of truth for the severity tier.

    SEVERITIES is the merge gate's machine-readable copy of the severity
    bullets in .claude/skills/footgun-detector/SKILL.md — same discipline as
    the category slugs: drift between the rulebook and the gate fails here.
    """
    assert _skill_severities() == gate.SEVERITIES


def test_finding_can_anchor_to_the_removed_side():
    normalized = validate_result(
        _result(findings=[_finding(side="LEFT", line=3)]),
        _scope(),
        DIFF,
    )

    assert normalized["findings"][0]["side"] == "LEFT"


def test_rendered_no_findings_evidence_is_specific_and_digest_bound():
    bundle = _bundle()
    digest = artifact_digest(bundle)
    rendered = render_evidence(bundle, digest)

    assert "no findings" in rendered
    assert "2 changed file(s), 24 detector categories, 5/5 lenses" in rendered
    assert "| `daft-shape` | `claude-code` | validated |" in rendered
    assert "| `authority` | `claude-code` | validated |" in rendered
    assert "The change replaces an unsafe call" not in rendered
    assert evidence_marker(HEAD_SHA, 0, digest) in rendered


def test_rendered_evidence_links_to_operational_bundle_without_inlining_it():
    bundle = _bundle()
    digest = artifact_digest(bundle)
    rendered = render_evidence(
        bundle,
        digest,
        run_url="https://example.test/runs/7",
        artifact_name="footgun-review-validated-7",
    )

    assert "```json" not in rendered
    assert "[workflow artifact](https://example.test/runs/7#artifacts)" in rendered
    assert "`footgun-review-validated-7`" in rendered
    assert f"{gate.FINAL_ARTIFACT_RETENTION_DAYS}-day operational retention" in rendered
    assert f"`sha256:{digest}`" in rendered


def test_receipt_size_is_independent_of_unbounded_lens_narration():
    lens_results = {lens: _lens_result(lens) for lens in gate.LENSES}
    for result in lens_results.values():
        result["summary"] = "summary-" + "s" * 20000
        result["review_context"][0]["assessment"] = "assessment-" + "c" * 20000
    bundle = _bundle(lens_results=lens_results)
    digest = artifact_digest(bundle)
    rendered = render_evidence(
        bundle,
        digest,
        run_url="https://example.test/runs/7",
        artifact_name="footgun-review-validated-7",
    )

    assert len(json.dumps(bundle, ensure_ascii=False).encode("utf-8")) > gate._PUBLISHED_BODY_LIMIT
    assert len(rendered.encode("utf-8")) <= gate._PUBLISHED_BODY_LIMIT
    assert "summary-" not in rendered
    assert "assessment-" not in rendered
    assert "[workflow artifact](https://example.test/runs/7#artifacts)" in rendered
    assert evidence_marker(HEAD_SHA, 0, digest) in rendered


def test_untrusted_artifact_metadata_cannot_turn_a_completed_review_incomplete():
    bundle = _bundle()
    digest = artifact_digest(bundle)
    rendered = render_evidence(
        bundle,
        digest,
        run_url=f"https://example.test/{'r' * 70000}",
        artifact_name="a" * 70000,
    )

    assert len(rendered.encode("utf-8")) <= gate._PUBLISHED_BODY_LIMIT
    assert "artifact link unavailable in this rendering" in rendered
    assert evidence_marker(HEAD_SHA, 0, digest) in rendered


def test_receipt_rejects_a_digest_that_does_not_bind_its_bundle():
    bundle = _bundle()

    with pytest.raises(GateError, match="digest does not match"):
        render_evidence(bundle, "0" * 64)


def test_normalize_command_does_not_render_unpublished_evidence(tmp_path):
    result = _result()
    result["summary"] = "summary-" + "s" * 20000
    result["review_context"][0]["assessment"] = "assessment-" + "c" * 20000
    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    result_path = tmp_path / "result.json"
    output_path = tmp_path / "validated" / "normalized.json"
    scope_path.write_text(json.dumps(_scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")
    result_path.write_text(json.dumps(result), encoding="utf-8")

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
                "--output",
                str(output_path),
            ]
        )
        == 0
    )

    normalized = json.loads(output_path.read_text(encoding="utf-8"))
    assert normalized["summary"] == result["summary"]
    assert (
        normalized["review_context"][0]["assessment"] == result["review_context"][0]["assessment"]
    )
    assert list(output_path.parent.iterdir()) == [output_path]


def test_attempt_command_requests_one_retry_with_exact_validator_feedback(tmp_path):
    result = _result()
    result["reviewed_files"] = ["a.md"]
    output_path, feedback_path, github_output = _run_attempt(tmp_path, result)

    assert not output_path.exists()
    assert "reviewed_files does not match scope" in feedback_path.read_text(encoding="utf-8")
    assert github_output.read_text(encoding="utf-8") == "valid=false\n"


def test_attempt_command_requests_retry_when_detector_returns_no_output(tmp_path):
    output_path, feedback_path, github_output = _run_attempt(tmp_path, None)

    assert not output_path.exists()
    assert "could not read valid JSON" in feedback_path.read_text(encoding="utf-8")
    assert github_output.read_text(encoding="utf-8") == "valid=false\n"


def test_attempt_command_writes_validated_result_without_requesting_retry(tmp_path):
    output_path, feedback_path, github_output = _run_attempt(tmp_path, _result())

    assert json.loads(output_path.read_text(encoding="utf-8"))["head_sha"] == HEAD_SHA
    assert not feedback_path.exists()
    assert github_output.read_text(encoding="utf-8") == "valid=true\n"


def test_workflow_has_one_bounded_validator_feedback_retry():
    workflow = (
        Path(__file__).resolve().parents[2] / ".github" / "workflows" / "deterministic-review.yml"
    ).read_text(encoding="utf-8")

    assert workflow.count("uses: anthropics/claude-code-action@") == 2
    assert "id: first_validation" in workflow
    assert "id: detector_retry" in workflow
    assert "steps.first_validation.outputs.valid != 'true'" in workflow
    assert ".footgun-review-validation.txt" in workflow
    assert "Require detector completion" not in workflow


def test_review_narration_is_not_generation_capped_without_quality_evidence():
    result = _result()
    result["summary"] = "summary-" + "s" * 20000
    result["review_context"][0]["assessment"] = "assessment-" + "c" * 20000

    normalized = validate_result(result, _scope(), DIFF)

    assert normalized["summary"] == result["summary"]
    assert (
        normalized["review_context"][0]["assessment"] == result["review_context"][0]["assessment"]
    )


def test_constrained_schema_does_not_turn_publication_budget_into_model_policy():
    schema = gate.result_schema()
    properties = schema["properties"]

    assert "maxLength" not in properties["summary"]
    context_item = properties["review_context"]["items"]["properties"]
    assert "maxLength" not in context_item["assessment"]


def test_bundle_preserves_each_lens_without_concatenating_narration():
    lens_results = {lens: _lens_result(lens) for lens in gate.LENSES}
    for lens, result in lens_results.items():
        result["summary"] = f"{lens} " + "s" * 100

    bundle = _bundle(lens_results=lens_results)

    assert "summary" not in bundle
    assert "review_context" not in bundle
    assert [receipt["result"]["summary"] for receipt in bundle["lenses"]] == [
        lens_results[lens]["summary"] for lens in gate.LENSES
    ]


def test_workflow_retains_the_complete_bundle_as_operational_evidence():
    workflow = REVIEW_WORKFLOW.read_text(encoding="utf-8")

    assert workflow.count("retention-days: 1") == 1
    assert workflow.count(f"retention-days: {gate.FINAL_ARTIFACT_RETENTION_DAYS}") == 1
    assert "path: ${{ github.workspace }}/.footgun-review-output/validated/review-bundle.json" in (
        workflow
    )
    assert '--output "${REVIEW_DIR}/validated/review-bundle.json"' in workflow
    assert '--result "${RUNNER_TEMP}/detector-result/review-bundle.json"' in workflow


def test_workflow_prompts_do_not_impose_unevaluated_prose_caps():
    workflow = REVIEW_WORKFLOW.read_text(encoding="utf-8")

    assert "`summary` is capped at" not in workflow
    assert "every `assessment` at" not in workflow


def test_workflow_materializes_large_diffs_from_inert_git_objects():
    workflow = (
        Path(__file__).resolve().parents[2] / ".github" / "workflows" / "deterministic-review.yml"
    ).read_text(encoding="utf-8")

    # Three independent materializations: every lens job (one matrix
    # definition), the merging footgun-review job, and review-complete each
    # rebuild the same inert scope from trusted git objects.
    assert "application/vnd.github.diff" not in workflow
    assert workflow.count("fetch-depth: 0") == 3
    assert workflow.count('git fetch --no-tags origin "refs/pull/${PR_NUMBER}/head"') == 3
    assert workflow.count('if [[ "${FETCHED_HEAD}" != "${HEAD_SHA}" ]]') == 3
    assert workflow.count("python3 scripts/footgun_review_gate.py scope") == 3
    assert workflow.count('cmp --silent "${RUNNER_TEMP}/git-scope.json" "${SCOPE_FILE}"') == 3


REVIEW_WORKFLOW = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "deterministic-review.yml"
)

# The reaffirm gate turns "this head already has a completed clean review" into
# a no-op run. Every step that spends the detector, publishes a verdict, or
# blocks the merge must therefore be skipped on a reaffirmed head. These four
# are the deliberate exceptions.
UNGATED_REVIEW_STEPS = frozenset(
    {
        # Fails closed for non-maintainers before any spend; must always run.
        "Restrict to maintainer PRs",
        # The decision itself.
        "Reaffirm a completed review of this exact head",
        # A real guard: the detector job must have succeeded either way.
        "Require successful detector job",
        # failure()-gated, so a reaffirmed (nothing-failed) run never reaches it.
        "Report incomplete review",
    }
)

REAFFIRM_GATES = (
    "steps.reaffirm.outputs.reaffirmed != 'true'",
    "needs.reaffirm.outputs.reaffirmed != 'true'",
    "needs.footgun-review.outputs.reaffirmed != 'true'",
)


def _review_workflow_steps() -> list[tuple[str, str]]:
    """Return (name, body) for every step in the review workflow."""

    chunks = re.split(r"\n      - name: ", REVIEW_WORKFLOW.read_text(encoding="utf-8"))[1:]
    return [(chunk.split("\n", 1)[0], chunk) for chunk in chunks]


def _review_workflow_jobs() -> dict[str, str]:
    """Return {job_id: job_body} for every job in the review workflow."""

    jobs_block = REVIEW_WORKFLOW.read_text(encoding="utf-8").split("\njobs:\n", 1)[1]
    headers = list(re.finditer(r"^  ([a-z][a-z-]*):$", jobs_block, re.MULTILINE))
    return {
        match.group(1): jobs_block[
            match.start() : headers[index + 1].start() if index + 1 < len(headers) else None
        ]
        for index, match in enumerate(headers)
    }


def test_ready_for_review_never_cancels_the_running_review_of_the_same_head():
    workflow = REVIEW_WORKFLOW.read_text(encoding="utf-8")

    # A push supersedes the head and still cancels. Marking a draft ready
    # carries no new head, so it queues behind the in-flight review instead of
    # destroying it (#639, #646).
    assert "cancel-in-progress: ${{ github.event.action != 'ready_for_review' }}" in workflow
    assert "cancel-in-progress: true" not in workflow


def test_reaffirm_accepts_only_a_completed_clean_review_of_the_exact_head():
    workflow = REVIEW_WORKFLOW.read_text(encoding="utf-8")

    assert "if: github.event.action == 'ready_for_review'" in workflow
    assert "checks: read" in workflow
    assert "reaffirmed: ${{ steps.reaffirm.outputs.reaffirmed }}" in workflow
    # Bound to this exact sha, and only a concluded success counts.
    assert "commits/${HEAD_SHA}/check-runs?check_name=review-complete" in workflow
    assert '[.check_runs[] | select(.conclusion == "success")] | length' in workflow
    # An API failure falls through to the full review rather than reaffirming.
    assert "|| passed=0" in workflow


def test_reaffirmed_head_skips_every_spending_and_publishing_step():
    # A job whose own `if:` carries a reaffirm gate (the lens matrix) skips
    # wholesale; in every other job, each step must carry a gate itself.
    ungated = []
    for job_id, job_body in _review_workflow_jobs().items():
        job_header = job_body.split("\n      - name: ", 1)[0]
        if any(gate in job_header for gate in REAFFIRM_GATES):
            continue
        chunks = re.split(r"\n      - name: ", job_body)[1:]
        for chunk in chunks:
            name = chunk.split("\n", 1)[0]
            if name not in UNGATED_REVIEW_STEPS and not any(
                gate in chunk for gate in REAFFIRM_GATES
            ):
                ungated.append(f"{job_id}: {name}")

    assert ungated == []


def test_lens_matrix_skips_wholesale_on_a_reaffirmed_head():
    jobs = _review_workflow_jobs()

    lens_header = jobs["lens-review"].split("\n      - name: ", 1)[0]
    assert "needs.reaffirm.outputs.reaffirmed != 'true'" in lens_header
    # The aggregate stays a required context, so it must run (and succeed by
    # carrying the reaffirm decision forward) rather than skip.
    footgun_header = jobs["footgun-review"].split("\n      - name: ", 1)[0]
    assert "!cancelled()" in footgun_header
    assert "reaffirmed: ${{ needs.reaffirm.outputs.reaffirmed }}" in footgun_header


def test_reaffirmed_head_cannot_fire_the_empty_finding_count_steps():
    # `finding_count` is empty when `prepare` is skipped, and '' != '0' is
    # true, so these two would otherwise fire with no findings behind them.
    bodies = {name: body for name, body in _review_workflow_steps()}

    for name in ("Publish findings as blocking review threads", "Block merge on findings"):
        assert "needs.footgun-review.outputs.reaffirmed != 'true' &&" in bodies[name]


def test_a_superseded_review_never_publishes_an_incomplete_verdict():
    workflow = REVIEW_WORKFLOW.read_text(encoding="utf-8")

    # always() ran review-complete after the run was cancelled by its own
    # concurrency group, which failed the first step and posted "Footgun
    # review — incomplete" beside the superseding run's pass. !cancelled()
    # still runs the job when the detector job merely failed.
    assert "if: ${{ !cancelled() && github.event_name != 'merge_group' }}" in workflow
    assert "if: always() && github.event_name != 'merge_group'" not in workflow
    assert "if: needs.footgun-review.result != 'success'" in workflow


def test_review_payload_batches_each_finding_as_an_inline_thread():
    lens_results = {lens: _lens_result(lens) for lens in gate.LENSES}
    lens_results["authority"]["findings"] = [_finding()]
    bundle = _bundle(lens_results=lens_results)
    digest = artifact_digest(bundle)
    payload = review_payload(bundle, digest)

    assert payload["commit_id"] == HEAD_SHA
    assert payload["event"] == "COMMENT"
    assert evidence_marker(HEAD_SHA, 1, digest) in payload["body"]
    assert payload["comments"] == [
        {
            "path": "old.py",
            "line": 3,
            "side": "RIGHT",
            "body": payload["comments"][0]["body"],
        }
    ]
    assert "**Fix:**" in payload["comments"][0]["body"]


def test_verify_no_findings_requires_exact_bot_authored_marker():
    digest = "d" * 64
    marker = evidence_marker(HEAD_SHA, 0, digest)

    verify_posted_evidence(
        issue_comments=[[_bot_item(body=f"review\n{marker}")]],
        reviews=[[]],
        review_comments=[[]],
        head_sha=HEAD_SHA,
        finding_count=0,
        digest=digest,
    )

    with pytest.raises(GateError, match="no-findings evidence"):
        verify_posted_evidence(
            issue_comments=[[_bot_item(body="## Footgun review\n\nNo notes.")]],
            reviews=[[]],
            review_comments=[[]],
            head_sha=HEAD_SHA,
            finding_count=0,
            digest=digest,
        )


def test_verify_findings_requires_exact_review_head_and_inline_thread_count():
    digest = "e" * 64
    marker = evidence_marker(HEAD_SHA, 2, digest)
    review = _bot_item(id=17, commit_id=HEAD_SHA, body=marker)
    comments = [
        _bot_item(pull_request_review_id=17, body=f"finding head={HEAD_SHA}"),
        _bot_item(pull_request_review_id=17, body=f"finding head={HEAD_SHA}"),
    ]

    verify_posted_evidence(
        issue_comments=[],
        reviews=[review],
        review_comments=comments,
        head_sha=HEAD_SHA,
        finding_count=2,
        digest=digest,
    )

    with pytest.raises(GateError, match="findings review"):
        verify_posted_evidence(
            issue_comments=[],
            reviews=[review],
            review_comments=comments[:1],
            head_sha=HEAD_SHA,
            finding_count=2,
            digest=digest,
        )


def test_digest_is_canonical_across_dictionary_key_order():
    value = {"b": [2], "a": {"d": 4, "c": 3}}
    reordered = json.loads('{"a":{"c":3,"d":4},"b":[2]}')

    assert artifact_digest(value) == artifact_digest(reordered)


def _skill_category_slugs() -> tuple[str, ...]:
    skill = (
        Path(__file__).resolve().parents[2] / ".claude" / "skills" / "footgun-detector" / "SKILL.md"
    )
    section = re.search(
        r"^### Footgun categories$(.*?)^## ",
        skill.read_text(encoding="utf-8"),
        re.MULTILINE | re.DOTALL,
    )
    assert section is not None, "skill file lost its 'Footgun categories' section"
    headings = re.findall(r"^#### (.+)$", section.group(1), re.MULTILINE)
    return tuple(re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-") for heading in headings)


def test_required_categories_track_the_skill_rulebook():
    """The skill file is the single source of truth for footgun categories.

    REQUIRED_CATEGORIES is the merge gate's machine-readable copy of the
    category headings in .claude/skills/footgun-detector/SKILL.md. Adding,
    removing, renaming, or reordering a category in one place without the
    other silently changes what the gate enforces — so drift fails here.
    """
    assert _skill_category_slugs() == REQUIRED_CATEGORIES


def test_observability_categories_extend_failure_and_unwind_review():
    assert len(REQUIRED_CATEGORIES) == 24
    assert REQUIRED_CATEGORIES[-2:] == (
        "observability-boundary-and-authority",
        "telemetry-safety-and-cardinality",
    )
    assert "fail-open-failure-paths" in REQUIRED_CATEGORIES
    assert "error-path-unwind" in REQUIRED_CATEGORIES


def test_lenses_partition_required_categories():
    """Every required category belongs to exactly one non-empty lens.

    The merge re-checks this at runtime, but drift should fail here first:
    a category added to REQUIRED_CATEGORIES without a lens assignment would
    otherwise only surface as a red merge on the next reviewed PR.
    """
    assigned = [category for categories in gate.LENSES.values() for category in categories]

    assert all(gate.LENSES.values())
    assert len(assigned) == len(set(assigned))
    assert set(assigned) == set(REQUIRED_CATEGORIES)


def test_lens_validation_requires_exactly_the_lens_categories():
    validated = validate_result(
        _lens_result("authority"), _scope(), DIFF, categories=gate.LENSES["authority"]
    )

    assert validated["reviewed_categories"] == sorted(gate.LENSES["authority"])
    with pytest.raises(GateError, match="reviewed_categories does not match"):
        validate_result(_result(), _scope(), DIFF, categories=gate.LENSES["authority"])


def test_lens_validation_rejects_findings_outside_the_lens():
    # fail-open-failure-paths belongs to the authority lens, not daft-shape.
    result = _lens_result("daft-shape", findings=[_finding()])

    with pytest.raises(GateError, match="not a reviewed category"):
        validate_result(result, _scope(), DIFF, categories=gate.LENSES["daft-shape"])


def test_lens_schema_narrows_the_category_enum():
    schema = gate.result_schema(gate.LENSES["observability"])
    enum = schema["properties"]["findings"]["items"]["properties"]["category"]["enum"]

    assert enum == list(gate.LENSES["observability"])
    assert gate.result_schema()["properties"]["findings"]["items"]["properties"]["category"][
        "enum"
    ] == list(REQUIRED_CATEGORIES)


def test_merge_packages_one_full_coverage_review_bundle(tmp_path):
    results_dir = tmp_path / "lenses"
    results_dir.mkdir()
    for lens in gate.LENSES:
        findings = [_finding()] if lens == "authority" else None
        (results_dir / f"{lens}.json").write_text(
            json.dumps(_lens_result(lens, findings=findings)), encoding="utf-8"
        )
    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    output_path = tmp_path / "validated" / "review-bundle.json"
    scope_path.write_text(json.dumps(_scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")

    assert (
        gate.main(
            [
                "merge",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--results-dir",
                str(results_dir),
                "--output",
                str(output_path),
            ]
        )
        == 0
    )

    bundle = json.loads(output_path.read_text(encoding="utf-8"))
    assert validate_review_bundle(bundle, _scope(), DIFF) == bundle
    assert bundle["kind"] == gate.REVIEW_BUNDLE_KIND
    assert bundle["reviewed_categories"] == sorted(REQUIRED_CATEGORIES)
    assert "summary" not in bundle
    assert "review_context" not in bundle
    assert "findings" not in bundle
    assert [receipt["lens"] for receipt in bundle["lenses"]] == list(gate.LENSES)
    assert [receipt["backend"] for receipt in bundle["lenses"]] == [
        gate.LENS_BACKENDS[lens] for lens in gate.LENSES
    ]
    assert all(receipt["status"] == "validated" for receipt in bundle["lenses"])
    assert all(
        receipt["artifact_digest"] == artifact_digest(receipt["result"])
        for receipt in bundle["lenses"]
    )
    assert [finding["category"] for finding in review_bundle_findings(bundle)] == [
        "fail-open-failure-paths"
    ]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda bundle: bundle["lenses"][0].update(backend="opencode"),
            "backend must be",
        ),
        (
            lambda bundle: bundle["lenses"][0].update(status="failed"),
            "status must be 'validated'",
        ),
        (
            lambda bundle: bundle["lenses"][0]["result"].update(
                summary=bundle["lenses"][0]["result"]["summary"] + " tampered"
            ),
            "digest does not match",
        ),
        (
            lambda bundle: bundle["lenses"].pop(),
            "lenses do not match",
        ),
    ],
)
def test_review_bundle_rejects_completion_metadata_or_evidence_tampering(mutation, message):
    bundle = json.loads(json.dumps(_bundle()))
    mutation(bundle)

    with pytest.raises(GateError, match=message):
        validate_review_bundle(bundle, _scope(), DIFF)


def test_merge_fails_closed_on_a_missing_lens_result(tmp_path):
    results_dir = tmp_path / "lenses"
    results_dir.mkdir()
    for lens in gate.LENSES:
        if lens == "observability":
            continue
        (results_dir / f"{lens}.json").write_text(json.dumps(_lens_result(lens)), encoding="utf-8")
    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    scope_path.write_text(json.dumps(_scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")

    with pytest.raises(SystemExit, match="missing validated lens result"):
        gate.main(
            [
                "merge",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--results-dir",
                str(results_dir),
                "--output",
                str(tmp_path / "normalized.json"),
            ]
        )


def test_merge_deduplicates_identical_findings_within_a_lens():
    lens_results = {lens: _lens_result(lens) for lens in gate.LENSES}
    lens_results["authority"]["findings"] = [_finding(), _finding()]

    bundle = gate.merge_lens_results(lens_results, _scope(), DIFF)

    assert len(review_bundle_findings(bundle)) == 1


def test_extract_recovers_the_structured_object_from_fenced_prose():
    payload = _result()
    raw = f"Here is my review.\n```json\n{json.dumps(payload)}\n```\nDone."

    assert gate.extract_structured_json(raw) == payload


def test_extract_prefers_the_last_head_bound_object():
    raw = (
        json.dumps({"head_sha": HEAD_SHA, "attempt": 1})
        + "\nCorrection below.\n"
        + json.dumps({"head_sha": HEAD_SHA, "attempt": 2})
        + "\nTotals: "
        + json.dumps({"files": 2})
    )

    assert gate.extract_structured_json(raw) == {"head_sha": HEAD_SHA, "attempt": 2}


def test_extract_command_passes_unparsable_output_through(tmp_path):
    raw_path = tmp_path / "raw.txt"
    output_path = tmp_path / "extracted.json"
    raw_path.write_text("no structured result here { broken", encoding="utf-8")

    assert gate.main(["extract", "--raw", str(raw_path), "--output", str(output_path)]) == 0

    assert output_path.read_text(encoding="utf-8") == "no structured result here { broken"


def test_workflow_matrix_names_every_lens_and_backend_exactly_once():
    workflow = REVIEW_WORKFLOW.read_text(encoding="utf-8")
    matrix_entries = re.findall(
        r"- lens: ([a-z-]+)\n\s+backend: ([a-z-]+)",
        workflow,
    )

    assert dict(matrix_entries) == gate.LENS_BACKENDS


def test_merge_dedupe_never_softens_a_blocking_finding():
    # Duplicate findings inside one lens that disagree on severity collapse
    # to the blocking one — deduplication must never soften the gate.
    lens_results = {lens: _lens_result(lens) for lens in gate.LENSES}
    lens_results["authority"]["findings"] = [_finding(severity="advisory"), _finding()]

    bundle = gate.merge_lens_results(lens_results, _scope(), DIFF)

    assert [finding["severity"] for finding in review_bundle_findings(bundle)] == ["blocking"]


def test_merge_prepare_publish_contract_is_independent_of_narration_size(tmp_path):
    """Exercise the workflow's real merge → prepare → publish identity path."""
    results_dir = tmp_path / "lenses"
    results_dir.mkdir()
    for lens in gate.LENSES:
        findings = [_finding()] if lens == "authority" else None
        result = _lens_result(lens, findings=findings)
        result["summary"] = f"summary-{lens}-" + "s" * 20000
        for entry in result["review_context"]:
            entry["assessment"] = f"assessment-{lens}-" + "a" * 20000
        (results_dir / f"{lens}.json").write_text(json.dumps(result), encoding="utf-8")

    scope_path = tmp_path / "scope.json"
    diff_path = tmp_path / "review.diff"
    bundle_path = tmp_path / "merge-output" / "review-bundle.json"
    output_dir = tmp_path / "prepare-output"
    github_output = tmp_path / "github-output.txt"
    scope_path.write_text(json.dumps(_scope()), encoding="utf-8")
    diff_path.write_text(DIFF, encoding="utf-8")

    assert (
        gate.main(
            [
                "merge",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--results-dir",
                str(results_dir),
                "--output",
                str(bundle_path),
            ]
        )
        == 0
    )
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert len(json.dumps(bundle).encode("utf-8")) > gate._PUBLISHED_BODY_LIMIT

    assert (
        gate.main(
            [
                "prepare",
                "--scope",
                str(scope_path),
                "--diff",
                str(diff_path),
                "--result",
                str(bundle_path),
                "--output-dir",
                str(output_dir),
                "--github-output",
                str(github_output),
                "--artifact-name",
                "footgun-review-validated-9",
                "--run-url",
                "https://example.test/runs/9",
            ]
        )
        == 0
    )

    prepared_bundle = json.loads((output_dir / "review-bundle.json").read_text(encoding="utf-8"))
    assert prepared_bundle == bundle
    digest = artifact_digest(bundle)
    outputs = github_output.read_text(encoding="utf-8")
    assert f"artifact_digest={digest}\n" in outputs
    assert "finding_count=1\n" in outputs
    assert "blocking_finding_count=1\n" in outputs

    evidence = (output_dir / "evidence.md").read_text(encoding="utf-8")
    assert len(evidence.encode("utf-8")) <= gate._PUBLISHED_BODY_LIMIT
    assert "summary-authority-" not in evidence
    assert "assessment-authority-" not in evidence
    assert "[workflow artifact](https://example.test/runs/9#artifacts)" in evidence
    assert evidence_marker(HEAD_SHA, 1, digest) in evidence

    payload = json.loads((output_dir / "review.json").read_text(encoding="utf-8"))
    assert payload["body"] == evidence.rstrip("\n")
    assert len(payload["comments"]) == 1
    review = _bot_item(id=17, commit_id=HEAD_SHA, body=payload["body"])
    comments = [
        _bot_item(pull_request_review_id=17, body=comment["body"])
        for comment in payload["comments"]
    ]
    verify_posted_evidence(
        issue_comments=[],
        reviews=[review],
        review_comments=comments,
        head_sha=HEAD_SHA,
        finding_count=1,
        digest=digest,
    )


def test_compact_receipt_omits_model_narration_even_when_it_would_fit():
    bundle = _bundle()
    digest = artifact_digest(bundle)

    rendered = render_evidence(bundle, digest)

    for receipt in bundle["lenses"]:
        assert receipt["result"]["summary"] not in rendered
        for entry in receipt["result"]["review_context"]:
            assert entry["assessment"] not in rendered

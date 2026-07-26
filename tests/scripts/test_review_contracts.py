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

"""Tests for reviewable prompt policy and model-authored return contracts."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from review_contracts import (  # noqa: E402
    DESIGN_CATEGORIES,
    FOOTGUN_LENSES,
    LENS_REVIEWERS,
    LENSES,
    REQUIRED_CATEGORIES,
    ReviewError,
    adjudication_result_schema,
    expected_review_pairs,
    human_design_brief_schema,
    lens_result_schema,
    normalize_human_design_brief,
    normalize_lens_result,
    render_adjudication_prompt,
    render_design_brief_prompt,
    render_lens_retry_prompt,
    render_lens_review_prompt,
    review_matrix,
)
from review_test_support import (  # noqa: E402
    ANCHORS,
    FILES,
    HEAD_SHA,
    design_finding,
    footgun_finding,
    raw_design_brief,
    raw_result,
)


def _heading_slugs(path: Path, section_heading: str) -> tuple[str, ...]:
    text = path.read_text(encoding="utf-8")
    section = re.search(
        rf"^{re.escape(section_heading)}$(.*?)(?=^## |\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert section is not None
    headings = re.findall(r"^#### (.+)$", section.group(1), re.MULTILINE)
    return tuple(re.sub(r"[^a-z0-9]+", "-", item.lower()).strip("-") for item in headings)


def test_footgun_categories_track_the_rulebook_in_review_order():
    skill = _ROOT / ".claude" / "skills" / "footgun-detector" / "SKILL.md"

    assert _heading_slugs(skill, "### Footgun categories") == REQUIRED_CATEGORIES
    assigned = [category for categories in FOOTGUN_LENSES.values() for category in categories]
    assert len(assigned) == len(set(assigned))
    assert set(assigned) == set(REQUIRED_CATEGORIES)


def test_design_categories_track_the_advisory_rulebook():
    skill = _ROOT / ".claude" / "skills" / "design-coherence" / "SKILL.md"

    assert _heading_slugs(skill, "## Design categories") == DESIGN_CATEGORIES
    assert "advisory-only" in skill.read_text(encoding="utf-8")


def test_review_plan_is_six_lenses_by_two_distinct_reviewers():
    matrix = review_matrix()

    assert len(LENSES) == 6
    assert len(matrix) == 12
    assert [(item["lens"], item["reviewer"]) for item in matrix] == list(expected_review_pairs())
    assert all(len(reviewers) == len(set(reviewers)) == 2 for reviewers in LENS_REVIEWERS.values())


def test_footgun_return_schema_contains_failure_evidence_not_provenance_echoes():
    schema = lens_result_schema("authority")
    properties = schema["properties"]
    finding = properties["findings"]["items"]

    assert set(properties) == {"head_sha", "summary", "review_context", "findings"}
    assert "reviewed_files" not in properties
    assert "reviewed_categories" not in properties
    assert "reviewer_id" not in properties
    assert "failing_input_or_sequence" in finding["required"]
    assert finding["properties"]["category"]["enum"] == list(LENSES["authority"])
    assert schema["additionalProperties"] is False


def test_design_return_schema_requires_repository_evidence_and_preserved_behavior():
    schema = lens_result_schema("design-coherence")
    finding = schema["properties"]["findings"]["items"]

    assert "severity" not in finding["properties"]
    assert finding["properties"]["repository_evidence"]["minItems"] == 1
    assert "behavior_preserved" in finding["required"]
    assert finding["additionalProperties"] is False


def test_lens_normalizer_rejects_model_authored_provenance():
    result = raw_result()
    result["reviewer_id"] = "claude"

    with pytest.raises(ReviewError, match="fields do not match"):
        normalize_lens_result(
            result,
            lens="authority",
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )


def test_footgun_normalizer_requires_concrete_failing_sequence():
    finding = footgun_finding()
    finding.pop("failing_input_or_sequence")

    with pytest.raises(ReviewError, match="fields do not match"):
        normalize_lens_result(
            raw_result(findings=[finding]),
            lens="authority",
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )


def test_design_normalizer_rejects_behavior_changing_cleanliness_advice():
    finding = design_finding(behavior_preserved=False)

    with pytest.raises(ReviewError, match="must preserve behavior"):
        normalize_lens_result(
            raw_result(findings=[finding]),
            lens="design-coherence",
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )


def test_design_normalizer_rejects_hallucinated_repository_evidence():
    finding = design_finding()
    finding["repository_evidence"][0]["symbol"] = "DefinitelyNotARealSymbol"

    with pytest.raises(ReviewError, match="symbol does not occur"):
        normalize_lens_result(
            raw_result(findings=[finding]),
            lens="design-coherence",
            head_sha=HEAD_SHA,
            scoped_files=FILES,
            anchors=ANCHORS,
        )


def test_prompts_render_from_named_files_with_adjacent_exact_schemas():
    lens_prompt = render_lens_review_prompt(
        pr_number=17,
        head_sha=HEAD_SHA,
        lens="authority",
        reviewer_id="claude",
    )
    retry_prompt = render_lens_retry_prompt(
        pr_number=17,
        head_sha=HEAD_SHA,
        lens="authority",
        reviewer_id="claude",
    )
    adjudication_prompt = render_adjudication_prompt(
        pr_number=17,
        head_sha=HEAD_SHA,
        cluster_id="c" * 64,
    )
    brief_prompt = render_design_brief_prompt(
        pr_number=17,
        head_sha=HEAD_SHA,
        bundle_digest="d" * 64,
    )

    assert "independent reviewer `claude`" in lens_prompt
    assert ".claude/skills/footgun-detector/SKILL.md" in lens_prompt
    assert '"failing_input_or_sequence"' in lens_prompt
    assert "single bounded correction attempt" in retry_prompt
    assert "Act as a falsifier, not a voter" in adjudication_prompt
    assert '"recommended_severity"' in adjudication_prompt
    assert "ready-for-human-review" in brief_prompt
    assert '"change_cohorts"' in brief_prompt


def test_prompts_are_plain_reviewable_markdown_files():
    prompt_dir = _ROOT / ".github" / "review" / "prompts"

    assert {path.name for path in prompt_dir.glob("*.md")} == {
        "adjudication.md",
        "design-brief.md",
        "lens-retry.md",
        "lens-review.md",
    }
    assert all(path.read_text(encoding="utf-8").strip() for path in prompt_dir.glob("*.md"))


def test_adjudication_and_brief_schemas_reject_unreviewed_fields():
    assert adjudication_result_schema()["additionalProperties"] is False
    brief_schema = human_design_brief_schema()

    assert brief_schema["additionalProperties"] is False
    assert brief_schema["properties"]["readiness"]["enum"] == ["ready-for-human-review"]


def test_human_brief_must_bind_bundle_and_cover_every_changed_file():
    raw = raw_design_brief("d" * 64)
    raw["change_cohorts"][0]["files"] = ["old.py"]

    with pytest.raises(ReviewError, match="cover every changed file"):
        normalize_human_design_brief(
            raw,
            head_sha=HEAD_SHA,
            bundle_digest="d" * 64,
            scoped_files=FILES,
            cluster_ids=set(),
        )


def test_human_brief_must_surface_every_required_decision_cluster():
    cluster_id = "c" * 64

    with pytest.raises(ReviewError, match="omits required review clusters"):
        normalize_human_design_brief(
            raw_design_brief("d" * 64),
            head_sha=HEAD_SHA,
            bundle_digest="d" * 64,
            scoped_files=FILES,
            cluster_ids={cluster_id},
            required_decision_cluster_ids={cluster_id},
        )

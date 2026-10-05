# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_failure_status_requires_a_created_deployment():
    workflow = (ROOT / ".github" / "workflows" / "docs.yml").read_text(encoding="utf-8")
    failure_step = workflow[workflow.index("- name: Update deployment status (failure)") :]

    assert "if: failure() && steps.gh-deploy.outputs.deployment-id != ''" in failure_step
    assert "DEPLOYMENT_ID: ${{ steps.gh-deploy.outputs.deployment-id }}" in failure_step
    assert "deployment_id: Number(process.env.DEPLOYMENT_ID)," in failure_step
    assert "deployment_id: ${{" not in failure_step


def workflow():
    return yaml.load((ROOT / ".github/workflows/docs.yml").read_text(), Loader=yaml.BaseLoader)


def evaluate(expression, *, event, deploy, ref):
    """Evaluate the small boolean/string subset used by the actual workflow gates."""
    expression = expression.removeprefix("${{").removesuffix("}}").strip()
    for key, name in (
        ("github.event_name", "event"),
        ("inputs.deploy", "deploy"),
        ("github.ref", "ref"),
    ):
        expression = expression.replace(key, name)
    expression = expression.replace("&&", "and").replace("||", "or")
    expression = re.sub(r"\btrue\b", "True", expression)
    expression = re.sub(r"\bfalse\b", "False", expression)
    tree = ast.parse(expression, mode="eval")
    allowed = (
        ast.Expression,
        ast.BoolOp,
        ast.Compare,
        ast.Name,
        ast.Load,
        ast.Constant,
        ast.And,
        ast.Or,
        ast.Eq,
        ast.NotEq,
    )
    assert all(isinstance(node, allowed) for node in ast.walk(tree))
    return eval(
        compile(tree, "workflow gate", "eval"),
        {"__builtins__": {}},
        {"event": event, "deploy": deploy, "ref": ref},
    )


@pytest.mark.parametrize(
    "event,deploy,ref,eligible",
    (
        ("workflow_dispatch", True, "refs/heads/main", True),
        ("workflow_dispatch", False, "refs/heads/main", False),
        ("workflow_dispatch", True, "refs/heads/feature", False),
        ("workflow_dispatch", True, "refs/tags/v0.7.0", False),
        ("push", True, "refs/heads/main", False),
        ("pull_request", True, "refs/pull/891/merge", False),
    ),
)
def test_only_explicit_main_dispatch_is_eligible_for_production(event, deploy, ref, eligible):
    assert (
        evaluate(workflow()["jobs"]["deploy"]["if"], event=event, deploy=deploy, ref=ref)
        is eligible
    )


def test_ordinary_main_builds_cannot_cancel_running_production_dispatch():
    concurrency = workflow()["concurrency"]

    def group(event, deploy):
        return re.sub(
            r"\$\{\{(.*?)\}\}",
            lambda match: str(
                evaluate(match.group(1), event=event, deploy=deploy, ref="refs/heads/main")
            ),
            concurrency["group"],
        )

    assert group("push", False) != group("workflow_dispatch", True)
    assert group("workflow_dispatch", False) != group("workflow_dispatch", True)
    assert (
        evaluate(
            concurrency["cancel-in-progress"],
            event="workflow_dispatch",
            deploy=True,
            ref="refs/heads/main",
        )
        is False
    )
    assert (
        evaluate(
            concurrency["cancel-in-progress"], event="push", deploy=False, ref="refs/heads/main"
        )
        is True
    )


def test_production_dispatch_targets_the_guarded_main_branch():
    steps = workflow()["jobs"]["deploy"]["steps"]
    publish = next(step for step in steps if step["name"] == "Deploy to Cloudflare Pages")
    assert publish["env"]["DOCS_DEPLOY_BRANCH"] == "main"

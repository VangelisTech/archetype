# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_failure_status_requires_a_created_deployment():
    workflow = (ROOT / ".github" / "workflows" / "docs.yml").read_text(encoding="utf-8")
    failure_step = workflow[workflow.index("- name: Update deployment status (failure)") :]

    assert "if: failure() && steps.gh-deploy.outputs.deployment-id != ''" in failure_step
    assert "DEPLOYMENT_ID: ${{ steps.gh-deploy.outputs.deployment-id }}" in failure_step
    assert "deployment_id: Number(process.env.DEPLOYMENT_ID)," in failure_step
    assert "deployment_id: ${{" not in failure_step

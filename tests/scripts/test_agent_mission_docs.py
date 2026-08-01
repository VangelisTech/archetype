# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Regression contracts for the Agent Missions operator guide."""

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_GUIDE = _ROOT / "docs" / "guide" / "agent-missions.md"


def test_modal_auth_volume_setup_selects_environment_name_and_v2() -> None:
    guide = _GUIDE.read_text()
    setup_command = (
        'modal volume create -e "$CODING_AGENT_MODAL_ENVIRONMENT" \\\n'
        '    "$CODEX_AUTH_VOLUME" --version 2'
    )

    assert setup_command in guide
    assert guide.index(setup_command) < guide.index(
        "python examples/11_coding_agent_mission.py --login"
    )

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Coding-agent specialization of the generic mission contract."""

from archetype.core.component import Component


class CodingAgentEpisode(Component):
    """Provider and live-handle identity for one repository mission."""

    mission_id: str = ""
    provider: str = ""
    sandbox_id: str = ""
    harness: str = ""

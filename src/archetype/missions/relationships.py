# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""First-class relationships used by the Agent Missions task graph."""

from archetype.graph import Relation


class PartOfMission(Relation):
    """Task ``source`` belongs to mission ``target``."""


class DependsOn(Relation):
    """Task ``source`` may run only after task ``target`` is accepted."""

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""First-class relationships used by the Agent Missions task graph."""

from archetype.graph import Relation


class PartOfMission(Relation):
    """Task ``source`` belongs to mission ``target``."""


class DependsOn(Relation):
    """Task ``source`` may run only after task ``target`` is accepted."""


class Guards(Relation):
    """Validator ``source`` guards task ``target``."""


class Executes(Relation):
    """Agent execution ``source`` executes task ``target``."""


class RunsIn(Relation):
    """Agent execution ``source`` ran in sandbox ``target``."""


class ProducedBy(Relation):
    """Output ``source`` was produced by execution ``target``."""

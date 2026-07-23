# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""First-class relationship entities for mission graphs and provenance."""

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


class CandidateFor(Relation):
    """Immutable candidate ``source`` is a review subject for task ``target``."""


class AuthoredBy(Relation):
    """Candidate ``source`` was produced by author execution ``target``."""


class Reviews(Relation):
    """Critic execution ``source`` reviews candidate ``target``."""


class Supersedes(Relation):
    """New candidate ``source`` makes prior candidate ``target`` stale."""

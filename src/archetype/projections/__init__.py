# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Projections family library: read models over the append-only ledger.

Design: ``docs/design/graph-system.md`` (stage 0). Frame-pure read models in
:mod:`archetype.projections.frames`; async handle sugar in
:mod:`archetype.projections.worlds`; R5 sync parity in
:mod:`archetype.projections.sync`. Imports only core and third-party
packages (design D1); every layer may import it. Family-specific projection
logic belongs inside its family — this package holds the generic,
cross-family read models.
"""

from archetype.projections import sync
from archetype.projections.frames import activity, latest, overview
from archetype.projections.worlds import QueriesLike, world_overview

__all__ = [
    "QueriesLike",
    "activity",
    "latest",
    "overview",
    "sync",
    "world_overview",
]

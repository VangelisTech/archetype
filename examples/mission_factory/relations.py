# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local catalog relation for mission-factory assets."""

from archetype.graph import Policy, Relation


class AssetChildOf(Relation):
    """Catalog membership, distinct from copied ``ChildOf`` composition."""

    exclusive = True
    on_delete_target = Policy.DELETE

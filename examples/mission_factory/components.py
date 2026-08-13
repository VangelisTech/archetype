# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Example-local schemas for mission-factory prefab assets.

The library stores workflow recipes and presentation contracts as ordinary
ECS content.  It does not duplicate live Agent Missions state or transition
authority.  A driver compiles trusted recipe rows into supported mission
authoring values, while a separate visual adapter may project committed state
onto the model, socket, animation, and interaction descriptions below.
"""

from __future__ import annotations

from archetype.core.component import Component


class FactoryAsset(Component):
    """Stable identity for one library, collection, visual, or line asset."""

    key: str
    display_name: str
    kind: str
    version: int = 1


class AssetPart(Component):
    """Stable local identity for one compositional child of a prefab."""

    key: str
    kind: str
    order: int = 0


class BlueprintSlot(Component):
    """One named role inside an instantiated mission-line blueprint."""

    key: str
    role: str
    visual_asset: str = ""
    order: int = 0


class TaskRecipe(Component):
    """Trusted task-authoring data interpreted by the example compiler."""

    prompt_template: str
    max_dispatches: int = 3
    publication_policy: str = "commit_and_push"
    critic_max_reviews: int = 2
    critic_timeout_seconds: int = 2700


class ValidatorRecipe(Component):
    """One validator recipe; ``Guards`` meaning remains explicit rule data."""

    name: str
    command: list[str]
    expected_returncode: int = 0
    timeout_seconds: int = 900


class ConnectionRule(Component):
    """A non-hierarchical relationship to materialize by stable slot name."""

    relation: str
    source_slot: str
    target_slot: str


class BehaviorBinding(Component):
    """The existing authority and observations that give a visual asset meaning."""

    authority: str
    observes: list[str]
    effect: str


class VisualGeometry(Component):
    """Machine-checkable GLB generation and placement constraints."""

    model_uri: str
    model_status: str = "brief"
    format: str = "glb"
    coordinate_system: str = "y_up"
    origin: str = "ground_center"
    footprint_x: int = 1
    footprint_y: int = 1
    width_m: float = 5.0
    depth_m: float = 5.0
    height_m: float = 5.0
    max_triangles: int = 10_000


class ObjectBrief(Component):
    """AI-ready object description with explicit exclusions."""

    prompt: str
    negative_prompt: str


class ModelSocket(Component):
    """Required named transform in the generated GLB node hierarchy."""

    name: str
    role: str


class PresentationState(Component):
    """One precedence-ordered semantic signal to visual-state binding."""

    signal: str
    visual_state: str
    animation_clip: str
    priority: int


class InteractionBinding(Component):
    """One UI action; authorization remains at the named application boundary."""

    name: str
    permission: str
    action: str
    confirmation_required: bool = False

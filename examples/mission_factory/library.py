# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Author mission-factory visuals and ``BugFixLine`` as durable ECS content."""

from __future__ import annotations

from dataclasses import dataclass

from archetype.core.component import Component
from archetype.graph import ChildOf, Prefab, WorldLike, link

from .components import (
    AssetPart,
    BehaviorBinding,
    BlueprintSlot,
    ConnectionRule,
    FactoryAsset,
    InteractionBinding,
    ModelSocket,
    ObjectBrief,
    PresentationState,
    TaskRecipe,
    ValidatorRecipe,
    VisualGeometry,
)
from .relations import AssetChildOf
from .specs import (
    BUGFIX_NODES,
    BUGFIX_RULES,
    BUGFIX_TASKS,
    BUGFIX_VALIDATORS,
    NEGATIVE_PROMPT,
    VISUAL_ASSETS,
    VisualAssetSpec,
)


@dataclass(frozen=True, slots=True)
class MissionFactoryLibrary:
    """Stable ids returned for one authored asset catalog."""

    root: int
    structures: int
    units: int
    logistics: int
    lines: int
    visual_assets: tuple[tuple[str, int], ...]
    bugfix_line: int

    def visual_asset(self, key: str) -> int:
        """Resolve one visual asset id by its stable key."""

        try:
            return dict(self.visual_assets)[key]
        except KeyError as exc:
            raise KeyError(f"mission factory has no visual asset {key!r}") from exc


async def _child(
    world: WorldLike,
    parent: int,
    *,
    key: str,
    kind: str,
    order: int,
    components: tuple[Component, ...],
) -> int:
    entity = await world.spawn(
        Prefab(name=key),
        AssetPart(key=key, kind=kind, order=order),
        *components,
    )
    await link(world, ChildOf(source=entity, target=parent))
    return entity


async def _author_visual_asset(
    world: WorldLike,
    collection: int,
    spec: VisualAssetSpec,
) -> int:
    width, depth, height = spec.dimensions_m
    asset = await world.spawn(
        Prefab(name=spec.key),
        FactoryAsset(
            key=spec.key,
            display_name=spec.display_name,
            kind="visual",
        ),
        VisualGeometry(
            model_uri=f"assets/mission_factory/{spec.key}.glb",
            footprint_x=spec.footprint[0],
            footprint_y=spec.footprint[1],
            width_m=width,
            depth_m=depth,
            height_m=height,
            max_triangles=spec.max_triangles,
        ),
        ObjectBrief(prompt=spec.prompt, negative_prompt=NEGATIVE_PROMPT),
    )
    await link(world, AssetChildOf(source=asset, target=collection))

    order = 0
    for socket_name, socket_role in spec.sockets:
        order += 1
        await _child(
            world,
            asset,
            key=f"{spec.key}.socket.{socket_name}",
            kind="socket",
            order=order,
            components=(ModelSocket(name=socket_name, role=socket_role),),
        )
    for behavior in spec.behaviors:
        order += 1
        await _child(
            world,
            asset,
            key=f"{spec.key}.behavior.{order}",
            kind="behavior",
            order=order,
            components=(
                BehaviorBinding(
                    authority=behavior.authority,
                    observes=list(behavior.observes),
                    effect=behavior.effect,
                ),
            ),
        )
    for state in spec.states:
        order += 1
        await _child(
            world,
            asset,
            key=f"{spec.key}.state.{state.signal}",
            kind="state",
            order=order,
            components=(
                PresentationState(
                    signal=state.signal,
                    visual_state=state.visual_state,
                    animation_clip=state.animation_clip,
                    priority=state.priority,
                ),
            ),
        )
    for interaction in spec.interactions:
        order += 1
        await _child(
            world,
            asset,
            key=f"{spec.key}.interaction.{interaction.name}",
            kind="interaction",
            order=order,
            components=(
                InteractionBinding(
                    name=interaction.name,
                    permission=interaction.permission,
                    action=interaction.action,
                    confirmation_required=interaction.confirmation_required,
                ),
            ),
        )
    return asset


async def _author_bugfix_line(world: WorldLike, lines: int) -> int:
    line = await world.spawn(
        Prefab(name="bugfix-line"),
        FactoryAsset(
            key="bugfix_line",
            display_name="Bug Fix Line",
            kind="line",
        ),
    )
    await link(world, AssetChildOf(source=line, target=lines))

    tasks = {task.key: task for task in BUGFIX_TASKS}
    node_ids: dict[str, int] = {}
    for node in BUGFIX_NODES:
        components: list[Component] = [
            BlueprintSlot(
                key=node.key,
                role=node.role,
                visual_asset=node.visual_asset,
                order=node.order,
            )
        ]
        task = tasks.get(node.key)
        if task is not None:
            components.append(
                TaskRecipe(
                    prompt_template=task.prompt_template,
                    max_dispatches=task.max_dispatches,
                    critic_max_reviews=task.critic_max_reviews,
                    critic_timeout_seconds=task.critic_timeout_seconds,
                )
            )
        node_ids[node.key] = await _child(
            world,
            line,
            key=f"bugfix-line.slot.{node.key}",
            kind=node.role,
            order=node.order,
            components=tuple(components),
        )

    for order, validator in enumerate(BUGFIX_VALIDATORS, start=1):
        await _child(
            world,
            node_ids[validator.task_key],
            key=f"bugfix-line.validator.{validator.key}",
            kind="validator",
            order=order,
            components=(
                BlueprintSlot(
                    key=validator.key,
                    role="validator",
                    visual_asset="validator_gate",
                    order=order,
                ),
                ValidatorRecipe(
                    name=validator.name,
                    command=list(validator.command),
                    expected_returncode=validator.expected_returncode,
                    timeout_seconds=validator.timeout_seconds,
                ),
            ),
        )

    for order, rule in enumerate(BUGFIX_RULES, start=1):
        await _child(
            world,
            line,
            key=f"bugfix-line.rule.{order}",
            kind="rule",
            order=100 + order,
            components=(
                ConnectionRule(
                    relation=rule.relation,
                    source_slot=rule.source_slot,
                    target_slot=rule.target_slot,
                ),
            ),
        )
    return line


async def author_mission_factory_library(world: WorldLike) -> MissionFactoryLibrary:
    """Stage the nine-object visual kit and one reusable ``BugFixLine`` prefab."""

    root = await world.spawn(
        FactoryAsset(
            key="mission_factory",
            display_name="Mission Factory",
            kind="library",
        )
    )
    collections: dict[str, int] = {}
    for key in ("structures", "units", "logistics", "lines"):
        collection = await world.spawn(
            FactoryAsset(
                key=key,
                display_name=key.replace("_", " ").title(),
                kind="collection",
            )
        )
        collections[key] = collection
        await link(world, AssetChildOf(source=collection, target=root))

    visual_assets: list[tuple[str, int]] = []
    for spec in VISUAL_ASSETS:
        asset = await _author_visual_asset(world, collections[spec.collection], spec)
        visual_assets.append((spec.key, asset))

    bugfix_line = await _author_bugfix_line(world, collections["lines"])
    return MissionFactoryLibrary(
        root=root,
        structures=collections["structures"],
        units=collections["units"],
        logistics=collections["logistics"],
        lines=collections["lines"],
        visual_assets=tuple(visual_assets),
        bugfix_line=bugfix_line,
    )

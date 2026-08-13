# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Interpret trusted mission-line rule entities at the authoring boundary."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, cast

from daft import DataFrame, Expression, col

from archetype.graph import ChildOf, WorldLike, edges
from archetype.missions import (
    AgentTask,
    CommandValidator,
    CriticPolicy,
    MissionSubmission,
    RepositoryPublicationPolicy,
)

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
from .contracts import BugFixLineInputs


def _rows_at(
    frame: DataFrame,
    at: int,
    entity_ids: set[int] | None = None,
) -> list[dict[str, Any]]:
    """Materialize trusted authoring rows once at the compiler boundary."""

    planned = frame.where(cast(Expression, col("tick") == at))
    if entity_ids is not None:
        planned = planned.where(cast(Expression, col("entity_id").is_in(entity_ids)))
    return planned.to_pylist()


async def _subtree(world: WorldLike, root: int, at: int) -> tuple[set[int], dict[int, int]]:
    relation = ChildOf.get_prefix()
    edge_rows = (
        (await edges(world, ChildOf, at=at))
        .select(f"{relation}source", f"{relation}target")
        .to_pylist()
    )
    children: dict[int, list[int]] = defaultdict(list)
    parent_by_child: dict[int, int] = {}
    for row in edge_rows:
        source = int(row[f"{relation}source"])
        target = int(row[f"{relation}target"])
        children[target].append(source)
        parent_by_child[source] = target

    found = {root}
    pending = [root]
    while pending:
        parent = pending.pop()
        for child in children.get(parent, []):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found, parent_by_child


def _render(value: str, inputs: BugFixLineInputs) -> str:
    try:
        return value.format(
            issue=inputs.issue,
            test_path=inputs.test_path,
            base_ref=inputs.base_ref,
        )
    except (IndexError, KeyError, ValueError) as exc:
        raise ValueError(f"unsupported BugFixLine template {value!r}") from exc


async def compile_bugfix_line(
    world: WorldLike,
    blueprint_root: int,
    inputs: BugFixLineInputs,
    *,
    at: int,
) -> MissionSubmission:
    """Compile one copied blueprint subtree into supported Agent Missions values.

    Generic prefab instantiation copies the ``ChildOf`` subtree and component
    values only.  This trusted example driver resolves stable slot names and
    explicitly interprets the allowlisted ``DependsOn`` and ``Guards`` rule
    entities.  It never broadens generic relation-copy behavior.
    """

    subtree, _parents = await _subtree(world, blueprint_root, at)
    asset_rows = _rows_at(await world.query(FactoryAsset), at, {blueprint_root})
    if len(asset_rows) != 1 or asset_rows[0]["factoryasset__key"] != "bugfix_line":
        raise ValueError("blueprint root is not a BugFixLine instance")

    slot_rows = _rows_at(await world.query(BlueprintSlot), at, subtree)
    slots = {row["blueprintslot__key"]: int(row["entity_id"]) for row in slot_rows}
    if len(slots) != len(slot_rows):
        raise ValueError("BugFixLine contains duplicate stable slot keys")

    task_rows = _rows_at(await world.query(BlueprintSlot, TaskRecipe), at, subtree)
    validator_rows = _rows_at(await world.query(BlueprintSlot, ValidatorRecipe), at, subtree)
    rule_rows = _rows_at(await world.query(ConnectionRule), at, subtree)

    dependencies: dict[str, list[str]] = defaultdict(list)
    validators_by_task: dict[str, list[dict[str, Any]]] = defaultdict(list)
    validators_by_slot = {row["blueprintslot__key"]: row for row in validator_rows}
    task_keys = {row["blueprintslot__key"] for row in task_rows}

    for row in rule_rows:
        relation = row["connectionrule__relation"]
        source = row["connectionrule__source_slot"]
        target = row["connectionrule__target_slot"]
        if source not in slots or target not in slots:
            raise ValueError(f"{relation} rule references an unknown blueprint slot")
        if relation == "DependsOn":
            if source not in task_keys or target not in task_keys:
                raise ValueError("DependsOn rules must connect task slots")
            dependencies[source].append(target)
        elif relation == "Guards":
            validator = validators_by_slot.get(source)
            if validator is None or target not in task_keys:
                raise ValueError("Guards rules must connect validator and task slots")
            validators_by_task[target].append(validator)
        else:
            raise ValueError(f"unsupported mission-factory relation {relation!r}")

    tasks: list[AgentTask] = []
    for row in sorted(task_rows, key=lambda item: item["blueprintslot__order"]):
        key = row["blueprintslot__key"]
        validator_values = sorted(
            validators_by_task[key],
            key=lambda item: item["blueprintslot__order"],
        )
        if not validator_values:
            raise ValueError(f"task slot {key!r} has no Guards validators")
        validators = tuple(
            CommandValidator(
                name=validator["validatorrecipe__name"],
                command=tuple(
                    _render(argument, inputs) for argument in validator["validatorrecipe__command"]
                ),
                expected_returncode=validator["validatorrecipe__expected_returncode"],
                timeout_seconds=validator["validatorrecipe__timeout_seconds"],
            )
            for validator in validator_values
        )
        tasks.append(
            AgentTask(
                name=key,
                prompt=_render(row["taskrecipe__prompt_template"], inputs),
                validators=validators,
                depends_on=tuple(sorted(dependencies[key])),
                max_dispatches=row["taskrecipe__max_dispatches"],
                publication_policy=RepositoryPublicationPolicy(
                    row["taskrecipe__publication_policy"]
                ),
                critic_policy=CriticPolicy(
                    max_reviews=row["taskrecipe__critic_max_reviews"],
                    timeout_seconds=row["taskrecipe__critic_timeout_seconds"],
                ),
            )
        )

    return MissionSubmission(
        repository=inputs.repository,
        branch=inputs.branch,
        base_ref=inputs.base_ref,
        name=inputs.name,
        tasks=tuple(tasks),
    )


async def export_visual_briefs(world: WorldLike, *, at: int) -> list[dict[str, Any]]:
    """Export AI-ready briefs from the same committed ECS rows used by the library."""

    assets = _rows_at(
        await world.query(FactoryAsset, VisualGeometry, ObjectBrief),
        at,
    )
    asset_ids = {int(row["entity_id"]) for row in assets}
    _subtree_ids, parent_by_child = await _subtree_for_roots(world, asset_ids, at)

    sockets = _rows_at(await world.query(AssetPart, ModelSocket), at)
    behaviors = _rows_at(await world.query(AssetPart, BehaviorBinding), at)
    states = _rows_at(await world.query(AssetPart, PresentationState), at)
    interactions = _rows_at(await world.query(AssetPart, InteractionBinding), at)

    def children_for(
        rows: list[dict[str, Any]],
        parent: int,
    ) -> list[dict[str, Any]]:
        return sorted(
            (row for row in rows if parent_by_child.get(int(row["entity_id"])) == parent),
            key=lambda item: item["assetpart__order"],
        )

    exported: list[dict[str, Any]] = []
    for row in sorted(assets, key=lambda item: item["factoryasset__key"]):
        entity_id = int(row["entity_id"])
        exported.append(
            {
                "key": row["factoryasset__key"],
                "display_name": row["factoryasset__display_name"],
                "model": {
                    "uri": row["visualgeometry__model_uri"],
                    "status": row["visualgeometry__model_status"],
                    "format": row["visualgeometry__format"],
                    "coordinate_system": row["visualgeometry__coordinate_system"],
                    "origin": row["visualgeometry__origin"],
                    "footprint": [
                        row["visualgeometry__footprint_x"],
                        row["visualgeometry__footprint_y"],
                    ],
                    "dimensions_m": [
                        row["visualgeometry__width_m"],
                        row["visualgeometry__depth_m"],
                        row["visualgeometry__height_m"],
                    ],
                    "max_triangles": row["visualgeometry__max_triangles"],
                },
                "prompt": row["objectbrief__prompt"],
                "negative_prompt": row["objectbrief__negative_prompt"],
                "sockets": [
                    {"name": child["modelsocket__name"], "role": child["modelsocket__role"]}
                    for child in children_for(sockets, entity_id)
                ],
                "behaviors": [
                    {
                        "authority": child["behaviorbinding__authority"],
                        "observes": child["behaviorbinding__observes"],
                        "effect": child["behaviorbinding__effect"],
                    }
                    for child in children_for(behaviors, entity_id)
                ],
                "states": [
                    {
                        "signal": child["presentationstate__signal"],
                        "visual_state": child["presentationstate__visual_state"],
                        "animation_clip": child["presentationstate__animation_clip"],
                        "priority": child["presentationstate__priority"],
                    }
                    for child in children_for(states, entity_id)
                ],
                "interactions": [
                    {
                        "name": child["interactionbinding__name"],
                        "permission": child["interactionbinding__permission"],
                        "action": child["interactionbinding__action"],
                        "confirmation_required": child["interactionbinding__confirmation_required"],
                    }
                    for child in children_for(interactions, entity_id)
                ],
            }
        )
    return exported


async def _subtree_for_roots(
    world: WorldLike,
    roots: set[int],
    at: int,
) -> tuple[set[int], dict[int, int]]:
    """Return all copied visual children and their direct parents."""

    relation = ChildOf.get_prefix()
    edge_rows = (
        (await edges(world, ChildOf, at=at))
        .select(f"{relation}source", f"{relation}target")
        .to_pylist()
    )
    parent_by_child = {
        int(row[f"{relation}source"]): int(row[f"{relation}target"]) for row in edge_rows
    }
    children: dict[int, list[int]] = defaultdict(list)
    for child, parent in parent_by_child.items():
        children[parent].append(child)
    found = set(roots)
    pending = list(roots)
    while pending:
        for child in children.get(pending.pop(), []):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found, parent_by_child

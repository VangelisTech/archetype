# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Agent Missions read projections: mission list, task DAG, task card."""

from typing import Any, cast

from daft import DataFrame, Expression, col
from fastapi import APIRouter, Depends

from archetype.api.deps import get_actor_ctx, get_command_gateway
from archetype.api.errors import raise_api_error
from archetype.api.models import dataframe_to_rows
from archetype.app.gateway.auth.models import ActorCtx
from archetype.app.gateway.interfaces import iCommandGateway
from archetype.core.component import Component
from archetype.missions.components import (
    AgentExecution,
    Mission,
    MissionState,
    Task,
    TaskDispatch,
    TaskPolicy,
    TaskState,
    TaskValidator,
    ValidationResult,
)
from archetype.missions.relations import DependsOn, Guards, PartOfMission

router = APIRouter(tags=["missions"])

_TASK_TYPES: list[type[Component]] = [Task, TaskState, TaskDispatch, TaskPolicy]


async def _components_frame(
    cs: iCommandGateway,
    ctx: ActorCtx,
    world_id: str,
    component_types: list[type[Component]],
    *,
    entity_ids: list[int] | None = None,
) -> DataFrame | None:
    try:
        info = await cs.get_world_info(ctx, world_id)
        query_world_id, run_id = str(info.world_id), str(info.run_id or "")
    except KeyError:
        query_world_id, run_id = str(world_id), ""
    return await cs.query_components(
        ctx,
        component_types,
        query_world_id,
        run_id,
        entity_ids=entity_ids,
    )


def _edge_rows(
    frame: DataFrame | None,
    relation: type[Component],
    *,
    where: Expression | None = None,
) -> list[dict[str, Any]]:
    """Serialize relation rows to unprefixed {source, target} pairs."""
    if frame is None:
        return []
    if where is not None:
        frame = frame.where(where)
    prefix = relation.get_prefix()
    rows = dataframe_to_rows(frame)
    return [{"source": row[f"{prefix}source"], "target": row[f"{prefix}target"]} for row in rows]


@router.get("/worlds/{world_id}/missions", response_model=list[dict[str, Any]])
async def list_missions(
    world_id: str,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """List mission entities with their current rollup state. Requires viewer or above."""
    try:
        frame = await _components_frame(cs, ctx, world_id, [Mission, MissionState])
        return [] if frame is None else dataframe_to_rows(frame)
    except Exception as exc:
        raise_api_error(exc)


@router.get(
    "/worlds/{world_id}/missions/{mission_id}/tasks",
    response_model=dict[str, Any],
)
async def get_mission_tasks(
    world_id: str,
    mission_id: int,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Project one mission's task DAG: task rows plus DependsOn edges. Requires viewer or above."""
    try:
        membership = await _components_frame(cs, ctx, world_id, [PartOfMission])
        edges = _edge_rows(
            membership,
            PartOfMission,
            where=cast(Expression, col(f"{PartOfMission.get_prefix()}target") == mission_id),
        )
        task_ids = [edge["source"] for edge in edges]
        if not task_ids:
            return {"mission_id": mission_id, "tasks": [], "depends_on": []}

        task_frame = await _components_frame(cs, ctx, world_id, _TASK_TYPES, entity_ids=task_ids)
        dependency_frame = await _components_frame(cs, ctx, world_id, [DependsOn])
        depends_on = _edge_rows(
            dependency_frame,
            DependsOn,
            where=cast(
                Expression,
                col(f"{DependsOn.get_prefix()}source").is_in(task_ids),
            ),
        )
        return {
            "mission_id": mission_id,
            "tasks": [] if task_frame is None else dataframe_to_rows(task_frame),
            "depends_on": depends_on,
        }
    except Exception as exc:
        raise_api_error(exc)


@router.get("/worlds/{world_id}/tasks/{task_id}", response_model=dict[str, Any])
async def get_task_card(
    world_id: str,
    task_id: int,
    cs: iCommandGateway = Depends(get_command_gateway),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Project one task card: task state, guarding validators, executions, and
    validation results. Requires viewer or above."""
    try:
        task_frame = await _components_frame(cs, ctx, world_id, _TASK_TYPES, entity_ids=[task_id])
        task_rows = [] if task_frame is None else dataframe_to_rows(task_frame)
        if not task_rows:
            raise KeyError(f"task {task_id} not found")

        guard_frame = await _components_frame(cs, ctx, world_id, [Guards])
        guard_edges = _edge_rows(
            guard_frame,
            Guards,
            where=cast(Expression, col(f"{Guards.get_prefix()}target") == task_id),
        )
        validator_ids = [edge["source"] for edge in guard_edges]
        validators: list[dict[str, Any]] = []
        if validator_ids:
            validator_frame = await _components_frame(
                cs, ctx, world_id, [TaskValidator], entity_ids=validator_ids
            )
            if validator_frame is not None:
                validators = dataframe_to_rows(validator_frame)

        executions: list[dict[str, Any]] = []
        execution_frame = await _components_frame(cs, ctx, world_id, [AgentExecution])
        if execution_frame is not None:
            executions = dataframe_to_rows(
                execution_frame.where(
                    cast(
                        Expression,
                        col(f"{AgentExecution.get_prefix()}task_id") == task_id,
                    )
                )
            )

        validations: list[dict[str, Any]] = []
        validation_frame = await _components_frame(cs, ctx, world_id, [ValidationResult])
        if validation_frame is not None:
            validations = dataframe_to_rows(
                validation_frame.where(
                    cast(
                        Expression,
                        col(f"{ValidationResult.get_prefix()}task_id") == task_id,
                    )
                )
            )

        return {
            "task": task_rows,
            "validators": validators,
            "executions": executions,
            "validations": validations,
        }
    except Exception as exc:
        raise_api_error(exc)

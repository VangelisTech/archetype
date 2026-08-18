# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Optional Agent Missions read-projection and mission-control routers."""

from typing import Annotated, Any, Literal, cast

from daft import DataFrame, Expression, col
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict

from archetype.api.deps import get_actor_ctx, get_dispatcher, get_mission_principal
from archetype.api.errors import raise_api_error
from archetype.api.models import dataframe_to_rows
from archetype.api.principals import MissionPrincipal
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx
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
from archetype.missions.control import MissionControlCatalog, MissionRunPin
from archetype.missions.execution_profiles import MissionProfileRequest
from archetype.missions.relations import DependsOn, Guards, PartOfMission
from archetype.world.models import ComponentTypeRef, GetWorldInfo, QueryComponents

_TASK_TYPES: list[type[Component]] = [Task, TaskState, TaskDispatch, TaskPolicy]
_MISSION_CONTROL_CAPABILITY = "missions:control"


class _FrozenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MissionControlSubmitRequest(_FrozenRequest):
    """Client-owned mission-control coordinates. Host execution stays off this body."""

    profile_id: str
    repository: str
    branch: str
    base_ref: str = "main"
    name: str = "agent-mission"


class MissionRunPinResponse(_FrozenRequest):
    """Bounded accepted-run projection with no host secrets or credentials."""

    run_id: str
    owner_principal_id: str
    profile_id: str
    profile_version: str
    profile_digest: str
    state: Literal["accepted"] = "accepted"


def _pin_response(pin: MissionRunPin) -> MissionRunPinResponse:
    return MissionRunPinResponse(
        run_id=pin.run_id,
        owner_principal_id=pin.owner_principal_id,
        profile_id=pin.profile_id,
        profile_version=pin.profile_version,
        profile_digest=pin.profile_digest,
    )


def get_mission_control(request: Request) -> MissionControlCatalog:
    """Return the host-composed mission-control catalog or fail closed."""

    resources = getattr(request.app.state, "resources", None)
    lookup = getattr(resources, "host_capability", None)
    if not callable(lookup):
        raise HTTPException(status_code=401, detail="Authentication required")
    try:
        catalog = lookup(_MISSION_CONTROL_CAPABILITY)
    except (KeyError, ValueError, RuntimeError):
        raise HTTPException(status_code=401, detail="Authentication required") from None
    if not isinstance(catalog, MissionControlCatalog):
        raise HTTPException(status_code=401, detail="Authentication required")
    return catalog


async def _components_frame(
    dispatcher: CommandDispatcher,
    ctx: ActorCtx,
    world_id: str,
    component_types: list[type[Component]],
    *,
    entity_ids: list[int] | None = None,
) -> DataFrame | None:
    try:
        info = await dispatcher.apply_as(ctx, GetWorldInfo(world_id=world_id))
        query_world_id, run_id = str(info.world_id), str(info.run_id or "")
    except KeyError:
        query_world_id, run_id = str(world_id), ""
    try:
        return await dispatcher.apply_as(
            ctx,
            QueryComponents(
                components=tuple(
                    ComponentTypeRef.from_type(component_type) for component_type in component_types
                ),
                world_id=query_world_id,
                run_id=run_id,
                entity_ids=tuple(entity_ids) if entity_ids is not None else None,
            ),
        )
    except KeyError:
        # A world that has never spawned this component table is a normal
        # state (no DependsOn edges yet, no executions yet): read as empty.
        return None


def _edge_rows(
    frame: DataFrame | None,
    relation: type[Component],
    *,
    where: Expression | None = None,
) -> list[dict[str, Any]]:
    """Serialize relation rows to unprefixed ``source``/``target`` pairs."""

    if frame is None:
        return []
    if where is not None:
        frame = frame.where(where)
    prefix = relation.get_prefix()
    rows = dataframe_to_rows(frame)
    return [{"source": row[f"{prefix}source"], "target": row[f"{prefix}target"]} for row in rows]


async def list_missions(
    world_id: str,
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    ctx: Annotated[ActorCtx, Depends(get_actor_ctx)],
):
    """List mission entities with their current rollup state."""

    try:
        frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [Mission, MissionState],
        )
        return [] if frame is None else dataframe_to_rows(frame)
    except Exception as exc:
        raise_api_error(exc)


async def get_mission_tasks(
    world_id: str,
    mission_id: int,
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    ctx: Annotated[ActorCtx, Depends(get_actor_ctx)],
):
    """Project one mission's task DAG and dependency edges."""

    try:
        membership = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [PartOfMission],
        )
        edges = _edge_rows(
            membership,
            PartOfMission,
            where=cast(Expression, col(f"{PartOfMission.get_prefix()}target") == mission_id),
        )
        task_ids = [edge["source"] for edge in edges]
        if not task_ids:
            return {"mission_id": mission_id, "tasks": [], "depends_on": []}

        task_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            _TASK_TYPES,
            entity_ids=task_ids,
        )
        dependency_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [DependsOn],
        )
        depends_on = _edge_rows(
            dependency_frame,
            DependsOn,
            where=col(f"{DependsOn.get_prefix()}source").is_in(task_ids),
        )
        return {
            "mission_id": mission_id,
            "tasks": [] if task_frame is None else dataframe_to_rows(task_frame),
            "depends_on": depends_on,
        }
    except Exception as exc:
        raise_api_error(exc)


async def get_task_card(
    world_id: str,
    task_id: int,
    dispatcher: Annotated[CommandDispatcher, Depends(get_dispatcher)],
    ctx: Annotated[ActorCtx, Depends(get_actor_ctx)],
):
    """Project one task's state, validators, executions, and results."""

    try:
        task_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            _TASK_TYPES,
            entity_ids=[task_id],
        )
        task_rows = [] if task_frame is None else dataframe_to_rows(task_frame)
        if not task_rows:
            raise KeyError(f"task {task_id} not found")

        guard_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [Guards],
        )
        guard_edges = _edge_rows(
            guard_frame,
            Guards,
            where=cast(Expression, col(f"{Guards.get_prefix()}target") == task_id),
        )
        validator_ids = [edge["source"] for edge in guard_edges]
        validators: list[dict[str, Any]] = []
        if validator_ids:
            validator_frame = await _components_frame(
                dispatcher,
                ctx,
                world_id,
                [TaskValidator],
                entity_ids=validator_ids,
            )
            if validator_frame is not None:
                validators = dataframe_to_rows(validator_frame)

        executions: list[dict[str, Any]] = []
        execution_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [AgentExecution],
        )
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
        validation_frame = await _components_frame(
            dispatcher,
            ctx,
            world_id,
            [ValidationResult],
        )
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


async def submit_mission_control_run(
    req: MissionControlSubmitRequest,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    catalog: Annotated[MissionControlCatalog, Depends(get_mission_control)],
) -> MissionRunPinResponse:
    """Accept a profile-bound mission run pin for a verified principal."""

    try:
        pin = catalog.submit(
            principal,
            MissionProfileRequest(
                profile_id=req.profile_id,
                repository=req.repository,
                branch=req.branch,
                base_ref=req.base_ref,
            ),
        )
        return _pin_response(pin)
    except Exception as exc:
        raise_api_error(exc)


async def get_mission_control_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    catalog: Annotated[MissionControlCatalog, Depends(get_mission_control)],
) -> MissionRunPinResponse:
    """Read one accepted run pin the principal is allowed to see."""

    try:
        return _pin_response(catalog.pin_for(principal, run_id))
    except Exception as exc:
        raise_api_error(exc)


async def cancel_mission_control_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    catalog: Annotated[MissionControlCatalog, Depends(get_mission_control)],
) -> MissionRunPinResponse:
    """Authorize cancellation for one accepted run pin."""

    try:
        return _pin_response(catalog.cancel(principal, run_id))
    except Exception as exc:
        raise_api_error(exc)


async def attach_mission_control_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    catalog: Annotated[MissionControlCatalog, Depends(get_mission_control)],
) -> MissionRunPinResponse:
    """Authorize first-person attachment for one accepted run pin."""

    try:
        return _pin_response(catalog.attach(principal, run_id))
    except Exception as exc:
        raise_api_error(exc)


async def steer_mission_control_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    catalog: Annotated[MissionControlCatalog, Depends(get_mission_control)],
) -> MissionRunPinResponse:
    """Authorize steering for one accepted run pin."""

    try:
        return _pin_response(catalog.steer(principal, run_id))
    except Exception as exc:
        raise_api_error(exc)


async def takeover_mission_control_run(
    run_id: str,
    principal: Annotated[MissionPrincipal, Depends(get_mission_principal)],
    catalog: Annotated[MissionControlCatalog, Depends(get_mission_control)],
) -> MissionRunPinResponse:
    """Authorize writable takeover for one accepted run pin."""

    try:
        return _pin_response(catalog.takeover(principal, run_id))
    except Exception as exc:
        raise_api_error(exc)


def create_router() -> APIRouter:
    """Create one fresh Missions router for an API host installation."""

    router = APIRouter(tags=["missions"])
    router.add_api_route(
        "/worlds/{world_id}/missions",
        list_missions,
        methods=["GET"],
        response_model=list[dict[str, Any]],
    )
    router.add_api_route(
        "/worlds/{world_id}/missions/{mission_id}/tasks",
        get_mission_tasks,
        methods=["GET"],
        response_model=dict[str, Any],
    )
    router.add_api_route(
        "/worlds/{world_id}/tasks/{task_id}",
        get_task_card,
        methods=["GET"],
        response_model=dict[str, Any],
    )
    router.add_api_route(
        "/v1/mission-control/runs",
        submit_mission_control_run,
        methods=["POST"],
        response_model=MissionRunPinResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    router.add_api_route(
        "/v1/mission-control/runs/{run_id}",
        get_mission_control_run,
        methods=["GET"],
        response_model=MissionRunPinResponse,
    )
    router.add_api_route(
        "/v1/mission-control/runs/{run_id}/cancel",
        cancel_mission_control_run,
        methods=["POST"],
        response_model=MissionRunPinResponse,
    )
    router.add_api_route(
        "/v1/mission-control/runs/{run_id}/attach",
        attach_mission_control_run,
        methods=["POST"],
        response_model=MissionRunPinResponse,
    )
    router.add_api_route(
        "/v1/mission-control/runs/{run_id}/steer",
        steer_mission_control_run,
        methods=["POST"],
        response_model=MissionRunPinResponse,
    )
    router.add_api_route(
        "/v1/mission-control/runs/{run_id}/takeover",
        takeover_mission_control_run,
        methods=["POST"],
        response_model=MissionRunPinResponse,
    )
    return router


# Compatibility for direct imports during the 0.6 migration. API hosting uses
# the manifest factory so each host receives its own router instance.
router = create_router()


__all__ = ["create_router", "router"]

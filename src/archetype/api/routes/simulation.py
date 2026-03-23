# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Simulation routes."""

from fastapi import APIRouter, Depends, HTTPException
from uuid_utils import UUID

from archetype.api.deps import get_simulation_service
from archetype.api.models import RunRequest, RunResultResponse, StepRequest
from archetype.app.simulation_service import SimulationService
from archetype.core.config import RunConfig

router = APIRouter(prefix="/worlds/{world_id}", tags=["simulation"])


@router.post("/step")
async def step_world(
    world_id: str,
    req: StepRequest | None = None,
    sim: SimulationService = Depends(get_simulation_service),
):
    if req is None:
        req = StepRequest()
    try:
        run_config = RunConfig(num_steps=1, debug=req.debug)
        commands_applied = await sim.step(UUID(world_id), run_config)
        return {"world_id": world_id, "commands_applied": commands_applied}
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.post("/run", response_model=RunResultResponse)
async def run_world(
    world_id: str,
    req: RunRequest,
    sim: SimulationService = Depends(get_simulation_service),
):
    try:
        run_config = RunConfig(num_steps=req.num_steps, debug=req.debug)
        result = await sim.run(UUID(world_id), run_config)
        return RunResultResponse(
            run_id=str(result.run_id),
            world_id=str(result.world_id),
            ticks_completed=result.ticks_completed,
            commands_applied=result.commands_applied,
            final_tick=result.final_tick,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None


@router.get("/processors")
async def list_processors(
    world_id: str,
    sim: SimulationService = Depends(get_simulation_service),
):
    try:
        procs = sim.list_processors(UUID(world_id))
        return [p.model_dump() for p in procs]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"World {world_id} not found") from None

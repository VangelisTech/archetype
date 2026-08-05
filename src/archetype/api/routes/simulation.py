# Copyright 2025 Vangelis Technologies Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simulation routes."""

from fastapi import APIRouter, Depends

from archetype.api.deps import get_actor_ctx, get_dispatcher
from archetype.api.errors import raise_api_error
from archetype.api.models import (
    EpisodeConfig,
    RolloutConfig,
    RunRequest,
    RunResultResponse,
    StepRequest,
    StepResponse,
)
from archetype.commands.dispatch import CommandDispatcher
from archetype.commands.models import ActorCtx
from archetype.world.models import (
    EpisodeResult,
    RolloutResult,
    Run,
    RunEpisode,
    RunRollout,
    Step,
)

router = APIRouter(prefix="/worlds/{world_id}", tags=["simulation"])


@router.post("/step", response_model=StepResponse)
async def step_world(
    world_id: str,
    req: StepRequest | None = None,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Advance one tick. Requires operator or admin."""
    try:
        run_config = (req or StepRequest()).to_run_config()
        commands_applied = await dispatcher.apply_as(
            ctx,
            Step(world_id=world_id, run_config=run_config),
        )
        return StepResponse(commands_applied=commands_applied)
    except Exception as exc:
        raise_api_error(exc)


@router.post("/run", response_model=RunResultResponse)
async def run_world(
    world_id: str,
    req: RunRequest,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Run a bounded simulation. Requires operator or admin."""
    try:
        result = await dispatcher.apply_as(
            ctx,
            Run(world_id=world_id, run_config=req.to_run_config()),
        )
        return RunResultResponse(
            run_id=str(result.run_id),
            world_id=str(result.world_id),
            ticks_completed=result.ticks_completed,
            commands_applied=result.commands_applied,
            final_tick=result.final_tick,
        )
    except Exception as exc:
        raise_api_error(exc)


@router.post("/episode", response_model=EpisodeResult)
async def run_episode(
    world_id: str,
    config: EpisodeConfig,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Run one episode. Requires operator or admin."""
    try:
        return await dispatcher.apply_as(
            ctx,
            RunEpisode(world_id=world_id, config=config),
        )
    except Exception as exc:
        raise_api_error(exc)


@router.post("/rollout", response_model=RolloutResult)
async def run_rollout(
    world_id: str,
    config: RolloutConfig,
    dispatcher: CommandDispatcher = Depends(get_dispatcher),
    ctx: ActorCtx = Depends(get_actor_ctx),
):
    """Run a rollout. Requires operator or admin; emits one rollout audit row."""
    try:
        return await dispatcher.apply_as(
            ctx,
            RunRollout(world_id=world_id, config=config),
        )
    except Exception as exc:
        raise_api_error(exc)

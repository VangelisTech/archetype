# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Temporal Worker composition for Agent Missions."""

from __future__ import annotations

from temporalio.client import Client
from temporalio.worker import Worker

from archetype.missions.run_supervisor import MissionRunExecutor
from archetype.orchestration.temporal import create_temporal_worker

from .activities import MissionActivities
from .contracts import MISSION_TASK_QUEUE
from .modal_job_activities import (
    MissionModalJobActivities,
    MissionModalJobService,
    MissionModalJobValueStore,
)
from .modal_job_workflow import MissionModalJobWorkflow
from .workflow import MissionWorkflow


def create_mission_worker(
    client: Client,
    executor: MissionRunExecutor,
    *,
    task_queue: str = MISSION_TASK_QUEUE,
) -> Worker:
    """Build a Worker without introducing a second orchestration authority."""

    activities = MissionActivities(executor)
    return create_temporal_worker(
        client,
        task_queue=task_queue,
        workflows=[MissionWorkflow],
        activities=[activities.submit, activities.execute],
        # Import Archetype once in the host interpreter.  The Workflow itself
        # only uses the JSON-native temporal contracts, while re-importing the
        # package tree inside the sandbox would execute Daft's UDF registration
        # side effects and violate deterministic-import restrictions.
        passthrough_modules=("archetype",),
    )


def create_mission_modal_job_worker(
    client: Client,
    jobs: MissionModalJobService,
    values: MissionModalJobValueStore,
    *,
    task_queue: str = MISSION_TASK_QUEUE,
) -> Worker:
    """Build the provider-job Worker without exposing callback-bearing seams."""

    activities = MissionModalJobActivities(jobs, values)
    return create_temporal_worker(
        client,
        task_queue=task_queue,
        workflows=[MissionModalJobWorkflow],
        activities=[
            activities.start,
            activities.poll,
            activities.collect,
            activities.cancel,
            activities.cleanup,
        ],
        passthrough_modules=("archetype",),
    )


__all__ = ["create_mission_modal_job_worker", "create_mission_worker"]

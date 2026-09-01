# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Import-isolated Temporal Worker for durable Modal Mission jobs."""

from __future__ import annotations

from temporalio.client import Client
from temporalio.worker import Worker

from archetype.orchestration.temporal import create_temporal_worker

from .contracts import MISSION_MODAL_JOB_TASK_QUEUE
from .modal_job_activities import (
    MissionModalJobActivities,
    MissionModalJobService,
    MissionModalJobValueStore,
)
from .modal_job_workflow import MissionModalJobWorkflow


def create_mission_modal_job_worker(
    client: Client,
    jobs: MissionModalJobService,
    values: MissionModalJobValueStore,
    *,
    task_queue: str = MISSION_MODAL_JOB_TASK_QUEUE,
) -> Worker:
    """Build the split Worker without importing legacy Mission supervision."""

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


__all__ = ["create_mission_modal_job_worker"]

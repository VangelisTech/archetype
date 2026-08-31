# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Provider-neutral Temporal Worker composition."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions


def create_temporal_worker(
    client: Client,
    *,
    task_queue: str,
    workflows: Sequence[type[Any]],
    activities: Sequence[Callable[..., Any]],
    passthrough_modules: Sequence[str] = (),
) -> Worker:
    """Build a Worker for family-owned Workflows and Activity adapters.

    This helper configures execution only; it does not start or own the Worker.
    Concrete process lifetime remains a wiring concern.
    """

    task_queue = task_queue.strip()
    if not task_queue:
        raise ValueError("Temporal task_queue must not be empty")
    if not workflows:
        raise ValueError("Temporal Worker requires at least one Workflow")
    if not activities:
        raise ValueError("Temporal Worker requires at least one Activity")

    restrictions = SandboxRestrictions.default
    if passthrough_modules:
        restrictions = restrictions.with_passthrough_modules(*passthrough_modules)
    return Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        workflow_runner=SandboxedWorkflowRunner(restrictions=restrictions),
    )


__all__ = ["create_temporal_worker"]

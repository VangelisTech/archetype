# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Optional Temporal adapter for generic between-tick Activity delivery.

The adapter owns provider-neutral Workflow identity and Worker composition.
Owning families still define their Workflow state, Activities, and recovery
meaning, and concrete process lifetime remains in :mod:`archetype.wiring`.
"""

from archetype.activities.temporal.identity import durable_workflow_id
from archetype.activities.temporal.worker import create_temporal_worker

__all__ = ["create_temporal_worker", "durable_workflow_id"]

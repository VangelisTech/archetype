# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Shared Temporal infrastructure for family-owned Workflows.

The adapter owns provider-neutral Workflow identity and Worker composition.
Domain families still own their Workflow state, Activity contracts, and
recovery meaning. Concrete client and Worker lifetime remains an application
wiring concern.
"""

from archetype.orchestration.temporal.identity import durable_workflow_id
from archetype.orchestration.temporal.worker import create_temporal_worker

__all__ = ["create_temporal_worker", "durable_workflow_id"]

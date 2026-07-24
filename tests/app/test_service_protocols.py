# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Executable checks that family protocols are active, complete boundaries."""

from __future__ import annotations

import inspect

import pytest

from archetype.app.missions.interfaces import (
    iMissionService,
    iTrajectoryService,
    iTranscriptIngestionService,
)
from archetype.app.missions.service import MissionService
from archetype.app.missions.trajectory_service import TrajectoryService
from archetype.app.missions.transcript_service import TranscriptIngestionService
from archetype.storage.interfaces import iStorageService
from archetype.storage.service import StorageService
from archetype.world.interfaces import iWorldLifecycle, iWorldRegistry
from archetype.world.lifecycle import WorldLifecycle
from archetype.world.registry import WorldRegistry

pytestmark = pytest.mark.contract("architecture.protocols.complete")


SERVICE_PROTOCOLS = (
    (StorageService, iStorageService),
    (WorldRegistry, iWorldRegistry),
    (WorldLifecycle, iWorldLifecycle),
    (MissionService, iMissionService),
    (TranscriptIngestionService, iTranscriptIngestionService),
    (TrajectoryService, iTrajectoryService),
)


def _public_operations(cls: type) -> set[str]:
    return {
        name
        for name, member in inspect.getmembers(cls)
        if not name.startswith("_") and (inspect.isfunction(member) or inspect.ismethod(member))
    }


@pytest.mark.parametrize(("implementation", "protocol"), SERVICE_PROTOCOLS)
def test_family_protocol_covers_every_public_service_operation(implementation, protocol) -> None:
    missing = _public_operations(implementation) - _public_operations(protocol)
    assert not missing, f"{protocol.__name__} is missing {sorted(missing)}"

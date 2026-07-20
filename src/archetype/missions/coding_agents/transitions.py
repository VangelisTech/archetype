# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Small persisted decision vocabulary for coding-agent missions."""

from enum import StrEnum


class AgentMissionStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class AgentTaskStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    ACCEPTED = "accepted"
    FAILED = "failed"

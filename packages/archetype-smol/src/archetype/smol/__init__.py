# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""A tiny synchronous, in-memory DataFrame ECS for education."""

from .component import Component
from .processor import Processor
from .world import RunResult, World

__all__ = ["Component", "Processor", "RunResult", "World"]

__version__ = "0.6.3"

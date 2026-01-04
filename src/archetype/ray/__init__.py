# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Archetype Ray Actor Abstraction Layer
======================================

A Ray actor-based agent abstraction that enables state tracking via Archetype,
leveraging the core ECS engine as vectorized services for agents to use as needed.

This realizes a composite AI system where:
- Agents run as Ray actors (async, distributed)
- Inference and training services are vectorized via Archetype core
- State is tracked via the ECS DataFrame engine
- Requests are fully async with automatic batching

Architecture:
    RayAgent (actor) → RayAgentWorld → Vectorized Services (Archetype Core)
                    ↓
                State Tracking (ECS DataFrames)
"""

from archetype.ray.agent import RayAgent
from archetype.ray.world import RayAgentWorld
from archetype.ray.services import VectorizedService

__all__ = [
    "RayAgent",
    "RayAgentWorld",
    "VectorizedService",
]

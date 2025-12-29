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

# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Services Layer: API Surface for External Consumers
===================================================

Services are the stable API contract for external consumers:
- FastAPI routes
- MCP tools
- CLI commands
- Agents (recursive simulation)

All services delegate to the runtime and episodes layers.

Available Services:
- SimulationService: Multi-world execution, parameter sweeps, Monte Carlo
- CommandService: Command queue management (universal simulation interface)
- WorldService: World-level command execution

Example:
    from archetype.app.services import SimulationService, CommandService

    # Via container (recommended)
    container = ServiceContainer(storage_config)
    sim = container.simulation_service
    cmd = container.command_service

    # Run simulations
    world_ids = await sim.run_parallel_worlds(10, run_config)

    # Submit commands
    cmd_id = await cmd.create_entity_command(world_id, components)
"""

from archetype.app.command_service import CommandService
from archetype.app.simulation_service import SimulationService
from archetype.app.world_service import WorldService

__all__ = [
    "SimulationService",
    "CommandService",
    "WorldService",
]

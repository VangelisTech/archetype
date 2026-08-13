# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Run the example-local problem-definition agent mission."""

from __future__ import annotations

import asyncio

from problem_definition_mission.mission import main

if __name__ == "__main__":
    asyncio.run(main())

# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Module entry point: ``python -m archetype.missions.mcp``."""

from archetype.missions.mcp.server import main

if __name__ == "__main__":
    raise SystemExit(main())

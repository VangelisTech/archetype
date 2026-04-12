# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Chronicle
=========

Personal archive — unified timeline and search across chat histories,
photos, and videos.  Each artifact becomes an entity in a single
``chronicle`` world, queryable with the same tools as any simulation.

Modules:
    components  Components: Artifact, ChatMessage, Photo, Video
    loaders     Source-specific readers + ``ingest_all`` orchestrator
"""

from archetype.chronicle.components import Artifact, ChatMessage, Photo, Video

__all__ = ["Artifact", "ChatMessage", "Photo", "Video"]

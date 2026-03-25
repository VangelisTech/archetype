# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Memory ECS components."""

from archetype.core.component import Component


class MemoryChunk(Component):
    """A raw text fragment ingested into agent memory."""

    chunk_id: str = ""
    session_id: str = ""
    content: str = ""
    source: str = "unknown"        # "conversation" | "tool_output" | "observation"
    sig_json: str = "[]"           # JSON-encoded MinHash signature List[uint32]
    is_duplicate: bool = False
    canonical_id: str = ""         # chunk_id of the kept chunk if is_duplicate
    similarity_score: float = 0.0  # Jaccard estimate vs canonical if duplicate

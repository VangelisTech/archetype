# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""ECS Components for conversation memory extraction."""

from archetype.core.component import Component


class Segment(Component):
    """A single user message from a conversation."""

    role: str = ""  # "user" or "assistant"
    content: str = ""  # raw text
    session_id: str = ""
    timestamp: str = ""
    turn_index: int = 0  # position in conversation


class Extraction(Component):
    """What the LLM extracted as worth remembering."""

    memory_text: str = ""  # the extracted insight
    memory_type: str = ""  # user, feedback, project, reference
    confidence: float = 0.0
    is_memorable: str = ""  # "yes" or "no" — gate for downstream processors


class Voice(Component):
    """The voice/stance of the user in this segment."""

    classification: str = ""  # actor, observer, observed
    reasoning: str = ""


class Perspective(Component):
    """Ben Davies' perspectival lens on the extraction."""

    lens: str = ""  # objective, subjective, abjective, superjective
    reasoning: str = ""

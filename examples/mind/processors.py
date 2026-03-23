# Copyright 2025 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
LLM-powered processors for the memory extraction pipeline.

Pipeline: Extract → Voice → Perspective
Each processor is a parallel LLM call across all entities via daft.functions.prompt.
"""

from daft import DataFrame, col
from daft.functions import prompt

from archetype.core.aio.async_processor import AsyncProcessor

from .components import Extraction, Perspective, Segment, Voice

# ── System prompts ──

EXTRACT_SYSTEM = """You are analyzing a conversation between a user and an AI assistant.
Your job is to extract what is worth remembering about the USER — not the task, not the code, not the AI's behavior.

Focus on:
- User preferences (how they like to work, communicate, be treated)
- User knowledge and expertise (what they know, what's new to them)
- User values (what they care about, what frustrates them)
- User goals (what they're building toward, why)
- User corrections (when they push back on an approach)

If the message is purely transactional (e.g. "yes", "push it", "ok") with no deeper signal, respond with NOTHING.
If there IS something worth remembering, respond with a single clear sentence about the user.

Respond in exactly this format:
MEMORABLE: yes or no
TYPE: user, feedback, project, or reference
CONFIDENCE: 0.0 to 1.0
MEMORY: the extraction (or empty if not memorable)"""

VOICE_SYSTEM = """You are classifying the VOICE of a user in a conversation segment.

Three voices:
- ACTOR: The user is doing something, making something, deciding something. They are the agent of action. ("I want to build X", "push it", "let's use Y")
- OBSERVER: The user is watching, evaluating, or reflecting on something external. They are witnessing. ("That looks wrong", "check if X works", "walk me through the changes")
- OBSERVED: The user is revealing something about themselves, intentionally or not. They are the subject being disclosed. ("I've never published to PyPI before", "I'm the founder of X", "this is my dream")

Respond with exactly one word: actor, observer, or observed
Then a brief reason on the next line."""

PERSPECTIVE_SYSTEM = """You are applying Ben Davies' perspectival framework to a memory about a user.

Four lenses:
- OBJECTIVE: Facts about the world. What IS, independent of the user's feelings. ("Uses Python 3.12", "Founded Vangelis Technologies", "Project uses Daft DataFrames")
- SUBJECTIVE: The user's inner experience — feelings, preferences, values. What they FEEL. ("Prefers terse collaboration", "Values shipping over perfection", "Finds verbose explanations frustrating")
- ABJECTIVE: What the user rejects, avoids, or throws away. What they cast off. ("Rejected pip in favor of uv", "Removed the DSL module entirely", "Doesn't want summaries at end of responses")
- SUPERJECTIVE: What the user aims for beyond themselves — aspirations, purpose, legacy. What they reach toward. ("Building AI-native simulation for emergent systems", "Wants agents to use Archetype to improve Archetype", "Dreams of recursive self-improvement")

Given a memory extraction about a user, classify it into exactly one lens.
Respond with exactly one word: objective, subjective, abjective, or superjective
Then a brief reason on the next line."""


# ── Processors ──


class ExtractProcessor(AsyncProcessor):
    """Extract what's worth remembering from each user message."""

    components = (Segment, Extraction)
    priority = 1

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "extraction__memory_text": prompt(
                    "User message:\n" + col("segment__content"),
                    system_message=EXTRACT_SYSTEM,
                    model="gpt-5-mini",
                ),
            }
        )


class VoiceProcessor(AsyncProcessor):
    """Classify the voice of the user in each segment."""

    components = (Segment, Voice)
    priority = 10

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "voice__reasoning": prompt(
                    "User message:\n" + col("segment__content"),
                    system_message=VOICE_SYSTEM,
                    model="gpt-5-mini",
                ),
            }
        )


class PerspectiveProcessor(AsyncProcessor):
    """Bucket extractions through Davies' perspectival lens."""

    components = (Extraction, Perspective)
    priority = 20

    async def process(self, df: DataFrame, **kwargs) -> DataFrame:
        return df.with_columns(
            {
                "perspective__reasoning": prompt(
                    "Memory about a user:\n" + col("extraction__memory_text"),
                    system_message=PERSPECTIVE_SYSTEM,
                    model="gpt-5-mini",
                ),
            }
        )

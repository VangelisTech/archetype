# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Process-host configuration for the Physical-AI world-library extension."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from archetype.physical_ai.hosted_activity_contracts import HostedEpisodeProvider
from archetype.physical_ai.hosted_modal import ModalHostedEpisodeConfig


@dataclass(frozen=True, slots=True, kw_only=True)
class PhysicalAIExtensionConfig:
    """Configure hosted Physical-AI providers at process composition time."""

    hosted_episode_provider_factory: (
        Callable[[ModalHostedEpisodeConfig], HostedEpisodeProvider] | None
    ) = None

    def __post_init__(self) -> None:
        factory = self.hosted_episode_provider_factory
        if factory is not None and not callable(factory):
            raise TypeError("hosted_episode_provider_factory must be callable")


__all__ = ["PhysicalAIExtensionConfig"]

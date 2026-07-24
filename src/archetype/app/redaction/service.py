# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility import for the canonical redaction scanner.

Root deletes this application-owned path after all consumers repoint in PR-4.
The ``os`` import preserves the existing scanner test's monkeypatch seam while
the concrete class identity remains canonical.
"""

import os as os

from archetype.redaction.service import RedactionService

__all__ = ["RedactionService"]

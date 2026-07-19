# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""Compatibility coverage for the deprecated Logfire hook observer."""

import pytest

from archetype.contrib.logfire_observer import logfire_hooks

pytestmark = pytest.mark.contract("observability.logging.correlated")


def test_logfire_hooks_is_a_warning_only_compatibility_shim() -> None:
    with pytest.warns(DeprecationWarning, match="process-host boundary"):
        assert logfire_hooks() == []

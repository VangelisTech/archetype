"""Credentialed infrastructure evidence classification."""

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.integration)
        item.add_marker(pytest.mark.external)

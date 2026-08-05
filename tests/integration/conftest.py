"""Integration-suite evidence classification."""

from pathlib import Path

import pytest

_HERE = Path(__file__).parent


def pytest_collection_modifyitems(items):
    for item in items:
        if Path(item.path).is_relative_to(_HERE):
            item.add_marker(pytest.mark.integration)

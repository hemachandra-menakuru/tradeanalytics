"""
Root conftest.py — applies the 'unit' marker to all unmarked tests by default.

Tests without an explicit @pytest.mark.unit/integration/smoke marker are treated
as unit tests. This opt-out model means running `pytest -m unit` (the default in
pyproject.toml) never accidentally skips new tests just because someone forgot to
add a marker.

To opt out of the unit tier, add @pytest.mark.integration or @pytest.mark.smoke.
"""
import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        existing = {m.name for m in item.iter_markers()}
        if not existing.intersection({"unit", "integration", "smoke"}):
            item.add_marker(pytest.mark.unit)

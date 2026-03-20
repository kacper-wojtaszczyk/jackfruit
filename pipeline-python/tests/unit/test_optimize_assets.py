"""
Unit tests for optimize assets and the make_optimize_asset factory.

Optimize assets use deps-only dependency (ordering guarantee, no data exchange),
so we test them via direct invocation with dg.build_asset_context() — same
pattern as the transform asset tests.
"""
import dagster as dg
import pytest

from pipeline_python.defs.assets import (
    make_optimize_asset,
    optimize_cams_data,
    optimize_ecmwf_data,
    transform_cams_data,
    transform_ecmwf_data,
)
from pipeline_python.storage.grid_store import GridData, GridStore


# ---------------------------------------------------------------------------
# Module-level state for mock tracking
# ---------------------------------------------------------------------------

_compact_calls: int = 0


# ---------------------------------------------------------------------------
# Mock resources
# ---------------------------------------------------------------------------


class MockGridStore(GridStore):
    """Records compact() calls via module-level counter."""

    def insert_grid(self, grid: GridData) -> int:
        return grid.row_count

    def compact(self) -> None:
        global _compact_calls
        _compact_calls += 1


class ErrorGridStore(GridStore):
    """Raises on compact() to test error propagation."""

    def insert_grid(self, grid: GridData) -> int:
        return 0

    def compact(self) -> None:
        raise ConnectionError("ClickHouse unavailable")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_mock_state():
    """Reset module-level mock state before each test."""
    global _compact_calls
    _compact_calls = 0


# ---------------------------------------------------------------------------
# Tests: asset factory
# ---------------------------------------------------------------------------


class TestMakeOptimizeAsset:
    """Tests for the make_optimize_asset factory function."""

    def test_factory_produces_distinct_asset_keys(self):
        """Each factory call should produce an asset with a unique key."""
        assert optimize_cams_data.key != optimize_ecmwf_data.key

    def test_cams_asset_has_correct_key(self):
        """optimize_cams_data should have the expected asset key."""
        assert optimize_cams_data.key == dg.AssetKey("optimize_cams_data")

    def test_ecmwf_asset_has_correct_key(self):
        """optimize_ecmwf_data should have the expected asset key."""
        assert optimize_ecmwf_data.key == dg.AssetKey("optimize_ecmwf_data")

    def test_cams_depends_on_transform_cams(self):
        """optimize_cams_data should depend on transform_cams_data."""
        assert transform_cams_data.key in optimize_cams_data.dependency_keys

    def test_ecmwf_depends_on_transform_ecmwf(self):
        """optimize_ecmwf_data should depend on transform_ecmwf_data."""
        assert transform_ecmwf_data.key in optimize_ecmwf_data.dependency_keys

    def test_factory_with_custom_source_name(self):
        """Factory should work with arbitrary source names."""
        custom = make_optimize_asset("foo", transform_cams_data)
        assert custom.key == dg.AssetKey("optimize_foo_data")

    def test_factory_preserves_upstream_dependency(self):
        """Factory should wire the given upstream as the dep, not a hardcoded one."""
        custom = make_optimize_asset("custom", transform_ecmwf_data)
        assert transform_ecmwf_data.key in custom.dependency_keys
        assert transform_cams_data.key not in custom.dependency_keys


# ---------------------------------------------------------------------------
# Tests: optimize_cams_data execution
# ---------------------------------------------------------------------------


class TestOptimizeCamsData:
    """Tests for executing optimize_cams_data."""

    def test_calls_compact(self):
        """Should call grid_store.compact() exactly once."""
        context = dg.build_asset_context(partition_key="2026-01-15")
        optimize_cams_data(context, MockGridStore())
        assert _compact_calls == 1

    def test_returns_materialize_result(self):
        """Should return MaterializeResult with table metadata."""
        context = dg.build_asset_context(partition_key="2026-01-15")
        result = optimize_cams_data(context, MockGridStore())
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata["table"] == "grid_data"

    def test_propagates_compact_error(self):
        """If compact() raises, the error should propagate (not be swallowed)."""
        context = dg.build_asset_context(partition_key="2026-01-15")
        with pytest.raises(ConnectionError, match="ClickHouse unavailable"):
            optimize_cams_data(context, ErrorGridStore())


# ---------------------------------------------------------------------------
# Tests: optimize_ecmwf_data execution
# ---------------------------------------------------------------------------


class TestOptimizeEcmwfData:
    """Tests for executing optimize_ecmwf_data."""

    def test_calls_compact(self):
        """Should call grid_store.compact() exactly once."""
        context = dg.build_asset_context(partition_key="2026-01-15")
        optimize_ecmwf_data(context, MockGridStore())
        assert _compact_calls == 1

    def test_returns_materialize_result(self):
        """Should return MaterializeResult with table metadata."""
        context = dg.build_asset_context(partition_key="2026-01-15")
        result = optimize_ecmwf_data(context, MockGridStore())
        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata["table"] == "grid_data"

    def test_propagates_compact_error(self):
        """If compact() raises, the error should propagate (not be swallowed)."""
        context = dg.build_asset_context(partition_key="2026-01-15")
        with pytest.raises(ConnectionError, match="ClickHouse unavailable"):
            optimize_ecmwf_data(context, ErrorGridStore())

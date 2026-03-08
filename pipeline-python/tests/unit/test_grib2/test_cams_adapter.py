"""Tests for the pygrib adapter."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline_python.grib2.adapters.cams_adapter import CamsReader

FIXTURE = Path(__file__).parent.parent.parent / "fixtures" / "019c7f73-419f-727c-8e56-95880501e36b.grib"


class TestCamsReader:
    """Tests for CamsReader.open()."""

    def test_iterates_all_messages(self):
        reader = CamsReader()
        with reader.open(str(FIXTURE)) as messages:
            count = sum(1 for _ in messages)
        assert count == 8

    def test_context_manager_closes_file(self):
        mock_gribs = MagicMock()
        mock_gribs.__iter__ = MagicMock(return_value=iter([]))
        with patch("pipeline_python.grib2.adapters.cams_adapter.pygrib.open", return_value=mock_gribs):
            with CamsReader().open("dummy.grib"):
                pass
        mock_gribs.close.assert_called_once()


class TestCamsMessage:
    """Tests for CamsMessage properties using the CAMS fixture."""

    @pytest.fixture()
    def first_message(self):
        """Return the first message from the CAMS fixture."""
        reader = CamsReader()
        with reader.open(str(FIXTURE)) as messages:
            message = next(iter(messages))
            yield message

    def test_variable_name_is_known(self, first_message):
        """Should return a mapped CAMS variable name."""
        assert first_message.variable_name in ("pm10", "pm2p5")

    def test_values_shape(self, first_message):
        """CAMS Europe grid is 420×700 (ny×nx)."""
        assert first_message.values.shape == (420, 700)

    def test_unit_is_string(self, first_message):
        assert isinstance(first_message.unit, str)
        assert len(first_message.unit) > 0

    def test_valid_time_is_datetime(self, first_message):
        from datetime import datetime
        assert isinstance(first_message.timestamp, datetime)

    def test_lats_shape_and_range(self, first_message):
        """Latitudes should cover the CAMS Europe domain (~30° to ~72°)."""
        lats = first_message.lats
        assert lats.shape == (420, 700)
        assert lats.min() > 25.0
        assert lats.max() < 75.0

    def test_lons_are_in_negative_180_to_180(self, first_message):
        """All longitudes must be normalized to [-180, 180]."""
        lons = first_message.lons
        assert lons.min() >= -180.0
        assert lons.max() <= 180.0

    def test_lons_range_matches_cams_europe(self, first_message):
        """Longitude range should be approximately -24.95 to 44.95.

        This is the test that would have caught the original bug.
        If you ever see -335 here, something has gone terribly wrong
        and the Earth has acquired bonus degrees.
        """
        lons = first_message.lons
        assert lons.min() == pytest.approx(-24.95, abs=0.5)
        assert lons.max() == pytest.approx(44.95, abs=0.5)
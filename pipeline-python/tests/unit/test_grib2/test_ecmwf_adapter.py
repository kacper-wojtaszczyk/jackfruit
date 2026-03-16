"""Tests for the ECMWF pygrib adapter."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline_python.grib2.adapters.ecmwf_adapter import EcmwfReader

FIXTURE = Path(__file__).parent.parent.parent / "fixtures" / "019cf6d7-02a0-745b-ac05-e1201d8f8a72.grib"


class TestEcmwfReader:
    """Tests for EcmwfReader.open()."""

    def test_iterates_all_messages(self):
        reader = EcmwfReader()
        with reader.open(str(FIXTURE)) as messages:
            count = sum(1 for _ in messages)
        assert count == 4

    def test_context_manager_closes_file(self):
        mock_gribs = MagicMock()
        mock_gribs.__iter__ = MagicMock(return_value=iter([]))
        with patch("pipeline_python.grib2.adapters.ecmwf_adapter.pygrib.open", return_value=mock_gribs):
            with EcmwfReader().open("dummy.grib"):
                pass
        mock_gribs.close.assert_called_once()


class TestEcmwfMessage:
    """Tests for EcmwfMessage properties using the ECMWF fixture."""

    @pytest.fixture()
    def first_message(self):
        """Return the first message from the ECMWF fixture."""
        reader = EcmwfReader()
        with reader.open(str(FIXTURE)) as messages:
            message = next(iter(messages))
            yield message

    def test_variable_name_is_known(self, first_message):
        """Should return a mapped ECMWF variable name."""
        assert first_message.variable_name in ("temperature", "dewpoint")

    def test_values_shape(self, first_message):
        """ECMWF global grid is 721x1440 (0.25° resolution)."""
        assert first_message.values.shape == (721, 1440)

    def test_unit_is_kelvin(self, first_message):
        """Both 2t and 2d are stored in Kelvin in the GRIB — adapter must be faithful."""
        assert first_message.unit == "K"

    def test_valid_time_is_datetime(self, first_message):
        assert isinstance(first_message.timestamp, datetime)

    def test_lats_shape_and_range(self, first_message):
        """Latitudes should cover the full globe (-90 to 90)."""
        lats = first_message.lats
        assert lats.shape == (721, 1440)
        assert lats.min() == pytest.approx(-90.0, abs=0.5)
        assert lats.max() == pytest.approx(90.0, abs=0.5)

    def test_lons_range(self, first_message):
        """Longitudes should cover -180 to 179.75."""
        lons = first_message.lons
        assert lons.min() == pytest.approx(-180.0, abs=0.5)
        assert lons.max() == pytest.approx(179.75, abs=0.5)

    def test_values_are_in_kelvin_range(self, first_message):
        """Values should be in Kelvin (sanity check: not Celsius)."""
        values = first_message.values
        assert values.min() > 150
        assert values.max() < 350

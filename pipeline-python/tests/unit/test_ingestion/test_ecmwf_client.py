from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile

import pytest

from pipeline_python.ingestion.ecmwf_client import EcmwfClient, _ECMWF_STEPS

_PATCH_TARGET = "pipeline_python.ingestion.ecmwf_client.Client"


@pytest.fixture
def ecmwf_client():
    return EcmwfClient(source="ecmwf")


@pytest.fixture
def mock_opendata():
    """Patch the Client class imported at the top of ecmwf_client.py."""
    mock_client_cls = Mock()
    mock_client_instance = Mock()
    mock_client_cls.return_value = mock_client_instance
    with patch(_PATCH_TARGET, mock_client_cls):
        yield mock_client_cls, mock_client_instance


class TestEcmwfClientRetrieveForecast:
    def test_creates_client_with_configured_source(self, mock_opendata):
        """Client should be instantiated with the source configured on the resource."""
        mock_client_cls, _ = mock_opendata
        client = EcmwfClient(source="azure")
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["temperature"],
                target=target,
            )
        mock_client_cls.assert_called_once_with(source="azure")

    def test_retrieve_passes_correct_params(self, ecmwf_client, mock_opendata):
        """Request dict passed to client.retrieve must have all required ECMWF API fields."""
        _, mock_client_instance = mock_opendata
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            ecmwf_client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["temperature", "dewpoint"],
                target=target,
            )
        request = mock_client_instance.retrieve.call_args[0][0]
        assert request["type"] == "fc"
        assert request["levtype"] == "sfc"
        assert request["param"] == ["2t", "2d"]
        assert request["step"] == _ECMWF_STEPS

    def test_steps_cover_0_to_48_in_3h_increments(self):
        """ECMWF_STEPS must be [0, 3, 6, ..., 48] — 17 elements, 3-hour intervals."""
        assert _ECMWF_STEPS == list(range(0, 49, 3))
        assert len(_ECMWF_STEPS) == 17

    def test_target_converted_to_string(self, ecmwf_client, mock_opendata):
        """target path must be passed as str to client.retrieve (multiurl requirement)."""
        _, mock_client_instance = mock_opendata
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            ecmwf_client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["temperature"],
                target=target,
            )
        target_arg = mock_client_instance.retrieve.call_args[0][1]
        assert isinstance(target_arg, str)

    def test_raises_key_error_for_unknown_variable(self, ecmwf_client, mock_opendata):
        """Unmapped variable names must raise KeyError immediately."""
        with pytest.raises(KeyError):
            ecmwf_client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["ozone"],
                target=Path("/tmp/out.grib"),
            )

    def test_filters_steps_by_max_leadtime(self, ecmwf_client, mock_opendata):
        """With max_leadtime_hours=24, steps should be [0, 3, ..., 24]."""
        _, mock_client_instance = mock_opendata
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            ecmwf_client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["temperature"],
                target=target,
                max_leadtime_hours=24,
            )
        request = mock_client_instance.retrieve.call_args[0][0]
        assert request["step"] == list(range(0, 25, 3))

    def test_rejects_leadtime_above_max(self, ecmwf_client, mock_opendata):
        """ValueError for leadtime exceeding 48 hours."""
        with pytest.raises(ValueError, match="maximum is 48"):
            ecmwf_client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["temperature"],
                target=Path("/tmp/out.grib"),
                max_leadtime_hours=49,
            )

    def test_rejects_negative_leadtime(self, ecmwf_client, mock_opendata):
        """ValueError for negative leadtime."""
        with pytest.raises(ValueError, match="minimum is 0"):
            ecmwf_client.retrieve_forecast(
                forecast_date=date(2026, 1, 15),
                variables=["temperature"],
                target=Path("/tmp/out.grib"),
                max_leadtime_hours=-1,
            )

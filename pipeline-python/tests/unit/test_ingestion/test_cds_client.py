from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile

import pytest

from pipeline_python.ingestion.cds_client import CdsClient

_PATCH_TARGET = "pipeline_python.ingestion.cds_client.cdsapi"


@pytest.fixture
def cds_client():
    return CdsClient(
        url="https://ads.atmosphere.copernicus.eu/api",
        api_key="test-key-123",
    )


@pytest.fixture
def mock_cdsapi():
    """Patch the cdsapi module imported at the top of cds_client.py."""
    mock_module = Mock()
    mock_client_instance = Mock()
    mock_module.Client.return_value = mock_client_instance
    mock_result = Mock()
    mock_client_instance.retrieve.return_value = mock_result
    with patch(_PATCH_TARGET, mock_module):
        yield mock_module, mock_client_instance, mock_result


class TestCdsClientRetrieveForecast:
    def test_maps_domain_variables_to_api_names(self, cds_client, mock_cdsapi):
        _, mock_client, _ = mock_cdsapi
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            cds_client.retrieve_forecast(
                forecast_date=date.fromisoformat("2026-01-15"),
                variables=["pm2p5", "pm10"],
                target=target,
            )
            params = mock_client.retrieve.call_args[0][1]
            assert params["variable"] == [
                "particulate_matter_2.5um",
                "particulate_matter_10um",
            ]

    def test_generates_hourly_leadtime_steps(self, cds_client, mock_cdsapi):
        _, mock_client, _ = mock_cdsapi
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            cds_client.retrieve_forecast(
                forecast_date=date.fromisoformat("2026-01-15"),
                variables=["pm2p5"],
                target=target,
                max_leadtime_hours=3,
            )
            params = mock_client.retrieve.call_args[0][1]
            assert params["leadtime_hour"] == ["0", "1", "2", "3"]

    def test_default_leadtime_is_48_hours(self, cds_client, mock_cdsapi):
        _, mock_client, _ = mock_cdsapi
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "out.grib"
            cds_client.retrieve_forecast(
                forecast_date=date.fromisoformat("2026-01-15"),
                variables=["pm2p5"],
                target=target,
            )
            params = mock_client.retrieve.call_args[0][1]
            assert len(params["leadtime_hour"]) == 49  # 0..48 inclusive

    def test_rejects_leadtime_above_48(self, cds_client):
        with pytest.raises(ValueError, match="48"):
            cds_client.retrieve_forecast(
                forecast_date=date.fromisoformat("2026-01-15"),
                variables=["pm2p5"],
                target=Path("/tmp/out.grib"),
                max_leadtime_hours=49,
            )

    def test_raises_key_error_for_unknown_variable(self, cds_client, mock_cdsapi):
        with pytest.raises(KeyError):
            cds_client.retrieve_forecast(
                forecast_date=date.fromisoformat("2026-01-15"),
                variables=["ozone"],
                target=Path("/tmp/out.grib"),
            )

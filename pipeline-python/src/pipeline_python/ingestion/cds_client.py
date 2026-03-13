import datetime
from pathlib import Path

import cdsapi
import dagster as dg
from pydantic import PrivateAttr

_CDS_API_DATASET = "cams-europe-air-quality-forecasts"

_VARIABLE_MAP = {
    "pm2p5": "particulate_matter_2.5um",
    "pm10": "particulate_matter_10um",
}

# CAMS forecast model maximum lead time (hours).
_MAX_LEADTIME = 96

class CdsClient(dg.ConfigurableResource):
    """
    Copernicus ADS client for downloading GRIB data.

    Wraps the cdsapi library as a Dagster ConfigurableResource.
    Credentials are passed explicitly (not via ~/.cdsapirc) for production use.

    Attributes:
        url: ADS API base URL (e.g., 'https://ads.atmosphere.copernicus.eu/api')
        api_key: ADS API key
    """

    url: str
    api_key: str
    _client: cdsapi.Client | None = PrivateAttr(default=None)

    def _get_client(self) -> cdsapi.Client:
        if self._client is None:
            self._client = cdsapi.Client(url=self.url, key=self.api_key)

        return self._client

    def retrieve_forecast(
            self,
            date: datetime.date,
            variables: list[str],
            target: Path,
            max_leadtime_hours: int = 48,
    ) -> None:
        """
        Submit a retrieval request and download the result to a local file.

        Blocks until the request is fulfilled — cdsapi polls the API internally
        with exponential backoff.

        Args:
            date: Date of forecast creation
            variables: List of variables to download
            target: Local path to write the downloaded GRIB file to
            max_leadtime_hours: Max lead time in hours

        Raises:
            Exception: If the API request fails (authentication, timeout, etc.)
        """
        if max_leadtime_hours > _MAX_LEADTIME:
            raise ValueError(
                f"CAMS forecast maximum is {_MAX_LEADTIME} hours, got {max_leadtime_hours}"
            )

        api_variables = [_VARIABLE_MAP[v] for v in variables]
        leadtime_hours = [str(h) for h in range(max_leadtime_hours + 1)]

        request = {
            "variable": api_variables,
            "model": ["ensemble"],
            "level": ["0"],
            "date": f"{date.isoformat()}/{date.isoformat()}",
            "type": ["forecast"],
            "time": ["00:00"],
            "leadtime_hour": leadtime_hours,
            "data_format": "grib",
        }

        client = self._get_client()
        client.retrieve(_CDS_API_DATASET, request).download(str(target))

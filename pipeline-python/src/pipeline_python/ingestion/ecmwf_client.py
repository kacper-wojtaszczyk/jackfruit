from datetime import date
from pathlib import Path

import dagster as dg
from ecmwf.opendata import Client

_ECMWF_STEPS = list(range(0, 48 + 1, 3))

_VARIABLE_MAP = {
    "temperature": "2t",
    "dewpoint": "2d",
}

class EcmwfClient(dg.ConfigurableResource):
    """
    ECMWF Open Data client for downloading global IFS forecast GRIB data.

    Attributes:
        source: ECMWF data source ("ecmwf" or any available mirror)
    """

    source: str

    def retrieve_forecast(
        self,
        forecast_date: date,
        variables: list[str],
        target: Path
    ) -> None:
        client = Client(source=self.source)
        api_variables = [_VARIABLE_MAP[v] for v in variables]
        request = {
            "date": forecast_date,
            "type": "fc",
            "stream": "oper",
            "levtype": "sfc",
            "time": 0,
            "step": _ECMWF_STEPS,
            "param": api_variables,
        }
        client.retrieve(request, str(target))

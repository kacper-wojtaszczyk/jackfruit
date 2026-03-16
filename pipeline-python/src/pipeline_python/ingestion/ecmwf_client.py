from datetime import date
from pathlib import Path

import dagster as dg
from ecmwf.opendata import Client

_MAX_LEADTIME = 48
_ECMWF_STEPS = list(range(0, _MAX_LEADTIME + 1, 3))

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
        target: Path,
        max_leadtime_hours: int = _MAX_LEADTIME
    ) -> None:
        if max_leadtime_hours > _MAX_LEADTIME:
            raise ValueError(
                f"ECMWF forecast maximum is {_MAX_LEADTIME} hours, got {max_leadtime_hours}"
            )
        if max_leadtime_hours < 0:
            raise ValueError(
                f"ECMWF forecast minimum is 0 hours, got {max_leadtime_hours}"
            )

        client = Client(source=self.source)
        api_variables = [_VARIABLE_MAP[v] for v in variables]
        request = {
            "date": forecast_date,
            "type": "fc",
            "stream": "oper",
            "levtype": "sfc",
            "time": 0,
            "step": [x for x in _ECMWF_STEPS if x <= max_leadtime_hours],
            "param": api_variables,
        }
        client.retrieve(request, str(target))

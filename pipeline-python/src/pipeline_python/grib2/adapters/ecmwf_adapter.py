# grib2/adapters/ecmwf_adapter.py
from contextlib import contextmanager, AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import numpy as np
import pygrib

_ECMWF_VARIABLE_NAMES = {
    "2t": "temperature",
    "2d": "dewpoint",
}


class EcmwfMessage:
    def __init__(self, message: pygrib.gribmessage):
        self._message = message
        self._data: tuple[np.ndarray, np.ndarray, np.ndarray] | None = None

    @property
    def variable_name(self) -> str:
        return _ECMWF_VARIABLE_NAMES[self._message.shortName]

    @property
    def unit(self) -> str:
        return self._message.parameterUnits

    @property
    def timestamp(self) -> datetime:
        return self._message.validDate

    @property
    def values(self) -> np.ndarray:
        return self._get_data()[0]

    @property
    def lats(self) -> np.ndarray:
        return self._get_data()[1]

    @property
    def lons(self) -> np.ndarray:
        return self._get_data()[2]

    def _get_data(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        if self._data is None:
            self._data = self._message.data()
        return self._data


class EcmwfReader:
    def open(self, path: str | Path) -> AbstractContextManager[Iterator[EcmwfMessage]]:
        return self._open(path)

    @contextmanager
    def _open(self, path: str | Path) -> Iterator[Iterator[EcmwfMessage]]:
        gribs = pygrib.open(str(path))
        try:
            yield (EcmwfMessage(grb) for grb in gribs)
        finally:
            gribs.close()
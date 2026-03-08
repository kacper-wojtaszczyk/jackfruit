from contextlib import contextmanager, AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import numpy as np
import pygrib

_constituent_names = {
    40008: "pm10",
    40009: "pm2p5",
}


class CamsMessage:
    def __init__(self, message: pygrib.gribmessage):
        self._message = message
        self._data: tuple[np.ndarray, np.ndarray, np.ndarray] | None = None

    @property
    def variable_name(self) -> str:
        return _constituent_names[self._message.constituentType]

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

class CamsReader:
    def open(self, path: str | Path) -> AbstractContextManager[Iterator[CamsMessage]]:
        return self._open(path)

    @contextmanager
    def _open(self, path: str | Path) -> Iterator[Iterator[CamsMessage]]:
        gribs = pygrib.open(str(path))
        try:
            yield (CamsMessage(grb) for grb in gribs)
        finally:
            gribs.close()
"""
Protocols for GRIB2 reading.

These define the interface between pipeline assets and whatever library
actually decodes GRIB files. The asset code imports these protocols for
type hints but never touches the underlying library directly.
"""
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from typing import Protocol, Iterator, runtime_checkable

import numpy as np


class GribMessage(Protocol):
    """
    Protocol for GRIB2 messages. That provides unified interface for
    GRIB Messages originating from different sources.
    """
    @property
    def variable_name(self) -> str: ...

    @property
    def unit(self) -> str : ...

    @property
    def timestamp(self) -> datetime: ...

    @property
    def values(self) -> np.ndarray : ...

    @property
    def lats(self) -> np.ndarray : ...

    @property
    def lons(self) -> np.ndarray : ...

@runtime_checkable
class GribReader(Protocol):
    def open(self, path: str | Path) -> AbstractContextManager[Iterator[GribMessage]]: ...

"""
GRIB2 reading abstraction.

Provides a library-agnostic interface for reading GRIB2 files.
The concrete implementation (currently CamsReader using pygrib) is hidden behind
the GribMessage/GribReader protocols.

Usage:
    from pipeline_python.grib2 import CamsReader

    reader = CamsReader()
    with reader.open(path) as messages:
        for msg in messages:
            print(msg.variable_name, msg.values.shape)
"""

from pipeline_python.grib2.adapters.cams_adapter import CamsReader
from pipeline_python.grib2.reader import GribMessage, GribReader

__all__ = [
    "GribMessage",
    "GribReader",
    "CamsReader",
]

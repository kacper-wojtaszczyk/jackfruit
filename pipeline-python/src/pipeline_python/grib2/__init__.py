"""
GRIB2 utilities with patched grib2io support.

This module provides a pre-patched grib2io instance and utility functions
for working with GRIB2 files, including non-standard PDTs like 4.40.

Usage:
    from pipeline_python.grib2 import grib2io, get_shortname

    with grib2io.open(path) as f:
        for msg in f:
            name = get_shortname(msg.section4[4])

The grib2io instance is automatically patched with PDT 4.40 support.
Import order doesn't matter — just import from this module.
"""

# Apply PDT 4.40 patch by importing the patch module
# This must happen before any grib2io usage
import pipeline_python.grib2.pdt40  # noqa: F401

# Re-export grib2io (now patched)
import grib2io

# Re-export shortname utilities
from pipeline_python.grib2.shortnames import get_shortname

__all__ = [
    "grib2io",
    "get_shortname",
]

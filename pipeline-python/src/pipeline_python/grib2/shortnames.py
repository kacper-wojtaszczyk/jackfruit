"""
Short name mappings for GRIB2 parameters.

Pure data module — no external dependencies.

This module provides human-readable short names for GRIB2 parameter codes.
Short names follow ECMWF/cfgrib conventions: lowercase, underscore-separated,
suitable for file paths and variable identifiers.

Currently supports:
- Atmospheric chemical constituents (Table 4.230) used in CAMS air quality data

Extensible for other parameter types (temperature, humidity, wind, etc.)
as needed.
"""

# GRIB2 Table 4.230 - Atmospheric Chemical Constituent Type
# Reference: https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-230.shtml
# Note: ECMWF/CAMS uses local codes (40xxx) that differ from WMO standard codes (62xxx)
CONSTITUENT_TYPE_NAMES: dict[int, str] = {
    # WMO standard codes
    0: "ozone",
    1: "water_vapour",
    2: "methane",
    3: "carbon_dioxide",
    4: "carbon_monoxide",
    5: "nitrogen_dioxide",
    6: "nitrous_oxide",
    7: "formaldehyde",
    8: "sulphur_dioxide",
    9: "ammonia",
    10: "ammonium",
    11: "nitrogen_monoxide",
    62099: "pm1p0",   # PM1.0 (WMO)
    62100: "pm2p5",   # PM2.5 (WMO)
    62101: "pm10",    # PM10 (WMO)
    # ECMWF/CAMS local codes
    40008: "pm10",    # PM10 (ECMWF local)
    40009: "pm2p5",   # PM2.5 (ECMWF local)
}


def get_shortname(code: int) -> str:
    """
    Get short name for an atmospheric chemical constituent type code.

    Args:
        code: GRIB2 Table 4.230 constituent type code

    Returns:
        Lowercase, underscore-separated short name suitable for file paths.
        Falls back to 'constituent_{code}' for unknown codes.

    Examples:
        >>> get_shortname(0)
        'ozone'
        >>> get_shortname(40009)
        'pm2p5'
        >>> get_shortname(99999)
        'constituent_99999'
    """
    return CONSTITUENT_TYPE_NAMES.get(code, f"constituent_{code}")

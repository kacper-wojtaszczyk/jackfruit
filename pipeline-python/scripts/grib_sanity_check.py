#!/usr/bin/env python3
"""
GRIB2 sanity check script.

Validates that grib2io can read GRIB files (including CAMS from ECMWF).

Usage:
    uv run scripts/grib_sanity_check.py <path/to/file.grib>

Run inside Docker:
    docker compose run dagster \
        uv run scripts/grib_sanity_check.py data/file.grib
"""

import argparse
import sys
from pathlib import Path

# Apply PDT 4.40 patch before importing grib2io
import pipeline_python.grib2io_patch  # noqa: F401

import grib2io



# GRIB2 Table 4.230 - Atmospheric Chemical Constituent Type
# Reference: https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-230.shtml
CONSTITUENT_TYPE_NAMES = {
    0: "Ozone (O3)",
    1: "Water Vapour (H2O)",
    2: "Methane (CH4)",
    3: "Carbon Dioxide (CO2)",
    4: "Carbon Monoxide (CO)",
    5: "Nitrogen Dioxide (NO2)",
    6: "Nitrous Oxide (N2O)",
    7: "Formaldehyde (HCHO)",
    8: "Sulphur Dioxide (SO2)",
    9: "Ammonia (NH3)",
    10: "Ammonium (NH4+)",
    11: "Nitrogen Monoxide (NO)",
    12: "Atomic Oxygen (O)",
    13: "Nitrate Radical (NO3)",
    14: "Hydroperoxyl Radical (HO2)",
    15: "Dinitrogen Pentoxide (N2O5)",
    16: "Nitrous Acid (HONO)",
    17: "Nitric Acid (HNO3)",
    18: "Peroxynitric Acid (HO2NO2)",
    19: "Hydrogen Peroxide (H2O2)",
    20: "Molecular Hydrogen (H2)",
    21: "Atomic Nitrogen (N)",
    22: "Sulphate (SO42-)",
    23: "Radon (Rn)",
    24: "Elemental Mercury (Hg(0))",
    25: "Divalent Mercury (Hg2+)",
    26: "Atomic Chlorine (Cl)",
    27: "Chlorine Monoxide (ClO)",
    28: "Dichlorine Peroxide (Cl2O2)",
    29: "Hypochlorous Acid (HOCl)",
    30: "Chlorine Nitrate (ClONO2)",
    31: "Chlorine Dioxide (ClO2)",
    32: "Atomic Bromine (Br)",
    33: "Bromine Monoxide (BrO)",
    34: "Bromine Chloride (BrCl)",
    35: "Hydrogen Bromide (HBr)",
    36: "Hypobromous Acid (HOBr)",
    37: "Bromine Nitrate (BrONO2)",
    10000: "Hydroxyl Radical (OH)",
    10001: "Methyl Peroxy Radical (CH3O2)",
    10002: "Methyl Hydroperoxide (CH3O2H)",
    10004: "Methanol (CH3OH)",
    10005: "Formic Acid (CH3OOH)",
    10006: "Hydrogen Cyanide (HCN)",
    10007: "Aceto Nitrile (CH3CN)",
    10008: "Ethane (C2H6)",
    10009: "Ethene (C2H4)",
    10010: "Ethyne (C2H2)",
    10011: "Ethanol (C2H5OH)",
    10012: "Acetic Acid (C2H5OOH)",
    10013: "Peroxyacetyl Nitrate (CH3C(O)OONO2)",
    10014: "Propane (C3H8)",
    10015: "Propene (C3H6)",
    10016: "Butanes (C4H10)",
    10017: "Isoprene (C5H8)",
    10018: "Alpha Pinene (C10H16)",
    10019: "Beta Pinene (C10H16)",
    10020: "Limonene (C10H16)",
    10021: "Benzene (C6H6)",
    10022: "Toluene (C7H8)",
    10023: "Xylene (C8H10)",
    10500: "Dimethyl Sulphide (CH3SCH3)",
    # Aerosols
    62000: "Total Aerosol",
    62001: "Dust Dry",
    62002: "Water In Ambient",
    62003: "Ammonium Dry",
    62004: "Nitrate Dry",
    62005: "Nitric Acid Trihydrate",
    62006: "Sulphate Dry",
    62007: "Mercury Dry",
    62008: "Sea Salt Dry",
    62009: "Black Carbon Dry",
    62010: "Particulate Organic Matter Dry",
    62011: "Primary Particulate Organic Matter Dry",
    62012: "Secondary Particulate Organic Matter Dry",
    # PM categories (WMO standard)
    62096: "NO (y) Tracer 1",
    62097: "NO (y) Tracer 2",
    62098: "NO (y) Tracer 3",
    62099: "Particulate Matter (PM1.0)",
    62100: "Particulate Matter (PM2.5)",  # PM2.5 (WMO)
    62101: "Particulate Matter (PM10)",   # PM10 (WMO)
    # ECMWF/CAMS local codes
    40008: "Particulate Matter (PM10)",   # PM10 (ECMWF local)
    40009: "Particulate Matter (PM2.5)",  # PM2.5 (ECMWF local)
}


def get_constituent_name(code: int) -> str:
    """Get human-readable name for atmospheric chemical constituent type."""
    return CONSTITUENT_TYPE_NAMES.get(code, f"Unknown ({code})")


def print_message(i: int, msg) -> None:
    """Print details for a single GRIB message."""
    print(f"─── Message {i + 1} ───")
    print(f"  shortName:     {msg.shortName}")
    print(f"  name:          {msg.fullName}")
    print(f"  units:         {msg.units}")

    # For PDT 4.40, show the atmospheric chemical constituent type
    if hasattr(msg, 'atmosphericChemicalConstituentType'):
        constituent_code = msg.atmosphericChemicalConstituentType.value
        constituent_name = get_constituent_name(constituent_code)
        print(f"  constituent:   {constituent_code} - {constituent_name}")

    print(f"  discipline:    {msg.discipline}")
    print(f"  refDate:       {msg.refDate}")
    print(f"  leadTime:      {msg.leadTime}")
    print(f"  validDate:     {msg.validDate}")
    print(f"  typeOfLevel:   {msg.typeOfFirstFixedSurface}")
    print(f"  level:         {msg.level}")
    print(f"  gridType:      {msg.griddef.shape}")
    print(f"  nx × ny:       {msg.nx} × {msg.ny}")
    print(f"  lat range:     {msg.latitudeFirstGridpoint}° to {msg.latitudeLastGridpoint}°")
    print(f"  lon range:     {msg.longitudeFirstGridpoint}° to {msg.longitudeLastGridpoint}°")
    data = msg.data
    print(f"  data shape:    {data.shape}")
    print(f"  data range:    {data.min():.6e} to {data.max():.6e} {msg.units}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate that grib2io can read a GRIB file.")
    parser.add_argument("path", type=Path, help="Path to GRIB file (relative or absolute)")
    args = parser.parse_args()

    grib_path = args.path.resolve()
    if not grib_path.exists():
        print(f"❌ GRIB file not found: {grib_path}", file=sys.stderr)
        return 1

    size_kb = grib_path.stat().st_size / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb / 1024:.1f} MB"
    print(f"📂 Reading: {grib_path.name}")
    print(f"   Size: {size_str}")
    print()

    with grib2io.open(str(grib_path)) as grb:
        print(f"📊 Total messages: {len(grb)}")
        print()
        for i, msg in enumerate(grb):
            print_message(i, msg)

    print("✅ grib2io successfully read the GRIB file!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

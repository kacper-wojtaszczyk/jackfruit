#!/usr/bin/env python3
"""
GRIB2 sanity check script.

Validates that grib2io can read GRIB files (including CAMS from ECMWF).

Usage:
    python scripts/grib_sanity_check.py <path/to/file.grib>

Run inside Docker:
    docker compose run dagster \
        python scripts/grib_sanity_check.py /data/file.grib
"""

import argparse
import sys
from pathlib import Path

import grib2io

# --- monkey-patch start ---
# Fix for missing PDT 4.40 (Atmospheric Chemical Constituent) in grib2io 2.6.0
import grib2io.templates as templates
from dataclasses import dataclass, field

# 1. Define the missing descriptor for Constituent Type (Octet 12-13)
class AtmosphericChemicalConstituentType:
    """[Atmospheric Chemical Constituent Type](https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-230.shtml)"""
    def __get__(self, obj, objtype=None):
        # Index 4 corresponds to Octet 12-13 (after ParamCat@2, ParamNum@3)
        return templates.Grib2Metadata(obj.section4[4+2], table='4.230')
    def __set__(self, obj, value):
        obj.section4[4+2] = value

# 2. Update keys for standard fields to account for the inserted field in PDT 40
# PDT 40 inserts 1 field (ConstituentType) at index 4, shifting subsequent fields by +1
_templates_update = {
    templates.TypeOfGeneratingProcess: 2,
    templates.BackgroundGeneratingProcessIdentifier: 3,
    templates.GeneratingProcess: 4,
    templates.HoursAfterDataCutoff: 5,
    templates.MinutesAfterDataCutoff: 6,
    templates.UnitOfForecastTime: 7,
    templates.ValueOfForecastTime: 8,
    templates.FixedSfc1Info: 9,
    templates.TypeOfFirstFixedSurface: 9,
    templates.ScaleFactorOfFirstFixedSurface: 10,
    templates.ScaledValueOfFirstFixedSurface: 11,
    templates.FixedSfc2Info: 12,
    templates.TypeOfSecondFixedSurface: 12,
    templates.ScaleFactorOfSecondFixedSurface: 13,
    templates.ScaledValueOfSecondFixedSurface: 14,
}

for cls, default_idx in _templates_update.items():
    cls._key[40] = default_idx + 1

# 3. Define and register the Template class
@dataclass(init=False)
class ProductDefinitionTemplate40(templates.ProductDefinitionTemplateBase, templates.ProductDefinitionTemplateSurface):
    """[Product Definition Template 4.40](https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_temp4-40.shtml)"""
    _len = 16
    _num = 40
    atmosphericChemicalConstituentType: templates.Grib2Metadata = field(init=False, repr=False, default=AtmosphericChemicalConstituentType())

    @classmethod
    def _attrs(cls):
        return [key for key in cls.__dataclass_fields__.keys() if not key.startswith('_')]

templates._pdt_by_pdtn[40] = ProductDefinitionTemplate40
# --- monkey-patch end ---


def print_message(i: int, msg) -> None:
    """Print details for a single GRIB message."""
    print(f"─── Message {i + 1} ───")
    print(f"  shortName:     {msg.shortName}")
    print(f"  name:          {msg.fullName}")
    print(f"  units:         {msg.units}")
    print(f"  discipline:    {msg.discipline}")
    print(f"  refDate:       {msg.refDate}")
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

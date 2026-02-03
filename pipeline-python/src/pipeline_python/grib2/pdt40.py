"""
Monkey patch for grib2io to support PDT 4.40 (Atmospheric Chemical Constituent).

grib2io 2.6.0 doesn't include Product Definition Template 4.40, which is used by
CAMS (Copernicus Atmosphere Monitoring Service) data for atmospheric chemical
constituents like PM2.5, PM10, O3, NO2, etc.

This patch adds support for PDT 4.40 by:
1. Defining the AtmosphericChemicalConstituentType descriptor
2. Updating field indices for standard fields (shifted by +1 due to inserted field)
3. Registering ProductDefinitionTemplate40 in grib2io's template registry

Reference:
    https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_temp4-40.shtml
"""

from dataclasses import dataclass, field

# Import grib2io first to ensure full initialization before accessing submodules
import grib2io  # noqa: F401
from grib2io import templates


class AtmosphericChemicalConstituentType:
    """
    Descriptor for Atmospheric Chemical Constituent Type (Octet 12-13 in PDT 4.40).

    In grib2io's section4 array:
    - Index 0: reserved
    - Index 1: PDT number
    - Index 2: parameter category (octet 10)
    - Index 3: parameter number (octet 11)
    - Index 4: constituent type (octet 12-13, 2-byte value already decoded)

    Reference:
        https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_table4-230.shtml
    """

    def __get__(self, obj, objtype=None):
        # Index 4 is the constituent type (after reserved[0], pdtn[1], paramCat[2], paramNum[3])
        return templates.Grib2Metadata(obj.section4[4], table="4.230")

    def __set__(self, obj, value):
        obj.section4[4] = value


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


@dataclass(init=False)
class ProductDefinitionTemplate40(
    templates.ProductDefinitionTemplateBase, templates.ProductDefinitionTemplateSurface
):
    """
    Product Definition Template 4.40 - Analysis or forecast at a horizontal level
    or in a horizontal layer at a point in time for atmospheric chemical constituents.

    Reference:
        https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/grib2_temp4-40.shtml
    """

    _len = 16
    _num = 40
    atmosphericChemicalConstituentType: templates.Grib2Metadata = field(
        init=False, repr=False, default=AtmosphericChemicalConstituentType()
    )

    @classmethod
    def _attrs(cls):
        return [key for key in cls.__dataclass_fields__.keys() if not key.startswith("_")]


# Register the template
templates._pdt_by_pdtn[40] = ProductDefinitionTemplate40

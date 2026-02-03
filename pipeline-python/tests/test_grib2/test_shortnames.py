"""
Tests for grib2.shortnames module.

Pure Python tests — no grib2io dependency required.
"""

from pipeline_python.grib2.shortnames import (
    CONSTITUENT_TYPE_NAMES,
    get_shortname,
)


class TestGetShortname:
    """Tests for get_shortname function."""

    def test_returns_known_wmo_codes(self):
        """Should return short names for known WMO codes."""
        assert get_shortname(0) == "ozone"
        assert get_shortname(1) == "water_vapour"
        assert get_shortname(4) == "carbon_monoxide"
        assert get_shortname(5) == "nitrogen_dioxide"
        assert get_shortname(8) == "sulphur_dioxide"

    def test_returns_known_pm_codes(self):
        """Should return short names for PM codes."""
        assert get_shortname(62099) == "pm1p0"   # WMO PM1.0
        assert get_shortname(62100) == "pm2p5"   # WMO PM2.5
        assert get_shortname(62101) == "pm10"    # WMO PM10

    def test_returns_ecmwf_local_codes(self):
        """Should return short names for ECMWF local codes."""
        assert get_shortname(40008) == "pm10"    # ECMWF PM10
        assert get_shortname(40009) == "pm2p5"   # ECMWF PM2.5

    def test_returns_fallback_for_unknown_codes(self):
        """Should return formatted fallback for unknown codes."""
        assert get_shortname(99999) == "constituent_99999"
        assert get_shortname(12345) == "constituent_12345"

    def test_handles_zero_code(self):
        """Should handle code 0 (Ozone) correctly."""
        assert get_shortname(0) == "ozone"


class TestConstituentTypeNames:
    """Tests for CONSTITUENT_TYPE_NAMES dictionary."""

    def test_is_dict(self):
        """CONSTITUENT_TYPE_NAMES should be a dictionary."""
        assert isinstance(CONSTITUENT_TYPE_NAMES, dict)

    def test_contains_expected_entries(self):
        """Should contain expected WMO and ECMWF entries."""
        expected_entries = {
            0: "ozone",
            4: "carbon_monoxide",
            5: "nitrogen_dioxide",
            8: "sulphur_dioxide",
            40008: "pm10",
            40009: "pm2p5",
            62100: "pm2p5",
            62101: "pm10",
        }

        for code, name in expected_entries.items():
            assert CONSTITUENT_TYPE_NAMES.get(code) == name

    def test_no_duplicate_values_for_important_codes(self):
        """PM codes should map to consistent names."""
        # ECMWF PM10 and WMO PM10 should map to same name
        assert CONSTITUENT_TYPE_NAMES[40008] == CONSTITUENT_TYPE_NAMES[62101]
        # ECMWF PM2.5 and WMO PM2.5 should map to same name
        assert CONSTITUENT_TYPE_NAMES[40009] == CONSTITUENT_TYPE_NAMES[62100]

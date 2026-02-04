"""
Tests for grib2.shortnames module.

Pure Python tests — no grib2io dependency required.
"""

from pipeline_python.grib2.shortnames import get_shortname


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

    def test_ecmwf_and_wmo_pm_codes_are_consistent(self):
        """ECMWF and WMO PM codes should resolve to the same short names."""
        # PM10: ECMWF 40008 and WMO 62101
        assert get_shortname(40008) == get_shortname(62101) == "pm10"
        # PM2.5: ECMWF 40009 and WMO 62100
        assert get_shortname(40009) == get_shortname(62100) == "pm2p5"

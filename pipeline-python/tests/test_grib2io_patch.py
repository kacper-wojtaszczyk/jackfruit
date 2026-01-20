"""
Tests for grib2io_patch module.

Tests the PDT 4.40 patch for atmospheric chemical constituents.
"""

from pipeline_python.grib2io_patch import (
    CONSTITUENT_TYPE_NAMES,
    AtmosphericChemicalConstituentType,
    ProductDefinitionTemplate40,
    get_constituent_name,
)


class TestGetConstituentName:
    """Tests for get_constituent_name function."""
    
    def test_returns_known_wmo_codes(self):
        """Should return short names for known WMO codes."""
        assert get_constituent_name(0) == "ozone"
        assert get_constituent_name(1) == "water_vapour"
        assert get_constituent_name(4) == "carbon_monoxide"
        assert get_constituent_name(5) == "nitrogen_dioxide"
        assert get_constituent_name(8) == "sulphur_dioxide"
    
    def test_returns_known_pm_codes(self):
        """Should return short names for PM codes."""
        assert get_constituent_name(62099) == "pm1p0"   # WMO PM1.0
        assert get_constituent_name(62100) == "pm2p5"   # WMO PM2.5
        assert get_constituent_name(62101) == "pm10"    # WMO PM10
    
    def test_returns_ecmwf_local_codes(self):
        """Should return short names for ECMWF local codes."""
        assert get_constituent_name(40008) == "pm10"    # ECMWF PM10
        assert get_constituent_name(40009) == "pm2p5"   # ECMWF PM2.5
    
    def test_returns_fallback_for_unknown_codes(self):
        """Should return formatted fallback for unknown codes."""
        assert get_constituent_name(99999) == "constituent_99999"
        assert get_constituent_name(12345) == "constituent_12345"
    
    def test_handles_zero_code(self):
        """Should handle code 0 (Ozone) correctly."""
        assert get_constituent_name(0) == "ozone"


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


class TestProductDefinitionTemplate40:
    """Tests for ProductDefinitionTemplate40 class."""
    
    def test_has_correct_template_number(self):
        """Template should have _num = 40."""
        assert ProductDefinitionTemplate40._num == 40
    
    def test_has_correct_length(self):
        """Template should have _len = 16."""
        assert ProductDefinitionTemplate40._len == 16
    
    def test_has_atmospheric_constituent_field(self):
        """Template should have atmosphericChemicalConstituentType field."""
        assert "atmosphericChemicalConstituentType" in ProductDefinitionTemplate40.__dataclass_fields__

    def test_inherits_from_base_templates(self):
        """Template should inherit from required base classes."""
        import grib2io.templates as templates
        
        assert issubclass(ProductDefinitionTemplate40, templates.ProductDefinitionTemplateBase)
        assert issubclass(ProductDefinitionTemplate40, templates.ProductDefinitionTemplateSurface)
    
    def test_attrs_method_returns_non_private_fields(self):
        """_attrs() should return public field names."""
        attrs = ProductDefinitionTemplate40._attrs()
        
        assert isinstance(attrs, list)
        assert "atmosphericChemicalConstituentType" in attrs
        # Should not include private fields
        assert "_num" not in attrs
        assert "_len" not in attrs


class TestAtmosphericChemicalConstituentType:
    """Tests for AtmosphericChemicalConstituentType descriptor."""
    
    def test_is_descriptor(self):
        """Should implement descriptor protocol."""
        descriptor = AtmosphericChemicalConstituentType()
        assert hasattr(descriptor, "__get__")
        assert hasattr(descriptor, "__set__")
    
    def test_get_reads_from_section4_index_4(self):
        """__get__ should read from section4[4]."""
        descriptor = AtmosphericChemicalConstituentType()
        
        # Create mock object with section4
        mock_obj = type("MockMsg", (), {})()
        mock_obj.section4 = [0, 40, 2, 3, 40009, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        
        result = descriptor.__get__(mock_obj, None)
        
        # Should wrap the value in Grib2Metadata
        assert hasattr(result, "value")
        assert result.value == 40009
    
    def test_set_writes_to_section4_index_4(self):
        """__set__ should write to section4[4]."""
        descriptor = AtmosphericChemicalConstituentType()
        
        # Create mock object with section4
        mock_obj = type("MockMsg", (), {})()
        mock_obj.section4 = [0, 40, 2, 3, 0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        
        descriptor.__set__(mock_obj, 40008)
        
        assert mock_obj.section4[4] == 40008


class TestPatchRegistration:
    """Tests that the patch is properly registered."""
    
    def test_template_40_is_registered(self):
        """ProductDefinitionTemplate40 should be registered in grib2io."""
        import grib2io.templates as templates
        
        assert 40 in templates._pdt_by_pdtn
        assert templates._pdt_by_pdtn[40] == ProductDefinitionTemplate40
    
    def test_descriptors_have_pdt_40_indices(self):
        """Field descriptors should have indices defined for PDT 40."""
        import grib2io.templates as templates
        
        # Check that key descriptors have PDT 40 indices
        assert 40 in templates.TypeOfGeneratingProcess._key
        assert 40 in templates.UnitOfForecastTime._key
        assert 40 in templates.ValueOfForecastTime._key
        assert 40 in templates.TypeOfFirstFixedSurface._key
        
        # Indices should be shifted by +1 compared to PDT 0
        # (PDT 40 has an extra field at index 4)
        assert templates.TypeOfGeneratingProcess._key[40] == templates.TypeOfGeneratingProcess._key[0] + 1

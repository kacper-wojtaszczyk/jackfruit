"""
Tests for grib2.pdt40 module.

Tests the PDT 4.40 patch for atmospheric chemical constituents.
These tests require grib2io to be installed.
"""

from pipeline_python.grib2.pdt40 import (
    AtmosphericChemicalConstituentType,
    ProductDefinitionTemplate40,
)


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
        from grib2io import templates

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
        from grib2io import templates

        assert 40 in templates._pdt_by_pdtn
        assert templates._pdt_by_pdtn[40] == ProductDefinitionTemplate40

    def test_descriptors_have_pdt_40_indices(self):
        """Field descriptors should have indices defined for PDT 40."""
        from grib2io import templates

        # Check that key descriptors have PDT 40 indices
        assert 40 in templates.TypeOfGeneratingProcess._key
        assert 40 in templates.UnitOfForecastTime._key
        assert 40 in templates.ValueOfForecastTime._key
        assert 40 in templates.TypeOfFirstFixedSurface._key

        # Indices should be shifted by +1 compared to PDT 0
        # (PDT 40 has an extra field at index 4)
        assert templates.TypeOfGeneratingProcess._key[40] == templates.TypeOfGeneratingProcess._key[0] + 1


class TestImportOrder:
    """Tests that import order doesn't matter."""

    def test_grib2io_import_after_package_works(self):
        """Importing grib2io after pipeline_python.grib2 should work."""
        # This test verifies the fix for the circular import issue
        # If we got here, the import already succeeded
        from pipeline_python.grib2 import grib2io
        assert grib2io is not None

    def test_package_provides_patched_grib2io(self):
        """The grib2io from package should have PDT 40 registered."""
        from pipeline_python.grib2 import grib2io
        from grib2io import templates

        # Verify PDT 40 is available
        assert 40 in templates._pdt_by_pdtn

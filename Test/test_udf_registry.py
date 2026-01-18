"""
Tests for UDF Registry System

This module tests the UDFRegistry class and auto-registration functionality.

Author: PySpark Migration Team
Created: 2026-01-17
"""

import pytest
from pyspark.sql.functions import col, lit

from Helpers.SparkUDFs.UDFRegistry import (
    UDFRegistry,
    UDFCategory,
    UDFMetadata,
    get_registry
)
from Helpers.SparkUDFs.RegisterUDFs import initialize_udf_registry
from Helpers.SparkUDFs import (
    hex_to_decimal_udf,
    point_to_x_udf,
    geodesic_distance_udf
)


@pytest.fixture(scope="function")
def clean_registry():
    """Reset the UDF registry before each test"""
    UDFRegistry.reset_instance()
    yield
    UDFRegistry.reset_instance()


class TestUDFRegistryBasics:
    """Test basic UDF registry functionality"""

    def test_singleton_pattern(self, clean_registry):
        """Verify UDFRegistry follows singleton pattern"""
        registry1 = UDFRegistry.get_instance()
        registry2 = UDFRegistry.get_instance()
        assert registry1 is registry2

    def test_direct_instantiation_fails(self, clean_registry):
        """Verify direct instantiation raises error (must use get_instance)"""
        UDFRegistry.get_instance()  # Create the singleton
        with pytest.raises(RuntimeError, match="singleton"):
            UDFRegistry()

    def test_get_registry_convenience_function(self, clean_registry):
        """Test convenience function returns same instance"""
        registry1 = get_registry()
        registry2 = UDFRegistry.get_instance()
        assert registry1 is registry2

    def test_empty_registry(self, clean_registry):
        """Test empty registry behavior"""
        registry = UDFRegistry.get_instance()
        assert registry.count() == 0
        assert registry.list_all() == []
        assert not registry.is_initialized()


class TestUDFRegistration:
    """Test UDF registration functionality"""

    def test_register_simple_udf(self, clean_registry):
        """Test registering a simple UDF"""
        registry = UDFRegistry.get_instance()

        registry.register(
            name='test_udf',
            udf_func=hex_to_decimal_udf,
            description='Test UDF',
            category=UDFCategory.CONVERSION
        )

        assert registry.count() == 1
        assert registry.exists('test_udf')
        assert 'test_udf' in registry.list_all()

    def test_register_with_full_metadata(self, clean_registry):
        """Test registering a UDF with complete metadata"""
        registry = UDFRegistry.get_instance()

        registry.register(
            name='hex_to_decimal',
            udf_func=hex_to_decimal_udf,
            description='Convert hex to decimal',
            category=UDFCategory.CONVERSION,
            input_types=['string'],
            output_type='long',
            example='df.withColumn("id", hex_to_decimal_udf(col("hex_id")))',
            version='1.0.0'
        )

        metadata = registry.get_metadata('hex_to_decimal')
        assert metadata.name == 'hex_to_decimal'
        assert metadata.description == 'Convert hex to decimal'
        assert metadata.category == UDFCategory.CONVERSION
        assert metadata.input_types == ['string']
        assert metadata.output_type == 'long'
        assert 'hex_to_decimal_udf' in metadata.example
        assert metadata.version == '1.0.0'

    def test_register_duplicate_fails(self, clean_registry):
        """Test that registering duplicate UDF name raises error"""
        registry = UDFRegistry.get_instance()

        registry.register(
            name='test_udf',
            udf_func=hex_to_decimal_udf,
            description='First UDF',
            category=UDFCategory.CONVERSION
        )

        with pytest.raises(ValueError, match="already registered"):
            registry.register(
                name='test_udf',
                udf_func=point_to_x_udf,
                description='Second UDF',
                category=UDFCategory.GEOSPATIAL
            )


class TestUDFRetrieval:
    """Test UDF retrieval functionality"""

    def test_get_existing_udf(self, clean_registry):
        """Test retrieving a registered UDF"""
        registry = UDFRegistry.get_instance()
        registry.register(
            name='hex_to_decimal',
            udf_func=hex_to_decimal_udf,
            description='Test',
            category=UDFCategory.CONVERSION
        )

        retrieved_udf = registry.get('hex_to_decimal')
        assert retrieved_udf is hex_to_decimal_udf

    def test_get_nonexistent_udf_fails(self, clean_registry):
        """Test that retrieving non-existent UDF raises error"""
        registry = UDFRegistry.get_instance()

        with pytest.raises(KeyError, match="not registered"):
            registry.get('nonexistent_udf')

    def test_get_metadata_existing_udf(self, clean_registry):
        """Test retrieving metadata for existing UDF"""
        registry = UDFRegistry.get_instance()
        registry.register(
            name='test_udf',
            udf_func=hex_to_decimal_udf,
            description='Test description',
            category=UDFCategory.CONVERSION
        )

        metadata = registry.get_metadata('test_udf')
        assert isinstance(metadata, UDFMetadata)
        assert metadata.name == 'test_udf'
        assert metadata.description == 'Test description'

    def test_get_metadata_nonexistent_fails(self, clean_registry):
        """Test that getting metadata for non-existent UDF fails"""
        registry = UDFRegistry.get_instance()

        with pytest.raises(KeyError, match="not registered"):
            registry.get_metadata('nonexistent_udf')

    def test_exists_check(self, clean_registry):
        """Test exists() method"""
        registry = UDFRegistry.get_instance()
        registry.register(
            name='test_udf',
            udf_func=hex_to_decimal_udf,
            description='Test',
            category=UDFCategory.CONVERSION
        )

        assert registry.exists('test_udf')
        assert not registry.exists('nonexistent_udf')


class TestUDFListing:
    """Test UDF listing and categorization"""

    def test_list_all_udfs(self, clean_registry):
        """Test listing all registered UDFs"""
        registry = UDFRegistry.get_instance()

        registry.register('udf_a', hex_to_decimal_udf, 'Test A', UDFCategory.CONVERSION)
        registry.register('udf_b', point_to_x_udf, 'Test B', UDFCategory.GEOSPATIAL)
        registry.register('udf_c', geodesic_distance_udf, 'Test C', UDFCategory.GEOSPATIAL)

        all_udfs = registry.list_all()
        assert len(all_udfs) == 3
        assert all_udfs == ['udf_a', 'udf_b', 'udf_c']  # Alphabetically sorted

    def test_list_by_category(self, clean_registry):
        """Test listing UDFs by category"""
        registry = UDFRegistry.get_instance()

        registry.register('udf_conv1', hex_to_decimal_udf, 'Conv 1', UDFCategory.CONVERSION)
        registry.register('udf_geo1', point_to_x_udf, 'Geo 1', UDFCategory.GEOSPATIAL)
        registry.register('udf_geo2', geodesic_distance_udf, 'Geo 2', UDFCategory.GEOSPATIAL)
        registry.register('udf_conv2', hex_to_decimal_udf, 'Conv 2', UDFCategory.CONVERSION)

        conversion_udfs = registry.list_by_category(UDFCategory.CONVERSION)
        geospatial_udfs = registry.list_by_category(UDFCategory.GEOSPATIAL)

        assert len(conversion_udfs) == 2
        assert set(conversion_udfs) == {'udf_conv1', 'udf_conv2'}
        assert len(geospatial_udfs) == 2
        assert set(geospatial_udfs) == {'udf_geo1', 'udf_geo2'}

    def test_get_categories(self, clean_registry):
        """Test getting all categories with registered UDFs"""
        registry = UDFRegistry.get_instance()

        registry.register('udf1', hex_to_decimal_udf, 'Test', UDFCategory.CONVERSION)
        registry.register('udf2', point_to_x_udf, 'Test', UDFCategory.GEOSPATIAL)

        categories = registry.get_categories()
        assert len(categories) == 2
        assert UDFCategory.CONVERSION in categories
        assert UDFCategory.GEOSPATIAL in categories
        assert UDFCategory.TEMPORAL not in categories  # No temporal UDFs registered


class TestAutoRegistration:
    """Test automatic UDF registration"""

    def test_initialize_udf_registry(self, clean_registry):
        """Test that initialize_udf_registry registers all UDFs"""
        registry = initialize_udf_registry()

        # Verify registry is populated
        assert registry.count() > 0
        assert registry.is_initialized()

        # Check for expected UDFs
        expected_udfs = [
            'point_to_tuple',
            'point_to_x',
            'point_to_y',
            'geodesic_distance',
            'xy_distance',
            'hex_to_decimal',
            'direction_and_dist_to_xy'
        ]

        for udf_name in expected_udfs:
            assert registry.exists(udf_name), f"UDF '{udf_name}' should be registered"

    def test_idempotent_initialization(self, clean_registry):
        """Test that multiple calls to initialize don't re-register"""
        registry1 = initialize_udf_registry()
        count1 = registry1.count()

        # Call again
        registry2 = initialize_udf_registry()
        count2 = registry2.count()

        # Should be same instance with same count
        assert registry1 is registry2
        assert count1 == count2

    def test_registered_udf_categories(self, clean_registry):
        """Test that registered UDFs have correct categories"""
        registry = initialize_udf_registry()

        # Geospatial UDFs
        geo_udfs = registry.list_by_category(UDFCategory.GEOSPATIAL)
        assert 'point_to_x' in geo_udfs
        assert 'geodesic_distance' in geo_udfs

        # Conversion UDFs
        conv_udfs = registry.list_by_category(UDFCategory.CONVERSION)
        assert 'hex_to_decimal' in conv_udfs
        assert 'direction_and_dist_to_xy' in conv_udfs

    def test_registered_udf_metadata(self, clean_registry):
        """Test that registered UDFs have proper metadata"""
        registry = initialize_udf_registry()

        # Check hex_to_decimal metadata
        hex_meta = registry.get_metadata('hex_to_decimal')
        assert hex_meta.description
        assert hex_meta.input_types == ['string']
        assert hex_meta.output_type == 'long'
        assert hex_meta.example
        assert hex_meta.version == '1.0.0'

        # Check geodesic_distance metadata
        geo_meta = registry.get_metadata('geodesic_distance')
        assert geo_meta.description
        assert len(geo_meta.input_types) == 4  # lat1, lon1, lat2, lon2
        assert geo_meta.output_type == 'double'


class TestUDFRegistryIntegration:
    """Integration tests using the registry with real PySpark operations"""

    def test_use_registry_udf_in_dataframe(self, clean_registry, spark_session):
        """Test using a registry-retrieved UDF in a real DataFrame operation"""
        registry = initialize_udf_registry()

        # Create test DataFrame
        data = [('0x1a', ), ('0x2b', ), ('0x3c', )]
        df = spark_session.createDataFrame(data, ['hex_id'])

        # Retrieve UDF from registry and use it
        hex_udf = registry.get('hex_to_decimal')
        df_result = df.withColumn('decimal_id', hex_udf(col('hex_id')))

        # Verify results
        results = [row['decimal_id'] for row in df_result.collect()]
        assert 26 in results  # 0x1a = 26
        assert 43 in results  # 0x2b = 43
        assert 60 in results  # 0x3c = 60

    def test_list_all_geospatial_udfs(self, clean_registry):
        """Test listing all geospatial UDFs for documentation"""
        registry = initialize_udf_registry()

        geo_udfs = registry.list_by_category(UDFCategory.GEOSPATIAL)

        # Should have multiple geospatial UDFs
        assert len(geo_udfs) >= 5
        assert 'point_to_x' in geo_udfs
        assert 'point_to_y' in geo_udfs
        assert 'geodesic_distance' in geo_udfs
        assert 'xy_distance' in geo_udfs


class TestUDFDocumentation:
    """Test documentation generation"""

    def test_generate_documentation(self, clean_registry):
        """Test that documentation can be generated"""
        registry = initialize_udf_registry()

        doc = registry.generate_documentation()

        assert isinstance(doc, str)
        assert len(doc) > 0
        assert 'PySpark UDF Registry Documentation' in doc
        assert 'GEOSPATIAL' in doc
        assert 'CONVERSION' in doc
        assert 'hex_to_decimal' in doc
        assert 'geodesic_distance' in doc

    def test_metadata_to_dict(self, clean_registry):
        """Test metadata serialization to dictionary"""
        registry = UDFRegistry.get_instance()

        registry.register(
            name='test_udf',
            udf_func=hex_to_decimal_udf,
            description='Test UDF',
            category=UDFCategory.CONVERSION,
            input_types=['string'],
            output_type='long',
            example='example code',
            version='2.0.0'
        )

        metadata = registry.get_metadata('test_udf')
        metadata_dict = metadata.to_dict()

        assert metadata_dict['name'] == 'test_udf'
        assert metadata_dict['description'] == 'Test UDF'
        assert metadata_dict['category'] == 'conversion'
        assert metadata_dict['input_types'] == ['string']
        assert metadata_dict['output_type'] == 'long'
        assert metadata_dict['example'] == 'example code'
        assert metadata_dict['version'] == '2.0.0'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

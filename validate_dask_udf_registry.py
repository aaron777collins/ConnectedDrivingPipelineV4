#!/usr/bin/env python3
"""
Validation script for DaskUDFRegistry.

This script validates the DaskUDFRegistry functionality including:
- Singleton pattern
- Function registration and retrieval
- Category filtering
- Documentation generation
- Auto-initialization

Author: Dask Migration Team
Created: 2026-01-17
"""

import sys
from Helpers.DaskUDFs.DaskUDFRegistry import DaskUDFRegistry, FunctionCategory, get_registry
from Helpers.DaskUDFs.RegisterDaskUDFs import initialize_dask_udf_registry


def test_singleton_pattern():
    """Test that DaskUDFRegistry follows singleton pattern"""
    print("Testing singleton pattern...")
    registry1 = DaskUDFRegistry.get_instance()
    registry2 = DaskUDFRegistry.get_instance()
    assert registry1 is registry2, "Registry instances should be the same"
    print("✓ Singleton pattern works correctly")


def test_registration_and_retrieval():
    """Test function registration and retrieval"""
    print("\nTesting function registration and retrieval...")

    # Initialize registry
    registry = initialize_dask_udf_registry()

    # Check all expected functions are registered
    expected_functions = [
        'point_to_tuple',
        'point_to_x',
        'point_to_y',
        'geodesic_distance',
        'xy_distance',
        'hex_to_decimal',
        'direction_and_dist_to_xy'
    ]

    for func_name in expected_functions:
        assert registry.exists(func_name), f"Function '{func_name}' should be registered"
        func = registry.get(func_name)
        assert callable(func), f"Retrieved '{func_name}' should be callable"

    print(f"✓ All {len(expected_functions)} functions registered and retrievable")


def test_category_filtering():
    """Test filtering functions by category"""
    print("\nTesting category filtering...")
    registry = get_registry()

    # Test geospatial category
    geo_funcs = registry.list_by_category(FunctionCategory.GEOSPATIAL)
    expected_geo = ['geodesic_distance', 'point_to_tuple', 'point_to_x', 'point_to_y', 'xy_distance']
    assert sorted(geo_funcs) == sorted(expected_geo), f"Geospatial functions mismatch"
    print(f"✓ Geospatial category: {len(geo_funcs)} functions")

    # Test conversion category
    conv_funcs = registry.list_by_category(FunctionCategory.CONVERSION)
    expected_conv = ['direction_and_dist_to_xy', 'hex_to_decimal']
    assert sorted(conv_funcs) == sorted(expected_conv), f"Conversion functions mismatch"
    print(f"✓ Conversion category: {len(conv_funcs)} functions")


def test_metadata():
    """Test function metadata retrieval"""
    print("\nTesting metadata retrieval...")
    registry = get_registry()

    # Test hex_to_decimal metadata
    hex_meta = registry.get_metadata('hex_to_decimal')
    assert hex_meta.name == 'hex_to_decimal', "Metadata name mismatch"
    assert hex_meta.category == FunctionCategory.CONVERSION, "Metadata category mismatch"
    assert 'str' in hex_meta.input_types, "Metadata input types mismatch"
    assert hex_meta.output_type == 'int', "Metadata output type mismatch"
    print(f"✓ Metadata for 'hex_to_decimal': {hex_meta.description[:50]}...")

    # Test point_to_x metadata
    point_meta = registry.get_metadata('point_to_x')
    assert point_meta.category == FunctionCategory.GEOSPATIAL, "Metadata category mismatch"
    assert point_meta.output_type == 'float', "Metadata output type mismatch"
    print(f"✓ Metadata for 'point_to_x': {point_meta.description[:50]}...")


def test_list_all():
    """Test listing all functions"""
    print("\nTesting list all functions...")
    registry = get_registry()

    all_funcs = registry.list_all()
    assert len(all_funcs) == 7, f"Expected 7 functions, got {len(all_funcs)}"
    assert all_funcs == sorted(all_funcs), "Functions should be sorted alphabetically"
    print(f"✓ Total functions registered: {len(all_funcs)}")
    print(f"  Functions: {', '.join(all_funcs)}")


def test_count():
    """Test function count"""
    print("\nTesting function count...")
    registry = get_registry()

    count = registry.count()
    assert count == 7, f"Expected 7 functions, got {count}"
    print(f"✓ Function count: {count}")


def test_documentation_generation():
    """Test documentation generation"""
    print("\nTesting documentation generation...")
    registry = get_registry()

    docs = registry.generate_documentation()
    assert "# Dask Function Registry Documentation" in docs, "Missing documentation header"
    assert "Total Functions: 7" in docs, "Missing function count"
    assert "GEOSPATIAL" in docs, "Missing geospatial category"
    assert "CONVERSION" in docs, "Missing conversion category"
    assert "point_to_x" in docs, "Missing point_to_x function"
    assert "hex_to_decimal" in docs, "Missing hex_to_decimal function"
    print("✓ Documentation generation successful")
    print(f"  Documentation length: {len(docs)} characters")


def test_error_handling():
    """Test error handling for invalid operations"""
    print("\nTesting error handling...")
    registry = get_registry()

    # Test retrieving non-existent function
    try:
        registry.get('nonexistent_function')
        assert False, "Should raise KeyError for non-existent function"
    except KeyError as e:
        assert "not registered" in str(e), "Error message should indicate function not registered"
        print("✓ KeyError raised correctly for non-existent function")

    # Test duplicate registration (need to reset for this)
    DaskUDFRegistry.reset_instance()
    new_registry = DaskUDFRegistry.get_instance()

    def dummy_func(x):
        return x

    new_registry.register(
        name='test_func',
        func=dummy_func,
        description='Test function',
        category=FunctionCategory.UTILITY
    )

    try:
        new_registry.register(
            name='test_func',
            func=dummy_func,
            description='Duplicate',
            category=FunctionCategory.UTILITY
        )
        assert False, "Should raise ValueError for duplicate registration"
    except ValueError as e:
        assert "already registered" in str(e), "Error message should indicate duplicate"
        print("✓ ValueError raised correctly for duplicate registration")

    # Reset to original state
    DaskUDFRegistry.reset_instance()
    initialize_dask_udf_registry()


def test_function_invocation():
    """Test that registered functions can be invoked correctly"""
    print("\nTesting function invocation...")
    registry = get_registry()

    # Test hex_to_decimal
    hex_func = registry.get('hex_to_decimal')
    result = hex_func('A5')
    assert result == 165, f"hex_to_decimal('A5') should return 165, got {result}"
    print("✓ hex_to_decimal('A5') = 165")

    # Test point_to_x
    point_to_x_func = registry.get('point_to_x')
    result = point_to_x_func('POINT (-104.6744332 41.1509182)')
    assert abs(result - (-104.6744332)) < 1e-6, f"point_to_x should return -104.6744332, got {result}"
    print(f"✓ point_to_x('POINT (-104.6744332 41.1509182)') = {result}")

    # Test geodesic_distance
    geo_dist_func = registry.get('geodesic_distance')
    result = geo_dist_func(41.25, -105.93, 41.25, -105.93)
    assert abs(result - 0.0) < 1e-6, f"geodesic_distance same point should be ~0, got {result}"
    print(f"✓ geodesic_distance(same point) = {result:.6f} meters")


def test_initialization_flag():
    """Test initialization flag management"""
    print("\nTesting initialization flag...")

    # Reset and create fresh instance
    DaskUDFRegistry.reset_instance()
    registry = DaskUDFRegistry.get_instance()

    assert not registry.is_initialized(), "Registry should not be initialized"

    # Initialize
    initialize_dask_udf_registry()
    assert registry.is_initialized(), "Registry should be initialized after initialize_dask_udf_registry()"

    # Try initializing again (should be idempotent)
    registry2 = initialize_dask_udf_registry()
    assert registry2 is registry, "Should return same registry instance"
    assert registry2.count() == 7, "Should still have 7 functions"

    print("✓ Initialization flag works correctly")


def main():
    """Run all validation tests"""
    print("="*60)
    print("DaskUDFRegistry Validation Suite")
    print("="*60)

    try:
        test_singleton_pattern()
        test_registration_and_retrieval()
        test_category_filtering()
        test_metadata()
        test_list_all()
        test_count()
        test_documentation_generation()
        test_error_handling()
        test_function_invocation()
        test_initialization_flag()

        print("\n" + "="*60)
        print("✓ ALL TESTS PASSED")
        print("="*60)
        return 0

    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())

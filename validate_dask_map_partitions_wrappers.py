#!/usr/bin/env python3
"""
Validation script for Dask map_partitions wrappers.

Tests all wrapper functions in MapPartitionsWrappers.py to ensure they:
1. Correctly apply UDF operations to Dask DataFrame partitions
2. Handle edge cases (None values, invalid data)
3. Produce expected outputs matching direct UDF application
4. Work with proper meta specifications

Run: python validate_dask_map_partitions_wrappers.py
"""

import sys
import dask.dataframe as dd
import pandas as pd
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, '/tmp/original-repo')

from Helpers.DaskUDFs import (
    # Wrappers
    extract_xy_coordinates,
    extract_coordinates_as_tuple,
    convert_hex_id_column,
    parse_and_convert_coordinates,
    calculate_distance_from_reference,
    calculate_pairwise_xy_distance,
    apply_positional_offset,
    apply_udf_to_column,
    batch_apply_udfs,
    apply_udf_conditionally,
    # Direct UDFs
    point_to_x,
    point_to_y,
    point_to_tuple,
    hex_to_decimal,
    geodesic_distance,
    xy_distance,
)


def test_extract_xy_coordinates():
    """Test extract_xy_coordinates wrapper."""
    print("\n[Test 1] Testing extract_xy_coordinates...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_position': [
            'POINT (-104.6744332 41.1509182)',
            'POINT (-105.0844 40.5853)',
            'POINT (-104.9903 39.7392)',
            None,
        ]
    })
    df = dd.from_pandas(df_pandas, npartitions=2)

    # Apply wrapper
    result_df = df.map_partitions(
        extract_xy_coordinates,
        point_col='coreData_position',
        meta=df._meta.assign(x_pos=0.0, y_pos=0.0)
    ).compute()

    # Validate
    assert 'x_pos' in result_df.columns
    assert 'y_pos' in result_df.columns

    # Check first row
    assert abs(result_df['x_pos'].iloc[0] - (-104.6744332)) < 1e-5
    assert abs(result_df['y_pos'].iloc[0] - 41.1509182) < 1e-5

    # Check None handling
    assert pd.isna(result_df['x_pos'].iloc[3])
    assert pd.isna(result_df['y_pos'].iloc[3])

    print("  ✓ extract_xy_coordinates works correctly")
    return True


def test_extract_coordinates_as_tuple():
    """Test extract_coordinates_as_tuple wrapper."""
    print("\n[Test 2] Testing extract_coordinates_as_tuple...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_position': [
            'POINT (-104.6744332 41.1509182)',
            'POINT (-105.0844 40.5853)',
        ]
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Apply wrapper (meta needs proper object column initialization)
    meta = df._meta.copy()
    meta['coords'] = pd.Series(dtype='object')

    result_df = df.map_partitions(
        extract_coordinates_as_tuple,
        point_col='coreData_position',
        meta=meta
    ).compute()

    # Validate
    assert 'coords' in result_df.columns
    coords = result_df['coords'].iloc[0]
    assert isinstance(coords, tuple)
    assert len(coords) == 2
    assert abs(coords[0] - (-104.6744332)) < 1e-5
    assert abs(coords[1] - 41.1509182) < 1e-5

    print("  ✓ extract_coordinates_as_tuple works correctly")
    return True


def test_convert_hex_id_column():
    """Test convert_hex_id_column wrapper."""
    print("\n[Test 3] Testing convert_hex_id_column...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_id': ['0x1A2B', '0xFF', '0x100', None]
    })
    df = dd.from_pandas(df_pandas, npartitions=2)

    # Apply wrapper
    result_df = df.map_partitions(
        convert_hex_id_column,
        hex_col='coreData_id',
        meta=df._meta.assign(coreData_id_decimal=0)
    ).compute()

    # Validate
    assert 'coreData_id_decimal' in result_df.columns
    assert result_df['coreData_id_decimal'].iloc[0] == 6699  # 0x1A2B
    assert result_df['coreData_id_decimal'].iloc[1] == 255   # 0xFF
    assert result_df['coreData_id_decimal'].iloc[2] == 256   # 0x100
    assert pd.isna(result_df['coreData_id_decimal'].iloc[3])

    print("  ✓ convert_hex_id_column works correctly")
    return True


def test_parse_and_convert_coordinates():
    """Test parse_and_convert_coordinates wrapper (combined operation)."""
    print("\n[Test 4] Testing parse_and_convert_coordinates...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_position': ['POINT (-104.6744332 41.1509182)', 'POINT (-105.0844 40.5853)'],
        'coreData_id': ['0x1A2B', '0xFF']
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Apply wrapper
    result_df = df.map_partitions(
        parse_and_convert_coordinates,
        meta=df._meta.assign(x_pos=0.0, y_pos=0.0, coreData_id_decimal=0)
    ).compute()

    # Validate coordinates
    assert 'x_pos' in result_df.columns
    assert 'y_pos' in result_df.columns
    assert abs(result_df['x_pos'].iloc[0] - (-104.6744332)) < 1e-5

    # Validate hex conversion
    assert 'coreData_id_decimal' in result_df.columns
    assert result_df['coreData_id_decimal'].iloc[0] == 6699

    print("  ✓ parse_and_convert_coordinates works correctly")
    return True


def test_calculate_distance_from_reference():
    """Test calculate_distance_from_reference wrapper."""
    print("\n[Test 5] Testing calculate_distance_from_reference...")

    # Create test data (Fort Collins area)
    df_pandas = pd.DataFrame({
        'latitude': [40.5853, 41.1509182],
        'longitude': [-105.0844, -104.6744332]
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Calculate distance from CSU (Fort Collins)
    result_df = df.map_partitions(
        calculate_distance_from_reference,
        lat_col='latitude',
        lon_col='longitude',
        ref_lat=40.5853,
        ref_lon=-105.0844,
        meta=df._meta.assign(distance_from_ref=0.0)
    ).compute()

    # Validate
    assert 'distance_from_ref' in result_df.columns
    # First row should be ~0 (same location)
    assert result_df['distance_from_ref'].iloc[0] < 1.0  # Within 1 meter
    # Second row should be >0
    assert result_df['distance_from_ref'].iloc[1] > 1000.0  # More than 1km

    print("  ✓ calculate_distance_from_reference works correctly")
    return True


def test_calculate_pairwise_xy_distance():
    """Test calculate_pairwise_xy_distance wrapper."""
    print("\n[Test 6] Testing calculate_pairwise_xy_distance...")

    # Create test data
    df_pandas = pd.DataFrame({
        'x1': [0.0, 10.0],
        'y1': [0.0, 20.0],
        'x2': [3.0, 13.0],
        'y2': [4.0, 24.0]
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Calculate distance
    result_df = df.map_partitions(
        calculate_pairwise_xy_distance,
        x1_col='x1',
        y1_col='y1',
        x2_col='x2',
        y2_col='y2',
        meta=df._meta.assign(xy_distance=0.0)
    ).compute()

    # Validate
    assert 'xy_distance' in result_df.columns
    # First row: distance from (0,0) to (3,4) = 5.0
    assert abs(result_df['xy_distance'].iloc[0] - 5.0) < 1e-5
    # Second row: distance from (10,20) to (13,24) = 5.0
    assert abs(result_df['xy_distance'].iloc[1] - 5.0) < 1e-5

    print("  ✓ calculate_pairwise_xy_distance works correctly")
    return True


def test_apply_positional_offset():
    """Test apply_positional_offset wrapper."""
    print("\n[Test 7] Testing apply_positional_offset...")

    # Create test data
    df_pandas = pd.DataFrame({
        'x_pos': [100.0, 200.0, 300.0],
        'y_pos': [100.0, 200.0, 300.0],
        'attack_direction': [0.0, 90.0, 180.0],  # North, East, South
        'attack_distance': [10.0, 20.0, 30.0],
        'isAttacker': [1, 0, 1]  # First and third are attackers
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Apply offset
    result_df = df.map_partitions(
        apply_positional_offset,
        meta=df._meta.assign(x_pos_offset=0.0, y_pos_offset=0.0)
    ).compute()

    # Validate
    assert 'x_pos_offset' in result_df.columns
    assert 'y_pos_offset' in result_df.columns

    # Row 0: attacker, should be offset
    assert result_df['x_pos_offset'].iloc[0] != result_df['x_pos'].iloc[0]

    # Row 1: benign, should NOT be offset
    assert result_df['x_pos_offset'].iloc[1] == result_df['x_pos'].iloc[1]
    assert result_df['y_pos_offset'].iloc[1] == result_df['y_pos'].iloc[1]

    # Row 2: attacker, should be offset
    assert result_df['x_pos_offset'].iloc[2] != result_df['x_pos'].iloc[2]

    print("  ✓ apply_positional_offset works correctly")
    return True


def test_apply_udf_to_column():
    """Test apply_udf_to_column generic wrapper."""
    print("\n[Test 8] Testing apply_udf_to_column...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_position': ['POINT (-104.6744332 41.1509182)', 'POINT (-105.0844 40.5853)']
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Apply generic wrapper with point_to_x
    result_df = df.map_partitions(
        apply_udf_to_column,
        udf_func=point_to_x,
        input_col='coreData_position',
        output_col='x_pos',
        meta=df._meta.assign(x_pos=0.0)
    ).compute()

    # Validate
    assert 'x_pos' in result_df.columns
    assert abs(result_df['x_pos'].iloc[0] - (-104.6744332)) < 1e-5

    print("  ✓ apply_udf_to_column works correctly")
    return True


def test_batch_apply_udfs():
    """Test batch_apply_udfs for multiple operations."""
    print("\n[Test 9] Testing batch_apply_udfs...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_position': ['POINT (-104.6744332 41.1509182)', 'POINT (-105.0844 40.5853)'],
        'coreData_id': ['0x1A2B', '0xFF']
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Define operations
    operations = [
        {'func': point_to_x, 'input_col': 'coreData_position', 'output_col': 'x_pos'},
        {'func': point_to_y, 'input_col': 'coreData_position', 'output_col': 'y_pos'},
        {'func': hex_to_decimal, 'input_col': 'coreData_id', 'output_col': 'id_decimal'},
    ]

    # Apply batch operations
    result_df = df.map_partitions(
        batch_apply_udfs,
        operations=operations,
        meta=df._meta.assign(x_pos=0.0, y_pos=0.0, id_decimal=0)
    ).compute()

    # Validate all outputs
    assert 'x_pos' in result_df.columns
    assert 'y_pos' in result_df.columns
    assert 'id_decimal' in result_df.columns

    assert abs(result_df['x_pos'].iloc[0] - (-104.6744332)) < 1e-5
    assert abs(result_df['y_pos'].iloc[0] - 41.1509182) < 1e-5
    assert result_df['id_decimal'].iloc[0] == 6699

    print("  ✓ batch_apply_udfs works correctly")
    return True


def test_apply_udf_conditionally():
    """Test apply_udf_conditionally for selective application."""
    print("\n[Test 10] Testing apply_udf_conditionally...")

    # Create test data
    df_pandas = pd.DataFrame({
        'coreData_id': ['0x1A2B', '0xFF', '0x100'],
        'isAttacker': [1, 0, 1]
    })
    df = dd.from_pandas(df_pandas, npartitions=1)

    # Apply hex conversion only to attackers
    result_df = df.map_partitions(
        apply_udf_conditionally,
        udf_func=hex_to_decimal,
        input_col='coreData_id',
        output_col='id_decimal',
        condition_col='isAttacker',
        condition_value=1,
        default_value=-1,  # Use -1 for non-attackers
        meta=df._meta.assign(id_decimal=0)
    ).compute()

    # Validate
    assert 'id_decimal' in result_df.columns
    assert result_df['id_decimal'].iloc[0] == 6699   # Attacker: converted
    assert result_df['id_decimal'].iloc[1] == -1     # Benign: default value
    assert result_df['id_decimal'].iloc[2] == 256    # Attacker: converted

    print("  ✓ apply_udf_conditionally works correctly")
    return True


def test_performance_comparison():
    """Compare performance: multiple apply() vs single map_partitions()."""
    print("\n[Test 11] Testing performance comparison...")

    # Create larger test data
    n_rows = 10000
    df_pandas = pd.DataFrame({
        'coreData_position': ['POINT (-104.6744332 41.1509182)'] * n_rows
    })
    df = dd.from_pandas(df_pandas, npartitions=10)

    # Method 1: Multiple apply() calls (SLOW)
    import time
    start = time.time()
    df_slow = df.copy()
    df_slow['x_pos'] = df_slow['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8'))
    df_slow['y_pos'] = df_slow['coreData_position'].apply(point_to_y, meta=('y_pos', 'f8'))
    result_slow = df_slow.compute()
    time_slow = time.time() - start

    # Method 2: Single map_partitions() call (FAST)
    start = time.time()
    df_fast = df.copy()
    df_fast = df_fast.map_partitions(
        extract_xy_coordinates,
        meta=df._meta.assign(x_pos=0.0, y_pos=0.0)
    )
    result_fast = df_fast.compute()
    time_fast = time.time() - start

    # Validate outputs are identical
    pd.testing.assert_frame_equal(
        result_slow[['x_pos', 'y_pos']],
        result_fast[['x_pos', 'y_pos']]
    )

    # Performance comparison
    speedup = time_slow / time_fast
    print(f"  Multiple apply() calls: {time_slow:.3f}s")
    print(f"  Single map_partitions(): {time_fast:.3f}s")
    print(f"  Speedup: {speedup:.2f}x")

    # map_partitions should be faster (or at least not slower)
    assert speedup >= 0.8, f"map_partitions slower than expected: {speedup:.2f}x"

    print("  ✓ Performance comparison validated")
    return True


def run_all_tests():
    """Run all validation tests."""
    print("="*70)
    print("Dask Map Partitions Wrappers Validation")
    print("="*70)

    tests = [
        test_extract_xy_coordinates,
        test_extract_coordinates_as_tuple,
        test_convert_hex_id_column,
        test_parse_and_convert_coordinates,
        test_calculate_distance_from_reference,
        test_calculate_pairwise_xy_distance,
        test_apply_positional_offset,
        test_apply_udf_to_column,
        test_batch_apply_udfs,
        test_apply_udf_conditionally,
        test_performance_comparison,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"  ✗ {test.__name__} FAILED: {e}")
            failed += 1

    print("\n" + "="*70)
    print(f"Results: {passed} passed, {failed} failed")
    print("="*70)

    if failed == 0:
        print("\n✓ All map_partitions wrapper tests PASSED!")
        return True
    else:
        print(f"\n✗ {failed} test(s) FAILED")
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

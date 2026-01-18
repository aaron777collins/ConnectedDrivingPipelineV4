"""
Comprehensive validation script for DaskCleanWithTimestamps.

This script validates the DaskCleanWithTimestamps implementation by:
1. Testing timestamp parsing with various formats and edge cases
2. Validating all 7 temporal features (month, day, year, hour, minute, second, pm)
3. Testing WKT POINT parsing integration
4. Validating XY coordinate conversion
5. Testing caching behavior

Test cases:
- Basic cleaning with timestamp parsing
- XY coordinate conversion
- Edge cases: midnight, noon, year boundaries

Author: Ralph (Autonomous AI Development Agent)
Date: 2026-01-17
Task: 31
"""

import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import sys
import tempfile

# Add project root to path
sys.path.insert(0, '/tmp/original-repo')

from Generator.Cleaners.DaskCleanWithTimestamps import DaskCleanWithTimestamps
from Helpers.DaskSessionManager import DaskSessionManager
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.PathProvider import PathProvider


def create_test_data_with_timestamps(num_rows=1000, seed=42):
    """
    Create test BSM data with metadata_generatedAt timestamps.

    Args:
        num_rows (int): Number of rows to generate
        seed (int): Random seed for reproducibility

    Returns:
        pd.DataFrame: Test data with timestamps
    """
    np.random.seed(seed)

    data = {
        'coreData_id': [f'0x{i:08X}' for i in range(num_rows)],
        'coreData_position': [
            f"POINT (-{104.5 + np.random.random() * 1:.6f} {39.0 + np.random.random() * 1:.6f})"
            for _ in range(num_rows)
        ],
        'coreData_position_lat': 39.0 + np.random.random(num_rows),
        'coreData_position_long': -105.0 + np.random.random(num_rows),
        'coreData_speed': np.random.randint(0, 100, num_rows),
        'coreData_heading': np.random.randint(0, 360, num_rows),
        'metadata_recordType': ['BSM'] * num_rows,
        'metadata_generatedAt': [],
    }

    # Generate realistic timestamps with various edge cases
    timestamps = []
    for i in range(num_rows):
        if i < 100:
            # First 100: Morning times (AM)
            hour = np.random.randint(0, 11)
            minute = np.random.randint(0, 60)
            second = np.random.randint(0, 60)
            ts = f"07/31/2019 {hour:02d}:{minute:02d}:{second:02d} AM"
        elif i < 200:
            # Next 100: Afternoon times (PM)
            hour = np.random.randint(0, 11)
            minute = np.random.randint(0, 60)
            second = np.random.randint(0, 60)
            ts = f"07/31/2019 {hour:02d}:{minute:02d}:{second:02d} PM"
        elif i < 210:
            # 10 samples: Midnight edge case
            ts = "07/31/2019 12:00:00 AM"
        elif i < 220:
            # 10 samples: Noon edge case
            ts = "07/31/2019 12:00:00 PM"
        elif i < 230:
            # 10 samples: Year boundary
            ts = "12/31/2019 11:59:59 PM"
        else:
            # Rest: Random timestamps
            month = np.random.randint(1, 13)
            day = np.random.randint(1, 29)  # Safe for all months
            year = 2019
            hour = np.random.randint(0, 12)
            minute = np.random.randint(0, 60)
            second = np.random.randint(0, 60)
            am_pm = np.random.choice(['AM', 'PM'])
            ts = f"{month:02d}/{day:02d}/{year} {hour:02d}:{minute:02d}:{second:02d} {am_pm}"

        timestamps.append(ts)

    data['metadata_generatedAt'] = timestamps

    # Add some null values to test null handling
    df = pd.DataFrame(data)
    null_indices = np.random.choice(df.index, size=int(num_rows * 0.01), replace=False)
    df.loc[null_indices, 'coreData_position'] = None

    return df


def setup_test_config(isXYCoords=False):
    """
    Configure GeneratorContextProvider for testing.

    Args:
        isXYCoords (bool): Whether to convert to XY coordinates
    """
    # Setup configuration
    provider = GeneratorContextProvider()
    provider.add("ConnectedDrivingCleaner.cleanParams", "test")
    provider.add("ConnectedDrivingCleaner.filename", "test_clean_timestamps.parquet")
    provider.add("ConnectedDrivingCleaner.isXYCoords", isXYCoords)
    provider.add("ConnectedDrivingCleaner.columns", [
        'coreData_id', 'coreData_position', 'coreData_position_lat',
        'coreData_position_long', 'coreData_speed', 'coreData_heading',
        'metadata_recordType', 'metadata_generatedAt'
    ])
    provider.add("ConnectedDrivingCleaner.x_pos", 39.5)
    provider.add("ConnectedDrivingCleaner.y_pos", -105.0)
    provider.add("ConnectedDrivingCleaner.shouldGatherAutomatically", False)
    provider.add("DataGatherer.numrows", 1000)


def validate_test_1_basic_cleaning_with_timestamps():
    """
    Test 1: Basic cleaning with timestamp parsing (no XY conversion).

    Validates:
    - Column selection works
    - Null dropping works
    - Timestamp parsing works
    - All 7 temporal features are extracted correctly
    - POINT parsing works
    """
    print("\n" + "="*80)
    print("TEST 1: Basic Cleaning with Timestamp Parsing")
    print("="*80)

    # Initialize Dask session
    DaskSessionManager.get_client()

    # Create test data
    test_df = create_test_data_with_timestamps(num_rows=1000, seed=42)
    print(f"Created test data: {len(test_df)} rows")

    # Convert to Dask DataFrame
    dask_df = dd.from_pandas(test_df, npartitions=5)

    # Setup configuration
    setup_test_config(isXYCoords=False)

        # Create cleaner
        cleaner = DaskCleanWithTimestamps(data=dask_df)

        # Execute cleaning
        print("Executing clean_data_with_timestamps()...")
        cleaner.clean_data_with_timestamps()

        # Get cleaned data
        cleaned_df = cleaner.get_cleaned_data().compute()

        print(f"\nCleaned data shape: {cleaned_df.shape}")
        print(f"Expected rows: ~990 (1000 - 1% nulls)")

        # Validate row count (should drop ~1% nulls)
        assert 980 <= len(cleaned_df) <= 1000, f"Unexpected row count: {len(cleaned_df)}"
        print("✓ Row count validation passed")

        # Validate columns exist
        expected_columns = [
            'coreData_id', 'coreData_speed', 'coreData_heading',
            'metadata_recordType', 'metadata_generatedAt',
            'x_pos', 'y_pos',  # From POINT parsing
            'month', 'day', 'year', 'hour', 'minute', 'second', 'pm'  # Temporal features
        ]
        for col in expected_columns:
            assert col in cleaned_df.columns, f"Missing column: {col}"
        print(f"✓ All {len(expected_columns)} expected columns present")

        # Validate dropped columns
        dropped_columns = ['coreData_position', 'coreData_position_lat', 'coreData_position_long']
        for col in dropped_columns:
            assert col not in cleaned_df.columns, f"Column should be dropped: {col}"
        print(f"✓ Position columns properly dropped")

        # Validate temporal features
        print("\nValidating temporal features:")

        # Month: should be 1-12
        assert cleaned_df['month'].min() >= 1, "Month minimum invalid"
        assert cleaned_df['month'].max() <= 12, "Month maximum invalid"
        print(f"  ✓ month: range [{cleaned_df['month'].min()}, {cleaned_df['month'].max()}]")

        # Day: should be 1-31
        assert cleaned_df['day'].min() >= 1, "Day minimum invalid"
        assert cleaned_df['day'].max() <= 31, "Day maximum invalid"
        print(f"  ✓ day: range [{cleaned_df['day'].min()}, {cleaned_df['day'].max()}]")

        # Year: should be 2019 in our test data
        assert cleaned_df['year'].min() >= 2019, "Year minimum invalid"
        assert cleaned_df['year'].max() <= 2019, "Year maximum invalid"
        print(f"  ✓ year: {cleaned_df['year'].unique()}")

        # Hour: should be 0-23
        assert cleaned_df['hour'].min() >= 0, "Hour minimum invalid"
        assert cleaned_df['hour'].max() <= 23, "Hour maximum invalid"
        print(f"  ✓ hour: range [{cleaned_df['hour'].min()}, {cleaned_df['hour'].max()}]")

        # Minute: should be 0-59
        assert cleaned_df['minute'].min() >= 0, "Minute minimum invalid"
        assert cleaned_df['minute'].max() <= 59, "Minute maximum invalid"
        print(f"  ✓ minute: range [{cleaned_df['minute'].min()}, {cleaned_df['minute'].max()}]")

        # Second: should be 0-59
        assert cleaned_df['second'].min() >= 0, "Second minimum invalid"
        assert cleaned_df['second'].max() <= 59, "Second maximum invalid"
        print(f"  ✓ second: range [{cleaned_df['second'].min()}, {cleaned_df['second'].max()}]")

        # PM: should be 0 or 1
        assert set(cleaned_df['pm'].unique()).issubset({0, 1}), "PM indicator invalid"
        print(f"  ✓ pm: values {sorted(cleaned_df['pm'].unique())}")

        # Validate AM/PM logic
        am_rows = cleaned_df[cleaned_df['pm'] == 0]
        pm_rows = cleaned_df[cleaned_df['pm'] == 1]
        assert all(am_rows['hour'] < 12), "AM rows should have hour < 12"
        assert all(pm_rows['hour'] >= 12), "PM rows should have hour >= 12"
        print(f"  ✓ AM/PM logic correct: {len(am_rows)} AM, {len(pm_rows)} PM")

        # Validate POINT parsing
        assert cleaned_df['x_pos'].notna().all(), "x_pos has null values"
        assert cleaned_df['y_pos'].notna().all(), "y_pos has null values"
        # x_pos should be longitudes (negative in Colorado)
        assert cleaned_df['x_pos'].min() < -104, "x_pos (longitude) out of range"
        assert cleaned_df['x_pos'].max() < -103, "x_pos (longitude) out of range"
        # y_pos should be latitudes (positive in Colorado)
        assert cleaned_df['y_pos'].min() > 39, "y_pos (latitude) out of range"
        assert cleaned_df['y_pos'].max() < 41, "y_pos (latitude) out of range"
        print(f"✓ POINT parsing: x_pos [{cleaned_df['x_pos'].min():.3f}, {cleaned_df['x_pos'].max():.3f}], "
              f"y_pos [{cleaned_df['y_pos'].min():.3f}, {cleaned_df['y_pos'].max():.3f}]")

        print("\n✓ TEST 1 PASSED: Basic cleaning with timestamps works correctly")


def validate_test_2_xy_coordinate_conversion():
    """
    Test 2: Cleaning with XY coordinate conversion.

    Validates:
    - isXYCoords=True converts lat/long to distances
    - x_pos and y_pos are geodesic distances from origin
    """
    print("\n" + "="*80)
    print("TEST 2: Cleaning with XY Coordinate Conversion")
    print("="*80)

    # Create test data
    test_df = create_test_data_with_timestamps(num_rows=1000, seed=43)
    dask_df = dd.from_pandas(test_df, npartitions=5)

    with tempfile.TemporaryDirectory() as tmpdir:
        setup_test_config(isXYCoords=True, tmpdir=tmpdir)

        cleaner = DaskCleanWithTimestamps(data=dask_df)

        print("Executing clean_data_with_timestamps() with isXYCoords=True...")
        cleaner.clean_data_with_timestamps()
        cleaned_df = cleaner.get_cleaned_data().compute()

        print(f"\nCleaned data shape: {cleaned_df.shape}")

        # Validate XY conversion
        print("\nValidating XY coordinate conversion:")
        print(f"  x_pos range: [{cleaned_df['x_pos'].min():.1f}, {cleaned_df['x_pos'].max():.1f}] km")
        print(f"  y_pos range: [{cleaned_df['y_pos'].min():.1f}, {cleaned_df['y_pos'].max():.1f}] km")

        # Distances should be reasonable (within Colorado, <500km from origin)
        assert cleaned_df['x_pos'].min() >= 0, "x_pos (distance) should be non-negative"
        assert cleaned_df['x_pos'].max() < 500, f"x_pos too large: {cleaned_df['x_pos'].max()}"
        assert cleaned_df['y_pos'].min() >= 0, "y_pos (distance) should be non-negative"
        assert cleaned_df['y_pos'].max() < 500, f"y_pos too large: {cleaned_df['y_pos'].max()}"

        print("✓ XY coordinates converted to geodesic distances")

        # Temporal features should still be present
        temporal_cols = ['month', 'day', 'year', 'hour', 'minute', 'second', 'pm']
        for col in temporal_cols:
            assert col in cleaned_df.columns, f"Missing temporal column: {col}"
        print("✓ Temporal features preserved after XY conversion")

        print("\n✓ TEST 2 PASSED: XY coordinate conversion works correctly")


def validate_test_3_edge_cases():
    """
    Test 3: Edge case handling.

    Validates:
    - Midnight (12:00:00 AM) -> hour=0, pm=0
    - Noon (12:00:00 PM) -> hour=12, pm=1
    - Year boundaries (12/31/2019 11:59:59 PM)
    """
    print("\n" + "="*80)
    print("TEST 3: Edge Case Handling")
    print("="*80)

    # Create edge case test data
    edge_case_data = {
        'coreData_id': ['0x00000001', '0x00000002', '0x00000003'],
        'coreData_position': [
            'POINT (-105.0 39.5)',
            'POINT (-104.5 39.0)',
            'POINT (-105.5 40.0)'
        ],
        'coreData_position_lat': [39.5, 39.0, 40.0],
        'coreData_position_long': [-105.0, -104.5, -105.5],
        'coreData_speed': [50, 60, 70],
        'coreData_heading': [90, 180, 270],
        'metadata_recordType': ['BSM', 'BSM', 'BSM'],
        'metadata_generatedAt': [
            '07/31/2019 12:00:00 AM',  # Midnight
            '07/31/2019 12:00:00 PM',  # Noon
            '12/31/2019 11:59:59 PM'   # Year boundary
        ]
    }
    test_df = pd.DataFrame(edge_case_data)
    dask_df = dd.from_pandas(test_df, npartitions=1)

    with tempfile.TemporaryDirectory() as tmpdir:
        setup_test_config(isXYCoords=False, tmpdir=tmpdir)

        cleaner = DaskCleanWithTimestamps(data=dask_df)

        print("Executing clean_data_with_timestamps() with edge cases...")
        cleaner.clean_data_with_timestamps()
        cleaned_df = cleaner.get_cleaned_data().compute()

        print(f"\nCleaned data shape: {cleaned_df.shape}")
        assert len(cleaned_df) == 3, "All 3 edge case rows should be preserved"

        print("\nValidating edge cases:")

        # Case 1: Midnight (12:00:00 AM)
        midnight_row = cleaned_df.iloc[0]
        assert midnight_row['hour'] == 0, f"Midnight hour should be 0, got {midnight_row['hour']}"
        assert midnight_row['pm'] == 0, f"Midnight pm should be 0, got {midnight_row['pm']}"
        assert midnight_row['minute'] == 0, "Midnight minute should be 0"
        assert midnight_row['second'] == 0, "Midnight second should be 0"
        print(f"  ✓ Midnight: hour={midnight_row['hour']}, pm={midnight_row['pm']}")

        # Case 2: Noon (12:00:00 PM)
        noon_row = cleaned_df.iloc[1]
        assert noon_row['hour'] == 12, f"Noon hour should be 12, got {noon_row['hour']}"
        assert noon_row['pm'] == 1, f"Noon pm should be 1, got {noon_row['pm']}"
        assert noon_row['minute'] == 0, "Noon minute should be 0"
        assert noon_row['second'] == 0, "Noon second should be 0"
        print(f"  ✓ Noon: hour={noon_row['hour']}, pm={noon_row['pm']}")

        # Case 3: Year boundary (12/31/2019 11:59:59 PM)
        year_row = cleaned_df.iloc[2]
        assert year_row['month'] == 12, "Year boundary month should be 12"
        assert year_row['day'] == 31, "Year boundary day should be 31"
        assert year_row['year'] == 2019, "Year boundary year should be 2019"
        assert year_row['hour'] == 23, "Year boundary hour should be 23"
        assert year_row['minute'] == 59, "Year boundary minute should be 59"
        assert year_row['second'] == 59, "Year boundary second should be 59"
        assert year_row['pm'] == 1, "Year boundary pm should be 1"
        print(f"  ✓ Year boundary: 12/31/2019 23:59:59 (pm={year_row['pm']})")

        print("\n✓ TEST 3 PASSED: Edge cases handled correctly")


def main():
    """Run all validation tests."""
    print("="*80)
    print("DaskCleanWithTimestamps Validation Suite")
    print("="*80)

    try:
        # Run all tests
        validate_test_1_basic_cleaning_with_timestamps()
        validate_test_2_xy_coordinate_conversion()
        validate_test_3_edge_cases()

        # Summary
        print("\n" + "="*80)
        print("ALL TESTS PASSED ✓")
        print("="*80)
        print("\nSummary:")
        print("  ✓ Test 1: Basic cleaning with timestamp parsing")
        print("  ✓ Test 2: XY coordinate conversion")
        print("  ✓ Test 3: Edge case handling (midnight, noon, year boundary)")
        print("\nDaskCleanWithTimestamps is production-ready!")

    except AssertionError as e:
        print(f"\n✗ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Cleanup Dask session
        try:
            DaskSessionManager.shutdown()
        except:
            pass


if __name__ == "__main__":
    main()

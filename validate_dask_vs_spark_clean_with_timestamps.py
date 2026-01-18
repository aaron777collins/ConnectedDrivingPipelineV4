"""
Validation script: DaskCleanWithTimestamps vs SparkCleanWithTimestamps

This script validates that DaskCleanWithTimestamps produces identical output
to SparkCleanWithTimestamps on identical datasets.

Tests:
1. Basic cleaning with timestamp parsing (no XY conversion)
2. Cleaning with timestamp parsing AND XY coordinate conversion
3. Edge case validation (midnight, noon, year boundaries)
4. Temporal feature correctness (month, day, year, hour, minute, second, pm)

Success criteria:
- Row counts match exactly
- All temporal features match (exact for integers)
- Floating-point values within tolerance (rtol=1e-5, atol=1e-8)
- String values match exactly
"""

import os
import sys
import pandas as pd
import numpy as np
import dask.dataframe as dd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add project root to path
sys.path.insert(0, '/tmp/original-repo')

from Generator.Cleaners.DaskCleanWithTimestamps import DaskCleanWithTimestamps
from Generator.Cleaners.SparkCleanWithTimestamps import SparkCleanWithTimestamps
from Helpers.DaskSessionManager import DaskSessionManager
from Helpers.SparkSessionManager import SparkSessionManager


def generate_test_data(num_rows: int = 1000, null_fraction: float = 0.01) -> pd.DataFrame:
    """
    Generate realistic BSM test data with timestamps.

    Args:
        num_rows: Number of rows to generate
        null_fraction: Fraction of rows with null POINT values

    Returns:
        pd.DataFrame with BSM schema including timestamps
    """
    np.random.seed(42)

    # Generate timestamps with specific edge cases
    timestamps = []

    # Add edge cases (midnight, noon, year boundaries)
    timestamps.extend([
        '07/31/2019 12:00:00 AM',  # Midnight
        '07/31/2019 12:00:00 PM',  # Noon
        '12/31/2019 11:59:59 PM',  # Year boundary - late night
        '01/01/2020 12:00:00 AM',  # Year boundary - midnight
        '01/01/2020 12:01:00 AM',  # Year boundary - just after midnight
        '06/15/2019 06:30:00 AM',  # Morning
        '06/15/2019 03:45:00 PM',  # Afternoon
        '06/15/2019 11:59:00 PM',  # Late night
    ])

    # Generate random timestamps for remaining rows
    for _ in range(num_rows - len(timestamps)):
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 29)  # Safe day for all months
        year = np.random.choice([2019, 2020])
        hour = np.random.randint(1, 13)  # 1-12 for 12-hour format
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        am_pm = np.random.choice(['AM', 'PM'])

        timestamp = f"{month:02d}/{day:02d}/{year} {hour:02d}:{minute:02d}:{second:02d} {am_pm}"
        timestamps.append(timestamp)

    # Generate WKT POINT coordinates (Colorado region)
    # Longitude: -105.5 to -104.5 (around Denver)
    # Latitude: 39.0 to 40.0
    longitudes = np.random.uniform(-105.5, -104.5, num_rows)
    latitudes = np.random.uniform(39.0, 40.0, num_rows)

    # Create POINT strings with space after "POINT" (correct WKT format)
    points = [f"POINT ({lon:.6f} {lat:.6f})" for lon, lat in zip(longitudes, latitudes)]

    # Inject nulls
    null_indices = np.random.choice(num_rows, size=int(num_rows * null_fraction), replace=False)
    for idx in null_indices:
        points[idx] = None
        latitudes[idx] = None
        longitudes[idx] = None

    df = pd.DataFrame({
        'metadata_generatedAt': timestamps,
        'coreData_position': points,
        'coreData_position_lat': latitudes,
        'coreData_position_long': longitudes,
        'coreData_speed': np.random.uniform(0, 35, num_rows),
        'coreData_heading': np.random.uniform(0, 360, num_rows),
        'metadata_receivedAt': timestamps,  # Reuse timestamps for simplicity
        'coreData_id': [f"vehicle_{i:04d}" for i in range(num_rows)],
        'metadata_recordType': ['BSM'] * num_rows,
    })

    return df


def validate_temporal_features(dask_df: pd.DataFrame, spark_df: pd.DataFrame, test_name: str) -> bool:
    """
    Validate that temporal features match between Dask and Spark outputs.

    Args:
        dask_df: Computed Dask DataFrame
        spark_df: Collected Spark DataFrame (as pandas)
        test_name: Name of test for logging

    Returns:
        bool: True if all validations pass
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"VALIDATING: {test_name}")
    logger.info(f"{'='*60}\n")

    # Sort both DataFrames by a stable column to ensure row-wise comparison
    # Use coreData_id for stable sorting
    dask_df = dask_df.sort_values('coreData_id').reset_index(drop=True)
    spark_df = spark_df.sort_values('coreData_id').reset_index(drop=True)

    all_passed = True

    # Check 1: Row counts
    logger.info(f"Check 1: Row counts")
    if len(dask_df) != len(spark_df):
        logger.error(f"  ✗ Row count mismatch: Dask={len(dask_df)}, Spark={len(spark_df)}")
        all_passed = False
    else:
        logger.info(f"  ✓ Row counts match: {len(dask_df)} rows")

    # Check 2: Column sets
    logger.info(f"\nCheck 2: Column sets")
    dask_cols = set(dask_df.columns)
    spark_cols = set(spark_df.columns)

    if dask_cols != spark_cols:
        logger.error(f"  ✗ Column mismatch")
        logger.error(f"    Dask-only: {dask_cols - spark_cols}")
        logger.error(f"    Spark-only: {spark_cols - dask_cols}")
        all_passed = False
    else:
        logger.info(f"  ✓ Column sets match: {len(dask_cols)} columns")
        logger.info(f"    Columns: {sorted(dask_cols)}")

    # Check 3: Temporal features (exact integer matching)
    logger.info(f"\nCheck 3: Temporal features (exact matching)")
    temporal_features = ['month', 'day', 'year', 'hour', 'minute', 'second', 'pm']

    for feature in temporal_features:
        if feature not in dask_df.columns or feature not in spark_df.columns:
            logger.warning(f"  ⚠ {feature} not found in one or both DataFrames")
            continue

        # Convert to same dtype for comparison
        dask_values = dask_df[feature].astype('int64')
        spark_values = spark_df[feature].astype('int64')

        if (dask_values == spark_values).all():
            logger.info(f"  ✓ {feature}: All values match exactly")

            # Show sample values
            sample_dask = dask_values.head(5).tolist()
            sample_spark = spark_values.head(5).tolist()
            logger.info(f"    Sample Dask: {sample_dask}")
            logger.info(f"    Sample Spark: {sample_spark}")
        else:
            mismatches = (dask_values != spark_values).sum()
            logger.error(f"  ✗ {feature}: {mismatches} mismatches out of {len(dask_values)} rows")

            # Show first few mismatches
            mismatch_idx = np.where(dask_values != spark_values)[0][:5]
            logger.error(f"    First mismatches at indices: {mismatch_idx.tolist()}")
            for idx in mismatch_idx:
                logger.error(f"      Row {idx}: Dask={dask_values.iloc[idx]}, Spark={spark_values.iloc[idx]}")

            all_passed = False

    # Check 4: Floating-point columns (coordinates, speed, heading)
    logger.info(f"\nCheck 4: Floating-point columns (rtol=1e-5, atol=1e-8)")
    float_cols = ['x_pos', 'y_pos', 'coreData_speed', 'coreData_heading']

    for col in float_cols:
        if col not in dask_df.columns or col not in spark_df.columns:
            continue

        try:
            # Use numpy allclose for floating-point comparison
            if np.allclose(dask_df[col], spark_df[col], rtol=1e-5, atol=1e-8, equal_nan=True):
                logger.info(f"  ✓ {col}: All values within tolerance")

                # Show range
                logger.info(f"    Range: [{dask_df[col].min():.6f}, {dask_df[col].max():.6f}]")
            else:
                # Calculate differences
                diff = np.abs(dask_df[col] - spark_df[col])
                max_diff = diff.max()
                mean_diff = diff.mean()

                logger.error(f"  ✗ {col}: Values differ beyond tolerance")
                logger.error(f"    Max difference: {max_diff:.10f}")
                logger.error(f"    Mean difference: {mean_diff:.10f}")

                all_passed = False
        except Exception as e:
            logger.error(f"  ✗ {col}: Comparison failed - {e}")
            all_passed = False

    # Check 5: String columns (exact matching)
    logger.info(f"\nCheck 5: String columns (exact matching)")
    string_cols = ['coreData_id']

    for col in string_cols:
        if col not in dask_df.columns or col not in spark_df.columns:
            continue

        if (dask_df[col] == spark_df[col]).all():
            logger.info(f"  ✓ {col}: All values match exactly")
        else:
            mismatches = (dask_df[col] != spark_df[col]).sum()
            logger.error(f"  ✗ {col}: {mismatches} mismatches")
            all_passed = False

    # Check 6: Edge cases validation (first 8 rows with known timestamps)
    logger.info(f"\nCheck 6: Edge case validation (first 8 rows)")
    edge_cases = [
        (0, 'Midnight', {'hour': 0, 'pm': 0, 'month': 7, 'day': 31, 'year': 2019}),
        (1, 'Noon', {'hour': 12, 'pm': 1, 'month': 7, 'day': 31, 'year': 2019}),
        (2, 'Year boundary late night', {'hour': 23, 'pm': 1, 'month': 12, 'day': 31, 'year': 2019}),
        (3, 'Year boundary midnight', {'hour': 0, 'pm': 0, 'month': 1, 'day': 1, 'year': 2020}),
        (4, 'After midnight', {'hour': 0, 'pm': 0, 'month': 1, 'day': 1, 'year': 2020}),
    ]

    for idx, name, expected in edge_cases:
        if idx >= len(dask_df):
            continue

        logger.info(f"  Edge case {idx} ({name}):")

        # Check Dask
        dask_row = dask_df.iloc[idx]
        dask_match = all(dask_row[k] == v for k, v in expected.items())

        # Check Spark
        spark_row = spark_df.iloc[idx]
        spark_match = all(spark_row[k] == v for k, v in expected.items())

        if dask_match and spark_match:
            logger.info(f"    ✓ Both match expected: {expected}")
        else:
            logger.error(f"    ✗ Mismatch detected")
            logger.error(f"      Expected: {expected}")
            logger.error(f"      Dask: hour={dask_row['hour']}, pm={dask_row['pm']}, month={dask_row['month']}, day={dask_row['day']}, year={dask_row['year']}")
            logger.error(f"      Spark: hour={spark_row['hour']}, pm={spark_row['pm']}, month={spark_row['month']}, day={spark_row['day']}, year={spark_row['year']}")
            all_passed = False

    # Final result
    logger.info(f"\n{'='*60}")
    if all_passed:
        logger.info(f"✓ {test_name}: ALL VALIDATIONS PASSED")
    else:
        logger.error(f"✗ {test_name}: SOME VALIDATIONS FAILED")
    logger.info(f"{'='*60}\n")

    return all_passed


def test_basic_cleaning_with_timestamps():
    """Test 1: Basic cleaning with timestamp parsing (no XY conversion)."""
    logger.info("\n" + "="*80)
    logger.info("TEST 1: Basic Cleaning with Timestamp Parsing (No XY Conversion)")
    logger.info("="*80)

    # Generate test data
    test_data = generate_test_data(num_rows=1000, null_fraction=0.01)
    logger.info(f"Generated {len(test_data)} rows of test data")

    # Save to temporary CSV
    temp_csv = '/tmp/test_timestamps.csv'
    test_data.to_csv(temp_csv, index=False)
    logger.info(f"Saved test data to {temp_csv}")

    # Test Dask implementation
    logger.info("\nCleaning with DaskCleanWithTimestamps...")
    dask_start = time.time()

    dask_cleaner = DaskCleanWithTimestamps(data=None, filename=temp_csv)
    dask_cleaner.clean_data_with_timestamps()
    dask_result = dask_cleaner.get_cleaned_data().compute()

    dask_time = time.time() - dask_start
    logger.info(f"Dask cleaning completed in {dask_time:.3f}s")
    logger.info(f"Dask result shape: {dask_result.shape}")

    # Test Spark implementation
    logger.info("\nCleaning with SparkCleanWithTimestamps...")
    spark_start = time.time()

    # Get Spark session
    spark_session = SparkSessionManager.get_session()

    # Read CSV with Spark
    spark_df = spark_session.read.csv(temp_csv, header=True, inferSchema=True)

    spark_cleaner = SparkCleanWithTimestamps(data=spark_df, filename=temp_csv)
    spark_cleaner.clean_data_with_timestamps()
    spark_result_df = spark_cleaner.get_cleaned_data()
    spark_result = spark_result_df.toPandas()

    spark_time = time.time() - spark_start
    logger.info(f"Spark cleaning completed in {spark_time:.3f}s")
    logger.info(f"Spark result shape: {spark_result.shape}")

    # Validate
    passed = validate_temporal_features(dask_result, spark_result, "Test 1: Basic Cleaning")

    # Cleanup
    if os.path.exists(temp_csv):
        os.remove(temp_csv)

    return passed


def test_cleaning_with_xy_conversion():
    """Test 2: Cleaning with timestamp parsing AND XY coordinate conversion."""
    logger.info("\n" + "="*80)
    logger.info("TEST 2: Cleaning with Timestamp Parsing AND XY Coordinate Conversion")
    logger.info("="*80)

    # Generate test data
    test_data = generate_test_data(num_rows=1000, null_fraction=0.01)
    logger.info(f"Generated {len(test_data)} rows of test data")

    # Save to temporary CSV
    temp_csv = '/tmp/test_timestamps_xy.csv'
    test_data.to_csv(temp_csv, index=False)
    logger.info(f"Saved test data to {temp_csv}")

    # Origin point for XY conversion (Denver, CO)
    origin = (39.5, -105.0)
    logger.info(f"Using origin point: {origin}")

    # Test Dask implementation
    logger.info("\nCleaning with DaskCleanWithTimestamps (XY conversion enabled)...")
    dask_start = time.time()

    # Note: We need to set isXYCoords=True via config, not constructor
    # The cleaners read this from context provider
    dask_cleaner = DaskCleanWithTimestamps(data=None, filename=temp_csv)
    # Override isXYCoords for this test
    dask_cleaner.isXYCoords = True
    dask_cleaner.clean_data_with_timestamps()
    dask_result = dask_cleaner.get_cleaned_data().compute()

    dask_time = time.time() - dask_start
    logger.info(f"Dask cleaning completed in {dask_time:.3f}s")
    logger.info(f"Dask result shape: {dask_result.shape}")

    # Test Spark implementation
    logger.info("\nCleaning with SparkCleanWithTimestamps (XY conversion enabled)...")
    spark_start = time.time()

    # Get Spark session
    spark_session = SparkSessionManager.get_session()

    # Read CSV with Spark
    spark_df = spark_session.read.csv(temp_csv, header=True, inferSchema=True)

    spark_cleaner = SparkCleanWithTimestamps(data=spark_df, filename=temp_csv)
    # Override isXYCoords for this test
    spark_cleaner.isXYCoords = True
    spark_cleaner.clean_data_with_timestamps()
    spark_result_df = spark_cleaner.get_cleaned_data()
    spark_result = spark_result_df.toPandas()

    spark_time = time.time() - spark_start
    logger.info(f"Spark cleaning completed in {spark_time:.3f}s")
    logger.info(f"Spark result shape: {spark_result.shape}")

    # Validate
    passed = validate_temporal_features(dask_result, spark_result, "Test 2: XY Conversion")

    # Cleanup
    if os.path.exists(temp_csv):
        os.remove(temp_csv)

    return passed


def main():
    """Run all validation tests."""
    logger.info("\n" + "#"*80)
    logger.info("# VALIDATION: DaskCleanWithTimestamps vs SparkCleanWithTimestamps")
    logger.info("#"*80)

    # Initialize Dask
    logger.info("\nInitializing Dask session...")
    dask_client = DaskSessionManager.get_client()
    logger.info(f"Dask workers: {len(dask_client.cluster.workers)}")

    # Initialize Spark
    logger.info("\nInitializing Spark session...")
    spark_session = SparkSessionManager.get_session()
    logger.info(f"Spark version: {spark_session.version}")

    # Run tests
    results = {}

    try:
        results['test1'] = test_basic_cleaning_with_timestamps()
    except Exception as e:
        logger.error(f"Test 1 failed with exception: {e}", exc_info=True)
        results['test1'] = False

    try:
        results['test2'] = test_cleaning_with_xy_conversion()
    except Exception as e:
        logger.error(f"Test 2 failed with exception: {e}", exc_info=True)
        results['test2'] = False

    # Final summary
    logger.info("\n" + "#"*80)
    logger.info("# FINAL SUMMARY")
    logger.info("#"*80)

    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{test_name}: {status}")

    all_passed = all(results.values())

    logger.info("\n" + "="*80)
    if all_passed:
        logger.info("✓ ALL TESTS PASSED - DaskCleanWithTimestamps matches SparkCleanWithTimestamps")
        logger.info("="*80)
        return 0
    else:
        logger.error("✗ SOME TESTS FAILED - Review errors above")
        logger.info("="*80)
        return 1


if __name__ == '__main__':
    sys.exit(main())

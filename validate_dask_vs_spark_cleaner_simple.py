"""
Simplified validation script for comparing DaskConnectedDrivingCleaner vs SparkConnectedDrivingCleaner.

This script validates that the Dask implementation produces identical outputs to the PySpark
implementation by comparing outputs on identical test datasets.
"""

import pandas as pd
import numpy as np
import dask.dataframe as dd
from pyspark.sql import SparkSession
import time
import os
import shutil
import sys

from Generator.Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
from Generator.Cleaners.SparkConnectedDrivingCleaner import SparkConnectedDrivingCleaner
from Helpers.DaskSessionManager import DaskSessionManager
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.PathProvider import PathProvider


def generate_test_data(n_rows=1000, null_fraction=0.01):
    """Generate realistic BSM test data with WKT POINT strings."""
    np.random.seed(42)

    # Generate coordinates (Colorado region: 39-40°N, -105.5 to -104.5°W)
    latitudes = np.random.uniform(39.0, 40.0, n_rows)
    longitudes = np.random.uniform(-105.5, -104.5, n_rows)

    # Create WKT POINT strings (note: space after "POINT" is required)
    points = [f"POINT ({lon:.6f} {lat:.6f})" for lon, lat in zip(longitudes, latitudes)]

    # Inject nulls
    null_indices = np.random.choice(n_rows, int(n_rows * null_fraction), replace=False)
    for idx in null_indices:
        points[idx] = None

    # Generate other BSM columns
    data = {
        'coreData_id': [f"{np.random.randint(0, 2**32):08X}" for _ in range(n_rows)],
        'coreData_position': points,
        'coreData_speed': np.random.uniform(0, 120, n_rows),
        'coreData_heading': np.random.uniform(0, 360, n_rows),
        'coreData_lat': latitudes,
        'coreData_long': longitudes,
        'timestamp': pd.date_range('2024-01-01', periods=n_rows, freq='1s').astype(str),
    }

    return pd.DataFrame(data)


def setup_providers(temp_dir, test_name):
    """Set up singleton providers for testing."""
    # Reset singletons
    PathProvider._instance = None
    GeneratorPathProvider._instance = None
    GeneratorContextProvider._instance = None

    # Setup PathProvider for Logger
    PathProvider(
        model="validation",
        contexts={
            "Logger.logpath": lambda model: os.path.join(temp_dir, "logs", f"{model}.log"),
        }
    )

    # Setup GeneratorPathProvider for cache paths
    GeneratorPathProvider(
        model=test_name,
        contexts={
            "FileCache.filepath": lambda model: os.path.join(temp_dir, f"{model}_cache"),
        }
    )

    # Create GeneratorContextProvider
    context = GeneratorContextProvider()
    return context


def configure_cleaner_context(context, n_rows, isXYCoords=False, filename="test_clean.parquet"):
    """Configure context for cleaner testing."""
    columns = [
        'coreData_id',
        'coreData_position',
        'coreData_speed',
        'coreData_heading',
        'coreData_lat',
        'coreData_long',
        'timestamp'
    ]

    context.add("DataGatherer.numrows", n_rows)
    context.add("ConnectedDrivingCleaner.cleanParams", {})
    context.add("ConnectedDrivingCleaner.filename", filename)
    context.add("ConnectedDrivingCleaner.isXYCoords", isXYCoords)
    context.add("ConnectedDrivingCleaner.shouldGatherAutomatically", False)
    context.add("ConnectedDrivingCleaner.x_pos", 39.5)  # Origin latitude
    context.add("ConnectedDrivingCleaner.y_pos", -105.0)  # Origin longitude
    context.add("ConnectedDrivingCleaner.columns", columns)


def compare_dataframes(dask_df, spark_df, test_name, rtol=1e-5, atol=1e-8):
    """Compare Dask and Spark DataFrames for equality."""
    print(f"\n{'='*70}")
    print(f"Comparing: {test_name}")
    print(f"{'='*70}")

    # Convert both to pandas
    dask_pdf = dask_df.compute() if hasattr(dask_df, 'compute') else dask_df
    spark_pdf = spark_df.toPandas()

    # Check row counts
    dask_rows = len(dask_pdf)
    spark_rows = len(spark_pdf)
    print(f"Row counts: Dask={dask_rows}, Spark={spark_rows}")

    if dask_rows != spark_rows:
        print(f"✗ FAILED: Row count mismatch")
        return False

    # Check column sets
    dask_cols = set(dask_pdf.columns)
    spark_cols = set(spark_pdf.columns)

    if dask_cols != spark_cols:
        print(f"✗ FAILED: Column mismatch")
        print(f"  Only in Dask: {dask_cols - spark_cols}")
        print(f"  Only in Spark: {spark_cols - dask_cols}")
        return False

    # Sort both DataFrames for stable comparison
    common_cols = sorted(list(dask_cols))
    dask_sorted = dask_pdf[common_cols].sort_values(by=common_cols).reset_index(drop=True)
    spark_sorted = spark_pdf[common_cols].sort_values(by=common_cols).reset_index(drop=True)

    # Compare column by column
    mismatches = []
    for col in common_cols:
        try:
            if dask_sorted[col].dtype in [np.float64, np.float32]:
                # Floating-point comparison with tolerance
                diff = np.abs(dask_sorted[col] - spark_sorted[col])
                max_diff = diff.max()

                if not np.allclose(dask_sorted[col], spark_sorted[col], rtol=rtol, atol=atol, equal_nan=True):
                    mismatches.append(f"{col}: max_diff={max_diff:.2e}")
                else:
                    print(f"  ✓ {col}: max_diff={max_diff:.2e} (within tolerance)")
            else:
                # Exact comparison for non-float types
                if not (dask_sorted[col] == spark_sorted[col]).all():
                    n_diff = (dask_sorted[col] != spark_sorted[col]).sum()
                    mismatches.append(f"{col}: {n_diff} values differ")
                else:
                    print(f"  ✓ {col}: exact match")
        except Exception as e:
            mismatches.append(f"{col}: comparison failed - {e}")

    if mismatches:
        print(f"\n✗ FAILED: Value mismatches detected:")
        for mismatch in mismatches:
            print(f"    {mismatch}")
        return False

    print(f"\n✓ PASSED: All columns match within tolerance (rtol={rtol}, atol={atol})")
    return True


def test_basic_cleaning(temp_dir, spark, n_rows=1000):
    """Test basic cleaning without XY conversion."""
    print(f"\n{'#'*80}")
    print(f"TEST 1: Basic Cleaning (No XY Conversion) - {n_rows} rows")
    print(f"{'#'*80}")

    # Generate test data
    test_data = generate_test_data(n_rows=n_rows, null_fraction=0.01)
    print(f"Generated {len(test_data)} rows with ~1% nulls")

    # Setup providers
    context = setup_providers(temp_dir, "test_basic_clean")
    configure_cleaner_context(context, n_rows, isXYCoords=False, filename="test_basic.parquet")

    try:
        # Test Dask cleaner
        print("\nRunning DaskConnectedDrivingCleaner...")
        dask_df = dd.from_pandas(test_data, npartitions=10)
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(data=dask_df)
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.get_cleaned_data()
        dask_time = time.time() - start_time
        print(f"Dask cleaning time: {dask_time:.3f}s")

        # Test Spark cleaner
        print("Running SparkConnectedDrivingCleaner...")
        spark_df = spark.createDataFrame(test_data)
        start_time = time.time()
        spark_cleaner = SparkConnectedDrivingCleaner(data=spark_df)
        spark_cleaner.clean_data()
        spark_result = spark_cleaner.get_cleaned_data()
        spark_time = time.time() - start_time
        print(f"Spark cleaning time: {spark_time:.3f}s")

        # Compare results
        success = compare_dataframes(dask_result, spark_result, f"Basic Cleaning - {n_rows} rows")

        return success

    except Exception as e:
        print(f"\n✗ Test failed with error: {type(e).__name__}: {e}")
        return False


def test_xy_conversion(temp_dir, spark, n_rows=1000):
    """Test cleaning with XY coordinate conversion."""
    print(f"\n{'#'*80}")
    print(f"TEST 2: Cleaning with XY Conversion - {n_rows} rows")
    print(f"{'#'*80}")

    # Generate test data
    test_data = generate_test_data(n_rows=n_rows, null_fraction=0.01)
    print(f"Generated {len(test_data)} rows with ~1% nulls")

    # Setup providers
    context = setup_providers(temp_dir, "test_xy_clean")
    configure_cleaner_context(context, n_rows, isXYCoords=True, filename="test_xy.parquet")

    try:
        # Test Dask cleaner
        print("\nRunning DaskConnectedDrivingCleaner with XY conversion...")
        print(f"  Origin: (39.5, -105.0)")
        dask_df = dd.from_pandas(test_data, npartitions=10)
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(data=dask_df)
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.get_cleaned_data()
        dask_time = time.time() - start_time
        print(f"Dask cleaning time: {dask_time:.3f}s")

        # Test Spark cleaner
        print("Running SparkConnectedDrivingCleaner with XY conversion...")
        spark_df = spark.createDataFrame(test_data)
        start_time = time.time()
        spark_cleaner = SparkConnectedDrivingCleaner(data=spark_df)
        spark_cleaner.clean_data()
        spark_result = spark_cleaner.get_cleaned_data()
        spark_time = time.time() - start_time
        print(f"Spark cleaning time: {spark_time:.3f}s")

        # Compare results (allow slightly more tolerance for geodesic distance calculations)
        success = compare_dataframes(
            dask_result, spark_result,
            f"XY Conversion - {n_rows} rows",
            rtol=1e-5,
            atol=1e-3  # Allow up to 1mm difference
        )

        return success

    except Exception as e:
        print(f"\n✗ Test failed with error: {type(e).__name__}: {e}")
        return False


def main():
    """Run all validation tests."""
    print("="*80)
    print("VALIDATION: DaskConnectedDrivingCleaner vs SparkConnectedDrivingCleaner")
    print("="*80)
    print("\nValidating that Dask implementation produces identical outputs to PySpark.\n")

    # Setup temp directory
    temp_dir = "/tmp/validate_cleaner_temp"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    results = []

    # Initialize Spark session (reuse across all tests)
    spark = SparkSession.builder \
        .appName("ValidationDaskVsSpark") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Initialize Dask client
        dask_client = DaskSessionManager.get_client()
        print(f"Dask dashboard: {dask_client.dashboard_link}\n")

        # Test 1: Basic cleaning (no XY conversion)
        success1 = test_basic_cleaning(temp_dir, spark, n_rows=1000)
        results.append(('Basic Cleaning (1k rows)', success1))

        # Test 2: XY conversion
        success2 = test_xy_conversion(temp_dir, spark, n_rows=1000)
        results.append(('XY Conversion (1k rows)', success2))

        # Print summary
        print(f"\n{'#'*80}")
        print("VALIDATION SUMMARY")
        print(f"{'#'*80}\n")

        passed = sum(1 for _, s in results if s)
        total = len(results)

        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ✓")
        print(f"Failed: {total - passed} ✗")
        print(f"Success Rate: {passed/total*100:.1f}%\n")

        print(f"{'Test':<40} {'Status':<10}")
        print(f"{'-'*50}")
        for test_name, success in results:
            status = '✓ PASSED' if success else '✗ FAILED'
            print(f"{test_name:<40} {status}")

        print(f"\n{'='*80}\n")

        if passed == total:
            print("✓ ALL VALIDATIONS PASSED")
            print("\nDaskConnectedDrivingCleaner produces IDENTICAL outputs to SparkConnectedDrivingCleaner!")
            print("Task 29 validation complete. ✓")
            return 0
        else:
            print("✗ SOME VALIDATIONS FAILED")
            return 1

    except Exception as e:
        print(f"\n✗ VALIDATION FAILED WITH ERROR:")
        print(f"  {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        # Cleanup
        spark.stop()
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print("\nCleanup complete.")


if __name__ == '__main__':
    sys.exit(main())

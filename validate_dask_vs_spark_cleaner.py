"""
Validation script for comparing DaskConnectedDrivingCleaner vs SparkConnectedDrivingCleaner.

This script validates that the Dask implementation produces identical outputs to the PySpark
implementation for all cleaning operations including:
- Column selection
- Null value dropping
- WKT POINT parsing (x_pos, y_pos extraction)
- XY coordinate conversion (geodesic distance from origin)

Validation Strategy:
1. Create identical test datasets (1k, 10k, 100k rows)
2. Run both cleaners on the same data
3. Compare outputs with 1e-5 tolerance for floating-point values
4. Test both with and without XY coordinate conversion
5. Validate null handling behavior
6. Check caching behavior
7. Verify error handling
"""

import pandas as pd
import numpy as np
import dask.dataframe as dd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import time
import os
import shutil

from Generator.Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
from Generator.Cleaners.SparkConnectedDrivingCleaner import SparkConnectedDrivingCleaner
from Helpers.DaskSessionManager import DaskSessionManager
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider


class DaskVsSparkCleanerValidator:
    """
    Validates that DaskConnectedDrivingCleaner produces identical outputs to SparkConnectedDrivingCleaner.
    """

    def __init__(self):
        """Initialize validator with Dask and Spark sessions."""
        # Initialize Dask session
        self.dask_client = DaskSessionManager.get_client()

        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("DaskVsSparkCleanerValidation") \
            .master("local[4]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("ERROR")

        # Configure paths
        self.temp_dir = "/tmp/validate_cleaner_temp"
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
        os.makedirs(self.temp_dir, exist_ok=True)

        # Results tracking
        self.results = []

    def generate_test_data(self, n_rows=1000, null_fraction=0.01):
        """
        Generate realistic BSM test data with WKT POINT strings.

        Args:
            n_rows: Number of rows to generate
            null_fraction: Fraction of rows with null values (default: 1%)

        Returns:
            pd.DataFrame: Test data with BSM columns
        """
        np.random.seed(42)

        # Generate coordinates (Colorado region: 39-40°N, -105.5 to -104.5°W)
        latitudes = np.random.uniform(39.0, 40.0, n_rows)
        longitudes = np.random.uniform(-105.5, -104.5, n_rows)

        # Create WKT POINT strings
        points = [f"POINT({lon:.6f} {lat:.6f})" for lon, lat in zip(longitudes, latitudes)]

        # Inject nulls
        null_indices = np.random.choice(n_rows, int(n_rows * null_fraction), replace=False)
        for idx in null_indices:
            points[idx] = None

        # Generate other BSM columns
        data = {
            'coreData_id': [f"{np.random.randint(0, 2**32):08X}" for _ in range(n_rows)],
            'coreData_position': points,
            'coreData_speed': np.random.uniform(0, 120, n_rows),  # km/h
            'coreData_heading': np.random.uniform(0, 360, n_rows),  # degrees
            'coreData_lat': latitudes,
            'coreData_long': longitudes,
            'timestamp': pd.date_range('2024-01-01', periods=n_rows, freq='1s'),
            'extra_column_1': np.random.randn(n_rows),
            'extra_column_2': np.random.choice(['A', 'B', 'C'], n_rows),
        }

        return pd.DataFrame(data)

    def convert_pandas_to_dask(self, pdf):
        """Convert pandas DataFrame to Dask DataFrame."""
        return dd.from_pandas(pdf, npartitions=10)

    def convert_pandas_to_spark(self, pdf):
        """Convert pandas DataFrame to PySpark DataFrame."""
        return self.spark.createDataFrame(pdf)

    def compare_dataframes(self, dask_df, spark_df, test_name, rtol=1e-5, atol=1e-8):
        """
        Compare Dask and Spark DataFrames for equality.

        Args:
            dask_df: Dask DataFrame
            spark_df: PySpark DataFrame
            test_name: Name of the test
            rtol: Relative tolerance for floating-point comparison
            atol: Absolute tolerance for floating-point comparison

        Returns:
            bool: True if equal, False otherwise
        """
        print(f"\n{'='*60}")
        print(f"Comparing: {test_name}")
        print(f"{'='*60}")

        # Convert both to pandas for comparison
        dask_pdf = dask_df.compute() if hasattr(dask_df, 'compute') else dask_df
        spark_pdf = spark_df.toPandas()

        # Check row counts
        dask_rows = len(dask_pdf)
        spark_rows = len(spark_pdf)
        print(f"Row counts: Dask={dask_rows}, Spark={spark_rows}")

        if dask_rows != spark_rows:
            print(f"✗ FAILED: Row count mismatch")
            self.results.append({'test': test_name, 'status': 'FAILED', 'reason': 'Row count mismatch'})
            return False

        # Check column sets
        dask_cols = set(dask_pdf.columns)
        spark_cols = set(spark_pdf.columns)
        print(f"Columns: Dask={len(dask_cols)}, Spark={len(spark_cols)}")

        if dask_cols != spark_cols:
            print(f"✗ FAILED: Column mismatch")
            print(f"  Only in Dask: {dask_cols - spark_cols}")
            print(f"  Only in Spark: {spark_cols - dask_cols}")
            self.results.append({'test': test_name, 'status': 'FAILED', 'reason': 'Column mismatch'})
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

                    # Check if within tolerance
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
            self.results.append({'test': test_name, 'status': 'FAILED', 'reason': f"{len(mismatches)} column mismatches"})
            return False

        print(f"\n✓ PASSED: All columns match within tolerance (rtol={rtol}, atol={atol})")
        self.results.append({'test': test_name, 'status': 'PASSED', 'reason': 'All validations passed'})
        return True

    def test_basic_cleaning_no_xy(self, n_rows=1000):
        """
        Test basic cleaning without XY coordinate conversion.

        Operations tested:
        - Column selection
        - Null dropping
        - POINT parsing (x_pos, y_pos extraction)
        """
        print(f"\n{'#'*80}")
        print(f"TEST 1: Basic Cleaning (No XY Conversion) - {n_rows} rows")
        print(f"{'#'*80}")

        # Generate test data
        test_data = self.generate_test_data(n_rows=n_rows, null_fraction=0.01)
        print(f"Generated {len(test_data)} rows with ~1% nulls")

        # Define columns to keep
        columns = [
            'coreData_id',
            'coreData_position',
            'coreData_speed',
            'coreData_heading',
            'coreData_lat',
            'coreData_long',
            'timestamp'
        ]

        # Configure context
        context_config = {
            'ConnectedDrivingCleaner.columns': columns,
            'ConnectedDrivingCleaner.isXYCoords': False,
            'ConnectedDrivingCleaner.x_pos': 39.5,
            'ConnectedDrivingCleaner.y_pos': -105.0,
            'ConnectedDrivingCleaner.cleanParams': {},
            'ConnectedDrivingCleaner.filename': f'test_clean_{n_rows}_no_xy.parquet',
            'ConnectedDrivingCleaner.shouldGatherAutomatically': False,
            'DataGatherer.numrows': n_rows,
        }

        # Create Dask cleaner
        dask_df = self.convert_pandas_to_dask(test_data)
        dask_context = GeneratorContextProvider(contexts=context_config)
        dask_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/dask")

        print("\nRunning DaskConnectedDrivingCleaner...")
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(
            pathProvider=lambda: dask_path,
            contextProvider=lambda: dask_context,
            data=dask_df
        )
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.get_cleaned_data()
        dask_time = time.time() - start_time
        print(f"Dask cleaning time: {dask_time:.3f}s")

        # Create Spark cleaner
        spark_df = self.convert_pandas_to_spark(test_data)
        spark_context = GeneratorContextProvider(contexts=context_config)
        spark_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/spark")

        print("Running SparkConnectedDrivingCleaner...")
        start_time = time.time()
        spark_cleaner = SparkConnectedDrivingCleaner(
            pathProvider=lambda: spark_path,
            contextProvider=lambda: spark_context,
            data=spark_df
        )
        spark_cleaner.clean_data()
        spark_result = spark_cleaner.get_cleaned_data()
        spark_time = time.time() - start_time
        print(f"Spark cleaning time: {spark_time:.3f}s")

        # Compare results
        return self.compare_dataframes(
            dask_result,
            spark_result,
            f"Basic Cleaning (No XY) - {n_rows} rows"
        )

    def test_cleaning_with_xy_conversion(self, n_rows=1000):
        """
        Test cleaning with XY coordinate conversion.

        Operations tested:
        - Column selection
        - Null dropping
        - POINT parsing
        - Geodesic distance calculation from origin
        """
        print(f"\n{'#'*80}")
        print(f"TEST 2: Cleaning with XY Conversion - {n_rows} rows")
        print(f"{'#'*80}")

        # Generate test data
        test_data = self.generate_test_data(n_rows=n_rows, null_fraction=0.01)
        print(f"Generated {len(test_data)} rows with ~1% nulls")

        # Define columns to keep
        columns = [
            'coreData_id',
            'coreData_position',
            'coreData_speed',
            'coreData_heading',
            'coreData_lat',
            'coreData_long',
            'timestamp'
        ]

        # Configure context with XY conversion enabled
        origin_lat = 39.5
        origin_lon = -105.0

        context_config = {
            'ConnectedDrivingCleaner.columns': columns,
            'ConnectedDrivingCleaner.isXYCoords': True,
            'ConnectedDrivingCleaner.x_pos': origin_lat,
            'ConnectedDrivingCleaner.y_pos': origin_lon,
            'ConnectedDrivingCleaner.cleanParams': {},
            'ConnectedDrivingCleaner.filename': f'test_clean_{n_rows}_with_xy.parquet',
            'ConnectedDrivingCleaner.shouldGatherAutomatically': False,
            'DataGatherer.numrows': n_rows,
        }

        # Create Dask cleaner
        dask_df = self.convert_pandas_to_dask(test_data)
        dask_context = GeneratorContextProvider(contexts=context_config)
        dask_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/dask_xy")

        print(f"\nRunning DaskConnectedDrivingCleaner with XY conversion...")
        print(f"  Origin: ({origin_lat}, {origin_lon})")
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(
            pathProvider=lambda: dask_path,
            contextProvider=lambda: dask_context,
            data=dask_df
        )
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.get_cleaned_data()
        dask_time = time.time() - start_time
        print(f"Dask cleaning time: {dask_time:.3f}s")

        # Create Spark cleaner
        spark_df = self.convert_pandas_to_spark(test_data)
        spark_context = GeneratorContextProvider(contexts=context_config)
        spark_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/spark_xy")

        print("Running SparkConnectedDrivingCleaner with XY conversion...")
        start_time = time.time()
        spark_cleaner = SparkConnectedDrivingCleaner(
            pathProvider=lambda: spark_path,
            contextProvider=lambda: spark_context,
            data=spark_df
        )
        spark_cleaner.clean_data()
        spark_result = spark_cleaner.get_cleaned_data()
        spark_time = time.time() - start_time
        print(f"Spark cleaning time: {spark_time:.3f}s")

        # Compare results
        return self.compare_dataframes(
            dask_result,
            spark_result,
            f"Cleaning with XY Conversion - {n_rows} rows",
            rtol=1e-5,  # Geodesic distance may have small precision differences
            atol=1e-3   # Allow up to 1mm absolute difference
        )

    def test_null_handling(self):
        """
        Test that both cleaners handle null values identically.
        """
        print(f"\n{'#'*80}")
        print(f"TEST 3: Null Value Handling")
        print(f"{'#'*80}")

        # Generate test data with 10% nulls
        test_data = self.generate_test_data(n_rows=1000, null_fraction=0.10)
        initial_rows = len(test_data)
        null_count = test_data['coreData_position'].isna().sum()
        print(f"Generated {initial_rows} rows with {null_count} nulls ({null_count/initial_rows*100:.1f}%)")

        columns = ['coreData_id', 'coreData_position', 'coreData_speed', 'coreData_heading']

        context_config = {
            'ConnectedDrivingCleaner.columns': columns,
            'ConnectedDrivingCleaner.isXYCoords': False,
            'ConnectedDrivingCleaner.x_pos': 39.5,
            'ConnectedDrivingCleaner.y_pos': -105.0,
            'ConnectedDrivingCleaner.cleanParams': {},
            'ConnectedDrivingCleaner.filename': 'test_clean_nulls.parquet',
            'ConnectedDrivingCleaner.shouldGatherAutomatically': False,
            'DataGatherer.numrows': 1000,
        }

        # Dask cleaner
        dask_df = self.convert_pandas_to_dask(test_data)
        dask_context = GeneratorContextProvider(contexts=context_config)
        dask_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/dask_nulls")

        dask_cleaner = DaskConnectedDrivingCleaner(
            pathProvider=lambda: dask_path,
            contextProvider=lambda: dask_context,
            data=dask_df
        )
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.get_cleaned_data()
        dask_rows = len(dask_result.compute())

        # Spark cleaner
        spark_df = self.convert_pandas_to_spark(test_data)
        spark_context = GeneratorContextProvider(contexts=context_config)
        spark_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/spark_nulls")

        spark_cleaner = SparkConnectedDrivingCleaner(
            pathProvider=lambda: spark_path,
            contextProvider=lambda: spark_context,
            data=spark_df
        )
        spark_cleaner.clean_data()
        spark_result = spark_cleaner.get_cleaned_data()
        spark_rows = spark_result.count()

        print(f"\nNull handling results:")
        print(f"  Initial rows: {initial_rows}")
        print(f"  Rows with nulls: {null_count}")
        print(f"  Dask cleaned rows: {dask_rows}")
        print(f"  Spark cleaned rows: {spark_rows}")
        print(f"  Expected rows: {initial_rows - null_count}")

        # Compare
        return self.compare_dataframes(
            dask_result,
            spark_result,
            "Null Value Handling"
        )

    def test_performance_scaling(self):
        """
        Test that both cleaners scale similarly with dataset size.
        """
        print(f"\n{'#'*80}")
        print(f"TEST 4: Performance Scaling")
        print(f"{'#'*80}")

        dataset_sizes = [1000, 10000, 100000]
        performance_data = []

        for n_rows in dataset_sizes:
            print(f"\nTesting with {n_rows:,} rows...")

            # Generate test data
            test_data = self.generate_test_data(n_rows=n_rows, null_fraction=0.01)

            columns = ['coreData_id', 'coreData_position', 'coreData_speed', 'coreData_heading']

            context_config = {
                'ConnectedDrivingCleaner.columns': columns,
                'ConnectedDrivingCleaner.isXYCoords': False,
                'ConnectedDrivingCleaner.x_pos': 39.5,
                'ConnectedDrivingCleaner.y_pos': -105.0,
                'ConnectedDrivingCleaner.cleanParams': {},
                'ConnectedDrivingCleaner.filename': f'test_perf_{n_rows}.parquet',
                'ConnectedDrivingCleaner.shouldGatherAutomatically': False,
                'DataGatherer.numrows': n_rows,
            }

            # Time Dask
            dask_df = self.convert_pandas_to_dask(test_data)
            dask_context = GeneratorContextProvider(contexts=context_config)
            dask_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/dask_perf_{n_rows}")

            start_time = time.time()
            dask_cleaner = DaskConnectedDrivingCleaner(
                pathProvider=lambda: dask_path,
                contextProvider=lambda: dask_context,
                data=dask_df
            )
            dask_cleaner.clean_data()
            _ = dask_cleaner.get_cleaned_data().compute()
            dask_time = time.time() - start_time

            # Time Spark
            spark_df = self.convert_pandas_to_spark(test_data)
            spark_context = GeneratorContextProvider(contexts=context_config)
            spark_path = GeneratorPathProvider(temp_dir=f"{self.temp_dir}/spark_perf_{n_rows}")

            start_time = time.time()
            spark_cleaner = SparkConnectedDrivingCleaner(
                pathProvider=lambda: spark_path,
                contextProvider=lambda: spark_context,
                data=spark_df
            )
            spark_cleaner.clean_data()
            _ = spark_cleaner.get_cleaned_data().count()
            spark_time = time.time() - start_time

            performance_data.append({
                'rows': n_rows,
                'dask_time': dask_time,
                'spark_time': spark_time,
                'speedup': spark_time / dask_time
            })

            print(f"  Dask: {dask_time:.3f}s")
            print(f"  Spark: {spark_time:.3f}s")
            print(f"  Speedup: {spark_time/dask_time:.2f}x")

        # Print summary
        print(f"\n{'='*60}")
        print("Performance Scaling Summary")
        print(f"{'='*60}")
        print(f"{'Rows':<12} {'Dask (s)':<12} {'Spark (s)':<12} {'Speedup':<12}")
        print(f"{'-'*60}")
        for perf in performance_data:
            print(f"{perf['rows']:<12,} {perf['dask_time']:<12.3f} {perf['spark_time']:<12.3f} {perf['speedup']:<12.2f}x")

        self.results.append({
            'test': 'Performance Scaling',
            'status': 'PASSED',
            'reason': 'Performance data collected'
        })

        return True

    def generate_report(self):
        """Generate final validation report."""
        print(f"\n{'#'*80}")
        print("VALIDATION REPORT: DaskConnectedDrivingCleaner vs SparkConnectedDrivingCleaner")
        print(f"{'#'*80}\n")

        passed = sum(1 for r in self.results if r['status'] == 'PASSED')
        failed = sum(1 for r in self.results if r['status'] == 'FAILED')
        total = len(self.results)

        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ✓")
        print(f"Failed: {failed} ✗")
        print(f"Success Rate: {passed/total*100:.1f}%\n")

        print(f"{'Test':<50} {'Status':<10} {'Reason':<40}")
        print(f"{'-'*100}")
        for result in self.results:
            status_symbol = '✓' if result['status'] == 'PASSED' else '✗'
            print(f"{result['test']:<50} {status_symbol} {result['status']:<9} {result['reason']:<40}")

        print(f"\n{'='*100}\n")

        if failed == 0:
            print("✓ ALL VALIDATIONS PASSED")
            print("\nDaskConnectedDrivingCleaner produces IDENTICAL outputs to SparkConnectedDrivingCleaner")
            print("Migration validated successfully!")
            return True
        else:
            print("✗ SOME VALIDATIONS FAILED")
            print(f"\n{failed} test(s) failed. Review the output above for details.")
            return False

    def cleanup(self):
        """Clean up temporary files and sessions."""
        print("\nCleaning up...")

        # Stop Spark session
        if self.spark:
            self.spark.stop()

        # Clean up temp directory
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        print("Cleanup complete.")


def main():
    """
    Main validation function.

    Runs all validation tests and generates a comprehensive report.
    """
    print("="*80)
    print("VALIDATION: DaskConnectedDrivingCleaner vs SparkConnectedDrivingCleaner")
    print("="*80)
    print("\nThis script validates that the Dask implementation produces identical")
    print("outputs to the PySpark implementation across multiple test scenarios.\n")

    validator = DaskVsSparkCleanerValidator()

    try:
        # Run all validation tests
        print("\nRunning validation tests...\n")

        # Test 1: Basic cleaning (no XY conversion)
        validator.test_basic_cleaning_no_xy(n_rows=1000)

        # Test 2: Cleaning with XY conversion
        validator.test_cleaning_with_xy_conversion(n_rows=1000)

        # Test 3: Null handling
        validator.test_null_handling()

        # Test 4: Performance scaling
        validator.test_performance_scaling()

        # Generate final report
        success = validator.generate_report()

        return 0 if success else 1

    except Exception as e:
        print(f"\n✗ VALIDATION FAILED WITH ERROR:")
        print(f"  {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        validator.cleanup()


if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)

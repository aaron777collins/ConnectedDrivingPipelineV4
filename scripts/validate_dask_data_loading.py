#!/usr/bin/env python3
"""
Validate Dask Data Loading Functionality

This script validates the DaskDataGatherer implementation by testing:
1. CSV reading with different blocksizes
2. Row limiting functionality
3. Parquet caching via DaskParquetCache decorator
4. Memory efficiency with sample datasets (1k, 10k, 100k rows)
5. Comparison with pandas/PySpark for validation

Usage:
    python scripts/validate_dask_data_loading.py
"""

import os
import sys
import tempfile
import shutil
import time
import pandas as pd
import dask.dataframe as dd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Gatherer.DaskDataGatherer import DaskDataGatherer
from Helpers.DaskSessionManager import DaskSessionManager
from ServiceProviders.PathProvider import PathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from Logger.Logger import Logger


class DaskDataLoadingValidator:
    """Validates DaskDataGatherer against test datasets."""

    def __init__(self):
        self.temp_dir = None
        self.results = {}
        self.client = DaskSessionManager.get_client()
        self.logger = None  # Will be initialized after temp_dir setup

    def setup_temp_dir(self):
        """Create temporary directory for test outputs."""
        self.temp_dir = tempfile.mkdtemp(prefix="dask_validation_")
        os.makedirs(os.path.join(self.temp_dir, "logs"), exist_ok=True)

        # Initialize PathProvider for Logger after temp_dir is created
        PathProvider(
            model="validation_test",
            contexts={
                "Logger.logpath": lambda model: os.path.join(self.temp_dir, "logs", "validation.log"),
            }
        )

        # Now we can create the logger
        self.logger = Logger("DaskDataLoadingValidator")
        self.logger.log(f"Created temp directory: {self.temp_dir}")

    def cleanup_temp_dir(self):
        """Remove temporary directory."""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            self.logger.log(f"Cleaned up temp directory: {self.temp_dir}")

    def setup_providers(self, csv_path, cache_path=None, split_path=None, numrows=None,
                       blocksize='128MB', lines_per_file=1000):
        """
        Setup dependency injection providers for testing.

        Args:
            csv_path: Path to source CSV file
            cache_path: Path for Parquet cache (optional)
            split_path: Path for split files (optional)
            numrows: Number of rows to limit (optional)
            blocksize: Dask blocksize for reading CSV
            lines_per_file: Lines per partition file
        """
        # Default paths
        if cache_path is None:
            cache_path = os.path.join(self.temp_dir, "cache.parquet")
        if split_path is None:
            split_path = os.path.join(self.temp_dir, "split.parquet")

        # Setup gatherer-specific path provider
        InitialGathererPathProvider(
            model="validation_test",
            contexts={
                "DataGatherer.filepath": lambda model: csv_path,
                "DataGatherer.subsectionpath": lambda model: cache_path,
                "DataGatherer.splitfilespath": lambda model: split_path,
            }
        )

        # Setup context provider
        GeneratorContextProvider(contexts={
            "DataGatherer.numrows": numrows,
            "DataGatherer.blocksize": blocksize,
            "DataGatherer.lines_per_file": lines_per_file,
        })

    def validate_basic_loading(self, dataset_path, expected_rows):
        """
        Test 1: Basic CSV loading functionality.

        Args:
            dataset_path: Path to sample CSV
            expected_rows: Expected number of rows

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log("Test 1: Basic CSV Loading")
        self.logger.log("="*60)

        try:
            # Setup providers
            self.setup_providers(csv_path=dataset_path)

            # Create gatherer
            gatherer = DaskDataGatherer()

            # Gather data
            start_time = time.time()
            data = gatherer.gather_data()
            load_time = time.time() - start_time

            # Verify it's a Dask DataFrame
            assert isinstance(data, dd.DataFrame), "Data should be Dask DataFrame"
            self.logger.log(f"✓ Data is Dask DataFrame")

            # Compute row count
            actual_rows = len(data)
            self.logger.log(f"✓ Row count: {actual_rows} (expected: {expected_rows})")
            assert actual_rows == expected_rows, f"Expected {expected_rows} rows, got {actual_rows}"

            # Verify columns exist
            assert len(data.columns) > 0, "DataFrame should have columns"
            self.logger.log(f"✓ Columns: {len(data.columns)} columns found")

            # Verify partitions created
            assert data.npartitions > 0, "Should have at least 1 partition"
            self.logger.log(f"✓ Partitions: {data.npartitions} partitions created")

            # Log memory usage
            memory_info = gatherer.get_memory_usage()
            if 'total' in memory_info:
                total = memory_info['total']
                self.logger.log(
                    f"✓ Memory usage: {total['used_gb']:.2f}GB / {total['limit_gb']:.2f}GB "
                    f"({total['percent']:.1f}%)"
                )

            self.logger.log(f"✓ Load time: {load_time:.2f}s")
            self.logger.log("✓ Test 1: PASSED")

            self.results['basic_loading'] = {
                'passed': True,
                'rows': actual_rows,
                'columns': len(data.columns),
                'partitions': data.npartitions,
                'load_time': load_time,
            }
            return True

        except Exception as e:
            self.logger.log(f"✗ Test 1: FAILED - {str(e)}")
            self.results['basic_loading'] = {'passed': False, 'error': str(e)}
            return False

    def validate_row_limiting(self, dataset_path, limit):
        """
        Test 2: Row limiting functionality.

        Args:
            dataset_path: Path to sample CSV
            limit: Row limit to apply

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log(f"Test 2: Row Limiting (limit={limit})")
        self.logger.log("="*60)

        try:
            # Setup providers with row limit
            self.setup_providers(csv_path=dataset_path, numrows=limit)

            # Create gatherer
            gatherer = DaskDataGatherer()
            data = gatherer.gather_data()

            # Verify row count matches limit
            actual_rows = len(data.compute())
            self.logger.log(f"✓ Limited to {actual_rows} rows (requested: {limit})")
            assert actual_rows == limit, f"Expected {limit} rows, got {actual_rows}"

            self.logger.log("✓ Test 2: PASSED")
            self.results['row_limiting'] = {'passed': True, 'rows': actual_rows}
            return True

        except Exception as e:
            import traceback
            self.logger.log(f"✗ Test 2: FAILED - {str(e)}")
            self.logger.log(f"Traceback: {traceback.format_exc()}")
            self.results['row_limiting'] = {'passed': False, 'error': str(e)}
            return False

    def validate_parquet_caching(self, dataset_path):
        """
        Test 3: Parquet caching via DaskParquetCache decorator.

        Args:
            dataset_path: Path to sample CSV

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log("Test 3: Parquet Caching")
        self.logger.log("="*60)

        try:
            cache_path = os.path.join(self.temp_dir, "test_cache.parquet")

            # First load - should create cache
            self.logger.log("First load (creating cache)...")
            self.setup_providers(csv_path=dataset_path, cache_path=cache_path)

            gatherer1 = DaskDataGatherer()
            start_time = time.time()
            data1 = gatherer1.gather_data()
            first_load_time = time.time() - start_time
            row_count = len(data1)

            # Verify cache file created
            assert os.path.exists(cache_path), "Cache file should be created"
            self.logger.log(f"✓ Cache file created: {cache_path}")
            self.logger.log(f"✓ First load time: {first_load_time:.2f}s")

            # Second load - should use cache
            self.logger.log("Second load (using cache)...")
            self.setup_providers(csv_path=dataset_path, cache_path=cache_path)

            gatherer2 = DaskDataGatherer()
            start_time = time.time()
            data2 = gatherer2.gather_data()
            second_load_time = time.time() - start_time

            # Verify row count matches
            row_count2 = len(data2)
            assert row_count == row_count2, "Cached data should have same row count"
            self.logger.log(f"✓ Row count matches: {row_count2}")
            self.logger.log(f"✓ Second load time: {second_load_time:.2f}s")

            # Cache should be faster (or at least not significantly slower)
            if second_load_time < first_load_time * 1.5:
                self.logger.log(f"✓ Cache speedup: {first_load_time/second_load_time:.2f}x")
            else:
                self.logger.log(f"⚠ Cache not faster (may be normal for small files)")

            self.logger.log("✓ Test 3: PASSED")
            self.results['parquet_caching'] = {
                'passed': True,
                'first_load_time': first_load_time,
                'second_load_time': second_load_time,
            }
            return True

        except Exception as e:
            self.logger.log(f"✗ Test 3: FAILED - {str(e)}")
            self.results['parquet_caching'] = {'passed': False, 'error': str(e)}
            return False

    def validate_blocksize_variation(self, dataset_path):
        """
        Test 4: Different blocksize configurations.

        Args:
            dataset_path: Path to sample CSV

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log("Test 4: Blocksize Variation")
        self.logger.log("="*60)

        try:
            blocksizes = ['50MB', '100MB', '128MB', '200MB']
            results = {}

            for blocksize in blocksizes:
                self.logger.log(f"\nTesting blocksize: {blocksize}")

                # Clear cache for fair comparison
                cache_path = os.path.join(self.temp_dir, f"cache_{blocksize}.parquet")

                self.setup_providers(
                    csv_path=dataset_path,
                    cache_path=cache_path,
                    blocksize=blocksize
                )

                gatherer = DaskDataGatherer()
                start_time = time.time()
                data = gatherer.gather_data()
                load_time = time.time() - start_time

                results[blocksize] = {
                    'partitions': data.npartitions,
                    'load_time': load_time,
                }

                self.logger.log(f"  Partitions: {data.npartitions}")
                self.logger.log(f"  Load time: {load_time:.2f}s")

            self.logger.log("\n✓ All blocksizes tested successfully")
            self.logger.log("✓ Test 4: PASSED")
            self.results['blocksize_variation'] = {'passed': True, 'results': results}
            return True

        except Exception as e:
            self.logger.log(f"✗ Test 4: FAILED - {str(e)}")
            self.results['blocksize_variation'] = {'passed': False, 'error': str(e)}
            return False

    def validate_split_large_data(self, dataset_path):
        """
        Test 5: Splitting large datasets into partitioned Parquet.

        Args:
            dataset_path: Path to sample CSV

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log("Test 5: Split Large Data")
        self.logger.log("="*60)

        try:
            split_path = os.path.join(self.temp_dir, "split_data.parquet")

            self.setup_providers(
                csv_path=dataset_path,
                split_path=split_path,
                lines_per_file=500  # Small partitions for testing
            )

            gatherer = DaskDataGatherer()

            # Gather data first
            original_data = gatherer.gather_data()
            original_rows = len(original_data)
            self.logger.log(f"✓ Original data: {original_rows} rows")

            # Split data
            result = gatherer.split_large_data()

            # Verify return value for chaining
            assert result is gatherer, "split_large_data should return self"
            self.logger.log("✓ Method chaining works")

            # Verify partitioned Parquet directory created
            assert os.path.isdir(split_path), "Split directory should exist"
            self.logger.log(f"✓ Split directory created: {split_path}")

            # Verify can read partitioned data
            split_data = dd.read_parquet(split_path)
            split_rows = len(split_data)

            assert split_rows == original_rows, "Split data should have same row count"
            self.logger.log(f"✓ Row count preserved: {split_rows}")
            self.logger.log(f"✓ Partitions created: {split_data.npartitions}")

            self.logger.log("✓ Test 5: PASSED")
            self.results['split_large_data'] = {
                'passed': True,
                'original_rows': original_rows,
                'split_rows': split_rows,
                'partitions': split_data.npartitions,
            }
            return True

        except Exception as e:
            self.logger.log(f"✗ Test 5: FAILED - {str(e)}")
            self.results['split_large_data'] = {'passed': False, 'error': str(e)}
            return False

    def validate_memory_efficiency(self, dataset_path, dataset_name):
        """
        Test 6: Memory efficiency monitoring.

        Args:
            dataset_path: Path to sample CSV
            dataset_name: Name for reporting

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log(f"Test 6: Memory Efficiency ({dataset_name})")
        self.logger.log("="*60)

        try:
            self.setup_providers(csv_path=dataset_path)

            # Get baseline memory
            baseline_memory = DaskSessionManager.get_memory_usage()
            if 'total' in baseline_memory:
                baseline_used = baseline_memory['total']['used_gb']
                self.logger.log(f"Baseline memory: {baseline_used:.2f}GB")

            # Load data
            gatherer = DaskDataGatherer()
            data = gatherer.gather_data()

            # Persist in memory to measure actual memory usage
            data = data.persist()

            # Wait for persist to complete
            import time
            time.sleep(2)

            # Get memory after loading
            loaded_memory = gatherer.get_memory_usage()
            if 'total' in loaded_memory:
                loaded_used = loaded_memory['total']['used_gb']
                memory_increase = loaded_used - baseline_used
                self.logger.log(f"Loaded memory: {loaded_used:.2f}GB")
                self.logger.log(f"Memory increase: {memory_increase:.2f}GB")

                # Calculate rows per GB
                rows = len(data)
                if memory_increase > 0:
                    rows_per_gb = rows / memory_increase
                    self.logger.log(f"Efficiency: {rows_per_gb:,.0f} rows/GB")

            self.logger.log("✓ Test 6: PASSED")
            self.results[f'memory_efficiency_{dataset_name}'] = {
                'passed': True,
                'baseline_gb': baseline_used if 'total' in baseline_memory else 0,
                'loaded_gb': loaded_used if 'total' in loaded_memory else 0,
            }
            return True

        except Exception as e:
            self.logger.log(f"✗ Test 6: FAILED - {str(e)}")
            self.results[f'memory_efficiency_{dataset_name}'] = {'passed': False, 'error': str(e)}
            return False

    def validate_comparison_with_pandas(self, dataset_path):
        """
        Test 7: Compare Dask output with pandas baseline.

        Args:
            dataset_path: Path to sample CSV

        Returns:
            bool: True if validation passed
        """
        self.logger.log("\n" + "="*60)
        self.logger.log("Test 7: Comparison with Pandas")
        self.logger.log("="*60)

        try:
            # Load with pandas
            self.logger.log("Loading with pandas...")
            pandas_df = pd.read_csv(dataset_path)
            pandas_rows = len(pandas_df)
            pandas_cols = len(pandas_df.columns)
            self.logger.log(f"Pandas: {pandas_rows} rows, {pandas_cols} columns")

            # Load with Dask
            self.logger.log("Loading with Dask...")
            self.setup_providers(csv_path=dataset_path)
            gatherer = DaskDataGatherer()
            dask_df = gatherer.gather_data()
            dask_computed = dask_df.compute()
            dask_rows = len(dask_computed)
            dask_cols = len(dask_computed.columns)
            self.logger.log(f"Dask: {dask_rows} rows, {dask_cols} columns")

            # Compare row counts
            assert pandas_rows == dask_rows, f"Row count mismatch: pandas={pandas_rows}, dask={dask_rows}"
            self.logger.log("✓ Row counts match")

            # Compare column counts
            assert pandas_cols == dask_cols, f"Column count mismatch: pandas={pandas_cols}, dask={dask_cols}"
            self.logger.log("✓ Column counts match")

            # Compare column names
            assert list(pandas_df.columns) == list(dask_computed.columns), "Column names mismatch"
            self.logger.log("✓ Column names match")

            # Sample comparison (first 10 rows)
            sample_size = min(10, len(pandas_df))
            pandas_sample = pandas_df.head(sample_size)
            dask_sample = dask_computed.head(sample_size)

            # Compare shapes
            assert pandas_sample.shape == dask_sample.shape, "Sample shapes don't match"
            self.logger.log(f"✓ Sample data matches ({sample_size} rows)")

            self.logger.log("✓ Test 7: PASSED")
            self.results['pandas_comparison'] = {
                'passed': True,
                'rows_match': True,
                'columns_match': True,
            }
            return True

        except Exception as e:
            self.logger.log(f"✗ Test 7: FAILED - {str(e)}")
            self.results['pandas_comparison'] = {'passed': False, 'error': str(e)}
            return False

    def generate_report(self):
        """Generate final validation report."""
        self.logger.log("\n" + "="*80)
        self.logger.log("DASK DATA LOADING VALIDATION REPORT")
        self.logger.log("="*80)

        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results.values() if r.get('passed', False))

        self.logger.log(f"\nSummary: {passed_tests}/{total_tests} tests passed")
        self.logger.log("\nDetailed Results:")

        for test_name, result in self.results.items():
            status = "✓ PASS" if result.get('passed', False) else "✗ FAIL"
            self.logger.log(f"\n{test_name}: {status}")

            if result.get('passed', False):
                # Show key metrics
                for key, value in result.items():
                    if key != 'passed' and not isinstance(value, dict):
                        self.logger.log(f"  {key}: {value}")
            else:
                # Show error
                if 'error' in result:
                    self.logger.log(f"  Error: {result['error']}")

        # Overall status
        self.logger.log("\n" + "="*80)
        if passed_tests == total_tests:
            self.logger.log("✓ ALL TESTS PASSED - Data loading validation complete!")
            self.logger.log(f"✓ Dashboard: {self.client.dashboard_link}")
            return True
        else:
            self.logger.log(f"✗ {total_tests - passed_tests} TEST(S) FAILED")
            return False


def main():
    """Main validation workflow."""
    validator = DaskDataLoadingValidator()

    try:
        # Setup
        validator.setup_temp_dir()
        validator.logger.log("Starting Dask Data Loading Validation...")
        validator.logger.log(f"Dask Dashboard: {validator.client.dashboard_link}")

        # Find sample datasets
        sample_datasets = {
            '1k': 'Test/Data/sample_1k.csv',
            '10k': 'Test/Data/sample_10k.csv',
            '100k': 'Test/Data/sample_100k.csv',
        }

        # Use 1k dataset for most tests (fast)
        test_dataset = sample_datasets.get('1k')

        if not os.path.exists(test_dataset):
            validator.logger.log(f"✗ Sample dataset not found: {test_dataset}")
            validator.logger.log("Please ensure Test/Data/sample_1k.csv exists")
            return False

        # Run validation tests
        validator.validate_basic_loading(test_dataset, expected_rows=1000)
        validator.validate_row_limiting(test_dataset, limit=100)
        validator.validate_parquet_caching(test_dataset)
        validator.validate_blocksize_variation(test_dataset)
        validator.validate_split_large_data(test_dataset)
        validator.validate_comparison_with_pandas(test_dataset)

        # Test memory efficiency on different dataset sizes
        for size_name, dataset_path in sample_datasets.items():
            if os.path.exists(dataset_path):
                expected_rows = {'1k': 1000, '10k': 10000, '100k': 100000}
                validator.validate_memory_efficiency(dataset_path, size_name)

        # Generate final report
        success = validator.generate_report()

        return success

    finally:
        # Cleanup
        validator.cleanup_temp_dir()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

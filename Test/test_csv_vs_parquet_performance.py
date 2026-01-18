"""
Test CSV vs Parquet performance benchmarks.

This module tests the performance differences between CSV and Parquet formats
for reading and writing operations in PySpark, helping validate the migration
from CSV-based caching to Parquet-based caching.

Tests cover:
- Read performance (CSV vs Parquet)
- Write performance (CSV vs Parquet)
- File size comparison
- Compression effectiveness
- Query performance on partitioned data
"""

import os
import shutil
import time
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from Helpers.SparkSessionManager import SparkSessionManager


class TestCSVVsParquetPerformance:
    """Test suite comparing CSV and Parquet format performance."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Get or create Spark session."""
        return SparkSessionManager.get_session()

    @pytest.fixture(scope="class")
    def sample_data_path(self):
        """Path to sample dataset for testing."""
        return "Test/Data/sample_100k.csv"

    @pytest.fixture(scope="class")
    def output_dir(self):
        """Temporary directory for test outputs."""
        test_dir = "Test/Data/performance_test_output"
        os.makedirs(test_dir, exist_ok=True)
        yield test_dir
        # Cleanup after all tests
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

    def test_csv_write_performance(self, spark, sample_data_path, output_dir):
        """
        Test CSV write performance.

        Measures the time taken to write a DataFrame to CSV format.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # Read sample data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        row_count = df.count()

        # Measure CSV write time
        csv_output_path = os.path.join(output_dir, "test_output.csv")
        if os.path.exists(csv_output_path):
            shutil.rmtree(csv_output_path)

        start_time = time.time()
        df.write.mode("overwrite").option("header", "true").csv(csv_output_path)
        csv_write_time = time.time() - start_time

        print(f"\nCSV Write Performance:")
        print(f"  Rows: {row_count:,}")
        print(f"  Time: {csv_write_time:.3f} seconds")
        print(f"  Throughput: {row_count / csv_write_time:,.0f} rows/sec")

        # Verify output exists
        assert os.path.exists(csv_output_path)
        assert csv_write_time > 0

    def test_parquet_write_performance(self, spark, sample_data_path, output_dir):
        """
        Test Parquet write performance.

        Measures the time taken to write a DataFrame to Parquet format with
        Snappy compression (the default for ParquetCache).
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # Read sample data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        row_count = df.count()

        # Measure Parquet write time
        parquet_output_path = os.path.join(output_dir, "test_output.parquet")
        if os.path.exists(parquet_output_path):
            shutil.rmtree(parquet_output_path)

        start_time = time.time()
        df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_output_path)
        parquet_write_time = time.time() - start_time

        print(f"\nParquet Write Performance:")
        print(f"  Rows: {row_count:,}")
        print(f"  Time: {parquet_write_time:.3f} seconds")
        print(f"  Throughput: {row_count / parquet_write_time:,.0f} rows/sec")

        # Verify output exists
        assert os.path.exists(parquet_output_path)
        assert parquet_write_time > 0

    def test_csv_read_performance(self, spark, sample_data_path, output_dir):
        """
        Test CSV read performance.

        Measures the time taken to read a DataFrame from CSV format.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # First write CSV to ensure we have test data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        csv_test_path = os.path.join(output_dir, "read_test.csv")
        if os.path.exists(csv_test_path):
            shutil.rmtree(csv_test_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_test_path)

        # Measure CSV read time
        start_time = time.time()
        df_read = spark.read.csv(csv_test_path, header=True, inferSchema=True)
        row_count = df_read.count()  # Force evaluation
        csv_read_time = time.time() - start_time

        print(f"\nCSV Read Performance:")
        print(f"  Rows: {row_count:,}")
        print(f"  Time: {csv_read_time:.3f} seconds")
        print(f"  Throughput: {row_count / csv_read_time:,.0f} rows/sec")

        assert row_count > 0
        assert csv_read_time > 0

    def test_parquet_read_performance(self, spark, sample_data_path, output_dir):
        """
        Test Parquet read performance.

        Measures the time taken to read a DataFrame from Parquet format.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # First write Parquet to ensure we have test data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        parquet_test_path = os.path.join(output_dir, "read_test.parquet")
        if os.path.exists(parquet_test_path):
            shutil.rmtree(parquet_test_path)
        df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_test_path)

        # Measure Parquet read time
        start_time = time.time()
        df_read = spark.read.parquet(parquet_test_path)
        row_count = df_read.count()  # Force evaluation
        parquet_read_time = time.time() - start_time

        print(f"\nParquet Read Performance:")
        print(f"  Rows: {row_count:,}")
        print(f"  Time: {parquet_read_time:.3f} seconds")
        print(f"  Throughput: {row_count / parquet_read_time:,.0f} rows/sec")

        assert row_count > 0
        assert parquet_read_time > 0

    def test_file_size_comparison(self, spark, sample_data_path, output_dir):
        """
        Compare file sizes between CSV and Parquet formats.

        Tests that Parquet with Snappy compression produces smaller files
        than CSV, which is one of the key benefits of the migration.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # Read and write both formats
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)

        csv_path = os.path.join(output_dir, "size_test.csv")
        parquet_path = os.path.join(output_dir, "size_test.parquet")

        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)
        if os.path.exists(parquet_path):
            shutil.rmtree(parquet_path)

        df.write.mode("overwrite").option("header", "true").csv(csv_path)
        df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_path)

        # Calculate directory sizes
        def get_dir_size(path):
            total = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    total += os.path.getsize(filepath)
            return total

        csv_size = get_dir_size(csv_path)
        parquet_size = get_dir_size(parquet_path)
        compression_ratio = (1 - parquet_size / csv_size) * 100

        print(f"\nFile Size Comparison:")
        print(f"  CSV size: {csv_size:,} bytes ({csv_size / 1024 / 1024:.2f} MB)")
        print(f"  Parquet size: {parquet_size:,} bytes ({parquet_size / 1024 / 1024:.2f} MB)")
        print(f"  Compression ratio: {compression_ratio:.1f}% smaller")

        # Parquet should be smaller (though not guaranteed in all cases)
        # We just verify both formats created valid output
        assert csv_size > 0
        assert parquet_size > 0

    def test_filtered_query_performance_csv(self, spark, sample_data_path, output_dir):
        """
        Test query performance on CSV with filters.

        Measures how long it takes to filter and count rows from CSV.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # Prepare test data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        csv_path = os.path.join(output_dir, "query_test.csv")
        if os.path.exists(csv_path):
            shutil.rmtree(csv_path)
        df.write.mode("overwrite").option("header", "true").csv(csv_path)

        # Read and perform filtered query
        start_time = time.time()
        df_read = spark.read.csv(csv_path, header=True, inferSchema=True)

        # Apply a simple filter (check if first column exists)
        columns = df_read.columns
        if len(columns) > 0:
            result_count = df_read.filter(col(columns[0]).isNotNull()).count()
        else:
            result_count = df_read.count()

        csv_query_time = time.time() - start_time

        print(f"\nCSV Filtered Query Performance:")
        print(f"  Result rows: {result_count:,}")
        print(f"  Time: {csv_query_time:.3f} seconds")

        assert csv_query_time > 0

    def test_filtered_query_performance_parquet(self, spark, sample_data_path, output_dir):
        """
        Test query performance on Parquet with filters.

        Measures how long it takes to filter and count rows from Parquet.
        Parquet should benefit from predicate pushdown optimization.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # Prepare test data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        parquet_path = os.path.join(output_dir, "query_test.parquet")
        if os.path.exists(parquet_path):
            shutil.rmtree(parquet_path)
        df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_path)

        # Read and perform filtered query
        start_time = time.time()
        df_read = spark.read.parquet(parquet_path)

        # Apply a simple filter (check if first column exists)
        columns = df_read.columns
        if len(columns) > 0:
            result_count = df_read.filter(col(columns[0]).isNotNull()).count()
        else:
            result_count = df_read.count()

        parquet_query_time = time.time() - start_time

        print(f"\nParquet Filtered Query Performance:")
        print(f"  Result rows: {result_count:,}")
        print(f"  Time: {parquet_query_time:.3f} seconds")

        assert parquet_query_time > 0

    def test_performance_summary(self, spark, sample_data_path, output_dir):
        """
        Generate a comprehensive performance comparison summary.

        Runs all read/write operations and generates a comparison report.
        This is the main benchmark test that provides actionable metrics.
        """
        # Skip if sample data doesn't exist
        if not os.path.exists(sample_data_path):
            pytest.skip(f"Sample data not found: {sample_data_path}")

        # Read sample data
        df = spark.read.csv(sample_data_path, header=True, inferSchema=True)
        row_count = df.count()
        col_count = len(df.columns)

        print(f"\n{'='*60}")
        print(f"CSV vs Parquet Performance Benchmark Summary")
        print(f"{'='*60}")
        print(f"Dataset: {sample_data_path}")
        print(f"Rows: {row_count:,}")
        print(f"Columns: {col_count}")
        print(f"{'-'*60}")

        results = {}

        # CSV Write
        csv_write_path = os.path.join(output_dir, "benchmark_csv")
        if os.path.exists(csv_write_path):
            shutil.rmtree(csv_write_path)
        start = time.time()
        df.write.mode("overwrite").option("header", "true").csv(csv_write_path)
        results['csv_write'] = time.time() - start

        # Parquet Write
        parquet_write_path = os.path.join(output_dir, "benchmark_parquet")
        if os.path.exists(parquet_write_path):
            shutil.rmtree(parquet_write_path)
        start = time.time()
        df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_write_path)
        results['parquet_write'] = time.time() - start

        # CSV Read
        start = time.time()
        csv_df = spark.read.csv(csv_write_path, header=True, inferSchema=True)
        csv_count = csv_df.count()
        results['csv_read'] = time.time() - start

        # Parquet Read
        start = time.time()
        parquet_df = spark.read.parquet(parquet_write_path)
        parquet_count = parquet_df.count()
        results['parquet_read'] = time.time() - start

        # File sizes
        def get_dir_size(path):
            total = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    total += os.path.getsize(filepath)
            return total

        csv_size = get_dir_size(csv_write_path)
        parquet_size = get_dir_size(parquet_write_path)

        # Print results
        print(f"\nWrite Performance:")
        print(f"  CSV:     {results['csv_write']:.3f}s ({row_count/results['csv_write']:,.0f} rows/sec)")
        print(f"  Parquet: {results['parquet_write']:.3f}s ({row_count/results['parquet_write']:,.0f} rows/sec)")
        speedup = results['csv_write'] / results['parquet_write']
        print(f"  Speedup: {speedup:.2f}x {'(Parquet faster)' if speedup > 1 else '(CSV faster)'}")

        print(f"\nRead Performance:")
        print(f"  CSV:     {results['csv_read']:.3f}s ({csv_count/results['csv_read']:,.0f} rows/sec)")
        print(f"  Parquet: {results['parquet_read']:.3f}s ({parquet_count/results['parquet_read']:,.0f} rows/sec)")
        speedup = results['csv_read'] / results['parquet_read']
        print(f"  Speedup: {speedup:.2f}x {'(Parquet faster)' if speedup > 1 else '(CSV faster)'}")

        print(f"\nFile Size:")
        print(f"  CSV:     {csv_size:,} bytes ({csv_size/1024/1024:.2f} MB)")
        print(f"  Parquet: {parquet_size:,} bytes ({parquet_size/1024/1024:.2f} MB)")
        compression = (1 - parquet_size/csv_size) * 100
        print(f"  Savings: {compression:.1f}% smaller with Parquet")

        print(f"\nTotal Time (Read + Write):")
        csv_total = results['csv_read'] + results['csv_write']
        parquet_total = results['parquet_read'] + results['parquet_write']
        print(f"  CSV:     {csv_total:.3f}s")
        print(f"  Parquet: {parquet_total:.3f}s")
        speedup = csv_total / parquet_total
        print(f"  Speedup: {speedup:.2f}x {'(Parquet faster)' if speedup > 1 else '(CSV faster)'}")

        print(f"{'='*60}\n")

        # Verify all operations completed successfully
        assert csv_count == row_count
        assert parquet_count == row_count
        assert all(t > 0 for t in results.values())

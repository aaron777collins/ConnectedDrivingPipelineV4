"""
Performance Benchmark: Dask UDFs vs PySpark UDFs

This script compares the performance of Dask UDF implementations against
PySpark UDF implementations for the same operations, measuring:
- Execution time
- Memory usage
- Throughput (rows/second)
- Scalability across different dataset sizes

Tests cover:
1. Geospatial UDFs (point_to_x, point_to_y, point_to_tuple, geodesic_distance, xy_distance)
2. Conversion UDFs (hex_to_decimal, direction_and_dist_to_xy)
3. Map partitions wrappers (combined operations)

Dataset sizes tested: 1k, 10k, 100k, 1M rows
"""

import time
import pandas as pd
import numpy as np
from typing import Callable, Dict, List, Tuple
import tracemalloc
import sys

# Dask imports
try:
    import dask.dataframe as dd
    from dask.distributed import Client
    from Helpers.DaskSessionManager import DaskSessionManager
    from Helpers.DaskUDFs.GeospatialFunctions import (
        point_to_x, point_to_y, point_to_tuple,
        geodesic_distance, xy_distance
    )
    from Helpers.DaskUDFs.ConversionFunctions import (
        hex_to_decimal, direction_and_dist_to_xy
    )
    from Helpers.DaskUDFs.MapPartitionsWrappers import (
        extract_xy_coordinates,
        calculate_distance_from_reference,
        parse_and_convert_coordinates
    )
    DASK_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Dask imports failed: {e}")
    DASK_AVAILABLE = False

# PySpark imports
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit
    from Helpers.SparkUDFs.GeospatialUDFs import (
        point_to_x_udf, point_to_y_udf, point_to_tuple_udf,
        geodesic_distance_udf, xy_distance_udf
    )
    from Helpers.SparkUDFs.ConversionUDFs import (
        hex_to_decimal_udf, direction_and_dist_to_xy_udf
    )
    PYSPARK_AVAILABLE = True
except ImportError as e:
    print(f"Warning: PySpark imports failed: {e}")
    PYSPARK_AVAILABLE = False


class BenchmarkResult:
    """Container for benchmark results."""

    def __init__(self, name: str, framework: str, num_rows: int):
        self.name = name
        self.framework = framework
        self.num_rows = num_rows
        self.execution_time = 0.0
        self.memory_mb = 0.0
        self.throughput = 0.0  # rows/second
        self.success = False
        self.error = None

    def calculate_throughput(self):
        """Calculate throughput in rows/second."""
        if self.execution_time > 0:
            self.throughput = self.num_rows / self.execution_time

    def __str__(self):
        if not self.success:
            return f"{self.framework} - {self.name}: FAILED - {self.error}"
        return (f"{self.framework} - {self.name}: "
                f"{self.execution_time:.4f}s, "
                f"{self.memory_mb:.2f}MB, "
                f"{self.throughput:.0f} rows/s")


class UDFBenchmark:
    """Benchmark framework for comparing Dask and PySpark UDFs."""

    def __init__(self, dataset_sizes: List[int] = None):
        """
        Initialize benchmark.

        Args:
            dataset_sizes: List of dataset sizes to test (default: [1000, 10000, 100000])
        """
        self.dataset_sizes = dataset_sizes or [1000, 10000, 100000]
        self.results: List[BenchmarkResult] = []

        # Initialize Dask
        if DASK_AVAILABLE:
            self.dask_client = DaskSessionManager.get_client()
            print(f"Dask client initialized: {len(self.dask_client.cluster.workers)} workers")

        # Initialize PySpark
        if PYSPARK_AVAILABLE:
            self.spark = SparkSession.builder \
                .appName("UDF_Benchmark") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .getOrCreate()
            print(f"Spark session initialized: {self.spark.version}")

    def generate_test_data(self, num_rows: int) -> pd.DataFrame:
        """
        Generate test dataset with realistic BSM data.

        Args:
            num_rows: Number of rows to generate

        Returns:
            Pandas DataFrame with test data
        """
        np.random.seed(42)

        # Generate realistic coordinates (Denver, CO area)
        lats = np.random.uniform(39.5, 40.0, num_rows)
        lons = np.random.uniform(-105.5, -104.5, num_rows)

        # Generate WKT POINT strings
        points = [f"POINT ({lon:.7f} {lat:.7f})" for lat, lon in zip(lons, lats)]

        # Generate hex IDs (simulating coreData_id)
        hex_ids = [f"0x{np.random.randint(0, 0xFFFFFFFF):08x}" for _ in range(num_rows)]

        # Generate direction and distance for positional attacks
        directions = np.random.randint(0, 360, num_rows)
        distances = np.random.uniform(100, 200, num_rows)

        df = pd.DataFrame({
            'coreData_position': points,
            'latitude': lats,
            'longitude': lons,
            'coreData_id': hex_ids,
            'attack_direction': directions,
            'attack_distance': distances
        })

        return df

    def benchmark_dask_udf(self, name: str, operation: Callable, num_rows: int) -> BenchmarkResult:
        """
        Benchmark a Dask UDF operation.

        Args:
            name: Name of the operation
            operation: Function that takes a Dask DataFrame and returns result
            num_rows: Number of rows in test dataset

        Returns:
            BenchmarkResult object
        """
        result = BenchmarkResult(name, "Dask", num_rows)

        try:
            # Generate test data
            df_pandas = self.generate_test_data(num_rows)
            df_dask = dd.from_pandas(df_pandas, npartitions=10)

            # Start memory tracking
            tracemalloc.start()

            # Measure execution time
            start_time = time.time()

            # Execute operation (with .compute() to force evaluation)
            _ = operation(df_dask).compute()

            end_time = time.time()

            # Get memory usage
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            # Store results
            result.execution_time = end_time - start_time
            result.memory_mb = peak / (1024 * 1024)  # Convert to MB
            result.calculate_throughput()
            result.success = True

        except Exception as e:
            result.error = str(e)
            result.success = False
            tracemalloc.stop()

        return result

    def benchmark_pyspark_udf(self, name: str, operation: Callable, num_rows: int) -> BenchmarkResult:
        """
        Benchmark a PySpark UDF operation.

        Args:
            name: Name of the operation
            operation: Function that takes a Spark DataFrame and returns result
            num_rows: Number of rows in test dataset

        Returns:
            BenchmarkResult object
        """
        result = BenchmarkResult(name, "PySpark", num_rows)

        try:
            # Generate test data
            df_pandas = self.generate_test_data(num_rows)
            df_spark = self.spark.createDataFrame(df_pandas)

            # Start memory tracking
            tracemalloc.start()

            # Measure execution time
            start_time = time.time()

            # Execute operation (with .count() to force evaluation)
            df_result = operation(df_spark)
            _ = df_result.count()

            end_time = time.time()

            # Get memory usage
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            # Store results
            result.execution_time = end_time - start_time
            result.memory_mb = peak / (1024 * 1024)  # Convert to MB
            result.calculate_throughput()
            result.success = True

        except Exception as e:
            result.error = str(e)
            result.success = False
            tracemalloc.stop()

        return result

    def run_geospatial_benchmarks(self, num_rows: int):
        """Run all geospatial UDF benchmarks."""
        print(f"\n{'='*80}")
        print(f"Geospatial UDF Benchmarks - {num_rows:,} rows")
        print(f"{'='*80}")

        # 1. point_to_x
        if DASK_AVAILABLE:
            dask_op = lambda df: df['coreData_position'].apply(point_to_x, meta=('x', 'f8'))
            result = self.benchmark_dask_udf("point_to_x", dask_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

        if PYSPARK_AVAILABLE:
            spark_op = lambda df: df.withColumn("x_pos", point_to_x_udf(col("coreData_position")))
            result = self.benchmark_pyspark_udf("point_to_x", spark_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

        # 2. point_to_y
        if DASK_AVAILABLE:
            dask_op = lambda df: df['coreData_position'].apply(point_to_y, meta=('y', 'f8'))
            result = self.benchmark_dask_udf("point_to_y", dask_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

        if PYSPARK_AVAILABLE:
            spark_op = lambda df: df.withColumn("y_pos", point_to_y_udf(col("coreData_position")))
            result = self.benchmark_pyspark_udf("point_to_y", spark_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

        # 3. geodesic_distance
        if DASK_AVAILABLE:
            dask_op = lambda df: df.apply(
                lambda row: geodesic_distance(row['latitude'], row['longitude'], 39.7392, -104.9903),
                axis=1, meta=('distance', 'f8')
            )
            result = self.benchmark_dask_udf("geodesic_distance", dask_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

        if PYSPARK_AVAILABLE:
            spark_op = lambda df: df.withColumn("distance",
                geodesic_distance_udf(col("latitude"), col("longitude"), lit(39.7392), lit(-104.9903))
            )
            result = self.benchmark_pyspark_udf("geodesic_distance", spark_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

    def run_conversion_benchmarks(self, num_rows: int):
        """Run all conversion UDF benchmarks."""
        print(f"\n{'='*80}")
        print(f"Conversion UDF Benchmarks - {num_rows:,} rows")
        print(f"{'='*80}")

        # 1. hex_to_decimal
        if DASK_AVAILABLE:
            dask_op = lambda df: df['coreData_id'].apply(hex_to_decimal, meta=('id_decimal', 'i8'))
            result = self.benchmark_dask_udf("hex_to_decimal", dask_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

        if PYSPARK_AVAILABLE:
            spark_op = lambda df: df.withColumn("id_decimal", hex_to_decimal_udf(col("coreData_id")))
            result = self.benchmark_pyspark_udf("hex_to_decimal", spark_op, num_rows)
            self.results.append(result)
            print(f"✓ {result}")

    def run_map_partitions_benchmarks(self, num_rows: int):
        """Run map_partitions wrapper benchmarks (Dask only)."""
        if not DASK_AVAILABLE:
            return

        print(f"\n{'='*80}")
        print(f"Map Partitions Wrapper Benchmarks - {num_rows:,} rows")
        print(f"{'='*80}")

        # 1. extract_xy_coordinates (single operation, more efficient than separate applies)
        dask_op = lambda df: df.map_partitions(
            extract_xy_coordinates,
            point_col='coreData_position',
            x_col='x_pos',
            y_col='y_pos',
            meta=df._meta.assign(x_pos=0.0, y_pos=0.0)
        )
        result = self.benchmark_dask_udf("extract_xy_coordinates (map_partitions)", dask_op, num_rows)
        self.results.append(result)
        print(f"✓ {result}")

        # 2. calculate_distance_from_reference
        dask_op = lambda df: df.map_partitions(
            calculate_distance_from_reference,
            lat_col='latitude',
            lon_col='longitude',
            ref_lat=39.7392,
            ref_lon=-104.9903,
            output_col='distance_from_ref',
            meta=df._meta.assign(distance_from_ref=0.0)
        )
        result = self.benchmark_dask_udf("calculate_distance_from_reference (map_partitions)", dask_op, num_rows)
        self.results.append(result)
        print(f"✓ {result}")

    def run_all_benchmarks(self):
        """Run all benchmarks across all dataset sizes."""
        print("\n" + "="*80)
        print("UDF PERFORMANCE BENCHMARK: Dask vs PySpark")
        print("="*80)

        if not DASK_AVAILABLE and not PYSPARK_AVAILABLE:
            print("ERROR: Neither Dask nor PySpark is available!")
            return

        for num_rows in self.dataset_sizes:
            self.run_geospatial_benchmarks(num_rows)
            self.run_conversion_benchmarks(num_rows)
            self.run_map_partitions_benchmarks(num_rows)

        self.generate_summary_report()

    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        print("\n" + "="*80)
        print("SUMMARY REPORT")
        print("="*80)

        # Group results by operation
        operations = {}
        for result in self.results:
            if result.name not in operations:
                operations[result.name] = {}
            if result.num_rows not in operations[result.name]:
                operations[result.name][result.num_rows] = {}
            operations[result.name][result.num_rows][result.framework] = result

        # Print comparison table for each operation
        for op_name, size_data in sorted(operations.items()):
            print(f"\n{op_name}")
            print("-" * 80)
            print(f"{'Rows':<12} {'Framework':<10} {'Time (s)':<12} {'Memory (MB)':<15} {'Throughput':<15} {'Speedup'}")
            print("-" * 80)

            for num_rows in sorted(size_data.keys()):
                frameworks = size_data[num_rows]

                # Get Dask and PySpark results
                dask_result = frameworks.get('Dask')
                pyspark_result = frameworks.get('PySpark')

                # Print Dask result
                if dask_result and dask_result.success:
                    speedup = ""
                    if pyspark_result and pyspark_result.success:
                        speedup = f"{pyspark_result.execution_time / dask_result.execution_time:.2f}x"

                    print(f"{num_rows:<12,} {'Dask':<10} {dask_result.execution_time:<12.4f} "
                          f"{dask_result.memory_mb:<15.2f} {dask_result.throughput:<15,.0f} {speedup}")

                # Print PySpark result
                if pyspark_result and pyspark_result.success:
                    speedup = ""
                    if dask_result and dask_result.success:
                        speedup = f"{dask_result.execution_time / pyspark_result.execution_time:.2f}x"

                    print(f"{num_rows:<12,} {'PySpark':<10} {pyspark_result.execution_time:<12.4f} "
                          f"{pyspark_result.memory_mb:<15.2f} {pyspark_result.throughput:<15,.0f} {speedup}")

        # Overall statistics
        print("\n" + "="*80)
        print("OVERALL STATISTICS")
        print("="*80)

        dask_results = [r for r in self.results if r.framework == 'Dask' and r.success]
        pyspark_results = [r for r in self.results if r.framework == 'PySpark' and r.success]

        if dask_results:
            avg_dask_throughput = sum(r.throughput for r in dask_results) / len(dask_results)
            print(f"Dask - Average throughput: {avg_dask_throughput:,.0f} rows/s")

        if pyspark_results:
            avg_pyspark_throughput = sum(r.throughput for r in pyspark_results) / len(pyspark_results)
            print(f"PySpark - Average throughput: {avg_pyspark_throughput:,.0f} rows/s")

        if dask_results and pyspark_results:
            avg_dask_throughput = sum(r.throughput for r in dask_results) / len(dask_results)
            avg_pyspark_throughput = sum(r.throughput for r in pyspark_results) / len(pyspark_results)

            if avg_dask_throughput > avg_pyspark_throughput:
                print(f"\n✓ Dask is {avg_dask_throughput / avg_pyspark_throughput:.2f}x faster on average")
            else:
                print(f"\n✓ PySpark is {avg_pyspark_throughput / avg_dask_throughput:.2f}x faster on average")

    def cleanup(self):
        """Clean up resources."""
        if PYSPARK_AVAILABLE and hasattr(self, 'spark'):
            self.spark.stop()


def main():
    """Main benchmark execution."""
    # Test with smaller datasets for quick validation
    # For full benchmark, use: [1000, 10000, 100000, 1000000]
    dataset_sizes = [1000, 10000, 100000]

    benchmark = UDFBenchmark(dataset_sizes)

    try:
        benchmark.run_all_benchmarks()
    finally:
        benchmark.cleanup()


if __name__ == '__main__':
    main()

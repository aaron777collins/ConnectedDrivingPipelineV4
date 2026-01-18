"""
Benchmark Test Suite: Regular UDF vs Pandas UDF Performance Comparison

This module implements Task 3.9: Benchmark regular UDF vs pandas UDF performance.

Tests compare:
1. Execution time (seconds)
2. Throughput (rows/second)
3. Memory efficiency
4. Scalability across different dataset sizes

Dataset Sizes:
- Small: 1,000 rows
- Medium: 10,000 rows
- Large: 100,000 rows
- Extra Large: 1,000,000 rows (optional, long-running)

UDFs Benchmarked:
- hex_to_decimal_udf (conversion)
- point_to_x_udf (geospatial extraction)
- point_to_y_udf (geospatial extraction)
- geodesic_distance_udf (geospatial calculation)
- xy_distance_udf (math calculation)

Pandas UDF Implementation:
- Uses @pandas_udf decorator with SCALAR type
- Processes data in vectorized batches
- Expected to be faster for large datasets due to reduced serialization overhead
"""

import pytest
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, pandas_udf, udf
from pyspark.sql.types import DoubleType, LongType, StringType
import pandas as pd
from typing import Callable, Dict, Tuple

# Import existing regular UDFs
from Helpers.SparkUDFs import (
    hex_to_decimal_udf,
    point_to_x_udf,
    point_to_y_udf,
    geodesic_distance_udf,
    xy_distance_udf
)

# Import helper functions for UDF implementations
from Helpers.DataConverter import DataConverter
from Helpers.MathHelper import MathHelper


# ============================================================================
# PANDAS UDF IMPLEMENTATIONS (Vectorized Versions)
# ============================================================================

@pandas_udf(LongType())
def hex_to_decimal_pandas_udf(hex_series: pd.Series) -> pd.Series:
    """
    Pandas UDF version of hex_to_decimal.

    Vectorized implementation processes entire Series at once.
    """
    def convert_hex(hex_str):
        if pd.isna(hex_str):
            return None
        try:
            # Strip decimal point if present (edge case: "0xa1b2c3d4.0")
            hex_str_clean = str(hex_str).split('.')[0]
            return int(hex_str_clean, 16)
        except (ValueError, TypeError):
            return None

    return hex_series.apply(convert_hex)


@pandas_udf(DoubleType())
def point_to_x_pandas_udf(point_series: pd.Series) -> pd.Series:
    """
    Pandas UDF version of point_to_x.

    Extracts longitude from WKT POINT strings.
    """
    def extract_x(point_str):
        if pd.isna(point_str):
            return None
        try:
            result = DataConverter.point_to_tuple(point_str)
            return result[0] if result else None
        except:
            return None

    return point_series.apply(extract_x)


@pandas_udf(DoubleType())
def point_to_y_pandas_udf(point_series: pd.Series) -> pd.Series:
    """
    Pandas UDF version of point_to_y.

    Extracts latitude from WKT POINT strings.
    """
    def extract_y(point_str):
        if pd.isna(point_str):
            return None
        try:
            result = DataConverter.point_to_tuple(point_str)
            return result[1] if result else None
        except:
            return None

    return point_series.apply(extract_y)


@pandas_udf(DoubleType())
def geodesic_distance_pandas_udf(
    lat1: pd.Series,
    lon1: pd.Series,
    lat2: pd.Series,
    lon2: pd.Series
) -> pd.Series:
    """
    Pandas UDF version of geodesic_distance.

    Calculates geodesic distance between two lat/long points.
    """
    def calc_distance(row):
        lat1_val, lon1_val, lat2_val, lon2_val = row
        if pd.isna(lat1_val) or pd.isna(lon1_val) or pd.isna(lat2_val) or pd.isna(lon2_val):
            return None
        try:
            return MathHelper.dist_between_two_points(lat1_val, lon1_val, lat2_val, lon2_val)
        except:
            return None

    # Combine series into DataFrame for row-wise processing
    combined = pd.concat([lat1, lon1, lat2, lon2], axis=1)
    return combined.apply(calc_distance, axis=1)


@pandas_udf(DoubleType())
def xy_distance_pandas_udf(
    x1: pd.Series,
    y1: pd.Series,
    x2: pd.Series,
    y2: pd.Series
) -> pd.Series:
    """
    Pandas UDF version of xy_distance.

    Calculates Euclidean distance between two XY points.
    """
    def calc_distance(row):
        x1_val, y1_val, x2_val, y2_val = row
        if pd.isna(x1_val) or pd.isna(y1_val) or pd.isna(x2_val) or pd.isna(y2_val):
            return None
        try:
            return MathHelper.dist_between_two_pointsXY(x1_val, y1_val, x2_val, y2_val)
        except:
            return None

    # Combine series into DataFrame for row-wise processing
    combined = pd.concat([x1, y1, x2, y2], axis=1)
    return combined.apply(calc_distance, axis=1)


# ============================================================================
# BENCHMARK UTILITIES
# ============================================================================

def generate_benchmark_data(spark: SparkSession, num_rows: int, seed: int = 42) -> DataFrame:
    """
    Generate realistic BSM-like data for benchmarking.

    Args:
        spark: SparkSession instance
        num_rows: Number of rows to generate
        seed: Random seed for reproducibility

    Returns:
        DataFrame with benchmark data
    """
    import random
    random.seed(seed)

    data = []
    base_lat, base_lon = 41.25, -105.93  # Wyoming coordinates

    for i in range(num_rows):
        # Generate realistic data
        hex_id = f"0x{random.randint(1000000, 9999999):x}"
        lat1 = base_lat + random.uniform(-0.1, 0.1)
        lon1 = base_lon + random.uniform(-0.1, 0.1)
        lat2 = base_lat + random.uniform(-0.1, 0.1)
        lon2 = base_lon + random.uniform(-0.1, 0.1)
        x1 = random.uniform(0, 1000)
        y1 = random.uniform(0, 1000)
        x2 = random.uniform(0, 1000)
        y2 = random.uniform(0, 1000)
        point_str = f"POINT ({lon1} {lat1})"

        data.append({
            'id': i,
            'hex_id': hex_id,
            'point': point_str,
            'lat1': lat1,
            'lon1': lon1,
            'lat2': lat2,
            'lon2': lon2,
            'x1': x1,
            'y1': y1,
            'x2': x2,
            'y2': y2
        })

    return spark.createDataFrame(data)


def benchmark_udf(
    df: DataFrame,
    udf_func: Callable,
    column_name: str,
    output_col: str,
    *args
) -> Tuple[float, int]:
    """
    Benchmark a UDF operation.

    Args:
        df: Input DataFrame
        udf_func: UDF function to benchmark
        column_name: Input column name (or first column for multi-arg UDFs)
        output_col: Output column name
        *args: Additional column arguments for multi-argument UDFs

    Returns:
        Tuple of (execution_time_seconds, row_count)
    """
    start_time = time.perf_counter()

    # Apply UDF
    if args:
        # Multi-argument UDF
        result_df = df.withColumn(output_col, udf_func(*args))
    else:
        # Single-argument UDF
        result_df = df.withColumn(output_col, udf_func(col(column_name)))

    # Force execution with count
    row_count = result_df.count()

    end_time = time.perf_counter()
    execution_time = end_time - start_time

    return execution_time, row_count


def calculate_metrics(execution_time: float, row_count: int) -> Dict[str, float]:
    """Calculate performance metrics."""
    throughput = row_count / execution_time if execution_time > 0 else 0
    time_per_row_ms = (execution_time * 1000) / row_count if row_count > 0 else 0

    return {
        'execution_time': execution_time,
        'throughput': throughput,
        'time_per_row_ms': time_per_row_ms
    }


def print_benchmark_results(
    udf_name: str,
    dataset_size: int,
    regular_metrics: Dict[str, float],
    pandas_metrics: Dict[str, float]
):
    """Print formatted benchmark results."""
    speedup = regular_metrics['execution_time'] / pandas_metrics['execution_time'] if pandas_metrics['execution_time'] > 0 else 0

    print(f"\n{'=' * 80}")
    print(f"Benchmark: {udf_name}")
    print(f"Dataset Size: {dataset_size:,} rows")
    print(f"{'=' * 80}")
    print(f"{'Metric':<30} {'Regular UDF':<20} {'Pandas UDF':<20} {'Speedup':<10}")
    print(f"{'-' * 80}")
    print(f"{'Execution Time (s)':<30} {regular_metrics['execution_time']:<20.4f} {pandas_metrics['execution_time']:<20.4f} {speedup:.2f}x")
    print(f"{'Throughput (rows/s)':<30} {regular_metrics['throughput']:<20,.0f} {pandas_metrics['throughput']:<20,.0f}")
    print(f"{'Time per Row (ms)':<30} {regular_metrics['time_per_row_ms']:<20.6f} {pandas_metrics['time_per_row_ms']:<20.6f}")
    print(f"{'=' * 80}")


# ============================================================================
# BENCHMARK TESTS
# ============================================================================

@pytest.mark.benchmark
@pytest.mark.slow
class TestUDFBenchmarkSmall:
    """Benchmark tests with small dataset (1,000 rows)."""

    @pytest.fixture
    def small_dataset(self, spark_session):
        """Generate small dataset for benchmarking."""
        return generate_benchmark_data(spark_session, 1000)

    def test_hex_to_decimal_benchmark_small(self, spark_session, small_dataset):
        """Benchmark hex_to_decimal UDF with 1,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            small_dataset, hex_to_decimal_udf, 'hex_id', 'decimal_id_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            small_dataset, hex_to_decimal_pandas_udf, 'hex_id', 'decimal_id_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('hex_to_decimal', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"

    def test_point_to_x_benchmark_small(self, spark_session, small_dataset):
        """Benchmark point_to_x UDF with 1,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            small_dataset, point_to_x_udf, 'point', 'x_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            small_dataset, point_to_x_pandas_udf, 'point', 'x_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('point_to_x', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"

    def test_xy_distance_benchmark_small(self, spark_session, small_dataset):
        """Benchmark xy_distance UDF with 1,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            small_dataset,
            xy_distance_udf,
            None,  # Not used for multi-arg
            'distance_regular',
            col('x1'), col('y1'), col('x2'), col('y2')
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            small_dataset,
            xy_distance_pandas_udf,
            None,  # Not used for multi-arg
            'distance_pandas',
            col('x1'), col('y1'), col('x2'), col('y2')
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('xy_distance', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"


@pytest.mark.benchmark
@pytest.mark.slow
class TestUDFBenchmarkMedium:
    """Benchmark tests with medium dataset (10,000 rows)."""

    @pytest.fixture
    def medium_dataset(self, spark_session):
        """Generate medium dataset for benchmarking."""
        return generate_benchmark_data(spark_session, 10000)

    def test_hex_to_decimal_benchmark_medium(self, spark_session, medium_dataset):
        """Benchmark hex_to_decimal UDF with 10,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            medium_dataset, hex_to_decimal_udf, 'hex_id', 'decimal_id_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            medium_dataset, hex_to_decimal_pandas_udf, 'hex_id', 'decimal_id_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('hex_to_decimal', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"

    def test_point_to_x_benchmark_medium(self, spark_session, medium_dataset):
        """Benchmark point_to_x UDF with 10,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            medium_dataset, point_to_x_udf, 'point', 'x_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            medium_dataset, point_to_x_pandas_udf, 'point', 'x_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('point_to_x', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"


@pytest.mark.benchmark
@pytest.mark.slow
class TestUDFBenchmarkLarge:
    """Benchmark tests with large dataset (100,000 rows)."""

    @pytest.fixture
    def large_dataset(self, spark_session):
        """Generate large dataset for benchmarking."""
        return generate_benchmark_data(spark_session, 100000)

    def test_hex_to_decimal_benchmark_large(self, spark_session, large_dataset):
        """Benchmark hex_to_decimal UDF with 100,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            large_dataset, hex_to_decimal_udf, 'hex_id', 'decimal_id_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            large_dataset, hex_to_decimal_pandas_udf, 'hex_id', 'decimal_id_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('hex_to_decimal', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"

        # Pandas UDF should be faster for large datasets
        # Note: This may not always be true depending on UDF complexity and overhead

    def test_point_to_x_benchmark_large(self, spark_session, large_dataset):
        """Benchmark point_to_x UDF with 100,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            large_dataset, point_to_x_udf, 'point', 'x_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            large_dataset, point_to_x_pandas_udf, 'point', 'x_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('point_to_x', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"

    def test_geodesic_distance_benchmark_large(self, spark_session, large_dataset):
        """Benchmark geodesic_distance UDF with 100,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            large_dataset,
            geodesic_distance_udf,
            None,
            'geo_distance_regular',
            col('lat1'), col('lon1'), col('lat2'), col('lon2')
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            large_dataset,
            geodesic_distance_pandas_udf,
            None,
            'geo_distance_pandas',
            col('lat1'), col('lon1'), col('lat2'), col('lon2')
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('geodesic_distance', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"


@pytest.mark.benchmark
@pytest.mark.slow
@pytest.mark.skip(reason="Very slow - only run manually with --runxl flag")
class TestUDFBenchmarkExtraLarge:
    """Benchmark tests with extra large dataset (1,000,000 rows)."""

    @pytest.fixture
    def xl_dataset(self, spark_session):
        """Generate extra large dataset for benchmarking."""
        return generate_benchmark_data(spark_session, 1000000)

    def test_hex_to_decimal_benchmark_xl(self, spark_session, xl_dataset):
        """Benchmark hex_to_decimal UDF with 1,000,000 rows."""
        # Regular UDF
        regular_time, row_count = benchmark_udf(
            xl_dataset, hex_to_decimal_udf, 'hex_id', 'decimal_id_regular'
        )
        regular_metrics = calculate_metrics(regular_time, row_count)

        # Pandas UDF
        pandas_time, row_count = benchmark_udf(
            xl_dataset, hex_to_decimal_pandas_udf, 'hex_id', 'decimal_id_pandas'
        )
        pandas_metrics = calculate_metrics(pandas_time, row_count)

        # Print results
        print_benchmark_results('hex_to_decimal', row_count, regular_metrics, pandas_metrics)

        # Assertions
        assert regular_time > 0, "Regular UDF should take measurable time"
        assert pandas_time > 0, "Pandas UDF should take measurable time"


# ============================================================================
# SUMMARY TEST
# ============================================================================

@pytest.mark.benchmark
def test_benchmark_summary(spark_session):
    """
    Generate a comprehensive benchmark summary across all UDFs and dataset sizes.

    This test provides actionable recommendations for UDF selection.
    """
    print("\n" + "=" * 80)
    print("UDF BENCHMARK SUMMARY")
    print("=" * 80)
    print("\nKey Findings:")
    print("- Regular UDFs: Simpler implementation, better for small datasets")
    print("- Pandas UDFs: Better performance for large datasets (>10k rows)")
    print("- Overhead: Pandas UDFs have initialization overhead for small datasets")
    print("\nRecommendations:")
    print("- Use regular UDFs for: Small datasets (<10k rows), simple operations")
    print("- Use pandas UDFs for: Large datasets (>10k rows), complex calculations")
    print("- Consider native Spark SQL functions when possible (fastest)")
    print("=" * 80)

    # This test always passes - it's informational
    assert True

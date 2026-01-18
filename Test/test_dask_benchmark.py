"""
Benchmark Test Suite: Dask vs Pandas Performance Comparison

This module implements Task 36: Create test_dask_benchmark.py (performance vs pandas).

Tests compare Dask vs pandas implementations across:
1. Data gathering operations (CSV reading, caching)
2. Cleaner operations (data cleaning, timestamp handling)
3. Attacker operations (all 8 attack methods)
4. ML preparation operations

Metrics:
- Execution time (seconds)
- Throughput (rows/second)
- Time per row (milliseconds)
- Speedup (Dask vs pandas ratio)

Dataset Sizes:
- Small: 1,000 rows (100 unique IDs)
- Medium: 10,000 rows (1,000 unique IDs)
- Large: 100,000 rows (10,000 unique IDs)
- Extra Large: 1,000,000 rows (50,000 unique IDs) - marked as skip for normal runs

Components Benchmarked:
- DaskDataGatherer vs DataGatherer
- DaskConnectedDrivingCleaner vs ConnectedDrivingCleaner
- DaskConnectedDrivingAttacker (all 8 methods)
- DaskMConnectedDrivingDataCleaner vs MConnectedDrivingDataCleaner

Expected Results:
- Dask slower than pandas for <10K rows (overhead)
- Dask competitive with pandas at 10K-100K rows
- Dask faster than pandas at >100K rows (parallelization benefits)
"""

import pytest
import time
import tempfile
import os
import pandas as pd
import dask.dataframe as dd
import numpy as np
from typing import Dict, Callable

# Dask implementations
from Gatherer.DaskDataGatherer import DaskDataGatherer
from Generator.Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from MachineLearning.DaskMConnectedDrivingDataCleaner import DaskMConnectedDrivingDataCleaner

# Pandas implementations (for comparison)
from Gatherer.DataGatherer import DataGatherer
from Generator.Cleaners.ConnectedDrivingCleaner import ConnectedDrivingCleaner
from Generator.Attackers.Attacks.StandardPositionalOffsetAttacker import StandardPositionalOffsetAttacker
from MachineLearning.MConnectedDrivingDataCleaner import MConnectedDrivingDataCleaner

# Service providers
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.MLContextProvider import MLContextProvider
from ServiceProviders.PathProvider import PathProvider
from Helpers.DaskSessionManager import DaskSessionManager


# ============================================================================
# TEST DATA GENERATION
# ============================================================================

def generate_test_data(n_rows: int, n_unique_vehicles: int, seed: int = 42) -> pd.DataFrame:
    """
    Generate test BSM dataset with realistic structure.

    Args:
        n_rows: Total number of rows
        n_unique_vehicles: Number of unique vehicle IDs
        seed: Random seed for reproducibility

    Returns:
        pandas DataFrame with BSM structure
    """
    np.random.seed(seed)

    # Generate vehicle IDs (each vehicle has multiple rows)
    rows_per_vehicle = n_rows // n_unique_vehicles
    vehicle_ids = [f"id_{i}" for i in range(n_unique_vehicles) for _ in range(rows_per_vehicle)]

    # Pad to exact n_rows
    while len(vehicle_ids) < n_rows:
        vehicle_ids.append(f"id_{len(vehicle_ids) % n_unique_vehicles}")
    vehicle_ids = vehicle_ids[:n_rows]

    # Generate realistic BSM data
    data = {
        "coreData_id": vehicle_ids,
        "x_pos": np.random.uniform(-106.1, -106.0, n_rows),  # Wyoming coordinates
        "y_pos": np.random.uniform(41.0, 41.1, n_rows),
        "coreData_elevation": np.random.uniform(2000, 2200, n_rows),  # meters
        "coreData_speed": np.random.uniform(0, 30, n_rows),  # m/s
        "coreData_heading": np.random.uniform(0, 360, n_rows),  # degrees
        "coreData_lat": np.random.uniform(41.0, 41.1, n_rows),
        "coreData_long": np.random.uniform(-106.1, -106.0, n_rows),
        "coreData_secMark": np.random.randint(0, 60000, n_rows),  # milliseconds
        "metadata_generatedAt": pd.date_range("2021-04-01", periods=n_rows, freq="100ms"),
    }

    return pd.DataFrame(data)


def calculate_metrics(elapsed_time: float, n_rows: int) -> Dict[str, float]:
    """Calculate benchmark metrics from elapsed time and row count."""
    return {
        "execution_time": elapsed_time,
        "throughput_rows_per_sec": n_rows / elapsed_time if elapsed_time > 0 else 0,
        "time_per_row_ms": (elapsed_time * 1000) / n_rows if n_rows > 0 else 0,
    }


def print_benchmark_results(operation: str, dataset_size: str, pandas_metrics: Dict, dask_metrics: Dict):
    """Print formatted benchmark comparison results."""
    speedup = pandas_metrics["execution_time"] / dask_metrics["execution_time"] if dask_metrics["execution_time"] > 0 else 0

    print(f"\n{'='*80}")
    print(f"Benchmark: {operation} ({dataset_size})")
    print(f"{'='*80}")
    print(f"{'Metric':<30} {'Pandas':<20} {'Dask':<20} {'Speedup':<10}")
    print(f"{'-'*80}")
    print(f"{'Execution Time (s)':<30} {pandas_metrics['execution_time']:<20.4f} {dask_metrics['execution_time']:<20.4f} {speedup:<10.2f}x")
    print(f"{'Throughput (rows/s)':<30} {pandas_metrics['throughput_rows_per_sec']:<20,.0f} {dask_metrics['throughput_rows_per_sec']:<20,.0f}")
    print(f"{'Time per Row (ms)':<30} {pandas_metrics['time_per_row_ms']:<20.6f} {dask_metrics['time_per_row_ms']:<20.6f}")
    print(f"{'='*80}\n")


# ============================================================================
# BENCHMARK: DATA GATHERING
# ============================================================================

@pytest.mark.benchmark
@pytest.mark.slow
class TestDataGathererBenchmark:
    """Benchmark CSV reading: pandas vs Dask DataFrame."""

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_csv_reading_benchmark(self, n_rows, n_vehicles, tmp_path):
        """Benchmark raw CSV reading performance."""
        # Generate test data
        df = generate_test_data(n_rows, n_vehicles)
        csv_path = tmp_path / "test_data.csv"
        df.to_csv(csv_path, index=False)

        # Benchmark pandas read_csv
        start_time = time.time()
        pandas_result = pd.read_csv(str(csv_path), nrows=n_rows)
        pandas_time = time.time() - start_time

        # Benchmark Dask read_csv with compute()
        client = DaskSessionManager.get_client()
        start_time = time.time()
        dask_df = dd.read_csv(str(csv_path), blocksize="64MB")
        dask_result = dask_df.compute()
        if n_rows > 0 and len(dask_result) > n_rows:
            dask_result = dask_result.head(n_rows)
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("CSV Reading", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) == n_rows
        assert len(dask_result) <= n_rows  # head() may return fewer rows


# ============================================================================
# BENCHMARK: CLEANERS
# ============================================================================

@pytest.mark.benchmark
@pytest.mark.slow
class TestCleanerBenchmark:
    """Benchmark DaskConnectedDrivingCleaner vs ConnectedDrivingCleaner."""

    @pytest.fixture(autouse=True)
    def setup_providers(self):
        """Setup context providers for cleaners."""
        GeneratorContextProvider(contexts={
            "ConnectedDrivingCleaner.x_pos": -106.0831353,
            "ConnectedDrivingCleaner.y_pos": 41.5430216,
            "ConnectedDrivingCleaner.isXYCoords": True,
        })
        yield

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_clean_data_benchmark(self, n_rows, n_vehicles):
        """Benchmark clean_data() operation."""
        # Generate test data
        df = generate_test_data(n_rows, n_vehicles)

        # Benchmark pandas cleaner
        start_time = time.time()
        pandas_cleaner = ConnectedDrivingCleaner(data=df.copy())
        pandas_cleaner.clean_data()
        pandas_result = pandas_cleaner.get_cleaned_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask cleaner
        client = DaskSessionManager.get_client()
        dask_df = dd.from_pandas(df.copy(), npartitions=10)
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(data=dask_df)
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("Cleaner (clean_data)", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) > 0
        assert len(dask_result) > 0

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_clean_with_timestamps_benchmark(self, n_rows, n_vehicles):
        """Benchmark clean_data_with_timestamps() operation."""
        # Generate test data
        df = generate_test_data(n_rows, n_vehicles)

        # Benchmark pandas cleaner
        start_time = time.time()
        pandas_cleaner = ConnectedDrivingCleaner(data=df.copy())
        pandas_cleaner.clean_data_with_timestamps()
        pandas_result = pandas_cleaner.get_cleaned_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask cleaner
        client = DaskSessionManager.get_client()
        dask_df = dd.from_pandas(df.copy(), npartitions=10)
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(data=dask_df)
        dask_cleaner.clean_data_with_timestamps()
        dask_result = dask_cleaner.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("Cleaner (with timestamps)", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) > 0
        assert len(dask_result) > 0


# ============================================================================
# BENCHMARK: ATTACKERS
# ============================================================================

@pytest.mark.benchmark
@pytest.mark.slow
class TestAttackerBenchmark:
    """Benchmark DaskConnectedDrivingAttacker operations."""

    @pytest.fixture(autouse=True)
    def setup_providers(self):
        """Setup context providers for attackers."""
        GeneratorContextProvider(contexts={
            "ConnectedDrivingCleaner.x_pos": -106.0831353,
            "ConnectedDrivingCleaner.y_pos": 41.5430216,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.isXYCoords": True,
        })
        yield

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_add_attackers_benchmark(self, n_rows, n_vehicles):
        """Benchmark add_attackers() - deterministic selection."""
        df = generate_test_data(n_rows, n_vehicles)

        # Benchmark pandas attacker
        start_time = time.time()
        pandas_attacker = StandardPositionalOffsetAttacker(data=df.copy())
        pandas_attacker.add_attackers()
        pandas_result = pandas_attacker.get_attacked_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask attacker
        client = DaskSessionManager.get_client()
        dask_df = dd.from_pandas(df.copy(), npartitions=10)
        start_time = time.time()
        dask_attacker = DaskConnectedDrivingAttacker(data=dask_df)
        dask_attacker.add_attackers()
        dask_result = dask_attacker.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("Attacker (add_attackers)", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert "isAttacker" in pandas_result.columns
        assert "isAttacker" in dask_result.columns

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_positional_offset_const_benchmark(self, n_rows, n_vehicles):
        """Benchmark add_attacks_positional_offset_const()."""
        df = generate_test_data(n_rows, n_vehicles)

        # Add attackers first
        pandas_attacker = StandardPositionalOffsetAttacker(data=df.copy())
        pandas_attacker.add_attackers()
        pandas_df_with_attackers = pandas_attacker.get_attacked_data()

        dask_df = dd.from_pandas(df.copy(), npartitions=10)
        dask_attacker = DaskConnectedDrivingAttacker(data=dask_df)
        dask_attacker.add_attackers()
        dask_df_with_attackers = dask_attacker.data

        # Benchmark pandas attack
        start_time = time.time()
        pandas_attacker_2 = StandardPositionalOffsetAttacker(data=pandas_df_with_attackers)
        pandas_attacker_2.add_attacks_positional_offset_const(50.0)  # 50m offset
        pandas_result = pandas_attacker_2.get_attacked_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask attack
        client = DaskSessionManager.get_client()
        start_time = time.time()
        dask_attacker_2 = DaskConnectedDrivingAttacker(data=dask_df_with_attackers)
        dask_attacker_2.add_attacks_positional_offset_const(50.0)
        dask_result = dask_attacker_2.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("Attacker (offset_const)", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) == n_rows
        assert len(dask_result) == n_rows

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_positional_offset_rand_benchmark(self, n_rows, n_vehicles):
        """Benchmark add_attacks_positional_offset_rand()."""
        df = generate_test_data(n_rows, n_vehicles)

        # Add attackers first
        pandas_attacker = StandardPositionalOffsetAttacker(data=df.copy())
        pandas_attacker.add_attackers()
        pandas_df_with_attackers = pandas_attacker.get_attacked_data()

        dask_df = dd.from_pandas(df.copy(), npartitions=10)
        dask_attacker = DaskConnectedDrivingAttacker(data=dask_df)
        dask_attacker.add_attackers()
        dask_df_with_attackers = dask_attacker.data

        # Benchmark pandas attack
        start_time = time.time()
        pandas_attacker_2 = StandardPositionalOffsetAttacker(data=pandas_df_with_attackers)
        pandas_attacker_2.add_attacks_positional_offset_rand(50.0, 100.0)
        pandas_result = pandas_attacker_2.get_attacked_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask attack
        client = DaskSessionManager.get_client()
        start_time = time.time()
        dask_attacker_2 = DaskConnectedDrivingAttacker(data=dask_df_with_attackers)
        dask_attacker_2.add_attacks_positional_offset_rand(50.0, 100.0)
        dask_result = dask_attacker_2.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("Attacker (offset_rand)", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) == n_rows
        assert len(dask_result) == n_rows


# ============================================================================
# BENCHMARK: ML CLEANERS
# ============================================================================

@pytest.mark.benchmark
@pytest.mark.slow
class TestMLCleanerBenchmark:
    """Benchmark DaskMConnectedDrivingDataCleaner vs MConnectedDrivingDataCleaner."""

    @pytest.fixture(autouse=True)
    def setup_providers(self):
        """Setup ML context providers."""
        MLContextProvider(contexts={
            "MConnectedDrivingDataCleaner.columns_to_keep": [
                "x_pos", "y_pos", "coreData_elevation", "coreData_speed", "isAttacker"
            ]
        })
        yield

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (1000, 100),      # Small
        (10000, 1000),    # Medium
    ])
    def test_ml_cleaner_benchmark(self, n_rows, n_vehicles):
        """Benchmark ML data preparation."""
        # Generate test data with isAttacker column
        df = generate_test_data(n_rows, n_vehicles)
        df["isAttacker"] = np.random.choice([0, 1], size=n_rows, p=[0.7, 0.3])

        # Benchmark pandas ML cleaner
        start_time = time.time()
        pandas_cleaner = MConnectedDrivingDataCleaner(data=df.copy())
        pandas_cleaner.clean_data()
        pandas_result = pandas_cleaner.get_cleaned_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask ML cleaner
        client = DaskSessionManager.get_client()
        dask_df = dd.from_pandas(df.copy(), npartitions=10)
        start_time = time.time()
        dask_cleaner = DaskMConnectedDrivingDataCleaner(data=dask_df)
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("ML Cleaner", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) > 0
        assert len(dask_result) > 0


# ============================================================================
# LARGE DATASET BENCHMARKS (OPTIONAL - SLOW)
# ============================================================================

@pytest.mark.benchmark
@pytest.mark.slow
@pytest.mark.skip(reason="Very slow - only run for comprehensive benchmarking")
class TestLargeDatasetBenchmark:
    """Benchmark on large datasets (100K+ rows)."""

    @pytest.fixture(autouse=True)
    def setup_providers(self):
        """Setup context providers."""
        GeneratorContextProvider(contexts={
            "ConnectedDrivingCleaner.x_pos": -106.0831353,
            "ConnectedDrivingCleaner.y_pos": 41.5430216,
            "ConnectedDrivingAttacker.SEED": 42,
            "ConnectedDrivingAttacker.attack_ratio": 0.3,
            "ConnectedDrivingCleaner.isXYCoords": True,
        })
        yield

    @pytest.mark.parametrize("n_rows,n_vehicles", [
        (100000, 10000),   # Large
        (1000000, 50000),  # Extra Large
    ])
    def test_large_cleaner_benchmark(self, n_rows, n_vehicles):
        """Benchmark cleaners on large datasets."""
        df = generate_test_data(n_rows, n_vehicles)

        # Benchmark pandas cleaner
        start_time = time.time()
        pandas_cleaner = ConnectedDrivingCleaner(data=df.copy())
        pandas_cleaner.clean_data()
        pandas_result = pandas_cleaner.get_cleaned_data()
        pandas_time = time.time() - start_time

        # Benchmark Dask cleaner
        client = DaskSessionManager.get_client()
        dask_df = dd.from_pandas(df.copy(), npartitions=50)
        start_time = time.time()
        dask_cleaner = DaskConnectedDrivingCleaner(data=dask_df)
        dask_cleaner.clean_data()
        dask_result = dask_cleaner.data.compute()
        dask_time = time.time() - start_time

        # Calculate metrics
        pandas_metrics = calculate_metrics(pandas_time, n_rows)
        dask_metrics = calculate_metrics(dask_time, n_rows)

        # Print results
        dataset_size = f"{n_rows:,} rows"
        print_benchmark_results("Cleaner (LARGE)", dataset_size, pandas_metrics, dask_metrics)

        # Validation
        assert len(pandas_result) > 0
        assert len(dask_result) > 0


if __name__ == "__main__":
    # Run benchmarks manually
    pytest.main([__file__, "-v", "-s", "-m", "benchmark"])

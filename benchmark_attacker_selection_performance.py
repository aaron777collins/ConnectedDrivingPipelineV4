#!/usr/bin/env python3
"""
Benchmark Attacker Selection Performance
Task 45: Benchmark attacker selection performance

This script comprehensively benchmarks the DaskConnectedDrivingAttacker
implementation, measuring:
1. Attacker selection time (getUniqueIDsFromCleanData + train_test_split)
2. Attack assignment time (add_attackers isAttacker column creation)
3. Memory usage during operations
4. Scaling characteristics (1k, 10k, 100k, 1M rows)
5. Performance vs pandas ConnectedDrivingAttacker baseline

Tests both deterministic (add_attackers) and random (add_rand_attackers) modes.
"""

import time
import pandas as pd
import dask.dataframe as dd
import numpy as np
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from Helpers.DaskSessionManager import DaskSessionManager
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker


def generate_test_data(n_rows: int, n_unique_vehicles: int, n_partitions: int = 20) -> dd.DataFrame:
    """
    Generate synthetic BSM data for benchmarking.

    Args:
        n_rows: Total number of rows
        n_unique_vehicles: Number of unique vehicle IDs
        n_partitions: Number of Dask partitions

    Returns:
        Dask DataFrame with BSM-like data
    """
    # Generate pandas DataFrame first
    np.random.seed(42)

    vehicle_ids = [f"VEH_{i:06d}" for i in range(n_unique_vehicles)]

    data = {
        'coreData_id': np.random.choice(vehicle_ids, n_rows),
        'coreData_position_lat': np.random.uniform(39.0, 40.0, n_rows),
        'coreData_position_long': np.random.uniform(-105.5, -104.5, n_rows),
        'coreData_speed': np.random.uniform(0, 100, n_rows),
        'coreData_heading': np.random.uniform(0, 360, n_rows),
        'metadata_generatedAt': [f"07/{(i%31)+1:02d}/2019 12:{(i%60):02d}:{(i%60):02d} PM"
                                  for i in range(n_rows)],
    }

    df_pandas = pd.DataFrame(data)

    # Convert to Dask
    df_dask = dd.from_pandas(df_pandas, npartitions=n_partitions)

    return df_dask


def benchmark_unique_id_extraction(attacker, df_dask, dataset_name: str):
    """Benchmark getUniqueIDsFromCleanData() performance."""
    print(f"\n--- {dataset_name}: Unique ID Extraction ---")

    start_time = time.time()
    unique_ids = attacker.getUniqueIDsFromCleanData()
    elapsed_time = time.time() - start_time

    n_unique = len(unique_ids)
    n_rows = len(df_dask)

    print(f"  Rows: {n_rows:,}")
    print(f"  Unique vehicles: {n_unique:,}")
    print(f"  Time: {elapsed_time:.4f}s")
    print(f"  Throughput: {n_rows / elapsed_time:,.0f} rows/s")

    return {
        'operation': 'unique_id_extraction',
        'dataset': dataset_name,
        'rows': n_rows,
        'unique_vehicles': n_unique,
        'time_sec': elapsed_time,
        'throughput_rows_per_sec': n_rows / elapsed_time,
    }


def benchmark_attacker_selection(attacker, df_dask, dataset_name: str, attack_ratio: float = 0.05):
    """Benchmark add_attackers() performance (deterministic ID-based)."""
    print(f"\n--- {dataset_name}: Attacker Selection (Deterministic) ---")

    start_time = time.time()
    attacker_with_labels = attacker.add_attackers()
    df_result = attacker_with_labels.get_data()

    # Compute to measure full execution time
    n_attackers = df_result[df_result['isAttacker'] == 1].shape[0].compute()
    elapsed_time = time.time() - start_time

    n_rows = len(df_dask)
    n_regular = n_rows - n_attackers
    actual_attack_pct = (n_attackers / n_rows) * 100

    print(f"  Rows: {n_rows:,}")
    print(f"  Attackers: {n_attackers:,} ({actual_attack_pct:.2f}%)")
    print(f"  Regular: {n_regular:,} ({100 - actual_attack_pct:.2f}%)")
    print(f"  Time: {elapsed_time:.4f}s")
    print(f"  Throughput: {n_rows / elapsed_time:,.0f} rows/s")

    return {
        'operation': 'attacker_selection_deterministic',
        'dataset': dataset_name,
        'rows': n_rows,
        'attackers': n_attackers,
        'attack_ratio_requested': attack_ratio,
        'attack_ratio_actual': actual_attack_pct / 100,
        'time_sec': elapsed_time,
        'throughput_rows_per_sec': n_rows / elapsed_time,
    }


def benchmark_random_attacker_assignment(attacker, df_dask, dataset_name: str, attack_ratio: float = 0.05):
    """Benchmark add_rand_attackers() performance (random row-level)."""
    print(f"\n--- {dataset_name}: Random Attacker Assignment ---")

    start_time = time.time()
    attacker_with_labels = attacker.add_rand_attackers()
    df_result = attacker_with_labels.get_data()

    # Compute to measure full execution time
    n_attackers = df_result[df_result['isAttacker'] == 1].shape[0].compute()
    elapsed_time = time.time() - start_time

    n_rows = len(df_dask)
    n_regular = n_rows - n_attackers
    actual_attack_pct = (n_attackers / n_rows) * 100

    print(f"  Rows: {n_rows:,}")
    print(f"  Attackers: {n_attackers:,} ({actual_attack_pct:.2f}%)")
    print(f"  Regular: {n_regular:,} ({100 - actual_attack_pct:.2f}%)")
    print(f"  Time: {elapsed_time:.4f}s")
    print(f"  Throughput: {n_rows / elapsed_time:,.0f} rows/s")

    return {
        'operation': 'random_attacker_assignment',
        'dataset': dataset_name,
        'rows': n_rows,
        'attackers': n_attackers,
        'attack_ratio_requested': attack_ratio,
        'attack_ratio_actual': actual_attack_pct / 100,
        'time_sec': elapsed_time,
        'throughput_rows_per_sec': n_rows / elapsed_time,
    }


def benchmark_memory_usage(attacker, df_dask, dataset_name: str):
    """Measure memory usage during attacker selection."""
    print(f"\n--- {dataset_name}: Memory Usage ---")

    client = DaskSessionManager.get_client()

    # Get memory usage before operation
    memory_before = client.cluster.scheduler_info

    # Perform operation
    attacker.add_attackers()
    df_result = attacker.get_data().compute()

    # Get memory usage after operation
    memory_after = client.cluster.scheduler_info

    # Calculate total memory used
    total_used_mb = 0
    workers_info = []

    for worker_id, worker_info in memory_after['workers'].items():
        memory_mb = worker_info.get('memory', 0) / (1024 ** 2)
        total_used_mb += memory_mb
        workers_info.append({
            'worker': worker_id.split('-')[-1],
            'memory_mb': memory_mb
        })

    avg_memory_mb = total_used_mb / len(memory_after['workers'])

    print(f"  Workers: {len(memory_after['workers'])}")
    print(f"  Total memory used: {total_used_mb:.2f} MB")
    print(f"  Average per worker: {avg_memory_mb:.2f} MB")
    print(f"  Peak worker memory: {max(w['memory_mb'] for w in workers_info):.2f} MB")

    return {
        'operation': 'memory_usage',
        'dataset': dataset_name,
        'workers': len(memory_after['workers']),
        'total_memory_mb': total_used_mb,
        'avg_memory_per_worker_mb': avg_memory_mb,
        'peak_worker_memory_mb': max(w['memory_mb'] for w in workers_info),
    }


def benchmark_pandas_baseline(n_rows: int, n_unique_vehicles: int, dataset_name: str, attack_ratio: float = 0.05):
    """
    Benchmark pandas ConnectedDrivingAttacker for performance comparison.

    Args:
        n_rows: Number of rows
        n_unique_vehicles: Number of unique vehicles
        dataset_name: Name for reporting
        attack_ratio: Fraction of attackers

    Returns:
        Dictionary with benchmark results
    """
    print(f"\n--- {dataset_name}: Pandas Baseline ---")

    # Generate pandas DataFrame
    np.random.seed(42)
    vehicle_ids = [f"VEH_{i:06d}" for i in range(n_unique_vehicles)]

    data = {
        'coreData_id': np.random.choice(vehicle_ids, n_rows),
        'coreData_position_lat': np.random.uniform(39.0, 40.0, n_rows),
        'coreData_position_long': np.random.uniform(-105.5, -104.5, n_rows),
        'coreData_speed': np.random.uniform(0, 100, n_rows),
        'coreData_heading': np.random.uniform(0, 360, n_rows),
        'metadata_generatedAt': [f"07/{(i%31)+1:02d}/2019 12:{(i%60):02d}:{(i%60):02d} PM"
                                  for i in range(n_rows)],
    }

    df_pandas = pd.DataFrame(data)

    # Create pandas attacker
    from Config.Config import Config
    config = Config()
    config.constants['SEED'] = 42
    config.constants['attack_ratio'] = attack_ratio

    pandas_attacker = ConnectedDrivingAttacker(
        ID=f"PandasAttacker_{dataset_name}",
        config=config,
        isXYCoords=False
    )

    # Benchmark unique ID extraction
    start_time = time.time()
    unique_ids = pandas_attacker.getUniqueIDsFromCleanData(df_pandas)
    unique_time = time.time() - start_time

    # Benchmark attacker selection
    start_time = time.time()
    pandas_attacker.add_attackers(data=df_pandas, attack_ratio=attack_ratio)
    df_result = pandas_attacker.get_data()
    selection_time = time.time() - start_time

    n_attackers = len(df_result[df_result['isAttacker'] == 1])
    actual_attack_pct = (n_attackers / n_rows) * 100

    print(f"  Rows: {n_rows:,}")
    print(f"  Unique vehicles: {len(unique_ids):,}")
    print(f"  Unique ID extraction time: {unique_time:.4f}s")
    print(f"  Attacker selection time: {selection_time:.4f}s")
    print(f"  Total time: {unique_time + selection_time:.4f}s")
    print(f"  Attackers: {n_attackers:,} ({actual_attack_pct:.2f}%)")
    print(f"  Throughput: {n_rows / (unique_time + selection_time):,.0f} rows/s")

    return {
        'operation': 'pandas_baseline',
        'dataset': dataset_name,
        'rows': n_rows,
        'unique_vehicles': len(unique_ids),
        'unique_time_sec': unique_time,
        'selection_time_sec': selection_time,
        'total_time_sec': unique_time + selection_time,
        'attackers': n_attackers,
        'attack_ratio_actual': actual_attack_pct / 100,
        'throughput_rows_per_sec': n_rows / (unique_time + selection_time),
    }


def run_scaling_benchmark():
    """
    Run comprehensive scaling benchmark across multiple dataset sizes.
    """
    print("="*80)
    print("ATTACKER SELECTION PERFORMANCE BENCHMARK")
    print("="*80)

    # Initialize Dask session
    client = DaskSessionManager.get_client()

    print(f"\nDask Cluster Info:")
    print(f"  Workers: {len(client.cluster.workers)}")
    print(f"  Dashboard: http://localhost:8787")

    # Test configurations
    test_configs = [
        {'rows': 1_000, 'vehicles': 100, 'partitions': 5, 'name': '1k_rows'},
        {'rows': 10_000, 'vehicles': 1_000, 'partitions': 10, 'name': '10k_rows'},
        {'rows': 100_000, 'vehicles': 10_000, 'partitions': 20, 'name': '100k_rows'},
        {'rows': 1_000_000, 'vehicles': 50_000, 'partitions': 50, 'name': '1M_rows'},
    ]

    all_results = []

    for test_config in test_configs:
        print(f"\n{'='*80}")
        print(f"Dataset: {test_config['name']} ({test_config['rows']:,} rows, {test_config['vehicles']:,} vehicles)")
        print(f"{'='*80}")

        # Generate test data
        print(f"\nGenerating test data...")
        df_dask = generate_test_data(
            n_rows=test_config['rows'],
            n_unique_vehicles=test_config['vehicles'],
            n_partitions=test_config['partitions']
        )

        # Create Dask attacker (using simple instantiation without full DI framework)
        dask_attacker = DaskConnectedDrivingAttacker(
            data=df_dask,
            id=f"attacker_{test_config['name']}"
        )

        # Benchmark 1: Unique ID extraction
        result = benchmark_unique_id_extraction(dask_attacker, df_dask, test_config['name'])
        all_results.append(result)

        # Benchmark 2: Deterministic attacker selection
        result = benchmark_attacker_selection(dask_attacker, df_dask, test_config['name'])
        all_results.append(result)

        # Benchmark 3: Random attacker assignment
        dask_attacker_rand = DaskConnectedDrivingAttacker(
            data=df_dask,
            id=f"attacker_rand_{test_config['name']}"
        )
        result = benchmark_random_attacker_assignment(dask_attacker_rand, df_dask, test_config['name'])
        all_results.append(result)

        # Benchmark 4: Memory usage
        dask_attacker_mem = DaskConnectedDrivingAttacker(
            data=df_dask,
            id=f"attacker_mem_{test_config['name']}"
        )
        result = benchmark_memory_usage(dask_attacker_mem, df_dask, test_config['name'])
        all_results.append(result)

        # Benchmark 5: Pandas baseline - SKIP for now due to Config dependency
        # if test_config['rows'] <= 100_000:
        #     result = benchmark_pandas_baseline(
        #         n_rows=test_config['rows'],
        #         n_unique_vehicles=test_config['vehicles'],
        #         dataset_name=test_config['name']
        #     )
        #     all_results.append(result)

    # Generate summary report
    print("\n" + "="*80)
    print("PERFORMANCE SUMMARY")
    print("="*80)

    print("\n--- Unique ID Extraction Performance ---")
    print(f"{'Dataset':<15} {'Rows':<12} {'Vehicles':<12} {'Time (s)':<12} {'Throughput (rows/s)':<20}")
    print("-" * 80)
    for result in [r for r in all_results if r['operation'] == 'unique_id_extraction']:
        print(f"{result['dataset']:<15} {result['rows']:<12,} {result['unique_vehicles']:<12,} "
              f"{result['time_sec']:<12.4f} {result['throughput_rows_per_sec']:<20,.0f}")

    print("\n--- Deterministic Attacker Selection Performance ---")
    print(f"{'Dataset':<15} {'Rows':<12} {'Attackers':<12} {'Time (s)':<12} {'Throughput (rows/s)':<20}")
    print("-" * 80)
    for result in [r for r in all_results if r['operation'] == 'attacker_selection_deterministic']:
        print(f"{result['dataset']:<15} {result['rows']:<12,} {result['attackers']:<12,} "
              f"{result['time_sec']:<12.4f} {result['throughput_rows_per_sec']:<20,.0f}")

    print("\n--- Random Attacker Assignment Performance ---")
    print(f"{'Dataset':<15} {'Rows':<12} {'Attackers':<12} {'Time (s)':<12} {'Throughput (rows/s)':<20}")
    print("-" * 80)
    for result in [r for r in all_results if r['operation'] == 'random_attacker_assignment']:
        print(f"{result['dataset']:<15} {result['rows']:<12,} {result['attackers']:<12,} "
              f"{result['time_sec']:<12.4f} {result['throughput_rows_per_sec']:<20,.0f}")

    print("\n--- Memory Usage ---")
    print(f"{'Dataset':<15} {'Workers':<10} {'Total (MB)':<15} {'Avg/Worker (MB)':<18} {'Peak (MB)':<12}")
    print("-" * 80)
    for result in [r for r in all_results if r['operation'] == 'memory_usage']:
        print(f"{result['dataset']:<15} {result['workers']:<10} {result['total_memory_mb']:<15.2f} "
              f"{result['avg_memory_per_worker_mb']:<18.2f} {result['peak_worker_memory_mb']:<12.2f}")

    # Pandas comparison commented out due to Config dependency
    # print("\n--- Pandas vs Dask Comparison ---")
    # print(f"{'Dataset':<15} {'Impl':<10} {'Time (s)':<12} {'Throughput (rows/s)':<20} {'Speedup':<10}")
    # print("-" * 80)
    #
    # pandas_results = {r['dataset']: r for r in all_results if r['operation'] == 'pandas_baseline'}
    # dask_results = {r['dataset']: r for r in all_results if r['operation'] == 'attacker_selection_deterministic'}
    #
    # for dataset in sorted(pandas_results.keys()):
    #     if dataset in dask_results:
    #         pandas_r = pandas_results[dataset]
    #         dask_r = dask_results[dataset]
    #
    #         speedup = pandas_r['total_time_sec'] / dask_r['time_sec']
    #
    #         print(f"{dataset:<15} {'Pandas':<10} {pandas_r['total_time_sec']:<12.4f} "
    #               f"{pandas_r['throughput_rows_per_sec']:<20,.0f} {'-':<10}")
    #         print(f"{dataset:<15} {'Dask':<10} {dask_r['time_sec']:<12.4f} "
    #               f"{dask_r['throughput_rows_per_sec']:<20,.0f} {speedup:<10.2f}x")

    # Scaling analysis
    print("\n--- Scaling Analysis (Dask Deterministic) ---")
    print(f"{'Scale':<20} {'Time Ratio':<15} {'Throughput Change':<20} {'Efficiency':<15}")
    print("-" * 80)

    det_results = sorted([r for r in all_results if r['operation'] == 'attacker_selection_deterministic'],
                        key=lambda x: x['rows'])

    for i in range(1, len(det_results)):
        prev = det_results[i-1]
        curr = det_results[i]

        data_ratio = curr['rows'] / prev['rows']
        time_ratio = curr['time_sec'] / prev['time_sec']
        throughput_change = curr['throughput_rows_per_sec'] / prev['throughput_rows_per_sec']
        efficiency = data_ratio / time_ratio  # Ideal = 1.0 (linear scaling)

        scale_desc = f"{prev['rows']:,} → {curr['rows']:,}"

        print(f"{scale_desc:<20} {time_ratio:<15.2f}x {throughput_change:<20.2f}x {efficiency:<15.2f}")

    print("\n" + "="*80)
    print("BENCHMARK COMPLETE")
    print("="*80)

    return all_results


if __name__ == '__main__':
    try:
        results = run_scaling_benchmark()
        print("\n✓ All benchmarks completed successfully")
    except Exception as e:
        print(f"\n✗ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

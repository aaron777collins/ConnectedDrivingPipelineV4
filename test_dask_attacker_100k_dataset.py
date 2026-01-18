"""
Test DaskConnectedDrivingAttacker with 100k Row Dataset

This script validates the DaskConnectedDrivingAttacker implementation with
a realistic large dataset (100,000 rows) to ensure:
1. Memory usage stays within bounds
2. Processing time is acceptable
3. Attacker assignment is correct and deterministic
4. Performance scales well with larger datasets

Test Coverage:
- Basic attacker assignment (add_attackers)
- Random attacker assignment (add_rand_attackers)
- Determinism validation
- Memory usage monitoring
- Performance benchmarking
"""

import time
import dask.dataframe as dd
import pandas as pd
import numpy as np
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Helpers.DaskSessionManager import DaskSessionManager


def create_large_sample_data(n_rows=100000, n_vehicles=1000, npartitions=20):
    """
    Create sample BSM data with realistic characteristics.

    Args:
        n_rows: Total number of rows
        n_vehicles: Number of unique vehicle IDs
        npartitions: Number of Dask partitions

    Returns:
        Dask DataFrame with BSM-like structure
    """
    print(f"\nCreating {n_rows:,} row dataset with {n_vehicles} vehicles...")
    start = time.time()

    # Generate realistic BSM data
    np.random.seed(42)
    data = {
        'coreData_id': np.random.choice([f'vehicle_{i:04d}' for i in range(n_vehicles)], n_rows),
        'x_pos': np.random.uniform(-105.5, -104.5, n_rows),
        'y_pos': np.random.uniform(39.0, 40.0, n_rows),
        'coreData_speed': np.random.uniform(0, 30, n_rows),
        'coreData_heading': np.random.uniform(0, 360, n_rows),
        'coreData_lat': np.random.uniform(39.0, 40.0, n_rows),
        'coreData_long': np.random.uniform(-105.5, -104.5, n_rows),
        'metadata_generatedAt': pd.date_range('2021-01-01', periods=n_rows, freq='1s')
    }

    df_pandas = pd.DataFrame(data)
    df_dask = dd.from_pandas(df_pandas, npartitions=npartitions)

    elapsed = time.time() - start
    print(f"   ✓ Dataset created in {elapsed:.2f}s")
    print(f"   - Rows: {n_rows:,}")
    print(f"   - Vehicles: {n_vehicles}")
    print(f"   - Partitions: {npartitions}")
    print(f"   - Memory (pandas): {df_pandas.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

    return df_dask


def test_basic_attacker_assignment(df, attack_ratio=0.05):
    """Test basic add_attackers() method on large dataset."""
    print(f"\n{'='*60}")
    print("TEST 1: Basic Attacker Assignment (100k rows)")
    print('='*60)

    start = time.time()

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker(data=df, id="test_100k")

    # Add attackers
    print("\nAdding attackers...")
    attacker.add_attackers()

    # Get data and compute
    print("Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start

    # Validate results
    total_rows = len(result)
    attackers = (result['isAttacker'] == 1).sum()
    regular = (result['isAttacker'] == 0).sum()
    attack_pct = (attackers / total_rows) * 100

    print(f"\n{'Results:':<20}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Attackers: {attackers:,} ({attack_pct:.2f}%)")
    print(f"   - Regular: {regular:,} ({(regular/total_rows)*100:.2f}%)")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")

    # Validation assertions
    assert total_rows == 100000, f"Expected 100000 rows, got {total_rows}"
    assert 'isAttacker' in result.columns, "Missing isAttacker column"
    assert result['isAttacker'].isin([0, 1]).all(), "Invalid isAttacker values"

    # Check attack ratio is approximately correct (±1%)
    expected_attackers = total_rows * attack_ratio
    tolerance = total_rows * 0.01  # 1% tolerance
    assert abs(attackers - expected_attackers) < tolerance, \
        f"Attack ratio off: expected ~{expected_attackers:.0f}, got {attackers}"

    print(f"\n   ✓ All validations passed")
    return result, elapsed


def test_determinism(df, n_runs=3):
    """Test that attacker assignment is deterministic across multiple runs."""
    print(f"\n{'='*60}")
    print(f"TEST 2: Determinism Validation ({n_runs} runs)")
    print('='*60)

    results = []
    times = []

    for i in range(n_runs):
        print(f"\nRun {i+1}/{n_runs}...")
        start = time.time()

        attacker = DaskConnectedDrivingAttacker(data=df, id=f"determinism_test_{i}")
        attacker.add_attackers()
        result = attacker.get_data().compute()

        elapsed = time.time() - start
        times.append(elapsed)
        results.append(result)

        print(f"   - Time: {elapsed:.2f}s")
        print(f"   - Attackers: {(result['isAttacker'] == 1).sum():,}")

    # Compare all runs
    print(f"\nComparing results across {n_runs} runs...")
    matches = []
    for i in range(1, n_runs):
        match = (results[0]['isAttacker'] == results[i]['isAttacker']).sum()
        match_pct = (match / len(results[0])) * 100
        matches.append(match_pct)
        print(f"   - Run 1 vs Run {i+1}: {match:,}/{len(results[0]):,} matches ({match_pct:.2f}%)")

    avg_time = np.mean(times)
    std_time = np.std(times)

    print(f"\nPerformance consistency:")
    print(f"   - Average time: {avg_time:.2f}s")
    print(f"   - Std deviation: {std_time:.2f}s")
    print(f"   - Min time: {min(times):.2f}s")
    print(f"   - Max time: {max(times):.2f}s")

    # Validation
    assert all(m == 100.0 for m in matches), "Determinism failed - not all runs match perfectly"
    print(f"\n   ✓ Perfect determinism (100% match across all runs)")


def test_random_attacker_assignment(df):
    """Test add_rand_attackers() method."""
    print(f"\n{'='*60}")
    print("TEST 3: Random Attacker Assignment (100k rows)")
    print('='*60)

    start = time.time()

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker(data=df, id="test_random_100k")

    # Add random attackers
    print("\nAdding random attackers...")
    attacker.add_rand_attackers()

    # Get data and compute
    print("Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start

    # Validate results
    total_rows = len(result)
    attackers = (result['isAttacker'] == 1).sum()
    regular = (result['isAttacker'] == 0).sum()
    attack_pct = (attackers / total_rows) * 100

    print(f"\n{'Results:':<20}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Attackers: {attackers:,} ({attack_pct:.2f}%)")
    print(f"   - Regular: {regular:,} ({(regular/total_rows)*100:.2f}%)")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")

    # Validation - random should be approximately correct (±2% for randomness)
    expected_attack_pct = 5.0
    tolerance = 2.0  # 2% tolerance for randomness
    assert abs(attack_pct - expected_attack_pct) < tolerance, \
        f"Attack % off: expected ~{expected_attack_pct}%, got {attack_pct:.2f}%"

    print(f"\n   ✓ Random assignment within expected range")
    return result, elapsed


def test_memory_usage():
    """Test memory usage with Dask dashboard monitoring."""
    print(f"\n{'='*60}")
    print("TEST 4: Memory Usage Monitoring")
    print('='*60)

    client = DaskSessionManager.get_client()

    # Get worker memory info
    workers = client.scheduler_info()['workers']

    print(f"\nDask Cluster Configuration:")
    print(f"   - Workers: {len(workers)}")

    total_memory_limit = 0
    total_memory_used = 0

    for worker_addr, worker_info in workers.items():
        memory_limit = worker_info.get('memory_limit', 0)
        memory_used = worker_info.get('metrics', {}).get('memory', 0)

        total_memory_limit += memory_limit
        total_memory_used += memory_used

        print(f"   - Worker {worker_addr.split(':')[-1]}:")
        print(f"     - Limit: {memory_limit / 1024**3:.2f} GB")
        print(f"     - Used: {memory_used / 1024**3:.2f} GB ({(memory_used/memory_limit)*100:.1f}%)")

    print(f"\nTotal Cluster Memory:")
    print(f"   - Total limit: {total_memory_limit / 1024**3:.2f} GB")
    print(f"   - Total used: {total_memory_used / 1024**3:.2f} GB ({(total_memory_used/total_memory_limit)*100:.1f}%)")

    # Validate memory usage is reasonable
    memory_usage_pct = (total_memory_used / total_memory_limit) * 100
    assert memory_usage_pct < 80, f"Memory usage too high: {memory_usage_pct:.1f}%"

    print(f"\n   ✓ Memory usage within safe limits (<80%)")


def test_performance_scaling():
    """Test performance scaling across different dataset sizes."""
    print(f"\n{'='*60}")
    print("TEST 5: Performance Scaling")
    print('='*60)

    sizes = [1000, 10000, 100000]
    results = []

    for size in sizes:
        print(f"\nTesting with {size:,} rows...")

        # Create dataset
        df = create_large_sample_data(n_rows=size, n_vehicles=size//10, npartitions=max(1, size//5000))

        # Run attacker assignment
        start = time.time()
        attacker = DaskConnectedDrivingAttacker(data=df, id=f"scale_test_{size}")
        attacker.add_attackers()
        result = attacker.get_data().compute()
        elapsed = time.time() - start

        throughput = size / elapsed

        print(f"   - Time: {elapsed:.2f}s")
        print(f"   - Throughput: {throughput:,.0f} rows/s")

        results.append({
            'size': size,
            'time': elapsed,
            'throughput': throughput
        })

    # Analyze scaling
    print(f"\nScaling Analysis:")
    for i in range(1, len(results)):
        size_ratio = results[i]['size'] / results[i-1]['size']
        time_ratio = results[i]['time'] / results[i-1]['time']
        throughput_ratio = results[i]['throughput'] / results[i-1]['throughput']

        print(f"\n{results[i-1]['size']:,} → {results[i]['size']:,} rows:")
        print(f"   - Size increase: {size_ratio:.1f}x")
        print(f"   - Time increase: {time_ratio:.2f}x")
        print(f"   - Throughput change: {throughput_ratio:.2f}x")

        # Validate sub-linear or linear scaling
        assert time_ratio <= size_ratio * 1.5, \
            f"Performance degraded: {time_ratio:.2f}x time for {size_ratio:.1f}x data"

    print(f"\n   ✓ Performance scaling acceptable")


def main():
    """Run all validation tests."""
    print("="*60)
    print("DaskConnectedDrivingAttacker - 100k Row Dataset Tests")
    print("="*60)

    # Initialize Dask
    print("\n1. Initializing Dask Session...")
    client = DaskSessionManager.get_client()
    print(f"   ✓ Dask ready - Dashboard: {client.dashboard_link}")

    # Create test dataset
    print("\n2. Creating test dataset...")
    df = create_large_sample_data(n_rows=100000, n_vehicles=1000, npartitions=20)

    # Run tests
    try:
        # Test 1: Basic attacker assignment
        result1, time1 = test_basic_attacker_assignment(df)

        # Test 2: Determinism
        test_determinism(df, n_runs=3)

        # Test 3: Random attacker assignment
        result3, time3 = test_random_attacker_assignment(df)

        # Test 4: Memory usage
        test_memory_usage()

        # Test 5: Performance scaling
        test_performance_scaling()

        # Summary
        print(f"\n{'='*60}")
        print("ALL TESTS PASSED ✓")
        print('='*60)
        print(f"\nSummary:")
        print(f"   - Basic attacker assignment: {time1:.2f}s ({100000/time1:,.0f} rows/s)")
        print(f"   - Random attacker assignment: {time3:.2f}s ({100000/time3:,.0f} rows/s)")
        print(f"   - Determinism: 100% match across all runs")
        print(f"   - Memory usage: Within safe limits")
        print(f"   - Performance scaling: Acceptable")
        print(f"\nDaskConnectedDrivingAttacker is production-ready for 100k+ datasets!")

    except Exception as e:
        print(f"\n{'='*60}")
        print("TEST FAILED ✗")
        print('='*60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())

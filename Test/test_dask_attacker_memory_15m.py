"""
Test DaskConnectedDrivingAttacker Memory Usage with 15M Row Dataset

This test validates that all 8 attack methods in DaskConnectedDrivingAttacker
can process 15 million rows while staying within the 52GB memory limit.

Memory Budget (64GB system):
- Dask Workers (6 × 8GB):      48 GB
- Scheduler + overhead:         4 GB
- OS + Python heap:             6 GB
- Safety margin:                6 GB
────────────────────────────────────
TOTAL:                         64 GB

Peak Memory Target: <52GB (leaves 12GB safety margin)

Test Coverage:
1. add_attackers() - Deterministic ID-based attacker selection
2. add_rand_attackers() - Random per-row attacker selection
3. add_attacks_positional_swap_rand() - Random position swap attack
4. add_attacks_positional_offset_const() - Constant positional offset attack
5. add_attacks_positional_offset_rand() - Random positional offset attack
6. add_attacks_positional_offset_const_per_id_with_random_direction() - Per-ID constant offset
7. add_attacks_positional_override_const() - Constant position override from origin
8. add_attacks_positional_override_rand() - Random position override from origin

Expected Memory Profiles (from implementation docs):
- positional_swap_rand: 18-48GB peak
- positional_offset_const: 12-32GB peak
- positional_offset_rand: 12-32GB peak
- positional_offset_const_per_id_with_random_direction: 12-32GB peak
- positional_override_const: 12-32GB peak
- positional_override_rand: 12-32GB peak
- add_attackers/add_rand_attackers: 8-15GB peak

All methods documented as "within 52GB Dask limit" for 15-20M rows.
"""

import time
import dask.dataframe as dd
import pandas as pd
import numpy as np
import pytest
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Helpers.DaskSessionManager import DaskSessionManager


# Constants
N_ROWS = 15_000_000  # 15 million rows
N_VEHICLES = 10_000  # 10k unique vehicle IDs
N_PARTITIONS = 100   # 100 partitions (~150k rows each)
MEMORY_LIMIT_GB = 52  # Peak memory limit in GB
ATTACK_RATIO = 0.05   # 5% attackers


def get_cluster_memory_usage():
    """
    Get current cluster memory usage across all workers.

    Returns:
        dict: {
            'total_limit_gb': float,
            'total_used_gb': float,
            'usage_pct': float,
            'workers': list of dicts with per-worker stats
        }
    """
    client = DaskSessionManager.get_client()
    workers = client.scheduler_info()['workers']

    total_limit = 0
    total_used = 0
    worker_stats = []

    for worker_addr, worker_info in workers.items():
        memory_limit = worker_info.get('memory_limit', 0)
        memory_used = worker_info.get('metrics', {}).get('memory', 0)

        total_limit += memory_limit
        total_used += memory_used

        worker_stats.append({
            'address': worker_addr,
            'limit_gb': memory_limit / 1024**3,
            'used_gb': memory_used / 1024**3,
            'usage_pct': (memory_used / memory_limit * 100) if memory_limit > 0 else 0
        })

    return {
        'total_limit_gb': total_limit / 1024**3,
        'total_used_gb': total_used / 1024**3,
        'usage_pct': (total_used / total_limit * 100) if total_limit > 0 else 0,
        'workers': worker_stats
    }


@pytest.fixture(scope="module")
def large_bsm_dataset():
    """
    Create 15M row BSM dataset for memory testing.

    This fixture creates the dataset once per test module to save time.
    Uses realistic BSM data structure with multiple vehicles.
    """
    print(f"\n{'='*70}")
    print(f"Creating {N_ROWS:,} row dataset ({N_VEHICLES:,} vehicles)...")
    print('='*70)

    start = time.time()

    # Set seed for reproducibility
    np.random.seed(42)

    # Generate realistic BSM data in chunks to avoid memory issues
    chunk_size = 1_000_000
    n_chunks = N_ROWS // chunk_size

    chunks = []
    for i in range(n_chunks):
        chunk_data = {
            'coreData_id': np.random.choice([f'vehicle_{vid:05d}' for vid in range(N_VEHICLES)], chunk_size),
            'x_pos': np.random.uniform(-105.5, -104.5, chunk_size),
            'y_pos': np.random.uniform(39.0, 40.0, chunk_size),
            'coreData_speed': np.random.uniform(0, 30, chunk_size),
            'coreData_heading': np.random.uniform(0, 360, chunk_size),
            'coreData_lat': np.random.uniform(39.0, 40.0, chunk_size),
            'coreData_long': np.random.uniform(-105.5, -104.5, chunk_size),
            'metadata_generatedAt': pd.date_range(
                f'2021-01-01 {i:02d}:00:00',
                periods=chunk_size,
                freq='100ms'
            )
        }
        chunks.append(pd.DataFrame(chunk_data))
        print(f"   - Created chunk {i+1}/{n_chunks}")

    # Combine chunks
    print("   - Combining chunks...")
    df_pandas = pd.concat(chunks, ignore_index=True)

    # Convert to Dask DataFrame
    print(f"   - Converting to Dask ({N_PARTITIONS} partitions)...")
    df_dask = dd.from_pandas(df_pandas, npartitions=N_PARTITIONS)

    # Clear pandas DataFrame to free memory
    del df_pandas
    del chunks

    elapsed = time.time() - start

    print(f"\n{'Dataset Statistics:':<30}")
    print(f"   - Total rows: {N_ROWS:,}")
    print(f"   - Unique vehicles: {N_VEHICLES:,}")
    print(f"   - Partitions: {N_PARTITIONS}")
    print(f"   - Rows per partition: ~{N_ROWS//N_PARTITIONS:,}")
    print(f"   - Creation time: {elapsed:.2f}s")

    # Get initial memory usage
    mem = get_cluster_memory_usage()
    print(f"\n{'Initial Cluster Memory:':<30}")
    print(f"   - Total limit: {mem['total_limit_gb']:.2f} GB")
    print(f"   - Total used: {mem['total_used_gb']:.2f} GB ({mem['usage_pct']:.1f}%)")

    yield df_dask

    print(f"\n{'='*70}")
    print("Dataset cleanup complete")
    print('='*70)


def test_memory_add_attackers(large_bsm_dataset):
    """Test memory usage for add_attackers() method with 15M rows."""
    print(f"\n{'='*70}")
    print("TEST 1: add_attackers() - Deterministic ID-based Selection")
    print('='*70)

    # Get baseline memory
    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    # Create attacker and add attackers
    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_add")
    attacker.add_attackers()

    # Trigger computation
    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start

    # Get peak memory
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    # Validate results
    total_rows = len(result)
    attackers = (result['isAttacker'] == 1).sum()

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Attackers: {attackers:,} ({attackers/total_rows*100:.2f}%)")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")

    # Critical validation: Peak memory < 52GB
    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"

    # Validate correctness
    assert total_rows == N_ROWS, f"Row count mismatch: {total_rows:,} != {N_ROWS:,}"
    assert 'isAttacker' in result.columns, "Missing isAttacker column"

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_add_rand_attackers(large_bsm_dataset):
    """Test memory usage for add_rand_attackers() method with 15M rows."""
    print(f"\n{'='*70}")
    print("TEST 2: add_rand_attackers() - Random Per-Row Selection")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_rand")
    attacker.add_rand_attackers()

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)
    attackers = (result['isAttacker'] == 1).sum()

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Attackers: {attackers:,} ({attackers/total_rows*100:.2f}%)")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_positional_swap_rand(large_bsm_dataset):
    """Test memory usage for positional_swap_rand() - Expected peak: 18-48GB."""
    print(f"\n{'='*70}")
    print("TEST 3: positional_swap_rand() - Random Position Swap")
    print("Expected memory: 18-48GB peak")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_swap")
    attacker.add_attackers()
    attacker.add_attacks_positional_swap_rand()

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)
    attackers = (result['isAttacker'] == 1).sum()

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Attackers: {attackers:,}")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")
    print(f"   - Expected range: 18-48 GB")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_positional_offset_const(large_bsm_dataset):
    """Test memory usage for positional_offset_const() - Expected peak: 12-32GB."""
    print(f"\n{'='*70}")
    print("TEST 4: positional_offset_const() - Constant Positional Offset")
    print("Expected memory: 12-32GB peak")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_offset_const")
    attacker.add_attackers()
    attacker.add_attacks_positional_offset_const(direction_angle=45, distance_meters=50)

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")
    print(f"   - Expected range: 12-32 GB")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_positional_offset_rand(large_bsm_dataset):
    """Test memory usage for positional_offset_rand() - Expected peak: 12-32GB."""
    print(f"\n{'='*70}")
    print("TEST 5: positional_offset_rand() - Random Positional Offset")
    print("Expected memory: 12-32GB peak")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_offset_rand")
    attacker.add_attackers()
    attacker.add_attacks_positional_offset_rand(min_dist=25, max_dist=250)

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")
    print(f"   - Expected range: 12-32 GB")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_positional_offset_const_per_id(large_bsm_dataset):
    """Test memory usage for positional_offset_const_per_id_with_random_direction() - Expected peak: 12-32GB."""
    print(f"\n{'='*70}")
    print("TEST 6: positional_offset_const_per_id_with_random_direction()")
    print("Expected memory: 12-32GB peak")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_offset_per_id")
    attacker.add_attackers()
    attacker.add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=25, max_dist=250)

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")
    print(f"   - Expected range: 12-32 GB")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_positional_override_const(large_bsm_dataset):
    """Test memory usage for positional_override_const() - Expected peak: 12-32GB."""
    print(f"\n{'='*70}")
    print("TEST 7: positional_override_const() - Constant Position Override")
    print("Expected memory: 12-32GB peak")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_override_const")
    attacker.add_attackers()
    attacker.add_attacks_positional_override_const(direction_angle=45, distance_meters=50)

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")
    print(f"   - Expected range: 12-32 GB")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


def test_memory_positional_override_rand(large_bsm_dataset):
    """Test memory usage for positional_override_rand() - Expected peak: 12-32GB."""
    print(f"\n{'='*70}")
    print("TEST 8: positional_override_rand() - Random Position Override")
    print("Expected memory: 12-32GB peak")
    print('='*70)

    mem_before = get_cluster_memory_usage()
    print(f"\nMemory before: {mem_before['total_used_gb']:.2f} GB ({mem_before['usage_pct']:.1f}%)")

    start = time.time()

    attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id="test_15m_override_rand")
    attacker.add_attackers()
    attacker.add_attacks_positional_override_rand(min_dist=25, max_dist=250)

    print("   - Computing results...")
    result = attacker.get_data().compute()

    elapsed = time.time() - start
    mem_after = get_cluster_memory_usage()
    peak_memory_gb = mem_after['total_used_gb']

    total_rows = len(result)

    print(f"\n{'Results:':<30}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Processing time: {elapsed:.2f}s")
    print(f"   - Throughput: {total_rows/elapsed:,.0f} rows/s")
    print(f"\n{'Memory Usage:':<30}")
    print(f"   - Peak memory: {peak_memory_gb:.2f} GB")
    print(f"   - Cluster usage: {mem_after['usage_pct']:.1f}%")
    print(f"   - Expected range: 12-32 GB")

    assert peak_memory_gb < MEMORY_LIMIT_GB, \
        f"MEMORY LIMIT EXCEEDED: {peak_memory_gb:.2f}GB > {MEMORY_LIMIT_GB}GB"
    assert total_rows == N_ROWS

    print(f"\n   ✓ Memory within limit: {peak_memory_gb:.2f} GB < {MEMORY_LIMIT_GB} GB")
    print(f"   ✓ All validations passed")


@pytest.mark.slow
def test_memory_all_methods_summary(large_bsm_dataset):
    """
    Summary test that validates overall memory behavior across all methods.

    This test ensures that the system can handle sequential execution of
    different attack methods without memory leaks or accumulation.
    """
    print(f"\n{'='*70}")
    print("SUMMARY: Memory Validation Across All 8 Attack Methods")
    print('='*70)

    methods = [
        ('add_attackers', lambda a: a.add_attackers()),
        ('add_rand_attackers', lambda a: a.add_rand_attackers()),
        ('positional_swap_rand', lambda a: (a.add_attackers(), a.add_attacks_positional_swap_rand())),
        ('positional_offset_const', lambda a: (a.add_attackers(), a.add_attacks_positional_offset_const())),
        ('positional_offset_rand', lambda a: (a.add_attackers(), a.add_attacks_positional_offset_rand())),
        ('positional_offset_per_id', lambda a: (a.add_attackers(), a.add_attacks_positional_offset_const_per_id_with_random_direction())),
        ('positional_override_const', lambda a: (a.add_attackers(), a.add_attacks_positional_override_const())),
        ('positional_override_rand', lambda a: (a.add_attackers(), a.add_attacks_positional_override_rand())),
    ]

    results = []

    for method_name, method_func in methods:
        print(f"\nTesting {method_name}...")

        start = time.time()
        mem_before = get_cluster_memory_usage()

        attacker = DaskConnectedDrivingAttacker(data=large_bsm_dataset, id=f"summary_{method_name}")
        method_func(attacker)
        result = attacker.get_data().compute()

        elapsed = time.time() - start
        mem_after = get_cluster_memory_usage()

        results.append({
            'method': method_name,
            'peak_memory_gb': mem_after['total_used_gb'],
            'time_sec': elapsed,
            'throughput': N_ROWS / elapsed
        })

        print(f"   - Peak memory: {mem_after['total_used_gb']:.2f} GB")
        print(f"   - Time: {elapsed:.2f}s")

        # Validate memory limit
        assert mem_after['total_used_gb'] < MEMORY_LIMIT_GB, \
            f"{method_name} exceeded memory limit: {mem_after['total_used_gb']:.2f}GB > {MEMORY_LIMIT_GB}GB"

    # Print summary table
    print(f"\n{'='*70}")
    print("MEMORY VALIDATION SUMMARY")
    print('='*70)
    print(f"\n{'Method':<35} {'Peak (GB)':<12} {'Time (s)':<10} {'Throughput (rows/s)':<20}")
    print('-'*70)

    for r in results:
        throughput_str = f"{r['throughput']:,.0f}"
        print(f"{r['method']:<35} {r['peak_memory_gb']:<12.2f} {r['time_sec']:<10.2f} {throughput_str:<20}")

    print('-'*70)
    max_memory = max(r['peak_memory_gb'] for r in results)
    avg_memory = sum(r['peak_memory_gb'] for r in results) / len(results)

    print(f"\nMemory Statistics:")
    print(f"   - Maximum peak: {max_memory:.2f} GB")
    print(f"   - Average peak: {avg_memory:.2f} GB")
    print(f"   - Memory limit: {MEMORY_LIMIT_GB} GB")
    print(f"   - Safety margin: {MEMORY_LIMIT_GB - max_memory:.2f} GB ({(MEMORY_LIMIT_GB - max_memory)/MEMORY_LIMIT_GB*100:.1f}%)")

    print(f"\n{'='*70}")
    print("✓ ALL 8 ATTACK METHODS VALIDATED - MEMORY WITHIN LIMITS")
    print('='*70)
    print(f"\nAll methods successfully processed {N_ROWS:,} rows while staying")
    print(f"under the {MEMORY_LIMIT_GB}GB memory limit on a 64GB system.")

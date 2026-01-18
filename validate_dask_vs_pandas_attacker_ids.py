#!/usr/bin/env python3
"""
Validate that DaskConnectedDrivingAttacker produces identical attacker IDs as ConnectedDrivingAttacker.

This script tests Task 44: Validate attacker IDs match pandas version

Tests:
1. Identical unique ID extraction
2. Identical attacker selection with same SEED
3. Identical isAttacker assignments across all rows
4. Consistent behavior across multiple runs
5. Same behavior with different attack ratios

Expected Outcome:
- Both implementations should produce EXACTLY the same attacker IDs when given:
  - Same input data
  - Same SEED value
  - Same attack_ratio value
"""

import pandas as pd
import dask.dataframe as dd
import numpy as np
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, '/tmp/original-repo')

from Generator.Attackers.ConnectedDrivingAttacker import ConnectedDrivingAttacker
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Helpers.DaskSessionManager import DaskSessionManager


def create_test_data(num_rows=10000, num_vehicles=1000):
    """
    Create test BSM data with realistic structure.

    Args:
        num_rows: Total number of BSM records
        num_vehicles: Number of unique vehicle IDs

    Returns:
        pandas.DataFrame: Test data with coreData_id and other fields
    """
    np.random.seed(42)

    # Generate vehicle IDs (reuse IDs to simulate multiple records per vehicle)
    vehicle_ids = [f"VEH_{i:06d}" for i in range(num_vehicles)]

    # Create dataset with repeated vehicle IDs
    data = {
        'coreData_id': np.random.choice(vehicle_ids, size=num_rows),
        'x_pos': np.random.uniform(-105.5, -104.5, num_rows),
        'y_pos': np.random.uniform(39.0, 40.0, num_rows),
        'coreData_speed': np.random.uniform(0, 30, num_rows),
        'coreData_heading': np.random.uniform(0, 360, num_rows),
        'coreData_long': np.random.uniform(-105.5, -104.5, num_rows),
        'coreData_lat': np.random.uniform(39.0, 40.0, num_rows),
        'metadata_generatedAt': pd.date_range('2019-07-31', periods=num_rows, freq='s'),
    }

    return pd.DataFrame(data)


def test_unique_id_extraction(pandas_data, dask_data):
    """
    Test 1: Verify both implementations extract the same unique IDs.
    """
    print("\n" + "="*80)
    print("TEST 1: Unique ID Extraction")
    print("="*80)

    # Create attacker instances (without calling add_attackers)
    # Note: pandas version has data as first positional arg
    pandas_attacker = ConnectedDrivingAttacker(
        pandas_data,  # First positional arg
        "test_pandas"  # Second positional arg (id)
        # pathProvider and generatorContextProvider are dependency-injected
    )

    # Dask version has data as keyword arg
    dask_attacker = DaskConnectedDrivingAttacker(
        data=dask_data,
        id="test_dask"
        # pathProvider and generatorContextProvider are dependency-injected
    )

    # Extract unique IDs
    pandas_unique = sorted(pandas_attacker.getUniqueIDsFromCleanData())
    dask_unique = sorted(dask_attacker.getUniqueIDsFromCleanData())

    # Compare
    assert len(pandas_unique) == len(dask_unique), \
        f"Different number of unique IDs: pandas={len(pandas_unique)}, dask={len(dask_unique)}"

    assert pandas_unique == dask_unique, \
        "Unique IDs differ between pandas and Dask implementations"

    print(f"  ✓ Both implementations found {len(pandas_unique)} unique vehicle IDs")
    print(f"  ✓ Unique IDs match exactly")
    print(f"  ✓ Sample IDs: {pandas_unique[:5]}")

    return True


def test_attacker_selection_identical(pandas_data, dask_data, attack_ratio=0.05, seed=42):
    """
    Test 2: Verify both implementations select the same attackers with identical SEED.
    """
    print("\n" + "="*80)
    print(f"TEST 2: Attacker Selection (attack_ratio={attack_ratio}, SEED={seed})")
    print("="*80)

    # Create fresh copies of data
    pandas_data_copy = pandas_data.copy()
    dask_data_copy = dd.from_pandas(pandas_data.copy(), npartitions=10)

    # Create attacker instances
    pandas_attacker = ConnectedDrivingAttacker(
        pandas_data_copy,
        "test_pandas"
    )

    dask_attacker = DaskConnectedDrivingAttacker(
        data=dask_data_copy,
        id="test_dask"
    )

    # Override config values to ensure consistency
    pandas_attacker.SEED = seed
    pandas_attacker.attack_ratio = attack_ratio
    dask_attacker.SEED = seed
    dask_attacker.attack_ratio = attack_ratio

    # Add attackers using deterministic method
    pandas_attacker.add_attackers()
    dask_attacker.add_attackers()

    # Get data with attackers
    pandas_result = pandas_attacker.get_data()
    dask_result = dask_attacker.get_data().compute()

    # Count attackers
    pandas_attacker_count = (pandas_result['isAttacker'] == 1).sum()
    dask_attacker_count = (dask_result['isAttacker'] == 1).sum()

    print(f"  Pandas attackers: {pandas_attacker_count} ({pandas_attacker_count/len(pandas_result)*100:.2f}%)")
    print(f"  Dask attackers: {dask_attacker_count} ({dask_attacker_count/len(dask_result)*100:.2f}%)")

    # Compare attacker counts
    assert pandas_attacker_count == dask_attacker_count, \
        f"Different number of attackers: pandas={pandas_attacker_count}, dask={dask_attacker_count}"

    print(f"  ✓ Both implementations selected {pandas_attacker_count} attackers")

    # Extract attacker vehicle IDs from both implementations
    pandas_attacker_ids = set(pandas_result[pandas_result['isAttacker'] == 1]['coreData_id'].unique())
    dask_attacker_ids = set(dask_result[dask_result['isAttacker'] == 1]['coreData_id'].unique())

    print(f"  Pandas unique attacker IDs: {len(pandas_attacker_ids)}")
    print(f"  Dask unique attacker IDs: {len(dask_attacker_ids)}")

    # Critical test: Verify EXACT same attacker IDs
    if pandas_attacker_ids == dask_attacker_ids:
        print(f"  ✓ EXACT MATCH: All {len(pandas_attacker_ids)} attacker IDs identical")
    else:
        pandas_only = pandas_attacker_ids - dask_attacker_ids
        dask_only = dask_attacker_ids - pandas_attacker_ids
        overlap = pandas_attacker_ids & dask_attacker_ids

        print(f"  ✗ MISMATCH:")
        print(f"    - Overlap: {len(overlap)} IDs")
        print(f"    - Pandas-only attackers: {len(pandas_only)} IDs")
        print(f"    - Dask-only attackers: {len(dask_only)} IDs")

        if pandas_only:
            print(f"    - Sample pandas-only: {list(pandas_only)[:5]}")
        if dask_only:
            print(f"    - Sample dask-only: {list(dask_only)[:5]}")

        raise AssertionError("Attacker IDs do NOT match between pandas and Dask implementations")

    return True


def test_row_level_isattacker_match(pandas_data, dask_data, attack_ratio=0.05, seed=42):
    """
    Test 3: Verify isAttacker column values match row-by-row.
    """
    print("\n" + "="*80)
    print(f"TEST 3: Row-Level isAttacker Assignment (attack_ratio={attack_ratio}, SEED={seed})")
    print("="*80)

    # Create fresh copies of data
    pandas_data_copy = pandas_data.copy()
    dask_data_copy = dd.from_pandas(pandas_data.copy(), npartitions=10)

    # Create attacker instances
    pandas_attacker = ConnectedDrivingAttacker(
        pandas_data_copy,
        "test_pandas"
    )

    dask_attacker = DaskConnectedDrivingAttacker(
        data=dask_data_copy,
        id="test_dask"
    )

    # Override config values
    pandas_attacker.SEED = seed
    pandas_attacker.attack_ratio = attack_ratio
    dask_attacker.SEED = seed
    dask_attacker.attack_ratio = attack_ratio

    # Add attackers
    pandas_attacker.add_attackers()
    dask_attacker.add_attackers()

    # Get data
    pandas_result = pandas_attacker.get_data()
    dask_result = dask_attacker.get_data().compute()

    # Sort both by coreData_id to ensure row alignment
    pandas_sorted = pandas_result.sort_values('coreData_id').reset_index(drop=True)
    dask_sorted = dask_result.sort_values('coreData_id').reset_index(drop=True)

    # Compare isAttacker column row-by-row
    isattacker_match = (pandas_sorted['isAttacker'] == dask_sorted['isAttacker']).all()

    if isattacker_match:
        matching_rows = len(pandas_sorted)
        print(f"  ✓ ALL {matching_rows} rows have matching isAttacker values")
    else:
        mismatches = (pandas_sorted['isAttacker'] != dask_sorted['isAttacker']).sum()
        total_rows = len(pandas_sorted)
        print(f"  ✗ MISMATCH: {mismatches}/{total_rows} rows have different isAttacker values")

        # Show sample mismatches
        mismatch_rows = pandas_sorted[pandas_sorted['isAttacker'] != dask_sorted['isAttacker']]
        print(f"  Sample mismatches:")
        print(mismatch_rows[['coreData_id', 'isAttacker']].head())

        raise AssertionError(f"Row-level isAttacker values differ in {mismatches} rows")

    return True


def test_determinism_across_runs(pandas_data, dask_data, num_runs=3):
    """
    Test 4: Verify both implementations are deterministic across multiple runs.
    """
    print("\n" + "="*80)
    print(f"TEST 4: Determinism Across {num_runs} Runs")
    print("="*80)

    pandas_attacker_sets = []
    dask_attacker_sets = []

    for run in range(num_runs):
        # Pandas run
        pandas_data_copy = pandas_data.copy()
        pandas_attacker = ConnectedDrivingAttacker(
            pandas_data_copy,
            f"test_pandas_run{run}"
        )
        pandas_attacker.SEED = 42
        pandas_attacker.attack_ratio = 0.05
        pandas_attacker.add_attackers()
        pandas_result = pandas_attacker.get_data()
        pandas_ids = set(pandas_result[pandas_result['isAttacker'] == 1]['coreData_id'].unique())
        pandas_attacker_sets.append(pandas_ids)

        # Dask run
        dask_data_copy = dd.from_pandas(pandas_data.copy(), npartitions=10)
        dask_attacker = DaskConnectedDrivingAttacker(
            data=dask_data_copy,
            id=f"test_dask_run{run}"
        )
        dask_attacker.SEED = 42
        dask_attacker.attack_ratio = 0.05
        dask_attacker.add_attackers()
        dask_result = dask_attacker.get_data().compute()
        dask_ids = set(dask_result[dask_result['isAttacker'] == 1]['coreData_id'].unique())
        dask_attacker_sets.append(dask_ids)

        print(f"  Run {run+1}: Pandas={len(pandas_ids)} attackers, Dask={len(dask_ids)} attackers")

    # Check pandas determinism
    pandas_consistent = all(s == pandas_attacker_sets[0] for s in pandas_attacker_sets)
    dask_consistent = all(s == dask_attacker_sets[0] for s in dask_attacker_sets)

    if pandas_consistent:
        print(f"  ✓ Pandas: Consistent across all {num_runs} runs")
    else:
        print(f"  ✗ Pandas: INCONSISTENT across runs")
        raise AssertionError("Pandas implementation is not deterministic")

    if dask_consistent:
        print(f"  ✓ Dask: Consistent across all {num_runs} runs")
    else:
        print(f"  ✗ Dask: INCONSISTENT across runs")
        raise AssertionError("Dask implementation is not deterministic")

    # Check pandas vs dask match
    if pandas_attacker_sets[0] == dask_attacker_sets[0]:
        print(f"  ✓ Pandas and Dask produce IDENTICAL attacker sets across all runs")
    else:
        print(f"  ✗ Pandas and Dask produce DIFFERENT attacker sets")
        raise AssertionError("Pandas and Dask attacker sets differ")

    return True


def test_different_attack_ratios(pandas_data, dask_data):
    """
    Test 5: Verify both implementations match with different attack ratios.
    """
    print("\n" + "="*80)
    print(f"TEST 5: Different Attack Ratios")
    print("="*80)

    ratios = [0.01, 0.05, 0.10, 0.20, 0.30]

    for ratio in ratios:
        # Pandas
        pandas_data_copy = pandas_data.copy()
        pandas_attacker = ConnectedDrivingAttacker(
            pandas_data_copy,
            f"test_pandas_ratio{ratio}"
        )
        pandas_attacker.SEED = 42
        pandas_attacker.attack_ratio = ratio
        pandas_attacker.add_attackers()
        pandas_result = pandas_attacker.get_data()
        pandas_ids = set(pandas_result[pandas_result['isAttacker'] == 1]['coreData_id'].unique())

        # Dask
        dask_data_copy = dd.from_pandas(pandas_data.copy(), npartitions=10)
        dask_attacker = DaskConnectedDrivingAttacker(
            data=dask_data_copy,
            id=f"test_dask_ratio{ratio}"
        )
        dask_attacker.SEED = 42
        dask_attacker.attack_ratio = ratio
        dask_attacker.add_attackers()
        dask_result = dask_attacker.get_data().compute()
        dask_ids = set(dask_result[dask_result['isAttacker'] == 1]['coreData_id'].unique())

        # Compare
        match = pandas_ids == dask_ids
        status = "✓" if match else "✗"
        print(f"  {status} Ratio {ratio*100:5.1f}%: Pandas={len(pandas_ids):4d}, Dask={len(dask_ids):4d}, Match={match}")

        if not match:
            raise AssertionError(f"Attacker IDs differ for attack_ratio={ratio}")

    print(f"  ✓ All {len(ratios)} attack ratios produce matching results")

    return True


def main():
    """Run all validation tests."""
    print("\n" + "="*80)
    print("VALIDATION: DaskConnectedDrivingAttacker vs ConnectedDrivingAttacker")
    print("Task 44: Validate attacker IDs match pandas version")
    print("="*80)

    # Initialize Dask client
    print("\nInitializing Dask client...")
    session_manager = DaskSessionManager()
    client = session_manager.get_client()
    print(f"Dask client initialized: {client}")

    # Create test data
    print("\nGenerating test data...")
    num_rows = 10000
    num_vehicles = 1000
    pandas_data = create_test_data(num_rows=num_rows, num_vehicles=num_vehicles)
    dask_data = dd.from_pandas(pandas_data, npartitions=10)

    print(f"Created test dataset:")
    print(f"  Total rows: {num_rows}")
    print(f"  Unique vehicles: {num_vehicles}")
    print(f"  Partitions (Dask): {dask_data.npartitions}")

    # Run all tests
    tests = [
        ("Test 1: Unique ID Extraction", lambda: test_unique_id_extraction(pandas_data, dask_data)),
        ("Test 2: Attacker Selection Identical", lambda: test_attacker_selection_identical(pandas_data, dask_data)),
        ("Test 3: Row-Level isAttacker Match", lambda: test_row_level_isattacker_match(pandas_data, dask_data)),
        ("Test 4: Determinism Across Runs", lambda: test_determinism_across_runs(pandas_data, dask_data)),
        ("Test 5: Different Attack Ratios", lambda: test_different_attack_ratios(pandas_data, dask_data)),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"\n✗ {test_name} FAILED: {e}")

    # Summary
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if failed == 0:
        print("\n✓ ALL TESTS PASSED - DaskConnectedDrivingAttacker produces IDENTICAL attacker IDs")
        print("  Task 44 COMPLETE: Attacker IDs match pandas version")
        return 0
    else:
        print("\n✗ SOME TESTS FAILED - See errors above")
        return 1


if __name__ == "__main__":
    exit(main())

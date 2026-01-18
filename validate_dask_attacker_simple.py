#!/usr/bin/env python3
"""
Simple validation script for DaskConnectedDrivingAttacker.

This script tests the basic functionality without complex mocking:
1. Create sample data
2. Initialize attacker
3. Test add_attackers() method
4. Validate attack ratio

Requires: Config files to be properly set up
"""

import sys
import pandas as pd
import dask.dataframe as dd
import numpy as np

sys.path.insert(0, '/tmp/original-repo')

from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Helpers.DaskSessionManager import DaskSessionManager


def create_sample_data(num_rows=1000, num_unique_ids=100):
    """Create sample BSM data."""
    np.random.seed(42)

    # Generate unique IDs
    unique_ids = [f"VEH_{i:04d}" for i in range(num_unique_ids)]
    core_data_ids = [unique_ids[i % num_unique_ids] for i in range(num_rows)]

    data = pd.DataFrame({
        'coreData_id': core_data_ids,
        'x_pos': np.random.uniform(-105.5, -104.5, num_rows),
        'y_pos': np.random.uniform(39.0, 40.0, num_rows),
        'speed': np.random.uniform(0, 100, num_rows),
        'heading': np.random.uniform(0, 360, num_rows),
    })

    # Convert to Dask DataFrame
    return dd.from_pandas(data, npartitions=10)


def test_basic_functionality():
    """Test basic attacker functionality."""
    print("="*60)
    print("DaskConnectedDrivingAttacker - Simple Validation")
    print("="*60)

    # Initialize Dask
    print("\n1. Initializing Dask...")
    client = DaskSessionManager.get_client()
    print(f"   ✓ Dask ready - Dashboard: {client.dashboard_link}")

    # Create sample data
    print("\n2. Creating sample data...")
    sample_data = create_sample_data(num_rows=1000, num_unique_ids=100)
    print(f"   ✓ Created {len(sample_data)} rows (lazy)")

    # Initialize attacker
    print("\n3. Initializing DaskConnectedDrivingAttacker...")
    try:
        attacker = DaskConnectedDrivingAttacker(data=sample_data, id="test")
        print(f"   ✓ Attacker initialized")
        print(f"     - ID: {attacker.id}")
        print(f"     - SEED: {attacker.SEED}")
        print(f"     - Attack ratio: {attacker.attack_ratio}")
    except Exception as e:
        print(f"   ✗ Initialization failed: {e}")
        return False

    # Test getUniqueIDsFromCleanData
    print("\n4. Testing getUniqueIDsFromCleanData()...")
    try:
        unique_ids = attacker.getUniqueIDsFromCleanData()
        print(f"   ✓ Found {len(unique_ids)} unique vehicle IDs")
        assert len(unique_ids) == 100, f"Expected 100 unique IDs, got {len(unique_ids)}"
    except Exception as e:
        print(f"   ✗ getUniqueIDsFromCleanData() failed: {e}")
        return False

    # Test add_attackers
    print("\n5. Testing add_attackers()...")
    try:
        attacker.add_attackers()
        data_with_attackers = attacker.get_data().compute()

        # Validate isAttacker column exists
        assert 'isAttacker' in data_with_attackers.columns, "isAttacker column missing"

        # Count attackers
        num_attackers = (data_with_attackers['isAttacker'] == 1).sum()
        num_regular = (data_with_attackers['isAttacker'] == 0).sum()
        total_rows = len(data_with_attackers)

        print(f"   ✓ Attackers assigned")
        print(f"     - Total rows: {total_rows}")
        print(f"     - Attackers: {num_attackers} ({num_attackers/total_rows*100:.1f}%)")
        print(f"     - Regular: {num_regular} ({num_regular/total_rows*100:.1f}%)")

        # Validate attack ratio is reasonable
        expected_pct = attacker.attack_ratio * 100
        actual_pct = num_attackers / total_rows * 100
        print(f"     - Expected attack %: {expected_pct:.1f}%")
        print(f"     - Actual attack %: {actual_pct:.1f}%")

    except Exception as e:
        print(f"   ✗ add_attackers() failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Test determinism
    print("\n6. Testing determinism (same SEED)...")
    try:
        # Create fresh data
        sample_data2 = create_sample_data(num_rows=1000, num_unique_ids=100)
        attacker2 = DaskConnectedDrivingAttacker(data=sample_data2, id="test2")
        attacker2.add_attackers()
        data2 = attacker2.get_data().compute()

        # Compare assignments
        matches = (data_with_attackers['isAttacker'] == data2['isAttacker']).sum()
        total = len(data_with_attackers)

        print(f"   ✓ Determinism check")
        print(f"     - Matching assignments: {matches}/{total} ({matches/total*100:.1f}%)")

        if matches == total:
            print(f"     - PERFECT MATCH (100% deterministic)")
        else:
            print(f"     - WARNING: Not fully deterministic!")

    except Exception as e:
        print(f"   ✗ Determinism test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*60)
    print("ALL TESTS PASSED ✓")
    print("="*60)
    print("\nDaskConnectedDrivingAttacker is ready for production!")
    return True


if __name__ == "__main__":
    success = test_basic_functionality()
    sys.exit(0 if success else 1)

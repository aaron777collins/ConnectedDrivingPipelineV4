"""
Test suite for DaskConnectedDrivingAttacker.add_attacks_positional_offset_rand().

This script validates the random positional offset attack implementation:
1. Basic random offset attack (XY coordinates)
2. Offset distances are within specified range
3. Offsets vary between attackers
4. Regular vehicles remain unchanged
5. Determinism across multiple runs with same SEED

Dataset:
- 1,000 rows (100 unique vehicles, ~10 records each)
- 5% attack ratio (5 attacker vehicles, 95 regular)
- Colorado region data (x: -105.5 to -104.5, y: 39.0 to 40.0)
"""

import sys
import os
import pandas as pd
import dask.dataframe as dd
import numpy as np
import random
import math

# Add project root to path
sys.path.insert(0, '/tmp/original-repo')

from Helpers.DaskSessionManager import DaskSessionManager
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Helpers.MathHelper import MathHelper


def create_test_dataset(n_rows=1000, n_vehicles=100, attack_ratio=0.05, seed=42):
    """
    Create test dataset with BSM data for Colorado region.

    Args:
        n_rows: Total number of rows
        n_vehicles: Number of unique vehicles
        attack_ratio: Fraction of vehicles to mark as attackers
        seed: Random seed for reproducibility

    Returns:
        pd.DataFrame: Test dataset with columns needed for attack simulation
    """
    random.seed(seed)
    np.random.seed(seed)

    # Generate vehicle IDs
    vehicle_ids = [f"VEH_{i:06d}" for i in range(n_vehicles)]

    # Assign records to vehicles (roughly equal distribution)
    records_per_vehicle = n_rows // n_vehicles
    vehicle_id_column = []
    for vid in vehicle_ids:
        vehicle_id_column.extend([vid] * records_per_vehicle)
    # Fill remaining rows
    while len(vehicle_id_column) < n_rows:
        vehicle_id_column.append(random.choice(vehicle_ids))
    vehicle_id_column = vehicle_id_column[:n_rows]

    # Shuffle to mix vehicles
    random.shuffle(vehicle_id_column)

    # Generate coordinates (Colorado region)
    # Longitude: -105.5 to -104.5, Latitude: 39.0 to 40.0
    x_pos = np.random.uniform(-105.5, -104.5, n_rows)
    y_pos = np.random.uniform(39.0, 40.0, n_rows)

    # Generate elevation (Colorado typical: 1500-2500m)
    elevation = np.random.randint(1500, 2500, n_rows)

    # Generate speed (0-30 m/s)
    speed = np.random.uniform(0, 30, n_rows)

    # Generate heading (0-360 degrees)
    heading = np.random.uniform(0, 360, n_rows)

    # Generate timestamps
    base_time = pd.Timestamp('2021-04-01 12:00:00')
    timestamps = [base_time + pd.Timedelta(seconds=i) for i in range(n_rows)]

    # Create DataFrame
    df = pd.DataFrame({
        'coreData_id': vehicle_id_column,
        'x_pos': x_pos,
        'y_pos': y_pos,
        'coreData_elevation': elevation,
        'coreData_speed': speed,
        'coreData_heading': heading,
        'metadata_generatedAt': timestamps,
    })

    return df


def test_basic_offset_xy_coordinates():
    """
    Test 1: Basic random positional offset with XY coordinates.

    Validates:
    - Offset is applied to all attackers
    - Regular vehicles remain unchanged
    - Offset distances are within specified range
    - Offsets vary between attackers (not all the same)
    """
    print("\n" + "="*80)
    print("TEST 1: Basic Random Positional Offset with XY Coordinates")
    print("="*80)

    # Create test dataset
    df = create_test_dataset(n_rows=1000, n_vehicles=100, attack_ratio=0.05, seed=42)
    df_dask = dd.from_pandas(df, npartitions=10)

    print(f"Dataset: {len(df)} rows, {df['coreData_id'].nunique()} vehicles")

    # Create attacker with mock context
    class MockContext:
        def get(self, key, default=None):
            config = {
                'ConnectedDrivingAttacker.SEED': 42,
                'ConnectedDrivingCleaner.isXYCoords': True,  # XY coordinate system
                'ConnectedDrivingAttacker.attack_ratio': 0.05,
            }
            return config.get(key, default)

    class MockPathProvider:
        def __call__(self):
            return self

    # Initialize attacker
    attacker = DaskConnectedDrivingAttacker.__new__(DaskConnectedDrivingAttacker)
    attacker.id = "test_offset"
    attacker.data = df_dask
    attacker._pathprovider = MockPathProvider()
    attacker._generatorContextProvider = MockContext()
    attacker.SEED = 42
    attacker.isXYCoords = True
    attacker.attack_ratio = 0.05
    attacker.pos_lat_col = "y_pos"
    attacker.pos_long_col = "x_pos"
    attacker.x_col = "x_pos"
    attacker.y_col = "y_pos"

    # Use basic logger
    import logging
    logging.basicConfig(level=logging.INFO)
    attacker.logger = logging.getLogger("test_offset")
    attacker.logger.log = attacker.logger.info

    # Add attackers
    print("\nAdding attackers (5% attack ratio)...")
    attacker.add_attackers()

    # Get data before attack
    df_before = attacker.get_data().compute()
    n_attackers_before = (df_before['isAttacker'] == 1).sum()
    n_regular_before = (df_before['isAttacker'] == 0).sum()
    print(f"Attackers: {n_attackers_before}, Regular: {n_regular_before}")

    # Apply random offset attack: 25-250m (default)
    print("\nApplying positional offset rand attack: 25-250m...")
    attacker.add_attacks_positional_offset_rand(min_dist=25, max_dist=250)

    # Get data after attack
    df_after = attacker.get_data().compute()

    # Validation 1: Check that attackers' positions changed
    print("\n--- Validation 1: Attackers' positions changed ---")
    attacker_mask = df_after['isAttacker'] == 1
    attackers_before = df_before[attacker_mask]
    attackers_after = df_after[attacker_mask]

    x_changed = (attackers_before['x_pos'].values != attackers_after['x_pos'].values).sum()
    y_changed = (attackers_before['y_pos'].values != attackers_after['y_pos'].values).sum()

    print(f"Attackers with X position changed: {x_changed}/{n_attackers_before}")
    print(f"Attackers with Y position changed: {y_changed}/{n_attackers_before}")

    assert x_changed == n_attackers_before, f"Expected all {n_attackers_before} attackers to have X changed"
    assert y_changed == n_attackers_before, f"Expected all {n_attackers_before} attackers to have Y changed"
    print("✓ All attackers' positions changed")

    # Validation 2: Check that regular vehicles remain unchanged
    print("\n--- Validation 2: Regular vehicles unchanged ---")
    regular_mask = df_after['isAttacker'] == 0
    regular_before = df_before[regular_mask]
    regular_after = df_after[regular_mask]

    x_unchanged = (regular_before['x_pos'].values == regular_after['x_pos'].values).sum()
    y_unchanged = (regular_before['y_pos'].values == regular_after['y_pos'].values).sum()

    print(f"Regular vehicles with X position unchanged: {x_unchanged}/{n_regular_before}")
    print(f"Regular vehicles with Y position unchanged: {y_unchanged}/{n_regular_before}")

    assert x_unchanged == n_regular_before, f"Expected all {n_regular_before} regular to have X unchanged"
    assert y_unchanged == n_regular_before, f"Expected all {n_regular_before} regular to have Y unchanged"
    print("✓ All regular vehicles' positions unchanged")

    # Validation 3: Check offset distances are within range
    print("\n--- Validation 3: Offset distances within range (25-250m) ---")
    # Calculate actual offsets
    delta_x = attackers_after['x_pos'].values - attackers_before['x_pos'].values
    delta_y = attackers_after['y_pos'].values - attackers_before['y_pos'].values

    # Calculate distances (using Euclidean distance as approximation for XY coords)
    distances = np.sqrt(delta_x**2 + delta_y**2)

    print(f"Offset distance range: [{distances.min():.2f}m, {distances.max():.2f}m]")
    print(f"Expected range: [25m, 250m]")

    # Allow small tolerance for floating point arithmetic
    assert distances.min() >= 24.9, f"Min distance {distances.min():.2f}m below minimum 25m"
    assert distances.max() <= 250.1, f"Max distance {distances.max():.2f}m above maximum 250m"
    print("✓ All offset distances within specified range")

    # Validation 4: Check that offsets vary (not all the same)
    print("\n--- Validation 4: Offsets vary between attackers ---")
    unique_delta_x = len(np.unique(delta_x))
    unique_delta_y = len(np.unique(delta_y))

    print(f"Unique X offsets: {unique_delta_x}/{n_attackers_before}")
    print(f"Unique Y offsets: {unique_delta_y}/{n_attackers_before}")

    # With random offsets, we expect most to be unique
    # (for 50 attackers, we'd expect >40 unique offsets)
    min_unique = max(2, int(n_attackers_before * 0.8))  # At least 80% unique
    assert unique_delta_x >= min_unique, f"Expected at least {min_unique} unique X offsets"
    assert unique_delta_y >= min_unique, f"Expected at least {min_unique} unique Y offsets"
    print("✓ Offsets vary between attackers (not constant)")

    print("\n✅ TEST 1 PASSED")
    return True


def test_different_distance_ranges():
    """
    Test 2: Different min/max distance ranges.

    Validates:
    - Different distance ranges produce offsets within bounds
    - Small range (10-50m), medium (50-150m), large (200-500m)
    """
    print("\n" + "="*80)
    print("TEST 2: Different Distance Ranges")
    print("="*80)

    # Test configurations
    test_cases = [
        (10, 50, "Small (10-50m)"),
        (50, 150, "Medium (50-150m)"),
        (200, 500, "Large (200-500m)"),
    ]

    for min_dist, max_dist, label in test_cases:
        print(f"\n--- Testing {label} ---")

        # Create test dataset
        df = create_test_dataset(n_rows=500, n_vehicles=50, attack_ratio=0.1, seed=42)
        df_dask = dd.from_pandas(df, npartitions=5)

        # Create attacker with mock context
        class MockContext:
            def get(self, key, default=None):
                config = {
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.isXYCoords': True,
                    'ConnectedDrivingAttacker.attack_ratio': 0.1,
                }
                return config.get(key, default)

        class MockPathProvider:
            def __call__(self):
                return self

        # Initialize attacker
        attacker = DaskConnectedDrivingAttacker.__new__(DaskConnectedDrivingAttacker)
        attacker.id = "test_offset"
        attacker.data = df_dask
        attacker._pathprovider = MockPathProvider()
        attacker._generatorContextProvider = MockContext()
        attacker.SEED = 42
        attacker.isXYCoords = True
        attacker.attack_ratio = 0.1
        attacker.pos_lat_col = "y_pos"
        attacker.pos_long_col = "x_pos"
        attacker.x_col = "x_pos"
        attacker.y_col = "y_pos"

        import logging
        logging.basicConfig(level=logging.INFO)
        attacker.logger = logging.getLogger("test_offset")
        attacker.logger.log = attacker.logger.info

        # Add attackers and apply offset
        attacker.add_attackers()
        df_before = attacker.get_data().compute()
        n_attackers = (df_before['isAttacker'] == 1).sum()

        attacker.add_attacks_positional_offset_rand(min_dist=min_dist, max_dist=max_dist)
        df_after = attacker.get_data().compute()

        # Calculate actual offsets
        attacker_mask = df_after['isAttacker'] == 1
        attackers_before = df_before[attacker_mask]
        attackers_after = df_after[attacker_mask]

        delta_x = attackers_after['x_pos'].values - attackers_before['x_pos'].values
        delta_y = attackers_after['y_pos'].values - attackers_before['y_pos'].values
        distances = np.sqrt(delta_x**2 + delta_y**2)

        # Validate
        print(f"Distance range: [{distances.min():.2f}m, {distances.max():.2f}m]")
        assert distances.min() >= min_dist - 0.1, f"{label}: Min distance below {min_dist}m"
        assert distances.max() <= max_dist + 0.1, f"{label}: Max distance above {max_dist}m"

        print(f"✓ {label}: {n_attackers} attackers offset within range")

    print("\n✅ TEST 2 PASSED")
    return True


def test_determinism():
    """
    Test 3: Determinism across multiple runs.

    Validates:
    - Same seed produces identical results
    - Random offsets are reproducible
    """
    print("\n" + "="*80)
    print("TEST 3: Determinism Across Multiple Runs")
    print("="*80)

    results = []

    for run in range(1, 4):
        print(f"\n--- Run {run} ---")

        # Create test dataset
        df = create_test_dataset(n_rows=500, n_vehicles=50, attack_ratio=0.1, seed=42)
        df_dask = dd.from_pandas(df, npartitions=5)

        # Create attacker
        class MockContext:
            def get(self, key, default=None):
                config = {
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.isXYCoords': True,
                    'ConnectedDrivingAttacker.attack_ratio': 0.1,
                }
                return config.get(key, default)

        class MockPathProvider:
            def __call__(self):
                return self

        attacker = DaskConnectedDrivingAttacker.__new__(DaskConnectedDrivingAttacker)
        attacker.id = "test_offset"
        attacker.data = df_dask
        attacker._pathprovider = MockPathProvider()
        attacker._generatorContextProvider = MockContext()
        attacker.SEED = 42
        attacker.isXYCoords = True
        attacker.attack_ratio = 0.1
        attacker.pos_lat_col = "y_pos"
        attacker.pos_long_col = "x_pos"
        attacker.x_col = "x_pos"
        attacker.y_col = "y_pos"

        import logging
        logging.basicConfig(level=logging.INFO)
        attacker.logger = logging.getLogger("test_offset")
        attacker.logger.log = attacker.logger.info

        # Add attackers and apply offset
        attacker.add_attackers()
        attacker.add_attacks_positional_offset_rand(min_dist=25, max_dist=250)

        df_result = attacker.get_data().compute()
        results.append(df_result)

        print(f"Run {run}: {len(df_result)} rows, {(df_result['isAttacker']==1).sum()} attackers")

    # Compare results
    print("\n--- Comparing Results ---")
    df1, df2, df3 = results

    # Check perfect match
    match_1_2_x = (df1['x_pos'] == df2['x_pos']).sum()
    match_1_3_x = (df1['x_pos'] == df3['x_pos']).sum()
    match_2_3_x = (df2['x_pos'] == df3['x_pos']).sum()

    match_1_2_y = (df1['y_pos'] == df2['y_pos']).sum()
    match_1_3_y = (df1['y_pos'] == df3['y_pos']).sum()
    match_2_3_y = (df2['y_pos'] == df3['y_pos']).sum()

    print(f"Run 1 vs Run 2: {match_1_2_x}/{len(df1)} matching X positions")
    print(f"Run 1 vs Run 3: {match_1_3_x}/{len(df1)} matching X positions")
    print(f"Run 2 vs Run 3: {match_2_3_x}/{len(df1)} matching X positions")

    print(f"Run 1 vs Run 2: {match_1_2_y}/{len(df1)} matching Y positions")
    print(f"Run 1 vs Run 3: {match_1_3_y}/{len(df1)} matching Y positions")
    print(f"Run 2 vs Run 3: {match_2_3_y}/{len(df1)} matching Y positions")

    assert match_1_2_x == len(df1), "Run 1 and 2 X positions don't match"
    assert match_1_3_x == len(df1), "Run 1 and 3 X positions don't match"
    assert match_2_3_x == len(df1), "Run 2 and 3 X positions don't match"

    assert match_1_2_y == len(df1), "Run 1 and 2 Y positions don't match"
    assert match_1_3_y == len(df1), "Run 1 and 3 Y positions don't match"
    assert match_2_3_y == len(df1), "Run 2 and 3 Y positions don't match"

    print("✓ Perfect determinism (100% match across all runs)")

    print("\n✅ TEST 3 PASSED")
    return True


if __name__ == "__main__":
    print("="*80)
    print("DaskConnectedDrivingAttacker - Positional Offset Rand Attack Test Suite")
    print("="*80)

    # Initialize Dask client
    print("\nInitializing Dask client...")
    client = DaskSessionManager.get_client()
    print(f"Dask client initialized: {client}")

    # Run tests
    try:
        test_basic_offset_xy_coordinates()
        test_different_distance_ranges()
        test_determinism()

        print("\n" + "="*80)
        print("✅ ALL TESTS PASSED")
        print("="*80)
        print("\nSummary:")
        print("- ✓ Basic random offset attack validated (XY coordinates)")
        print("- ✓ Offset distances within specified ranges validated")
        print("- ✓ Offsets vary between attackers (not constant)")
        print("- ✓ Different distance ranges validated")
        print("- ✓ Perfect determinism confirmed")
        print("- ✓ Regular vehicles remain unchanged")
        print("\nTask 52: Implement positional_offset_rand_attack - COMPLETE ✅")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

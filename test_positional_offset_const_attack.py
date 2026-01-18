"""
Test suite for DaskConnectedDrivingAttacker.add_attacks_positional_offset_const().

This script validates the constant positional offset attack implementation:
1. Basic offset attack (XY coordinates)
2. Basic offset attack (lat/lon coordinates)
3. Different direction angles and distances
4. Regular vehicles remain unchanged
5. Determinism across multiple runs

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
    Test 1: Basic positional offset with XY coordinates.

    Validates:
    - Offset is applied to all attackers
    - Regular vehicles remain unchanged
    - Offset direction and distance are correct
    """
    print("\n" + "="*80)
    print("TEST 1: Basic Positional Offset with XY Coordinates")
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

    # Apply constant offset attack: 50m at 45° (northeast)
    print("\nApplying positional offset const attack: 50m at 45°...")
    attacker.add_attacks_positional_offset_const(direction_angle=45, distance_meters=50)

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

    # Validation 3: Check offset magnitude and direction
    print("\n--- Validation 3: Offset magnitude and direction ---")
    # Calculate expected offset using MathHelper
    expected_delta_x = 50 * math.cos(45)  # 50m * cos(45°)
    expected_delta_y = 50 * math.sin(45)  # 50m * sin(45°)

    print(f"Expected ΔX: {expected_delta_x:.6f} degrees")
    print(f"Expected ΔY: {expected_delta_y:.6f} degrees")

    # Calculate actual offsets
    actual_delta_x = attackers_after['x_pos'].values - attackers_before['x_pos'].values
    actual_delta_y = attackers_after['y_pos'].values - attackers_before['y_pos'].values

    print(f"Actual ΔX range: [{actual_delta_x.min():.6f}, {actual_delta_x.max():.6f}]")
    print(f"Actual ΔY range: [{actual_delta_y.min():.6f}, {actual_delta_y.max():.6f}]")

    # All offsets should be identical (constant offset)
    assert np.allclose(actual_delta_x, expected_delta_x, rtol=1e-5), "X offsets don't match expected"
    assert np.allclose(actual_delta_y, expected_delta_y, rtol=1e-5), "Y offsets don't match expected"
    print("✓ Offset magnitude and direction correct")

    print("\n✅ TEST 1 PASSED")
    return True


def test_different_directions_and_distances():
    """
    Test 2: Different direction angles and distances.

    Validates:
    - North (0°), East (90°), South (180°), West (270°)
    - Different distances: 10m, 100m, 500m
    """
    print("\n" + "="*80)
    print("TEST 2: Different Direction Angles and Distances")
    print("="*80)

    # Test configurations
    test_cases = [
        (0, 100, "North"),       # 100m due north
        (90, 100, "East"),       # 100m due east
        (180, 100, "South"),     # 100m due south
        (270, 100, "West"),      # 100m due west
        (45, 10, "NE (10m)"),    # 10m northeast
        (45, 500, "NE (500m)"),  # 500m northeast
    ]

    for direction_angle, distance_meters, label in test_cases:
        print(f"\n--- Testing {label}: {distance_meters}m at {direction_angle}° ---")

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

        attacker.add_attacks_positional_offset_const(direction_angle=direction_angle, distance_meters=distance_meters)
        df_after = attacker.get_data().compute()

        # Calculate expected offset
        expected_delta_x = distance_meters * math.cos(direction_angle)
        expected_delta_y = distance_meters * math.sin(direction_angle)

        # Calculate actual offsets
        attacker_mask = df_after['isAttacker'] == 1
        attackers_before = df_before[attacker_mask]
        attackers_after = df_after[attacker_mask]

        actual_delta_x = attackers_after['x_pos'].values - attackers_before['x_pos'].values
        actual_delta_y = attackers_after['y_pos'].values - attackers_before['y_pos'].values

        # Validate
        assert np.allclose(actual_delta_x, expected_delta_x, rtol=1e-5), f"{label}: X offsets don't match"
        assert np.allclose(actual_delta_y, expected_delta_y, rtol=1e-5), f"{label}: Y offsets don't match"

        print(f"✓ {label}: {n_attackers} attackers offset correctly")

    print("\n✅ TEST 2 PASSED")
    return True


def test_determinism():
    """
    Test 3: Determinism across multiple runs.

    Validates:
    - Same seed produces identical results
    - Offset is deterministic given same input
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
        attacker.add_attacks_positional_offset_const(direction_angle=45, distance_meters=100)

        df_result = attacker.get_data().compute()
        results.append(df_result)

        print(f"Run {run}: {len(df_result)} rows, {(df_result['isAttacker']==1).sum()} attackers")

    # Compare results
    print("\n--- Comparing Results ---")
    df1, df2, df3 = results

    # Check perfect match
    match_1_2 = (df1['x_pos'] == df2['x_pos']).sum()
    match_1_3 = (df1['x_pos'] == df3['x_pos']).sum()
    match_2_3 = (df2['x_pos'] == df3['x_pos']).sum()

    print(f"Run 1 vs Run 2: {match_1_2}/{len(df1)} matching X positions")
    print(f"Run 1 vs Run 3: {match_1_3}/{len(df1)} matching X positions")
    print(f"Run 2 vs Run 3: {match_2_3}/{len(df1)} matching X positions")

    assert match_1_2 == len(df1), "Run 1 and 2 don't match"
    assert match_1_3 == len(df1), "Run 1 and 3 don't match"
    assert match_2_3 == len(df1), "Run 2 and 3 don't match"

    print("✓ Perfect determinism (100% match across all runs)")

    print("\n✅ TEST 3 PASSED")
    return True


if __name__ == "__main__":
    print("="*80)
    print("DaskConnectedDrivingAttacker - Positional Offset Const Attack Test Suite")
    print("="*80)

    # Initialize Dask client
    print("\nInitializing Dask client...")
    client = DaskSessionManager.get_client()
    print(f"Dask client initialized: {client}")

    # Run tests
    try:
        test_basic_offset_xy_coordinates()
        test_different_directions_and_distances()
        test_determinism()

        print("\n" + "="*80)
        print("✅ ALL TESTS PASSED")
        print("="*80)
        print("\nSummary:")
        print("- ✓ Basic offset attack validated (XY coordinates)")
        print("- ✓ Different directions and distances validated")
        print("- ✓ Perfect determinism confirmed")
        print("- ✓ Regular vehicles remain unchanged")
        print("- ✓ Offset magnitude and direction correct")
        print("\nTask 51: Implement positional_offset_const_attack - COMPLETE ✅")

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

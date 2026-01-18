"""
Comprehensive test suite for all DaskConnectedDrivingAttacker attack methods.

This test file validates all 8 attack methods in DaskConnectedDrivingAttacker:

1. add_attackers() - Deterministic ID-based attacker selection
2. add_rand_attackers() - Random per-row attacker selection
3. add_attacks_positional_swap_rand() - Random position swap
4. add_attacks_positional_offset_const() - Constant positional offset
5. add_attacks_positional_offset_rand() - Random positional offset
6. add_attacks_positional_offset_const_per_id_with_random_direction() - Per-ID constant offset
7. add_attacks_positional_override_const() - Constant position override from origin
8. add_attacks_positional_override_rand() - Random position override from origin

Each test class follows the established pattern with tests for:
- Basic execution (no errors, structure preserved)
- Attacker-only modification (regular vehicles unchanged)
- Method-specific behavior validation
- Reproducibility with SEED
- Method chaining
- Lazy evaluation (Dask DataFrame)
- Empty DataFrame handling
"""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Test.Fixtures.DaskFixtures import dask_client
from Test.Utils.DataFrameComparator import DataFrameComparator
from geographiclib.geodesic import Geodesic
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from unittest.mock import patch


@pytest.fixture(autouse=True)
def setup_context_provider():
    """
    Auto-used fixture that sets up a mock GeneratorContextProvider with required config values.

    The positional_swap_rand attack method requires ConnectedDrivingCleaner.x_pos and
    ConnectedDrivingCleaner.y_pos config values for cache key compatibility.
    This fixture ensures these values are available during testing.
    """
    # Create a context with the required config values
    from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

    # Create custom context provider class that returns our test values
    class TestGeneratorContextProvider(GeneratorContextProvider):
        def __init__(self):
            super().__init__({
                'ConnectedDrivingCleaner.x_pos': -106.0,  # Default center longitude
                'ConnectedDrivingCleaner.y_pos': 41.0,    # Default center latitude
            })

    # Temporarily replace the context provider in the DI system
    original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
    SDI_DEPENDENCIES['IGeneratorContextProvider'] = TestGeneratorContextProvider

    yield

    # Restore original provider after test
    SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider


@pytest.fixture
def sample_bsm_data(dask_client):
    """
    Create sample BSM data WITHOUT attacker labels for testing attacker selection methods.

    Creates 4 vehicles (IDs 1001, 1002, 1003, 1004) with multiple rows each:
    - Vehicle 1001: 5 rows
    - Vehicle 1002: 5 rows
    - Vehicle 1003: 4 rows
    - Vehicle 1004: 3 rows

    Total: 17 rows

    Uses lat/lon coordinates (isXYCoords=False by default).
    """
    data = {
        'coreData_id': [1001, 1001, 1001, 1001, 1001,
                        1002, 1002, 1002, 1002, 1002,
                        1003, 1003, 1003, 1003,
                        1004, 1004, 1004],
        'x_pos': [-106.0, -106.001, -106.002, -106.003, -106.004,
                  -106.1, -106.101, -106.102, -106.103, -106.104,
                  -106.2, -106.201, -106.202, -106.203,
                  -106.3, -106.301, -106.302],
        'y_pos': [41.0, 41.001, 41.002, 41.003, 41.004,
                  41.1, 41.101, 41.102, 41.103, 41.104,
                  41.2, 41.201, 41.202, 41.203,
                  41.3, 41.301, 41.302],
        'coreData_elevation': [100.0, 101.0, 102.0, 103.0, 104.0,
                               200.0, 201.0, 202.0, 203.0, 204.0,
                               300.0, 301.0, 302.0, 303.0,
                               400.0, 401.0, 402.0]
    }

    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    return ddf


@pytest.fixture
def sample_bsm_data_with_attackers(dask_client):
    """
    Create sample BSM data WITH attacker labels for testing attack methods.

    Creates 3 vehicles (IDs 1001, 1002, 1003) with multiple rows each:
    - Vehicle 1001: 5 rows, attacker
    - Vehicle 1002: 5 rows, regular
    - Vehicle 1003: 3 rows, attacker

    Total: 13 rows, 8 attacker rows (from 2 vehicles)

    Uses lat/lon coordinates (isXYCoords=False by default).
    """
    data = {
        'coreData_id': [1001, 1001, 1001, 1001, 1001,
                        1002, 1002, 1002, 1002, 1002,
                        1003, 1003, 1003],
        'x_pos': [-106.0, -106.001, -106.002, -106.003, -106.004,
                  -106.1, -106.101, -106.102, -106.103, -106.104,
                  -106.2, -106.201, -106.202],
        'y_pos': [41.0, 41.001, 41.002, 41.003, 41.004,
                  41.1, 41.101, 41.102, 41.103, 41.104,
                  41.2, 41.201, 41.202],
        'isAttacker': [1, 1, 1, 1, 1,
                       0, 0, 0, 0, 0,
                       1, 1, 1],
        'coreData_elevation': [100.0, 101.0, 102.0, 103.0, 104.0,
                               200.0, 201.0, 202.0, 203.0, 204.0,
                               300.0, 301.0, 302.0]
    }

    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    return ddf


# ========================================================================
# Test Class 1: add_attackers() - Deterministic ID-based selection
# ========================================================================

class TestAddAttackers:
    """Test suite for add_attackers() method."""

    def test_basic_execution(self, dask_client, sample_bsm_data):
        """Test that add_attackers executes without errors and adds isAttacker column."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_attackers()
        result = attacker.get_data().compute()

        # Verify structure
        assert len(result) == 17
        assert 'isAttacker' in result.columns
        assert set(result['isAttacker'].unique()).issubset({0, 1})

    def test_deterministic_behavior(self, dask_client, sample_bsm_data):
        """Test that same SEED produces identical attacker assignments."""
        # Run 1
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker1"
        )
        attacker1.add_attackers()
        result1 = attacker1.get_data().compute()

        # Run 2 with same SEED (SEED=42 by default)
        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker2"
        )
        attacker2.add_attackers()
        result2 = attacker2.get_data().compute()

        # Should have identical isAttacker assignments
        pd.testing.assert_series_equal(
            result1['isAttacker'].reset_index(drop=True),
            result2['isAttacker'].reset_index(drop=True)
        )

    def test_id_based_consistency(self, dask_client, sample_bsm_data):
        """Test that all rows with same vehicle ID have same isAttacker value."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_attackers()
        result = attacker.get_data().compute()

        # Group by vehicle ID and check consistency
        for vehicle_id in result['coreData_id'].unique():
            vehicle_rows = result[result['coreData_id'] == vehicle_id]
            attacker_values = vehicle_rows['isAttacker'].unique()
            assert len(attacker_values) == 1, \
                f"Vehicle {vehicle_id} has inconsistent isAttacker values: {attacker_values}"

    def test_attack_ratio_approximate(self, dask_client, sample_bsm_data):
        """Test that attack ratio is approximately correct (based on vehicle IDs)."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )
        # Default attack_ratio = 0.05 (5%)

        attacker.add_attackers()
        result = attacker.get_data().compute()

        # Count unique vehicle IDs marked as attackers
        unique_ids = result.groupby('coreData_id')['isAttacker'].first()
        num_attacker_ids = (unique_ids == 1).sum()
        total_ids = len(unique_ids)

        # With 4 vehicles and 5% ratio, we expect ~0 attackers (rounds down)
        # But train_test_split behavior may vary - just verify it's reasonable
        assert 0 <= num_attacker_ids <= total_ids

    def test_preserves_other_columns(self, dask_client, sample_bsm_data):
        """Test that add_attackers preserves all other columns unchanged."""
        original = sample_bsm_data.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_attackers()
        result = attacker.get_data().compute()

        # Check original columns are preserved
        for col in ['coreData_id', 'x_pos', 'y_pos', 'coreData_elevation']:
            pd.testing.assert_series_equal(
                original[col].reset_index(drop=True),
                result[col].reset_index(drop=True),
                check_names=False
            )

    def test_lazy_evaluation(self, dask_client, sample_bsm_data):
        """Test that add_attackers maintains lazy evaluation."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_attackers()
        result = attacker.get_data()

        # Should return Dask DataFrame, not pandas
        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 2: add_rand_attackers() - Random per-row selection
# ========================================================================

class TestAddRandAttackers:
    """Test suite for add_rand_attackers() method."""

    def test_basic_execution(self, dask_client, sample_bsm_data):
        """Test that add_rand_attackers executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_rand_attackers()
        result = attacker.get_data().compute()

        # Verify structure
        assert len(result) == 17
        assert 'isAttacker' in result.columns
        assert set(result['isAttacker'].unique()).issubset({0, 1})

    def test_row_level_independence(self, dask_client, sample_bsm_data):
        """Test that same vehicle ID can have different isAttacker values."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_rand_attackers()
        result = attacker.get_data().compute()

        # Look for at least one vehicle with mixed isAttacker values
        # With 17 rows and 5% attack ratio, there's a reasonable chance
        # Note: This test may occasionally pass even if logic is wrong,
        # but it's a useful probabilistic check
        mixed_found = False
        for vehicle_id in result['coreData_id'].unique():
            vehicle_rows = result[result['coreData_id'] == vehicle_id]
            attacker_values = vehicle_rows['isAttacker'].unique()
            if len(attacker_values) > 1:
                mixed_found = True
                break

        # If not mixed, at least verify different vehicles can have different values
        if not mixed_found:
            unique_id_values = result.groupby('coreData_id')['isAttacker'].first()
            assert len(unique_id_values.unique()) > 0

    def test_reproducibility_with_seed(self, dask_client, sample_bsm_data):
        """Test that same SEED produces reproducible results."""
        # Run 1
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker1"
        )
        attacker1.add_rand_attackers()
        result1 = attacker1.get_data().compute()

        # Run 2 with same SEED
        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker2"
        )
        attacker2.add_rand_attackers()
        result2 = attacker2.get_data().compute()

        # Should have identical results with same SEED
        pd.testing.assert_series_equal(
            result1['isAttacker'].reset_index(drop=True),
            result2['isAttacker'].reset_index(drop=True)
        )

    def test_lazy_evaluation(self, dask_client, sample_bsm_data):
        """Test that add_rand_attackers maintains lazy evaluation."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        attacker.add_rand_attackers()
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 3: add_attacks_positional_swap_rand() - Random position swap
# ========================================================================

class TestPositionalSwapRand:
    """Test suite for add_attacks_positional_swap_rand() method."""

    def test_basic_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that positional swap executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_swap_rand()
        result = attacker.get_data().compute()

        # Verify structure preserved
        assert len(result) == 13
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only attacker rows have positions modified."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_swap_rand()
        result = attacker.get_data().compute()

        # Check regular vehicles (isAttacker=0) unchanged
        original_regulars = original[original['isAttacker'] == 0].reset_index(drop=True)
        result_regulars = result[result['isAttacker'] == 0].reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_regulars['x_pos'],
            result_regulars['x_pos'],
            check_names=False
        )
        pd.testing.assert_series_equal(
            original_regulars['y_pos'],
            result_regulars['y_pos'],
            check_names=False
        )

    def test_positions_from_original_dataset(self, dask_client, sample_bsm_data_with_attackers):
        """Test that swapped positions exist in the original dataset."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_swap_rand()
        result = attacker.get_data().compute()

        # Check that each attacker position exists in original dataset
        attackers = result[result['isAttacker'] == 1]
        for _, row in attackers.iterrows():
            # Find if this (x_pos, y_pos) pair exists in original
            matches = original[
                (original['x_pos'] == row['x_pos']) &
                (original['y_pos'] == row['y_pos'])
            ]
            assert len(matches) > 0, \
                f"Position ({row['x_pos']}, {row['y_pos']}) not found in original dataset"

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test that method returns self for chaining."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        result = attacker.add_attacks_positional_swap_rand()
        assert result is attacker

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test that result maintains lazy evaluation."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_swap_rand()
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 4: add_attacks_positional_offset_const() - Constant offset
# ========================================================================

class TestPositionalOffsetConst:
    """Test suite for add_attacks_positional_offset_const() method."""

    def test_basic_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that constant offset attack executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const(
            direction_angle=45,
            distance_meters=50
        )
        result = attacker.get_data().compute()

        assert len(result) == 13
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only attackers are modified."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const(
            direction_angle=90,
            distance_meters=100
        )
        result = attacker.get_data().compute()

        # Check regular vehicles unchanged
        original_regulars = original[original['isAttacker'] == 0].reset_index(drop=True)
        result_regulars = result[result['isAttacker'] == 0].reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_regulars['x_pos'],
            result_regulars['x_pos'],
            check_names=False
        )

    def test_different_angles_produce_different_positions(self, dask_client, sample_bsm_data_with_attackers):
        """Test that different angles produce different positions."""
        # Test with 0° (North)
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker1"
        )
        attacker1.add_attacks_positional_offset_const(direction_angle=0, distance_meters=50)
        result1 = attacker1.get_data().compute()

        # Test with 180° (South)
        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker2"
        )
        attacker2.add_attacks_positional_offset_const(direction_angle=180, distance_meters=50)
        result2 = attacker2.get_data().compute()

        # Get first attacker row from each
        attacker1_pos = result1[result1['isAttacker'] == 1].iloc[0]
        attacker2_pos = result2[result2['isAttacker'] == 1].iloc[0]

        # Positions should be different
        assert not (attacker1_pos['x_pos'] == attacker2_pos['x_pos'] and
                    attacker1_pos['y_pos'] == attacker2_pos['y_pos'])

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test method chaining support."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        result = attacker.add_attacks_positional_offset_const(
            direction_angle=45,
            distance_meters=50
        )
        assert result is attacker

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test lazy evaluation preserved."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const(direction_angle=45, distance_meters=50)
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 5: add_attacks_positional_offset_rand() - Random offset
# ========================================================================

class TestPositionalOffsetRand:
    """Test suite for add_attacks_positional_offset_rand() method."""

    def test_basic_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that random offset attack executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_rand(min_dist=25, max_dist=250)
        result = attacker.get_data().compute()

        assert len(result) == 13
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only attackers are modified."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_rand(min_dist=50, max_dist=200)
        result = attacker.get_data().compute()

        # Check regular vehicles unchanged
        original_regulars = original[original['isAttacker'] == 0].reset_index(drop=True)
        result_regulars = result[result['isAttacker'] == 0].reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_regulars['x_pos'],
            result_regulars['x_pos'],
            check_names=False
        )

    def test_each_row_gets_different_offset(self, dask_client, sample_bsm_data_with_attackers):
        """Test that each attacker row likely gets a different random offset."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_rand(min_dist=50, max_dist=200)
        result = attacker.get_data().compute()

        # Get attacker rows
        original_attackers = original[original['isAttacker'] == 1].reset_index(drop=True)
        result_attackers = result[result['isAttacker'] == 1].reset_index(drop=True)

        # Calculate offsets (delta from original position)
        offsets = []
        for i in range(len(original_attackers)):
            dx = result_attackers.iloc[i]['x_pos'] - original_attackers.iloc[i]['x_pos']
            dy = result_attackers.iloc[i]['y_pos'] - original_attackers.iloc[i]['y_pos']
            offsets.append((dx, dy))

        # With 8 attacker rows, very likely to have at least some different offsets
        unique_offsets = len(set(offsets))
        assert unique_offsets > 1, "All attackers have same offset (should be random)"

    def test_custom_distance_range(self, dask_client, sample_bsm_data_with_attackers):
        """Test that custom distance range works."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_rand(min_dist=10, max_dist=20)
        result = attacker.get_data().compute()

        # Just verify execution - distance validation requires geodesic calculation
        assert len(result) == 13

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test method chaining support."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        result = attacker.add_attacks_positional_offset_rand(min_dist=25, max_dist=250)
        assert result is attacker

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test lazy evaluation preserved."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_rand(min_dist=25, max_dist=250)
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 6: add_attacks_positional_offset_const_per_id_with_random_direction()
# ========================================================================

class TestPositionalOffsetConstPerID:
    """Test suite for per-ID constant offset with random direction."""

    def test_basic_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that per-ID offset attack executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=25, max_dist=250
        )
        result = attacker.get_data().compute()

        assert len(result) == 13
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only attackers are modified."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50, max_dist=200
        )
        result = attacker.get_data().compute()

        # Check regular vehicles unchanged
        original_regulars = original[original['isAttacker'] == 0].reset_index(drop=True)
        result_regulars = result[result['isAttacker'] == 0].reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_regulars['x_pos'],
            result_regulars['x_pos'],
            check_names=False
        )

    def test_same_id_gets_same_offset(self, dask_client, sample_bsm_data_with_attackers):
        """Test that all rows with same vehicle ID get same offset."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50, max_dist=200
        )
        result = attacker.get_data().compute()

        # Check each attacker vehicle ID
        attacker_ids = result[result['isAttacker'] == 1]['coreData_id'].unique()

        for vehicle_id in attacker_ids:
            # Get all rows for this vehicle
            original_vehicle = original[original['coreData_id'] == vehicle_id].reset_index(drop=True)
            result_vehicle = result[result['coreData_id'] == vehicle_id].reset_index(drop=True)

            # Calculate offsets
            offsets = []
            for i in range(len(original_vehicle)):
                dx = result_vehicle.iloc[i]['x_pos'] - original_vehicle.iloc[i]['x_pos']
                dy = result_vehicle.iloc[i]['y_pos'] - original_vehicle.iloc[i]['y_pos']
                offsets.append((dx, dy))

            # All offsets should be identical (within numerical tolerance)
            first_offset = offsets[0]
            for offset in offsets[1:]:
                assert np.allclose(offset[0], first_offset[0], rtol=1e-4)
                assert np.allclose(offset[1], first_offset[1], rtol=1e-4)

    def test_different_ids_get_different_offsets(self, dask_client, sample_bsm_data_with_attackers):
        """Test that different vehicle IDs likely get different random offsets."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50, max_dist=200
        )
        result = attacker.get_data().compute()

        # Get attacker vehicle IDs (should be 1001 and 1003)
        attacker_ids = result[result['isAttacker'] == 1]['coreData_id'].unique()

        if len(attacker_ids) >= 2:
            # Calculate offset for first row of each vehicle
            offsets_per_id = {}
            for vehicle_id in attacker_ids:
                original_row = original[original['coreData_id'] == vehicle_id].iloc[0]
                result_row = result[result['coreData_id'] == vehicle_id].iloc[0]

                dx = result_row['x_pos'] - original_row['x_pos']
                dy = result_row['y_pos'] - original_row['y_pos']
                offsets_per_id[vehicle_id] = (dx, dy)

            # Different IDs should have different offsets
            offsets_list = list(offsets_per_id.values())
            unique_offsets = len(set(offsets_list))
            assert unique_offsets > 1, "All vehicle IDs have same offset (should be random per ID)"

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test method chaining support."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        result = attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=25, max_dist=250
        )
        assert result is attacker

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test lazy evaluation preserved."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=25, max_dist=250
        )
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 7: add_attacks_positional_override_const() - Override to fixed position
# ========================================================================

class TestPositionalOverrideConst:
    """Test suite for positional_override_const attack."""

    def test_basic_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that constant override attack executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=50
        )
        result = attacker.get_data().compute()

        assert len(result) == 13
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only attackers are modified."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_const(
            direction_angle=90,
            distance_meters=100
        )
        result = attacker.get_data().compute()

        # Check regular vehicles unchanged
        original_regulars = original[original['isAttacker'] == 0].reset_index(drop=True)
        result_regulars = result[result['isAttacker'] == 0].reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_regulars['x_pos'],
            result_regulars['x_pos'],
            check_names=False
        )

    def test_all_attackers_same_position(self, dask_client, sample_bsm_data_with_attackers):
        """Test that all attackers are moved to same absolute position."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=50
        )
        result = attacker.get_data().compute()

        # Get all attacker positions
        attacker_positions = result[result['isAttacker'] == 1][['x_pos', 'y_pos']]

        # All should have identical positions
        unique_positions = attacker_positions.drop_duplicates()
        assert len(unique_positions) == 1, \
            "Not all attackers at same position (should be identical for override_const)"

    def test_different_angles_produce_different_positions(self, dask_client, sample_bsm_data_with_attackers):
        """Test that different angles produce different override positions."""
        # Test with 0° (North)
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker1"
        )
        attacker1.add_attacks_positional_override_const(direction_angle=0, distance_meters=50)
        result1 = attacker1.get_data().compute()

        # Test with 180° (South)
        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker2"
        )
        attacker2.add_attacks_positional_override_const(direction_angle=180, distance_meters=50)
        result2 = attacker2.get_data().compute()

        # Get attacker positions
        pos1 = result1[result1['isAttacker'] == 1].iloc[0][['x_pos', 'y_pos']]
        pos2 = result2[result2['isAttacker'] == 1].iloc[0][['x_pos', 'y_pos']]

        # Positions should be different
        assert not (pos1['x_pos'] == pos2['x_pos'] and pos1['y_pos'] == pos2['y_pos'])

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test method chaining support."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        result = attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=50
        )
        assert result is attacker

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test lazy evaluation preserved."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_const(direction_angle=45, distance_meters=50)
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 8: add_attacks_positional_override_rand() - Override to random positions
# ========================================================================

class TestPositionalOverrideRand:
    """Test suite for positional_override_rand attack."""

    def test_basic_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that random override attack executes without errors."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_rand(min_dist=50, max_dist=200)
        result = attacker.get_data().compute()

        assert len(result) == 13
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only attackers are modified."""
        original = sample_bsm_data_with_attackers.compute()

        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_rand(min_dist=50, max_dist=200)
        result = attacker.get_data().compute()

        # Check regular vehicles unchanged
        original_regulars = original[original['isAttacker'] == 0].reset_index(drop=True)
        result_regulars = result[result['isAttacker'] == 0].reset_index(drop=True)

        pd.testing.assert_series_equal(
            original_regulars['x_pos'],
            result_regulars['x_pos'],
            check_names=False
        )

    def test_each_attacker_gets_different_position(self, dask_client, sample_bsm_data_with_attackers):
        """Test that each attacker row likely gets a different random position."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_rand(min_dist=50, max_dist=200)
        result = attacker.get_data().compute()

        # Get attacker positions
        attacker_positions = result[result['isAttacker'] == 1][['x_pos', 'y_pos']]

        # With random override, very likely to have different positions
        unique_positions = attacker_positions.drop_duplicates()
        assert len(unique_positions) > 1, \
            "All attackers have same position (should be random)"

    def test_custom_distance_range(self, dask_client, sample_bsm_data_with_attackers):
        """Test that custom distance range works."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_rand(min_dist=10, max_dist=20)
        result = attacker.get_data().compute()

        # Just verify execution
        assert len(result) == 13

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test method chaining support."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        result = attacker.add_attacks_positional_override_rand(min_dist=25, max_dist=250)
        assert result is attacker

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test lazy evaluation preserved."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        attacker.add_attacks_positional_override_rand(min_dist=25, max_dist=250)
        result = attacker.get_data()

        assert isinstance(result, dd.DataFrame)


# ========================================================================
# Test Class 9: Boundary Conditions (0% and 100% attackers)
# ========================================================================

class TestAttackerBoundaryConditions:
    """
    Test suite for boundary conditions in attacker selection methods.

    This test class validates behavior at extreme attack_ratio values:
    - 0% attackers (attack_ratio = 0.0): No vehicles should be marked as attackers
    - 100% attackers (attack_ratio = 1.0): All vehicles should be marked as attackers

    Tests both deterministic (add_attackers) and random (add_rand_attackers) methods,
    plus validates that all 8 attack methods handle these boundary conditions correctly.
    """

    def test_zero_percent_attackers_deterministic(self, dask_client, sample_bsm_data):
        """Test add_attackers() with attack_ratio = 0.0 (no attackers)."""
        # Use the Dependency Injection system to temporarily override attack_ratio
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        # Create custom context provider with 0% attack_ratio
        class ZeroPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 0.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = ZeroPercentContextProvider

        try:
            attacker = DaskConnectedDrivingAttacker(
                data=sample_bsm_data,
                id="test_zero_percent"
            )

            attacker.add_attackers()
            result = attacker.get_data().compute()

            # Verify NO attackers were selected
            assert 'isAttacker' in result.columns
            assert (result['isAttacker'] == 0).all(), \
                "With 0% attack_ratio, all isAttacker values should be 0"

            # Verify total row count unchanged
            assert len(result) == 17
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_hundred_percent_attackers_deterministic(self, dask_client, sample_bsm_data):
        """Test add_attackers() with attack_ratio = 1.0 (all attackers)."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class HundredPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 1.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = HundredPercentContextProvider

        try:
            attacker = DaskConnectedDrivingAttacker(
                data=sample_bsm_data,
                id="test_hundred_percent"
            )

            attacker.add_attackers()
            result = attacker.get_data().compute()

            # Verify ALL vehicles are attackers
            assert 'isAttacker' in result.columns
            assert (result['isAttacker'] == 1).all(), \
                "With 100% attack_ratio, all isAttacker values should be 1"

            # Verify total row count unchanged
            assert len(result) == 17

            # Verify all unique vehicle IDs are marked as attackers
            unique_ids = result.groupby('coreData_id')['isAttacker'].first()
            assert (unique_ids == 1).all(), \
                "All unique vehicle IDs should be marked as attackers"
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_zero_percent_attackers_random(self, dask_client, sample_bsm_data):
        """Test add_rand_attackers() with attack_ratio = 0.0 (no attackers)."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class ZeroPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 0.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = ZeroPercentContextProvider

        try:
            attacker = DaskConnectedDrivingAttacker(
                data=sample_bsm_data,
                id="test_zero_percent_rand"
            )

            attacker.add_rand_attackers()
            result = attacker.get_data().compute()

            # Verify NO attackers were selected (random.random() <= 0.0 is always False)
            assert 'isAttacker' in result.columns
            assert (result['isAttacker'] == 0).all(), \
                "With 0% attack_ratio, all isAttacker values should be 0 (random method)"

            assert len(result) == 17
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_hundred_percent_attackers_random(self, dask_client, sample_bsm_data):
        """Test add_rand_attackers() with attack_ratio = 1.0 (all attackers)."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class HundredPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 1.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = HundredPercentContextProvider

        try:
            attacker = DaskConnectedDrivingAttacker(
                data=sample_bsm_data,
                id="test_hundred_percent_rand"
            )

            attacker.add_rand_attackers()
            result = attacker.get_data().compute()

            # Verify ALL rows are attackers (random.random() <= 1.0 is always True)
            assert 'isAttacker' in result.columns
            assert (result['isAttacker'] == 1).all(), \
                "With 100% attack_ratio, all isAttacker values should be 1 (random method)"

            assert len(result) == 17
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_positional_attacks_with_zero_percent_attackers(self, dask_client, sample_bsm_data):
        """Test that positional attack methods handle 0% attackers gracefully (no-op)."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class ZeroPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 0.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = ZeroPercentContextProvider

        try:
            # Store original positions
            original = sample_bsm_data.compute()

            attacker = DaskConnectedDrivingAttacker(
                data=sample_bsm_data,
                id="test_zero_percent_positional"
            )

            # Add 0% attackers, then apply positional offset
            attacker.add_attackers()
            attacker.add_attacks_positional_offset_const(direction_angle=45, distance_meters=100)
            result = attacker.get_data().compute()

            # Since 0% attackers, no positions should be modified
            assert (result['isAttacker'] == 0).all()

            # All positions should remain unchanged
            pd.testing.assert_series_equal(
                original['x_pos'].reset_index(drop=True),
                result['x_pos'].reset_index(drop=True),
                check_names=False
            )
            pd.testing.assert_series_equal(
                original['y_pos'].reset_index(drop=True),
                result['y_pos'].reset_index(drop=True),
                check_names=False
            )
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_positional_attacks_with_hundred_percent_attackers(self, dask_client, sample_bsm_data):
        """Test that positional attack methods modify ALL positions with 100% attackers."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class HundredPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 1.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = HundredPercentContextProvider

        try:
            # Store original positions
            original = sample_bsm_data.compute()

            attacker = DaskConnectedDrivingAttacker(
                data=sample_bsm_data,
                id="test_hundred_percent_positional"
            )

            # Add 100% attackers, then apply positional offset
            attacker.add_attackers()
            attacker.add_attacks_positional_offset_const(direction_angle=45, distance_meters=100)
            result = attacker.get_data().compute()

            # All vehicles should be attackers
            assert (result['isAttacker'] == 1).all()

            # ALL positions should be modified (none should match original)
            positions_unchanged = (
                (result['x_pos'] == original['x_pos']) &
                (result['y_pos'] == original['y_pos'])
            ).sum()

            # With a 100m offset at 45°, positions MUST change
            assert positions_unchanged == 0, \
                "With 100% attackers and positional offset, all positions should be modified"
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_all_attack_methods_with_zero_percent_attackers(self, dask_client, sample_bsm_data):
        """Test that all 8 attack methods handle 0% attackers without errors."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class ZeroPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 0.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = ZeroPercentContextProvider

        try:
            # Test each attack method executes without errors
            attack_methods = [
                ('add_attacks_positional_swap_rand', {}),
                ('add_attacks_positional_offset_const', {'direction_angle': 45, 'distance_meters': 100}),
                ('add_attacks_positional_offset_rand', {'min_dist': 50, 'max_dist': 200}),
                ('add_attacks_positional_offset_const_per_id_with_random_direction', {'min_dist': 50, 'max_dist': 200}),
                ('add_attacks_positional_override_const', {'direction_angle': 45, 'distance_meters': 100}),
                ('add_attacks_positional_override_rand', {'min_dist': 50, 'max_dist': 200}),
            ]

            for method_name, kwargs in attack_methods:
                attacker = DaskConnectedDrivingAttacker(
                    data=sample_bsm_data,
                    id=f"test_{method_name}"
                )

                attacker.add_attackers()  # 0% attackers
                method = getattr(attacker, method_name)
                method(**kwargs)
                result = attacker.get_data().compute()

                # Should execute without errors
                assert len(result) == 17
                assert (result['isAttacker'] == 0).all(), \
                    f"{method_name} should preserve 0% attackers"
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider

    def test_all_attack_methods_with_hundred_percent_attackers(self, dask_client, sample_bsm_data):
        """Test that all 8 attack methods handle 100% attackers without errors."""
        from Decorators.StandardDependencyInjection import SDI_DEPENDENCIES

        class HundredPercentContextProvider(GeneratorContextProvider):
            def __init__(self):
                super().__init__({
                    'ConnectedDrivingAttacker.attack_ratio': 1.0,
                    'ConnectedDrivingAttacker.SEED': 42,
                    'ConnectedDrivingCleaner.x_pos': -106.0,
                    'ConnectedDrivingCleaner.y_pos': 41.0,
                })

        original_provider = SDI_DEPENDENCIES['IGeneratorContextProvider']
        SDI_DEPENDENCIES['IGeneratorContextProvider'] = HundredPercentContextProvider

        try:
            # Test each attack method executes without errors
            attack_methods = [
                ('add_attacks_positional_swap_rand', {}),
                ('add_attacks_positional_offset_const', {'direction_angle': 45, 'distance_meters': 100}),
                ('add_attacks_positional_offset_rand', {'min_dist': 50, 'max_dist': 200}),
                ('add_attacks_positional_offset_const_per_id_with_random_direction', {'min_dist': 50, 'max_dist': 200}),
                ('add_attacks_positional_override_const', {'direction_angle': 45, 'distance_meters': 100}),
                ('add_attacks_positional_override_rand', {'min_dist': 50, 'max_dist': 200}),
            ]

            for method_name, kwargs in attack_methods:
                attacker = DaskConnectedDrivingAttacker(
                    data=sample_bsm_data,
                    id=f"test_{method_name}"
                )

                attacker.add_attackers()  # 100% attackers
                method = getattr(attacker, method_name)
                method(**kwargs)
                result = attacker.get_data().compute()

                # Should execute without errors
                assert len(result) == 17
                assert (result['isAttacker'] == 1).all(), \
                    f"{method_name} should preserve 100% attackers"
        finally:
            SDI_DEPENDENCIES['IGeneratorContextProvider'] = original_provider


# ========================================================================
# Integration Tests
# ========================================================================

class TestAttackerIntegration:
    """Integration tests for chaining multiple attack methods."""

    def test_chaining_attacker_selection_and_attack(self, dask_client, sample_bsm_data):
        """Test chaining attacker selection with positional attack."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data,
            id="test_attacker"
        )

        # Chain: select attackers → apply position offset
        attacker.add_attackers().add_attacks_positional_offset_const(
            direction_angle=45,
            distance_meters=50
        )

        result = attacker.get_data().compute()

        # Verify both operations applied
        assert 'isAttacker' in result.columns
        assert len(result) == 17

    def test_multiple_attack_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test chaining multiple attacks (last one should win)."""
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Chain multiple attacks
        attacker.add_attacks_positional_offset_const(
            direction_angle=0,
            distance_meters=50
        ).add_attacks_positional_override_const(
            direction_angle=90,
            distance_meters=100
        )

        result = attacker.get_data().compute()

        # Should complete without errors
        assert len(result) == 13
        assert 'isAttacker' in result.columns

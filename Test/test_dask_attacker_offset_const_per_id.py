"""
Test suite for DaskConnectedDrivingAttacker positional_offset_const_per_id_with_random_direction attack.

This test file validates the new attack method that assigns random direction/distance per vehicle ID,
maintaining consistency across all rows for the same ID.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Test.Fixtures.DaskFixtures import dask_client
from Test.Utils.DataFrameComparator import DataFrameComparator


@pytest.fixture
def sample_bsm_data_with_attackers(dask_client):
    """
    Create sample BSM data with attacker labels for testing.

    Creates 3 vehicles (IDs 1001, 1002, 1003) with multiple rows each:
    - Vehicle 1001: 5 rows, attacker
    - Vehicle 1002: 5 rows, regular
    - Vehicle 1003: 5 rows, attacker

    Total: 15 rows, 10 attacker rows (from 2 vehicles)

    Note: Uses lat/lon coordinates (not XY) because MathHelper.direction_and_dist_to_XY
    has a bug (uses degrees instead of radians for cos/sin).
    """
    data = {
        'coreData_id': [1001, 1001, 1001, 1001, 1001,
                        1002, 1002, 1002, 1002, 1002,
                        1003, 1003, 1003, 1003, 1003],
        'x_pos': [-106.0, -106.001, -106.002, -106.003, -106.004,  # Longitude (x)
                  -106.1, -106.101, -106.102, -106.103, -106.104,
                  -106.2, -106.201, -106.202, -106.203, -106.204],
        'y_pos': [41.0, 41.001, 41.002, 41.003, 41.004,  # Latitude (y)
                  41.1, 41.101, 41.102, 41.103, 41.104,
                  41.2, 41.201, 41.202, 41.203, 41.204],
        'isAttacker': [1, 1, 1, 1, 1,
                       0, 0, 0, 0, 0,
                       1, 1, 1, 1, 1],
        'coreData_elevation': [100.0, 101.0, 102.0, 103.0, 104.0,
                               200.0, 201.0, 202.0, 203.0, 204.0,
                               300.0, 301.0, 302.0, 303.0, 304.0]
    }

    df = pd.DataFrame(data)
    ddf = dd.from_pandas(df, npartitions=2)

    return ddf


class TestPositionalOffsetConstPerIDWithRandomDirection:
    """Test suite for positional_offset_const_per_id_with_random_direction attack."""

    def test_basic_attack_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack method executes without errors."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify DataFrame structure is preserved
        assert len(result) == 15
        assert 'x_pos' in result.columns
        assert 'y_pos' in result.columns
        assert 'isAttacker' in result.columns

    def test_only_attackers_modified(self, dask_client, sample_bsm_data_with_attackers):
        """Test that only rows with isAttacker=1 are modified."""
        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify regular vehicles (isAttacker=0) are unchanged
        regular_mask = original['isAttacker'] == 0
        pd.testing.assert_frame_equal(
            result[regular_mask][['x_pos', 'y_pos', 'coreData_elevation']].reset_index(drop=True),
            original[regular_mask][['x_pos', 'y_pos', 'coreData_elevation']].reset_index(drop=True)
        )

    def test_same_id_gets_same_offset(self, dask_client, sample_bsm_data_with_attackers):
        """Test that all rows with the same vehicle ID get the same direction/distance."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result
        result = attacker.get_data().compute()

        # For each attacker vehicle ID, verify all rows have consistent offset
        attacker_ids = result[result['isAttacker'] == 1]['coreData_id'].unique()

        for vehicle_id in attacker_ids:
            # Get original and result positions for this vehicle
            orig_rows = original[original['coreData_id'] == vehicle_id]
            result_rows = result[result['coreData_id'] == vehicle_id]

            # Calculate offsets for each row
            x_offsets = result_rows['x_pos'].values - orig_rows['x_pos'].values
            y_offsets = result_rows['y_pos'].values - orig_rows['y_pos'].values

            # Verify all offsets are the same (constant per ID)
            # Note: Geodesic calculations have small numerical variations, so use rtol=1e-4
            # The attack applies the same direction/distance to each row for the same ID,
            # but geodesic calculations from slightly different start positions may have
            # tiny variations in the resulting lat/lon offsets.
            np.testing.assert_allclose(
                x_offsets,
                x_offsets[0] * np.ones_like(x_offsets),
                rtol=1e-4,
                err_msg=f"Vehicle {vehicle_id} has inconsistent X offsets"
            )
            np.testing.assert_allclose(
                y_offsets,
                y_offsets[0] * np.ones_like(y_offsets),
                rtol=1e-4,
                err_msg=f"Vehicle {vehicle_id} has inconsistent Y offsets"
            )

    def test_different_ids_get_different_offsets(self, dask_client, sample_bsm_data_with_attackers):
        """Test that different vehicle IDs get different random directions/distances."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Get attacker vehicle IDs (1001 and 1003)
        attacker_ids = result[result['isAttacker'] == 1]['coreData_id'].unique()
        assert len(attacker_ids) == 2, "Expected 2 attacker vehicles"

        # Calculate offset vectors for each vehicle ID
        offsets = {}
        for vehicle_id in attacker_ids:
            orig_row = original[original['coreData_id'] == vehicle_id].iloc[0]
            result_row = result[result['coreData_id'] == vehicle_id].iloc[0]

            offsets[vehicle_id] = {
                'x_offset': result_row['x_pos'] - orig_row['x_pos'],
                'y_offset': result_row['y_pos'] - orig_row['y_pos']
            }

        # Verify the two vehicles have different offsets
        # (extremely unlikely to be the same with random generation)
        id1, id2 = list(attacker_ids)
        offset1 = offsets[id1]
        offset2 = offsets[id2]

        # At least one coordinate offset should differ
        assert (abs(offset1['x_offset'] - offset2['x_offset']) > 1e-6 or
                abs(offset1['y_offset'] - offset2['y_offset']) > 1e-6), \
            "Different vehicle IDs should get different offsets"

    def test_offset_distance_within_range(self, dask_client, sample_bsm_data_with_attackers):
        """Test that offset distances are within specified min/max range."""
        from geographiclib.geodesic import Geodesic

        min_dist = 50
        max_dist = 100

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=min_dist,
            max_dist=max_dist
        )

        # Get result
        result = attacker.get_data().compute()

        # For each attacker row, verify offset distance is within range
        attacker_mask = result['isAttacker'] == 1
        orig_attackers = original[attacker_mask]
        result_attackers = result[attacker_mask]

        geod = Geodesic.WGS84

        for idx in range(len(orig_attackers)):
            orig_row = orig_attackers.iloc[idx]
            result_row = result_attackers.iloc[idx]

            # Calculate offset distance (Geodesic for lat/lon)
            g = geod.Inverse(orig_row['y_pos'], orig_row['x_pos'],
                            result_row['y_pos'], result_row['x_pos'])
            distance = g['s12']  # Distance in meters

            # Verify distance is within range (allow small floating-point tolerance)
            assert min_dist - 1e-3 <= distance <= max_dist + 1e-3, \
                f"Offset distance {distance} outside range [{min_dist}, {max_dist}]"

    def test_reproducibility_with_seed(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack is reproducible with the same SEED."""
        # Create two attacker instances with same SEED
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker1"
        )

        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker2"
        )

        # Apply attack to both
        attacker1.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        attacker2.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get results
        result1 = attacker1.get_data().compute()
        result2 = attacker2.get_data().compute()

        # Verify results are identical (same SEED should produce same offsets)
        pd.testing.assert_frame_equal(result1, result2)

    def test_custom_distance_range(self, dask_client, sample_bsm_data_with_attackers):
        """Test attack with custom min/max distance range."""
        from geographiclib.geodesic import Geodesic

        min_dist = 10
        max_dist = 20

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Apply attack with custom range
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=min_dist,
            max_dist=max_dist
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify offset distances are within custom range
        attacker_mask = result['isAttacker'] == 1
        orig_attackers = original[attacker_mask]
        result_attackers = result[attacker_mask]

        geod = Geodesic.WGS84

        for idx in range(len(orig_attackers)):
            orig_row = orig_attackers.iloc[idx]
            result_row = result_attackers.iloc[idx]

            # Calculate offset distance (Geodesic for lat/lon)
            g = geod.Inverse(orig_row['y_pos'], orig_row['x_pos'],
                            result_row['y_pos'], result_row['x_pos'])
            distance = g['s12']

            assert min_dist - 1e-3 <= distance <= max_dist + 1e-3

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the method supports chaining by returning self."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Test method chaining
        result = (attacker
                  .add_attacks_positional_offset_const_per_id_with_random_direction(min_dist=50, max_dist=100)
                  .get_data()
                  .compute())

        # Verify result is valid
        assert len(result) == 15
        assert 'isAttacker' in result.columns

    def test_preserves_other_columns(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack preserves all other columns unchanged."""
        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify coreData_id and isAttacker columns are unchanged (check values, not dtypes)
        np.testing.assert_array_equal(
            result['coreData_id'].values,
            original['coreData_id'].values
        )
        np.testing.assert_array_equal(
            result['isAttacker'].values,
            original['isAttacker'].values
        )

        # Verify elevation is unchanged for regular vehicles (check values, not dtypes)
        regular_mask = original['isAttacker'] == 0
        np.testing.assert_array_equal(
            result[regular_mask]['coreData_elevation'].values,
            original[regular_mask]['coreData_elevation'].values
        )

    def test_lazy_evaluation_preserved(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the method returns a Dask DataFrame (lazy evaluation)."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result (should be Dask DataFrame)
        result = attacker.get_data()

        # Verify it's a Dask DataFrame
        assert isinstance(result, dd.DataFrame), \
            "Result should be Dask DataFrame for lazy evaluation"

    def test_empty_dataframe(self, dask_client):
        """Test attack handling with empty DataFrame."""
        # Create empty DataFrame with required columns
        empty_data = pd.DataFrame({
            'coreData_id': [],
            'x_pos': [],
            'y_pos': [],
            'isAttacker': [],
            'coreData_elevation': []
        })

        ddf = dd.from_pandas(empty_data, npartitions=1)

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(data=ddf, id="test_attacker")

        # Apply attack
        attacker.add_attacks_positional_offset_const_per_id_with_random_direction(
            min_dist=50,
            max_dist=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify result is empty but valid
        assert len(result) == 0
        assert list(result.columns) == ['coreData_id', 'x_pos', 'y_pos', 'isAttacker', 'coreData_elevation']


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

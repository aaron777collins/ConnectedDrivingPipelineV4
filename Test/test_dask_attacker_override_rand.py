"""
Test suite for DaskConnectedDrivingAttacker positional_override_rand attack.

This test file validates the positional_override_rand attack method that overrides
attacker positions to random absolute positions from origin (0,0) or center point.

Note: Tests use lat/lon coordinates (isXYCoords=False, the default) to match
the established testing pattern in test_dask_attacker_offset_const_per_id.py.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from Test.Fixtures.DaskFixtures import dask_client
from Test.Utils.DataFrameComparator import DataFrameComparator
from geographiclib.geodesic import Geodesic


@pytest.fixture
def sample_bsm_data_with_attackers(dask_client):
    """
    Create sample BSM data with attacker labels for testing.

    Creates 3 vehicles (IDs 1001, 1002, 1003) with multiple rows each:
    - Vehicle 1001: 5 rows, attacker
    - Vehicle 1002: 5 rows, regular
    - Vehicle 1003: 3 rows, attacker

    Total: 13 rows, 8 attacker rows (from 2 vehicles)

    Uses lat/lon coordinates (isXYCoords=False by default).
    Center point defaults to (0.0, 0.0) in tests without context.
    """
    data = {
        'coreData_id': [1001, 1001, 1001, 1001, 1001,
                        1002, 1002, 1002, 1002, 1002,
                        1003, 1003, 1003],
        'x_pos': [-106.0, -106.001, -106.002, -106.003, -106.004,  # Longitude (x)
                  -106.1, -106.101, -106.102, -106.103, -106.104,
                  -106.2, -106.201, -106.202],
        'y_pos': [41.0, 41.001, 41.002, 41.003, 41.004,  # Latitude (y)
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


class TestPositionalOverrideRand:
    """Test suite for positional_override_rand attack."""

    def test_basic_attack_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack method executes without errors."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_override_rand(
            min_dist=50,
            max_dist=200
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify DataFrame structure is preserved
        assert len(result) == 13
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
        attacker.add_attacks_positional_override_rand(
            min_dist=50,
            max_dist=200
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify regular vehicles (isAttacker=0) positions are unchanged
        regular_mask = original['isAttacker'] == 0
        pd.testing.assert_frame_equal(
            original[regular_mask][['x_pos', 'y_pos']].reset_index(drop=True),
            result[regular_mask][['x_pos', 'y_pos']].reset_index(drop=True),
            check_dtype=False
        )

        # Verify attacker vehicles (isAttacker=1) positions ARE changed
        attacker_mask = original['isAttacker'] == 1
        assert not original[attacker_mask]['x_pos'].equals(result[attacker_mask]['x_pos'])
        assert not original[attacker_mask]['y_pos'].equals(result[attacker_mask]['y_pos'])

    def test_each_row_gets_different_position(self, dask_client, sample_bsm_data_with_attackers):
        """Test that each attacker row gets a different random position."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack with wide range to ensure variety
        attacker.add_attacks_positional_override_rand(
            min_dist=50,
            max_dist=500
        )

        # Get result
        result = attacker.get_data().compute()

        # Get attacker rows only
        attacker_rows = result[result['isAttacker'] == 1]

        # Check that at least some attackers have different positions
        # (With 8 attackers and random range 50-500m in 360 directions,
        # probability of all identical is extremely low)
        unique_positions = attacker_rows[['x_pos', 'y_pos']].drop_duplicates()

        # We expect at least 2 unique positions (very conservative)
        # In practice, with 8 attackers, we should get 6-8 unique positions
        assert len(unique_positions) >= 2, \
            "Expected attackers to have different random positions"

    def test_override_distances_within_range(self, dask_client, sample_bsm_data_with_attackers):
        """Test that override distances are within specified range."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack with specific range
        min_dist = 100
        max_dist = 300
        attacker.add_attacks_positional_override_rand(
            min_dist=min_dist,
            max_dist=max_dist
        )

        # Get result
        result = attacker.get_data().compute()

        # Calculate distances from origin (0.0, 0.0) for all attackers
        # Using geodesic distance for accurate measurement
        geod = Geodesic.WGS84
        center_lat, center_lon = 0.0, 0.0  # Origin for tests without context

        attacker_rows = result[result['isAttacker'] == 1]

        for idx, row in attacker_rows.iterrows():
            # Calculate geodesic distance from origin to attacker position
            result_geod = geod.Inverse(
                center_lat, center_lon,
                row['y_pos'], row['x_pos']
            )
            distance = result_geod['s12']  # Distance in meters

            # Allow small tolerance for geodesic calculations
            # Distance should be within [min_dist - 1, max_dist + 1]
            assert min_dist - 1 <= distance <= max_dist + 1, \
                f"Distance {distance}m not in range [{min_dist}, {max_dist}]"

    def test_reproducibility_with_seed(self, dask_client, sample_bsm_data_with_attackers):
        """Test that results are reproducible with same SEED."""
        # Create two attacker instances with same SEED
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker1"
        )

        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker2"
        )

        # Apply same attack to both
        attacker1.add_attacks_positional_override_rand(min_dist=50, max_dist=200)
        attacker2.add_attacks_positional_override_rand(min_dist=50, max_dist=200)

        # Get results
        result1 = attacker1.get_data().compute()
        result2 = attacker2.get_data().compute()

        # Results should be identical (same SEED = same random sequence)
        pd.testing.assert_frame_equal(
            result1[['x_pos', 'y_pos']],
            result2[['x_pos', 'y_pos']],
            check_dtype=False
        )

    def test_custom_distance_range(self, dask_client, sample_bsm_data_with_attackers):
        """Test attack with custom distance range (10-20m)."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack with narrow range
        min_dist = 10
        max_dist = 20
        attacker.add_attacks_positional_override_rand(
            min_dist=min_dist,
            max_dist=max_dist
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify distances are in narrow range
        geod = Geodesic.WGS84
        center_lat, center_lon = 0.0, 0.0

        attacker_rows = result[result['isAttacker'] == 1]

        for idx, row in attacker_rows.iterrows():
            result_geod = geod.Inverse(
                center_lat, center_lon,
                row['y_pos'], row['x_pos']
            )
            distance = result_geod['s12']

            # All distances should be in [10-1, 20+1] meter range
            assert min_dist - 1 <= distance <= max_dist + 1, \
                f"Distance {distance}m not in narrow range [{min_dist}, {max_dist}]"

    def test_method_chaining_support(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the method returns self for chaining."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack and verify it returns self
        result = attacker.add_attacks_positional_override_rand()

        assert result is attacker, "Method should return self for chaining"

        # Verify data was modified
        final_data = attacker.get_data().compute()
        assert len(final_data) == 13

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
        attacker.add_attacks_positional_override_rand(
            min_dist=50,
            max_dist=200
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify non-position columns are unchanged
        pd.testing.assert_series_equal(
            original['coreData_id'],
            result['coreData_id'],
            check_dtype=False
        )

        pd.testing.assert_series_equal(
            original['isAttacker'],
            result['isAttacker'],
            check_dtype=False
        )

        pd.testing.assert_series_equal(
            original['coreData_elevation'],
            result['coreData_elevation'],
            check_dtype=False
        )

    def test_lazy_evaluation_preserved(self, dask_client, sample_bsm_data_with_attackers):
        """Test that result is still a Dask DataFrame (lazy evaluation)."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_override_rand()

        # Get result (before compute)
        result = attacker.get_data()

        # Verify it's still a Dask DataFrame
        assert isinstance(result, dd.DataFrame), \
            "Result should be Dask DataFrame (lazy evaluation)"

    def test_empty_dataframe_handling(self, dask_client):
        """Test attack handles empty DataFrame gracefully."""
        # Create empty DataFrame with required columns
        empty_df = pd.DataFrame({
            'coreData_id': [],
            'x_pos': [],
            'y_pos': [],
            'isAttacker': []
        })
        empty_ddf = dd.from_pandas(empty_df, npartitions=1)

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=empty_ddf,
            id="test_attacker"
        )

        # Apply attack (should not crash)
        attacker.add_attacks_positional_override_rand()

        # Get result
        result = attacker.get_data().compute()

        # Verify result is still empty
        assert len(result) == 0

    def test_origin_based_positioning(self, dask_client, sample_bsm_data_with_attackers):
        """
        Test that positions are calculated from origin (0,0), not current position.

        This validates that override_rand uses absolute positioning from origin,
        not relative offset from current position.
        """
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        min_dist = 100
        max_dist = 200
        attacker.add_attacks_positional_override_rand(
            min_dist=min_dist,
            max_dist=max_dist
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify all attackers are positioned relative to origin (0,0)
        # Not relative to their original positions (-106, 41)
        geod = Geodesic.WGS84
        origin_lat, origin_lon = 0.0, 0.0

        attacker_rows = result[result['isAttacker'] == 1]

        for idx, row in attacker_rows.iterrows():
            # Calculate distance from origin
            result_geod = geod.Inverse(
                origin_lat, origin_lon,
                row['y_pos'], row['x_pos']
            )
            dist_from_origin = result_geod['s12']

            # Should be within [min_dist, max_dist] from ORIGIN
            # Not from original position (-106, 41)
            assert min_dist - 1 <= dist_from_origin <= max_dist + 1, \
                f"Position should be {min_dist}-{max_dist}m from origin, got {dist_from_origin}m"

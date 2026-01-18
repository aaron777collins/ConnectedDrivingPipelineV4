"""
Test suite for DaskConnectedDrivingAttacker positional_override_const attack.

This test file validates the positional_override_const attack method that overrides
attacker positions to absolute positions from origin (0,0) or center point.

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


class TestPositionalOverrideConst:
    """Test suite for positional_override_const attack."""

    def test_basic_attack_execution(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack method executes without errors."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=100
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
        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify regular vehicles (isAttacker=0) are unchanged
        regular_original = original[original['isAttacker'] == 0].reset_index(drop=True)
        regular_result = result[result['isAttacker'] == 0].reset_index(drop=True)

        # Compare with proper dtype handling (pandas .apply() can change dtypes)
        assert regular_original['x_pos'].equals(regular_result['x_pos'])
        assert regular_original['y_pos'].equals(regular_result['y_pos'])
        assert regular_original['coreData_elevation'].equals(regular_result['coreData_elevation'])

    def test_all_attackers_same_position(self, dask_client, sample_bsm_data_with_attackers):
        """Test that all attackers are moved to the same absolute position."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_override_const(
            direction_angle=90,
            distance_meters=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Get all attacker positions
        attackers = result[result['isAttacker'] == 1]

        # All attackers should have the same x_pos (lon) and y_pos (lat)
        assert len(attackers['x_pos'].unique()) == 1, "All attackers should have same longitude"
        assert len(attackers['y_pos'].unique()) == 1, "All attackers should have same latitude"

    def test_different_angles_produce_different_positions(self, dask_client, sample_bsm_data_with_attackers):
        """Test that different angles produce different positions."""
        # Test north (0°)
        attacker_north = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_north"
        )
        attacker_north.add_attacks_positional_override_const(direction_angle=0, distance_meters=100)
        result_north = attacker_north.get_data().compute()
        attackers_north = result_north[result_north['isAttacker'] == 1]

        # Test east (90°)
        attacker_east = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_east"
        )
        attacker_east.add_attacks_positional_override_const(direction_angle=90, distance_meters=100)
        result_east = attacker_east.get_data().compute()
        attackers_east = result_east[result_east['isAttacker'] == 1]

        # Positions should be different
        # North: should move latitude (y_pos), longitude (x_pos) stays ~0
        # East: should move longitude (x_pos), latitude (y_pos) stays ~0
        assert not np.allclose(attackers_north['x_pos'].values, attackers_east['x_pos'].values, rtol=1e-6)
        assert not np.allclose(attackers_north['y_pos'].values, attackers_east['y_pos'].values, rtol=1e-6)

    def test_method_chaining(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the method supports method chaining."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Test method chaining
        result = attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=100
        ).get_data().compute()

        # Verify result
        assert len(result) == 13
        assert (result['isAttacker'] == 1).sum() == 8

    def test_preserves_other_columns(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack preserves other columns unchanged."""
        # Get original data
        original = sample_bsm_data_with_attackers.compute()

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify columns other than x_pos, y_pos are unchanged
        # Note: coreData_id dtype might change due to pandas .apply()
        assert all(result['isAttacker'] == original['isAttacker'])
        assert all(result['coreData_elevation'] == original['coreData_elevation'])

    def test_lazy_evaluation(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the method returns a Dask DataFrame (lazy evaluation)."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_attacker"
        )

        # Apply attack
        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=100
        )

        # Verify result is Dask DataFrame (lazy)
        result = attacker.get_data()
        assert isinstance(result, dd.DataFrame), "Result should be Dask DataFrame"

    def test_empty_dataframe(self, dask_client):
        """Test that the method handles empty DataFrames gracefully."""
        # Create empty DataFrame
        empty_df = pd.DataFrame({
            'coreData_id': [],
            'x_pos': [],
            'y_pos': [],
            'isAttacker': [],
            'coreData_elevation': []
        })
        ddf = dd.from_pandas(empty_df, npartitions=1)

        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=ddf,
            id="test_empty"
        )

        # Apply attack (should not crash)
        attacker.add_attacks_positional_override_const(
            direction_angle=45,
            distance_meters=100
        )

        # Get result
        result = attacker.get_data().compute()

        # Verify result is empty
        assert len(result) == 0

    def test_custom_distance(self, dask_client, sample_bsm_data_with_attackers):
        """Test that custom distance parameter works correctly."""
        # Test with 50m distance
        attacker_50m = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_50m"
        )
        attacker_50m.add_attacks_positional_override_const(
            direction_angle=0,  # Due north
            distance_meters=50
        )
        result_50m = attacker_50m.get_data().compute()
        attackers_50m = result_50m[result_50m['isAttacker'] == 1]

        # Test with 200m distance
        attacker_200m = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_200m"
        )
        attacker_200m.add_attacks_positional_override_const(
            direction_angle=0,  # Due north
            distance_meters=200
        )
        result_200m = attacker_200m.get_data().compute()
        attackers_200m = result_200m[result_200m['isAttacker'] == 1]

        # Different distances should produce different latitudes
        # (both at direction=0°, so longitude should be ~same, latitude different)
        assert not np.allclose(attackers_50m['y_pos'].values, attackers_200m['y_pos'].values, rtol=1e-6)

    def test_reproducibility(self, dask_client, sample_bsm_data_with_attackers):
        """Test that the attack produces consistent results across multiple runs."""
        # Run 1
        attacker1 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_run1"
        )
        attacker1.add_attacks_positional_override_const(direction_angle=45, distance_meters=100)
        result1 = attacker1.get_data().compute()
        attackers1 = result1[result1['isAttacker'] == 1].sort_index()

        # Run 2
        attacker2 = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_run2"
        )
        attacker2.add_attacks_positional_override_const(direction_angle=45, distance_meters=100)
        result2 = attacker2.get_data().compute()
        attackers2 = result2[result2['isAttacker'] == 1].sort_index()

        # Results should be identical
        assert np.allclose(attackers1['x_pos'].values, attackers2['x_pos'].values, rtol=1e-9)
        assert np.allclose(attackers1['y_pos'].values, attackers2['y_pos'].values, rtol=1e-9)

    def test_position_override_from_origin(self, dask_client, sample_bsm_data_with_attackers):
        """Test that positions are calculated from origin (0,0) for lat/lon coordinates."""
        # Create attacker instance
        attacker = DaskConnectedDrivingAttacker(
            data=sample_bsm_data_with_attackers,
            id="test_origin"
        )

        # Apply attack with 0° (north) and 1000m distance
        attacker.add_attacks_positional_override_const(
            direction_angle=0,  # Due north
            distance_meters=1000
        )

        # Get result
        result = attacker.get_data().compute()

        # Get all attacker positions
        attackers = result[result['isAttacker'] == 1]

        # All attackers should have same position
        # For lat/lon from origin (0,0), 1000m north should give:
        # - Longitude (x_pos) ~ 0.0
        # - Latitude (y_pos) ~ 0.009 (1000m ~ 0.009 degrees at equator)
        geod = Geodesic.WGS84
        expected_result = geod.Direct(0.0, 0.0, 0, 1000)  # From (0,0), north 0°, 1000m
        expected_lat = expected_result['lat2']
        expected_lon = expected_result['lon2']

        # Verify all attackers have the expected position
        assert np.allclose(attackers['x_pos'].values, expected_lon, rtol=1e-6)
        assert np.allclose(attackers['y_pos'].values, expected_lat, rtol=1e-6)

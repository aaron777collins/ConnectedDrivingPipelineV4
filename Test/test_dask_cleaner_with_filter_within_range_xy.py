"""
Tests for DaskCleanerWithFilterWithinRangeXY.

This test suite validates the Euclidean (XY) distance filtering functionality of
DaskCleanerWithFilterWithinRangeXY using pytest and DaskFixtures.

Test Coverage:
1. Basic filtering (points within/outside range from origin)
2. Distance calculation accuracy (Euclidean from origin 0,0)
3. Edge cases (empty DataFrames, all points in range, no points in range)
4. Schema preservation
5. Lazy evaluation behavior
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame
from unittest.mock import Mock

from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXY import DaskCleanerWithFilterWithinRangeXY
from Helpers.DaskUDFs.GeospatialFunctions import xy_distance
from Helpers.MathHelper import MathHelper


@pytest.mark.dask
class TestDaskCleanerWithFilterWithinRangeXY:
    """Test suite for DaskCleanerWithFilterWithinRangeXY"""

    def test_within_rangeXY_filters_correctly(self, dask_client, dask_df_comparer):
        """Test that within_rangeXY filters points correctly based on Euclidean distance from origin"""
        # Create test data with known distances from origin (0, 0)
        # Points at various Euclidean distances
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [0.0, 3.0, 4.0, 7.0, 10.0],
            'y_pos': [0.0, 4.0, 3.0, 7.0, 10.0],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        # Distances: 0, 5, 5, ~9.9, ~14.14
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        # Create cleaner instance (bypass DI for testing)
        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 6.0  # Should include points 1, 2, 3 (distances 0, 5, 5)
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        # Apply filter
        result = cleaner.within_rangeXY(df)
        result_computed = result.compute()

        # Verify filtering: Calculate expected distances
        distances = []
        for _, row in pd.DataFrame(data).iterrows():
            dist = xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0)
            distances.append(dist)

        # Count how many points should be within 6.0 distance
        expected_count = sum(1 for d in distances if d <= 6.0)

        assert len(result_computed) == expected_count, \
            f"Expected {expected_count} points within range, got {len(result_computed)}"

        # Verify all remaining points are within range
        for _, row in result_computed.iterrows():
            dist = xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0)
            assert dist <= 6.0, f"Point at distance {dist} should not be in result"

    def test_within_rangeXY_preserves_schema(self, dask_client):
        """Test that within_rangeXY preserves DataFrame schema (no distance column in output)"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [1.0, 2.0, 3.0],
            'y_pos': [1.0, 2.0, 3.0],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df)
        result_computed = result.compute()

        # Verify schema: distance column should NOT be in output
        assert 'distance' not in result_computed.columns, \
            "Distance column should be dropped from output"

        # Verify original columns are preserved
        expected_columns = ['id', 'x_pos', 'y_pos', 'speed']
        assert list(result_computed.columns) == expected_columns, \
            f"Expected columns {expected_columns}, got {list(result_computed.columns)}"

    def test_within_rangeXY_empty_dataframe(self, dask_client):
        """Test that within_rangeXY handles empty DataFrames correctly"""
        data = pd.DataFrame({
            'id': pd.Series([], dtype='int64'),
            'x_pos': pd.Series([], dtype='float64'),
            'y_pos': pd.Series([], dtype='float64'),
            'speed': pd.Series([], dtype='float64')
        })
        df = dd.from_pandas(data, npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df)
        result_computed = result.compute()

        assert len(result_computed) == 0, "Empty DataFrame should remain empty"
        assert list(result_computed.columns) == ['id', 'x_pos', 'y_pos', 'speed'], \
            "Schema should be preserved for empty DataFrame"

    def test_within_rangeXY_all_points_in_range(self, dask_client):
        """Test when all points are within range"""
        # All points very close to origin
        data = {
            'id': [1, 2, 3],
            'x_pos': [0.1, 0.2, 0.3],
            'y_pos': [0.1, 0.2, 0.3],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 1.0  # Large enough to include all points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df)
        result_computed = result.compute()

        assert len(result_computed) == 3, "All points should be within range"

    def test_within_rangeXY_no_points_in_range(self, dask_client):
        """Test when no points are within range"""
        # All points far from origin
        data = {
            'id': [1, 2, 3],
            'x_pos': [100.0, 101.0, 102.0],
            'y_pos': [100.0, 101.0, 102.0],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0  # Too small to reach any of these points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df)
        result_computed = result.compute()

        assert len(result_computed) == 0, "No points should be within range"

    def test_within_rangeXY_lazy_evaluation(self, dask_client):
        """Test that within_rangeXY maintains lazy evaluation"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [1.0, 2.0, 3.0],
            'y_pos': [1.0, 2.0, 3.0],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 5.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df)

        # Verify result is still a Dask DataFrame (not computed)
        assert isinstance(result, DataFrame), \
            "Result should be a Dask DataFrame (lazy evaluation)"

    def test_within_rangeXY_uses_euclidean_distance(self, dask_client):
        """Test that Euclidean distance is correctly calculated"""
        # Create a simple 3-4-5 right triangle
        data = {
            'id': [1, 2, 3],
            'x_pos': [3.0, 4.0, 5.0],
            'y_pos': [4.0, 3.0, 0.0],
            'speed': [50.0, 55.0, 60.0]
        }
        # Distances from origin: 5.0, 5.0, 5.0
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        # Calculate expected distances
        dist1 = xy_distance(3.0, 4.0, 0.0, 0.0)  # Should be 5.0
        dist2 = xy_distance(4.0, 3.0, 0.0, 0.0)  # Should be 5.0
        dist3 = xy_distance(5.0, 0.0, 0.0, 0.0)  # Should be 5.0

        assert abs(dist1 - 5.0) < 0.001, f"Expected distance 5.0, got {dist1}"
        assert abs(dist2 - 5.0) < 0.001, f"Expected distance 5.0, got {dist2}"
        assert abs(dist3 - 5.0) < 0.001, f"Expected distance 5.0, got {dist3}"

        # Test with max_dist just above 5.0
        cleaner.max_dist = 5.01
        result = cleaner.within_rangeXY(df).compute()
        assert len(result) == 3, "All three points should be within range"

        # Test with max_dist just below 5.0
        cleaner.max_dist = 4.99
        result = cleaner.within_rangeXY(df).compute()
        assert len(result) == 0, "No points should be within range"

    def test_within_rangeXY_preserves_partition_count(self, dask_client):
        """Test that filtering preserves partition structure"""
        data = {
            'id': list(range(100)),
            'x_pos': [i * 0.1 for i in range(100)],
            'y_pos': [i * 0.1 for i in range(100)],
            'speed': [50.0 + i for i in range(100)]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=4)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 50.0  # Large enough to include most points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df)

        # Verify partitions are maintained
        assert result.npartitions == 4, \
            f"Expected 4 partitions, got {result.npartitions}"

    def test_within_rangeXY_origin_centered(self, dask_client):
        """Test that filter is always centered at origin (0, 0)"""
        # Create points symmetrically distributed around origin
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [3.0, -3.0, 0.0, 4.0, -4.0],
            'y_pos': [4.0, -4.0, 5.0, 3.0, -3.0],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        # Distances: 5.0, 5.0, 5.0, 5.0, 5.0
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 5.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df).compute()

        # All points are exactly at distance 5.0, so all should be included
        assert len(result) == 5, "All points at distance 5.0 should be included (<=)"

        # Verify all points are correctly within range
        for _, row in result.iterrows():
            dist = xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0)
            assert dist <= 5.0, f"Point {row['id']} at distance {dist} should be within range"

    def test_within_rangeXY_negative_coordinates(self, dask_client):
        """Test filtering works correctly with negative coordinates"""
        # Test points in different quadrants
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, -1.0, -1.0, 1.0],   # All four quadrants
            'y_pos': [1.0, 1.0, -1.0, -1.0],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        # All distances: sqrt(2) ≈ 1.414
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 1.5  # Just above sqrt(2)
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df).compute()

        # All four points should be included
        assert len(result) == 4, "All points in all quadrants should be within range"

        # Test with smaller distance
        cleaner.max_dist = 1.4  # Just below sqrt(2)
        result = cleaner.within_rangeXY(df).compute()
        assert len(result) == 0, "No points should be within range"

    def test_within_rangeXY_at_origin(self, dask_client):
        """Test point exactly at origin is included"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [0.0, 5.0, 10.0],
            'y_pos': [0.0, 5.0, 10.0],
            'speed': [50.0, 55.0, 60.0]
        }
        # Distances: 0.0, ~7.07, ~14.14
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXY.__new__(DaskCleanerWithFilterWithinRangeXY)
        cleaner.logger = Mock()
        cleaner.max_dist = 0.1  # Very small radius
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_rangeXY(df).compute()

        # Only the origin point should be included
        assert len(result) == 1, "Only point at origin should be within range"
        assert result.iloc[0]['id'] == 1, "Point 1 (at origin) should be included"
        assert result.iloc[0]['x_pos'] == 0.0
        assert result.iloc[0]['y_pos'] == 0.0

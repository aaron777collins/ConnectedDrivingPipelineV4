"""
Tests for DaskCleanerWithFilterWithinRange.

This test suite validates the geodesic distance filtering functionality of
DaskCleanerWithFilterWithinRange using pytest and DaskFixtures.

Test Coverage:
1. Basic filtering (points within/outside range)
2. Distance calculation accuracy (geodesic vs Euclidean)
3. Edge cases (empty DataFrames, all points in range, no points in range)
4. Schema preservation
5. Lazy evaluation behavior
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame
from unittest.mock import Mock

from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRange import DaskCleanerWithFilterWithinRange
from Helpers.DaskUDFs.GeospatialFunctions import geodesic_distance
from Helpers.MathHelper import MathHelper


@pytest.mark.dask
class TestDaskCleanerWithFilterWithinRange:
    """Test suite for DaskCleanerWithFilterWithinRange"""

    def test_within_range_filters_correctly(self, dask_client, dask_df_comparer):
        """Test that within_range filters points correctly based on geodesic distance"""
        # Create test data with known distances from origin (41.25, -105.93)
        # Points at various distances
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [-105.93, -105.94, -105.95, -106.00, -106.10],  # longitude
            'y_pos': [41.25, 41.26, 41.27, 41.30, 41.40],           # latitude
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        # Create cleaner instance (bypass DI for testing)
        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 5000.0  # 5km radius
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        # Apply filter
        result = cleaner.within_range(df)
        result_computed = result.compute()

        # Verify filtering: Calculate expected distances
        distances = []
        for _, row in pd.DataFrame(data).iterrows():
            dist = geodesic_distance(row['y_pos'], row['x_pos'], 41.25, -105.93)
            distances.append(dist)

        # Count how many points should be within 5km
        expected_count = sum(1 for d in distances if d <= 5000.0)

        assert len(result_computed) == expected_count, \
            f"Expected {expected_count} points within range, got {len(result_computed)}"

        # Verify all remaining points are within range
        for _, row in result_computed.iterrows():
            dist = geodesic_distance(row['y_pos'], row['x_pos'], 41.25, -105.93)
            assert dist <= 5000.0, f"Point at distance {dist}m should not be in result"

    def test_within_range_preserves_schema(self, dask_client):
        """Test that within_range preserves DataFrame schema (no distance column in output)"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [-105.93, -105.94, -105.95],
            'y_pos': [41.25, 41.26, 41.27],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 10000.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_range(df)
        result_computed = result.compute()

        # Verify schema: distance column should NOT be in output
        assert 'distance' not in result_computed.columns, \
            "Distance column should be dropped from output"

        # Verify original columns are preserved
        expected_columns = ['id', 'x_pos', 'y_pos', 'speed']
        assert list(result_computed.columns) == expected_columns, \
            f"Expected columns {expected_columns}, got {list(result_computed.columns)}"

    def test_within_range_empty_dataframe(self, dask_client):
        """Test that within_range handles empty DataFrames correctly"""
        data = pd.DataFrame({
            'id': pd.Series([], dtype='int64'),
            'x_pos': pd.Series([], dtype='float64'),
            'y_pos': pd.Series([], dtype='float64'),
            'speed': pd.Series([], dtype='float64')
        })
        df = dd.from_pandas(data, npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 1000.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_range(df)
        result_computed = result.compute()

        assert len(result_computed) == 0, "Empty DataFrame should remain empty"
        assert list(result_computed.columns) == ['id', 'x_pos', 'y_pos', 'speed'], \
            "Schema should be preserved for empty DataFrame"

    def test_within_range_all_points_in_range(self, dask_client):
        """Test when all points are within range"""
        # All points very close to origin
        data = {
            'id': [1, 2, 3],
            'x_pos': [-105.93, -105.9301, -105.9299],
            'y_pos': [41.25, 41.2501, 41.2499],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 1000.0  # 1km should include all these close points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_range(df)
        result_computed = result.compute()

        assert len(result_computed) == 3, "All points should be within range"

    def test_within_range_no_points_in_range(self, dask_client):
        """Test when no points are within range"""
        # All points far from origin
        data = {
            'id': [1, 2, 3],
            'x_pos': [-100.0, -100.1, -100.2],  # Far from origin
            'y_pos': [40.0, 40.1, 40.2],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 1000.0  # 1km - too small to reach these points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_range(df)
        result_computed = result.compute()

        assert len(result_computed) == 0, "No points should be within range"

    def test_within_range_lazy_evaluation(self, dask_client):
        """Test that within_range maintains lazy evaluation"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [-105.93, -105.94, -105.95],
            'y_pos': [41.25, 41.26, 41.27],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 5000.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_range(df)

        # Verify result is still a Dask DataFrame (not computed)
        assert isinstance(result, DataFrame), \
            "Result should be a Dask DataFrame (lazy evaluation)"

    def test_within_range_geodesic_vs_euclidean(self, dask_client):
        """Test that geodesic distance is used (not Euclidean)"""
        # Use a point far enough that geodesic and Euclidean differ significantly
        data = {
            'id': [1],
            'x_pos': [-105.0],  # ~0.93 degrees from origin
            'y_pos': [42.0],    # ~0.75 degrees from origin
            'speed': [50.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        # Calculate both distances
        geodesic_dist = geodesic_distance(42.0, -105.0, 41.25, -105.93)
        euclidean_dist = MathHelper.dist_between_two_pointsXY(-105.0, 42.0, -105.93, 41.25)

        # Verify they differ (geodesic accounts for Earth's curvature)
        assert abs(geodesic_dist - euclidean_dist) > 100, \
            "Geodesic and Euclidean distances should differ for this test case"

        # Test with max_dist between the two distances
        # This should use geodesic distance (the correct one)
        cleaner.max_dist = geodesic_dist + 1000  # Just above geodesic distance

        result = cleaner.within_range(df)
        result_computed = result.compute()

        # Point should be included if using geodesic distance
        assert len(result_computed) == 1, \
            "Point should be within range when using geodesic distance"

    def test_within_range_preserves_partition_count(self, dask_client):
        """Test that filtering preserves partition structure"""
        data = {
            'id': list(range(100)),
            'x_pos': [-105.93 + i * 0.001 for i in range(100)],
            'y_pos': [41.25 + i * 0.001 for i in range(100)],
            'speed': [50.0 + i for i in range(100)]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=4)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        cleaner.max_dist = 50000.0  # Large enough to include most points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        result = cleaner.within_range(df)

        # Verify partitions are maintained
        assert result.npartitions == 4, \
            f"Expected 4 partitions, got {result.npartitions}"

    def test_within_range_different_center_points(self, dask_client):
        """Test filtering with different center points"""
        # Create points with significant distance between them
        data = {
            'id': [1, 2, 3],
            'x_pos': [-105.93, -105.0, -107.0],  # spread across ~2 degrees longitude
            'y_pos': [41.25, 42.0, 40.5],        # spread across ~1.5 degrees latitude
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRange.__new__(DaskCleanerWithFilterWithinRange)
        cleaner.logger = Mock()
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.max_dist = 100.0  # 100 meters - very small radius

        # Test with center at first point - should only include point 1
        cleaner.x_pos = -105.93
        cleaner.y_pos = 41.25
        result1 = cleaner.within_range(df).compute()

        # Test with center at second point - should only include point 2
        cleaner.x_pos = -105.0
        cleaner.y_pos = 42.0
        result2 = cleaner.within_range(df).compute()

        # Results should differ - each should only contain their respective point
        assert len(result1) == 1, "First center should only capture point 1"
        assert len(result2) == 1, "Second center should only capture point 2"
        assert result1.iloc[0]['id'] == 1, "First result should contain point 1"
        assert result2.iloc[0]['id'] == 2, "Second result should contain point 2"

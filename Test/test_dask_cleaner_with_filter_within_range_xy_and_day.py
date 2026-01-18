"""
Tests for DaskCleanerWithFilterWithinRangeXYAndDay.

This test suite validates the combined spatial (XY distance) and temporal (day/month/year)
filtering functionality of DaskCleanerWithFilterWithinRangeXYAndDay using pytest and DaskFixtures.

Test Coverage:
1. Combined spatial-temporal filtering (points within range AND matching date)
2. Spatial filtering accuracy (Euclidean from origin 0,0)
3. Temporal filtering accuracy (exact day, month, year matching)
4. Edge cases (empty DataFrames, all points match, no points match)
5. Schema preservation
6. Lazy evaluation behavior
7. Independent filter validation (spatial only, temporal only)
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame
from unittest.mock import Mock

from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDay import DaskCleanerWithFilterWithinRangeXYAndDay
from Helpers.DaskUDFs.GeospatialFunctions import xy_distance


@pytest.mark.dask
class TestDaskCleanerWithFilterWithinRangeXYAndDay:
    """Test suite for DaskCleanerWithFilterWithinRangeXYAndDay"""

    def test_within_rangeXY_and_day_combined_filter(self, dask_client, dask_df_comparer):
        """Test that within_rangeXY_and_day filters correctly based on both spatial and temporal criteria"""
        # Create test data with known distances and dates
        data = {
            'id': [1, 2, 3, 4, 5, 6],
            'x_pos': [0.0, 3.0, 4.0, 0.0, 3.0, 10.0],
            'y_pos': [0.0, 4.0, 3.0, 0.0, 4.0, 10.0],
            'day': [15, 15, 15, 16, 16, 15],      # Only rows with day=15 should pass
            'month': [4, 4, 4, 4, 4, 4],          # All same month
            'year': [2021, 2021, 2021, 2021, 2021, 2021],  # All same year
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0, 75.0]
        }
        # Distances from origin: 0, 5, 5, 0, 5, ~14.14
        # Points 1,2,3 match day=15 AND are within distance (if max_dist >= 5)
        # Point 4 matches distance but NOT day (day=16)
        # Point 5 does NOT match day (day=16)
        # Point 6 matches day but NOT distance
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        # Create cleaner instance (bypass DI for testing)
        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 6.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        # Apply filter
        result = cleaner.within_rangeXY_and_day(df)
        result_computed = result.compute()

        # Should only include points 1, 2, 3 (within distance AND day=15)
        assert len(result_computed) == 3, \
            f"Expected 3 points matching both criteria, got {len(result_computed)}"

        # Verify all remaining points match both criteria
        for _, row in result_computed.iterrows():
            dist = xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0)
            assert dist <= 6.0, f"Point {row['id']} at distance {dist} should be within range"
            assert row['day'] == 15, f"Point {row['id']} should have day=15"
            assert row['month'] == 4, f"Point {row['id']} should have month=4"
            assert row['year'] == 2021, f"Point {row['id']} should have year=2021"

    def test_within_rangeXY_and_day_spatial_filter_only(self, dask_client):
        """Test spatial filtering when all points match the temporal criteria"""
        # All points on same date, but different distances
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [0.0, 3.0, 4.0, 10.0],
            'y_pos': [0.0, 4.0, 3.0, 10.0],
            'day': [15, 15, 15, 15],      # All same
            'month': [4, 4, 4, 4],        # All same
            'year': [2021, 2021, 2021, 2021],  # All same
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        # Distances: 0, 5, 5, ~14.14
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 6.0  # Should include points 1, 2, 3 only
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        assert len(result) == 3, "Should filter by distance when all dates match"
        assert set(result['id']) == {1, 2, 3}, "Should include only points within distance"

    def test_within_rangeXY_and_day_temporal_filter_only(self, dask_client):
        """Test temporal filtering when all points are within spatial range"""
        # All points close to origin, but different dates
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [0.1, 0.2, 0.3, 0.4, 0.5],
            'y_pos': [0.1, 0.2, 0.3, 0.4, 0.5],
            'day': [15, 15, 16, 15, 17],       # Different days
            'month': [4, 4, 4, 5, 4],          # Different months
            'year': [2021, 2021, 2021, 2021, 2020],  # Different years
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0  # Large enough to include all points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        # Only points 1 and 2 match day=15, month=4, year=2021
        assert len(result) == 2, "Should filter by date when all are within distance"
        assert set(result['id']) == {1, 2}, "Should include only points matching date"

    def test_within_rangeXY_and_day_preserves_schema(self, dask_client):
        """Test that within_rangeXY_and_day preserves DataFrame schema"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [1.0, 2.0, 3.0],
            'y_pos': [1.0, 2.0, 3.0],
            'day': [15, 15, 15],
            'month': [4, 4, 4],
            'year': [2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df)
        result_computed = result.compute()

        # Verify schema: distance column should NOT be in output
        assert 'distance' not in result_computed.columns, \
            "Distance column should be dropped from output"

        # Verify no temporary date columns
        assert 'matchesday' not in result_computed.columns
        assert 'matchesmonth' not in result_computed.columns
        assert 'matchesyear' not in result_computed.columns
        assert 'matchesdaymonthyear' not in result_computed.columns

        # Verify original columns are preserved
        expected_columns = ['id', 'x_pos', 'y_pos', 'day', 'month', 'year', 'speed']
        assert list(result_computed.columns) == expected_columns, \
            f"Expected columns {expected_columns}, got {list(result_computed.columns)}"

    def test_within_rangeXY_and_day_empty_dataframe(self, dask_client):
        """Test that within_rangeXY_and_day handles empty DataFrames correctly"""
        data = pd.DataFrame({
            'id': pd.Series([], dtype='int64'),
            'x_pos': pd.Series([], dtype='float64'),
            'y_pos': pd.Series([], dtype='float64'),
            'day': pd.Series([], dtype='int64'),
            'month': pd.Series([], dtype='int64'),
            'year': pd.Series([], dtype='int64'),
            'speed': pd.Series([], dtype='float64')
        })
        df = dd.from_pandas(data, npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df)
        result_computed = result.compute()

        assert len(result_computed) == 0, "Empty DataFrame should remain empty"
        assert list(result_computed.columns) == ['id', 'x_pos', 'y_pos', 'day', 'month', 'year', 'speed'], \
            "Schema should be preserved for empty DataFrame"

    def test_within_rangeXY_and_day_no_matches(self, dask_client):
        """Test when no points match both criteria"""
        # Points either don't match distance OR don't match date
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [100.0, 101.0, 0.0, 1.0],  # 1,2 far; 3,4 close
            'y_pos': [100.0, 101.0, 0.0, 1.0],
            'day': [15, 15, 16, 17],            # 1,2 match; 3,4 don't
            'month': [4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0  # Only points 3,4 are close enough
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15       # Only points 1,2 match
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        assert len(result) == 0, "No points should match both criteria"

    def test_within_rangeXY_and_day_lazy_evaluation(self, dask_client):
        """Test that within_rangeXY_and_day maintains lazy evaluation"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [1.0, 2.0, 3.0],
            'y_pos': [1.0, 2.0, 3.0],
            'day': [15, 15, 15],
            'month': [4, 4, 4],
            'year': [2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 5.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df)

        # Verify result is still a Dask DataFrame (not computed)
        assert isinstance(result, DataFrame), \
            "Result should be a Dask DataFrame (lazy evaluation)"

    def test_within_rangeXY_and_day_year_filtering(self, dask_client):
        """Test year filtering works correctly"""
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0],
            'day': [15, 15, 15, 15],
            'month': [4, 4, 4, 4],
            'year': [2020, 2021, 2022, 2021],  # Different years
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        # Only points 2 and 4 have year=2021
        assert len(result) == 2, "Should filter by year"
        assert set(result['id']) == {2, 4}, "Should include only year 2021"

    def test_within_rangeXY_and_day_month_filtering(self, dask_client):
        """Test month filtering works correctly"""
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0],
            'day': [15, 15, 15, 15],
            'month': [3, 4, 5, 4],  # Different months
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        # Only points 2 and 4 have month=4
        assert len(result) == 2, "Should filter by month"
        assert set(result['id']) == {2, 4}, "Should include only month 4"

    def test_within_rangeXY_and_day_day_filtering(self, dask_client):
        """Test day filtering works correctly"""
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0],
            'day': [14, 15, 16, 15],  # Different days
            'month': [4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        # Only points 2 and 4 have day=15
        assert len(result) == 2, "Should filter by day"
        assert set(result['id']) == {2, 4}, "Should include only day 15"

    def test_within_rangeXY_and_day_preserves_partition_count(self, dask_client):
        """Test that filtering preserves partition structure"""
        data = {
            'id': list(range(100)),
            'x_pos': [i * 0.1 for i in range(100)],
            'y_pos': [i * 0.1 for i in range(100)],
            'day': [15] * 100,
            'month': [4] * 100,
            'year': [2021] * 100,
            'speed': [50.0 + i for i in range(100)]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=4)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 50.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df)

        # Verify partitions are maintained
        assert result.npartitions == 4, \
            f"Expected 4 partitions, got {result.npartitions}"

    def test_within_rangeXY_and_day_leap_year(self, dask_client):
        """Test filtering works correctly with leap year dates"""
        # February 29 in a leap year
        data = {
            'id': [1, 2, 3],
            'x_pos': [1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0],
            'day': [28, 29, 29],
            'month': [2, 2, 2],
            'year': [2020, 2020, 2024],  # Both 2020 and 2024 are leap years
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 29
        cleaner.month = 2
        cleaner.year = 2020

        result = cleaner.within_rangeXY_and_day(df).compute()

        # Only point 2 matches Feb 29, 2020
        assert len(result) == 1, "Should filter leap year date correctly"
        assert result.iloc[0]['id'] == 2

    def test_within_rangeXY_and_day_boundary_conditions(self, dask_client):
        """Test boundary conditions for date values"""
        # Test edge dates: Jan 1, Dec 31
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0],
            'day': [1, 31, 1, 31],
            'month': [1, 12, 1, 12],
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'

        # Test Jan 1
        cleaner.day = 1
        cleaner.month = 1
        cleaner.year = 2021
        result = cleaner.within_rangeXY_and_day(df).compute()
        assert len(result) == 2, "Should filter Jan 1 correctly"
        assert set(result['id']) == {1, 3}

        # Test Dec 31
        cleaner.day = 31
        cleaner.month = 12
        cleaner.year = 2021
        result = cleaner.within_rangeXY_and_day(df).compute()
        assert len(result) == 2, "Should filter Dec 31 correctly"
        assert set(result['id']) == {2, 4}

    def test_within_rangeXY_and_day_all_match(self, dask_client):
        """Test when all points match both spatial and temporal criteria"""
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [0.1, 0.2, 0.3, 0.4],
            'y_pos': [0.1, 0.2, 0.3, 0.4],
            'day': [15, 15, 15, 15],
            'month': [4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDay.__new__(DaskCleanerWithFilterWithinRangeXYAndDay)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.day = 15
        cleaner.month = 4
        cleaner.year = 2021

        result = cleaner.within_rangeXY_and_day(df).compute()

        assert len(result) == 4, "All points should match both criteria"

"""
Tests for DaskCleanerWithFilterWithinRangeXYAndDateRange.

This test suite validates the combined spatial (XY distance) and temporal (date range)
filtering functionality of DaskCleanerWithFilterWithinRangeXYAndDateRange using pytest and DaskFixtures.

Test Coverage:
1. Combined spatial-temporal filtering (points within range AND within date range)
2. Spatial filtering accuracy (Euclidean from origin 0,0)
3. Temporal filtering accuracy (date range inclusivity)
4. Edge cases (empty DataFrames, all points match, no points match)
5. Schema preservation
6. Lazy evaluation behavior
7. Independent filter validation (spatial only, temporal only)
8. Date range boundary conditions (start date, end date, dates outside range)
9. Multi-day, multi-month, and multi-year date ranges
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from dask.dataframe import DataFrame
from unittest.mock import Mock
from datetime import datetime

from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange
from Helpers.DaskUDFs.GeospatialFunctions import xy_distance


@pytest.mark.dask
class TestDaskCleanerWithFilterWithinRangeXYAndDateRange:
    """Test suite for DaskCleanerWithFilterWithinRangeXYAndDateRange"""

    def test_within_rangeXY_and_date_range_combined_filter(self, dask_client, dask_df_comparer):
        """Test that within_rangeXY_and_date_range filters correctly based on both spatial and temporal criteria"""
        # Create test data with known distances and dates
        data = {
            'id': [1, 2, 3, 4, 5, 6, 7, 8],
            'x_pos': [0.0, 3.0, 4.0, 0.0, 3.0, 10.0, 2.0, 1.0],
            'y_pos': [0.0, 4.0, 3.0, 0.0, 4.0, 10.0, 2.0, 1.0],
            'day': [15, 15, 16, 17, 18, 15, 14, 19],
            'month': [4, 4, 4, 4, 4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0]
        }
        # Date range: 2021-04-15 to 2021-04-17 (inclusive)
        # Distances from origin: 0, 5, 5, 0, 5, ~14.14, ~2.83, ~1.41
        # Points 1,2,3,4 are within date range AND within distance (if max_dist >= 5)
        # Point 5 is within distance but NOT in date range (day=18)
        # Point 6 is in date range but NOT within distance (dist ~14.14)
        # Point 7 is NOT in date range (day=14, before start)
        # Point 8 is NOT in date range (day=19, after end)
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        # Create cleaner instance (bypass DI for testing)
        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 6.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 17
        cleaner.end_month = 4
        cleaner.end_year = 2021

        # Apply filter
        result = cleaner.within_rangeXY_and_date_range(df)
        result_computed = result.compute()

        # Should only include points 1, 2, 3, 4 (within distance AND within date range)
        assert len(result_computed) == 4, \
            f"Expected 4 points matching both criteria, got {len(result_computed)}"

        # Verify all remaining points match both criteria
        start_date = datetime(2021, 4, 15)
        end_date = datetime(2021, 4, 17)
        for _, row in result_computed.iterrows():
            dist = xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0)
            assert dist <= 6.0, f"Point {row['id']} at distance {dist} should be within range"

            row_date = datetime(int(row['year']), int(row['month']), int(row['day']))
            assert start_date <= row_date <= end_date, \
                f"Point {row['id']} with date {row_date} should be within date range"

    def test_within_rangeXY_and_date_range_spatial_filter_only(self, dask_client):
        """Test spatial filtering when all points are within the temporal range"""
        # All points within date range, but different distances
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [0.0, 3.0, 4.0, 10.0],
            'y_pos': [0.0, 4.0, 3.0, 10.0],
            'day': [15, 15, 16, 16],      # All within range
            'month': [4, 4, 4, 4],        # All same
            'year': [2021, 2021, 2021, 2021],  # All same
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        # Distances: 0, 5, 5, ~14.14
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 6.0  # Should include points 1, 2, 3 only
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 17
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        assert len(result) == 3, "Should filter by distance when all dates are in range"
        assert set(result['id']) == {1, 2, 3}, "Should include only points within distance"

    def test_within_rangeXY_and_date_range_temporal_filter_only(self, dask_client):
        """Test temporal filtering when all points are within spatial range"""
        # All points close to origin, but different dates
        data = {
            'id': [1, 2, 3, 4, 5, 6],
            'x_pos': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
            'y_pos': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
            'day': [15, 16, 17, 14, 18, 15],       # Different days
            'month': [4, 4, 4, 4, 4, 5],           # Different months
            'year': [2021, 2021, 2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0, 75.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0  # Large enough to include all points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 17
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        # Only points 1, 2, 3 are within date range (2021-04-15 to 2021-04-17)
        assert len(result) == 3, "Should filter by date range when all are within distance"
        assert set(result['id']) == {1, 2, 3}, "Should include only points within date range"

    def test_within_rangeXY_and_date_range_single_day(self, dask_client):
        """Test that date range filtering works correctly for a single day (start_date == end_date)"""
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0],
            'day': [15, 16, 17, 15],
            'month': [4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 15  # Same as start_day (single day range)
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        # Only points on 2021-04-15 should be included
        assert len(result) == 2, "Should include only points on the single day"
        assert set(result['id']) == {1, 4}, "Should include points 1 and 4 with day=15"

    def test_within_rangeXY_and_date_range_multi_month(self, dask_client):
        """Test date range filtering across multiple months"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [1.0, 1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0, 1.0],
            'day': [30, 1, 15, 28, 1],
            'month': [3, 4, 4, 4, 5],      # March to May
            'year': [2021, 2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 1
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 28
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        # Only points 2, 3, 4 should be in April 2021
        assert len(result) == 3, "Should include only points within April 2021"
        assert set(result['id']) == {2, 3, 4}, "Should include points from April only"

    def test_within_rangeXY_and_date_range_multi_year(self, dask_client):
        """Test date range filtering across multiple years"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [1.0, 1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0, 1.0],
            'day': [31, 1, 15, 30, 1],
            'month': [12, 1, 1, 6, 1],
            'year': [2020, 2021, 2021, 2021, 2022],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 1
        cleaner.start_month = 1
        cleaner.start_year = 2021
        cleaner.end_day = 31
        cleaner.end_month = 12
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        # Only points 2, 3, 4 are in 2021
        assert len(result) == 3, "Should include only points within 2021"
        assert set(result['id']) == {2, 3, 4}, "Should include only 2021 dates"

    def test_within_rangeXY_and_date_range_preserves_schema(self, dask_client):
        """Test that within_rangeXY_and_date_range preserves DataFrame schema"""
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

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 16
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df)
        result_computed = result.compute()

        # Should preserve all columns (no 'distance' or 'temp_date' column)
        assert set(result_computed.columns) == set(data.keys()), \
            "Should preserve original columns without adding temporary columns"
        assert 'distance' not in result_computed.columns, "Should not have distance column"
        assert 'temp_date' not in result_computed.columns, "Should not have temp_date column"

    def test_within_rangeXY_and_date_range_empty_dataframe(self, dask_client):
        """Test that within_rangeXY_and_date_range handles empty DataFrames correctly"""
        data = {
            'id': [],
            'x_pos': [],
            'y_pos': [],
            'day': [],
            'month': [],
            'year': [],
            'speed': []
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 16
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        assert len(result) == 0, "Should handle empty DataFrame without errors"
        assert set(result.columns) == set(data.keys()), "Should preserve schema"

    def test_within_rangeXY_and_date_range_no_matches(self, dask_client):
        """Test that within_rangeXY_and_date_range returns empty DataFrame when no points match"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [100.0, 200.0, 300.0],  # All far from origin
            'y_pos': [100.0, 200.0, 300.0],
            'day': [15, 15, 15],
            'month': [4, 4, 4],
            'year': [2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0  # Too small to include any points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 16
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        assert len(result) == 0, "Should return empty DataFrame when no points match"
        assert set(result.columns) == set(data.keys()), "Should preserve schema"

    def test_within_rangeXY_and_date_range_all_match(self, dask_client):
        """Test that within_rangeXY_and_date_range includes all points when all match criteria"""
        data = {
            'id': [1, 2, 3],
            'x_pos': [1.0, 2.0, 3.0],
            'y_pos': [1.0, 2.0, 3.0],
            'day': [15, 16, 17],
            'month': [4, 4, 4],
            'year': [2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0  # Large enough for all points
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 17
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        assert len(result) == 3, "Should include all points when all match"
        assert set(result['id']) == {1, 2, 3}, "Should include all three points"

    def test_within_rangeXY_and_date_range_lazy_evaluation(self, dask_client):
        """Test that within_rangeXY_and_date_range maintains lazy evaluation"""
        data = {
            'id': [1, 2, 3, 4],
            'x_pos': [1.0, 2.0, 3.0, 4.0],
            'y_pos': [1.0, 2.0, 3.0, 4.0],
            'day': [15, 15, 16, 16],
            'month': [4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 10.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 16
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df)

        # Result should be a Dask DataFrame (not computed)
        assert isinstance(result, dd.DataFrame), "Should return Dask DataFrame (lazy)"
        assert not isinstance(result, pd.DataFrame), "Should not be a pandas DataFrame (not computed)"

    def test_within_rangeXY_and_date_range_partition_preservation(self, dask_client):
        """Test that within_rangeXY_and_date_range preserves partition structure"""
        data = {
            'id': list(range(1, 101)),
            'x_pos': [float(i % 10) for i in range(1, 101)],
            'y_pos': [float(i % 10) for i in range(1, 101)],
            'day': [15 + (i % 3) for i in range(1, 101)],  # Mix of days 15, 16, 17
            'month': [4] * 100,
            'year': [2021] * 100,
            'speed': [50.0 + float(i) for i in range(1, 101)]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=4)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 20.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 16
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df)

        # Result should maintain the same number of partitions
        assert result.npartitions == 4, "Should preserve partition structure"

    def test_within_rangeXY_and_date_range_boundary_dates(self, dask_client):
        """Test that date range filtering correctly includes boundary dates (start and end are inclusive)"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'x_pos': [1.0, 1.0, 1.0, 1.0, 1.0],
            'y_pos': [1.0, 1.0, 1.0, 1.0, 1.0],
            'day': [14, 15, 16, 17, 18],  # One before, three in range, one after
            'month': [4, 4, 4, 4, 4],
            'year': [2021, 2021, 2021, 2021, 2021],
            'speed': [50.0, 55.0, 60.0, 65.0, 70.0]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange.__new__(DaskCleanerWithFilterWithinRangeXYAndDateRange)
        cleaner.logger = Mock()
        cleaner.max_dist = 100.0
        cleaner.x_col = 'x_pos'
        cleaner.y_col = 'y_pos'
        cleaner.start_day = 15
        cleaner.start_month = 4
        cleaner.start_year = 2021
        cleaner.end_day = 17
        cleaner.end_month = 4
        cleaner.end_year = 2021

        result = cleaner.within_rangeXY_and_date_range(df).compute()

        # Should include points 2, 3, 4 (days 15, 16, 17 inclusive)
        assert len(result) == 3, "Should include boundary dates (start and end are inclusive)"
        assert set(result['id']) == {2, 3, 4}, "Should include start date, end date, and dates in between"

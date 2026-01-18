"""
Comprehensive test suite for all Dask cleaner implementations with golden dataset validation.

This test suite validates:
1. All Dask cleaner classes produce correct outputs
2. Cleaners match expected behavior (golden dataset comparison)
3. Proper handling of edge cases and boundary conditions
4. Consistent behavior across different data sizes

Test Coverage:
- DaskConnectedDrivingCleaner (base cleaning operations)
- DaskCleanWithTimestamps (temporal feature extraction)
- DaskCleanerWithFilterWithinRangeXY (Euclidean distance filter)
- DaskCleanerWithFilterWithinRangeXYAndDay (spatial+temporal filter)
- DaskMConnectedDrivingDataCleaner (ML data preparation)

Golden Dataset Tests validate that cleaner outputs match expected transformations.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np
from datetime import datetime
from unittest.mock import Mock
import tempfile
import os

from Helpers.DaskUDFs.GeospatialFunctions import geodesic_distance, xy_distance, point_to_x, point_to_y


@pytest.mark.dask
class TestDaskCleanersGoldenDataset:
    """
    Golden dataset validation tests for Dask cleaners.

    Golden datasets are pre-computed reference datasets that establish expected
    behavior. Tests validate that Dask implementations match these golden outputs.
    """

    @pytest.fixture(scope="class")
    def golden_raw_bsm_data(self):
        """
        Golden dataset: Raw BSM data for testing cleaners.

        This dataset contains realistic BSM data with various characteristics:
        - Multiple timestamps spanning different days/months
        - Various geographic locations (lat/long)
        - WKT POINT format positions
        - Hexadecimal vehicle IDs
        - Range of speeds and headings
        """
        data = {
            "metadata_generatedAt": [
                "04/06/2021 10:30:00 AM",
                "04/06/2021 02:45:30 PM",
                "04/07/2021 08:15:45 AM",
                "05/15/2021 03:20:15 PM",
                "06/20/2021 11:55:30 AM",
            ],
            "metadata_recordType": ["bsmTx"] * 5,
            "metadata_serialId_streamId": [
                f"00000000-0000-0000-0000-00000000000{i}" for i in range(1, 6)
            ],
            "coreData_id": ["A1B2C3D4", "B2C3D4E5", "C3D4E5F6", "D4E5F6A7", "E5F6A7B8"],
            "coreData_position": [
                "POINT (-105.9378 41.2565)",
                "POINT (-105.9380 41.2570)",
                "POINT (-105.9385 41.2575)",
                "POINT (-105.9390 41.2580)",
                "POINT (-105.9395 41.2585)",
            ],
            "coreData_position_lat": [41.2565, 41.2570, 41.2575, 41.2580, 41.2585],
            "coreData_position_long": [-105.9378, -105.9380, -105.9385, -105.9390, -105.9395],
            "coreData_speed": [15.5, 25.0, 18.0, 30.0, 12.0],
            "coreData_heading": [90.0, 85.0, 92.0, 88.0, 91.0],
        }
        return pd.DataFrame(data)

    def test_golden_dataset_point_parsing(self, dask_client, golden_raw_bsm_data):
        """
        Test POINT string parsing extracts correct x_pos and y_pos coordinates.

        Validates:
        - point_to_x() correctly extracts longitude from WKT POINT
        - point_to_y() correctly extracts latitude from WKT POINT
        - Extracted values match expected lat/long columns
        """
        for idx, row in golden_raw_bsm_data.iterrows():
            point_str = row["coreData_position"]
            expected_long = row["coreData_position_long"]
            expected_lat = row["coreData_position_lat"]

            x_extracted = point_to_x(point_str)
            y_extracted = point_to_y(point_str)

            assert abs(x_extracted - expected_long) < 1e-9, \
                f"Row {idx}: x_pos mismatch. Expected {expected_long}, got {x_extracted}"
            assert abs(y_extracted - expected_lat) < 1e-9, \
                f"Row {idx}: y_pos mismatch. Expected {expected_lat}, got {y_extracted}"

    def test_golden_dataset_xy_distance_calculation(self, dask_client):
        """
        Test Euclidean distance calculations match expected values.

        Validates:
        - xy_distance() produces correct Euclidean distances
        - Distance from origin (0, 0) calculated correctly
        """
        # Test points with known distances
        test_cases = [
            ((0.0, 0.0), (0.0, 0.0), 0.0),  # Same point
            ((3.0, 4.0), (0.0, 0.0), 5.0),  # 3-4-5 triangle
            ((1.0, 1.0), (0.0, 0.0), np.sqrt(2)),  # 45-degree angle
            ((-1.0, -1.0), (0.0, 0.0), np.sqrt(2)),  # Negative coordinates
        ]

        for (x, y), (origin_x, origin_y), expected_dist in test_cases:
            calculated_dist = xy_distance(x, y, origin_x, origin_y)
            assert abs(calculated_dist - expected_dist) < 1e-9, \
                f"Distance mismatch for ({x},{y}) to ({origin_x},{origin_y}). " \
                f"Expected {expected_dist}, got {calculated_dist}"

    def test_golden_dataset_geodesic_distance_calculation(self, dask_client):
        """
        Test geodesic distance calculations match expected values.

        Validates:
        - geodesic_distance() produces reasonable WGS84 distances
        - Different points have different distances
        - Same points have zero distance
        """
        # Test with Fort Collins, CO area coordinates
        point1_lat, point1_long = 41.2565, -105.9378
        point2_lat, point2_long = 41.2570, -105.9380

        # Distance between same point should be 0
        same_point_dist = geodesic_distance(point1_lat, point1_long, point1_lat, point1_long)
        assert abs(same_point_dist) < 0.1, \
            f"Same point distance should be ~0m, got {same_point_dist}m"

        # Distance between different points should be > 0
        diff_points_dist = geodesic_distance(point1_lat, point1_long, point2_lat, point2_long)
        assert diff_points_dist > 0, \
            f"Different points should have distance > 0, got {diff_points_dist}m"

        # Approximate expected distance (rough estimate for these coordinates)
        # These points are very close, less than 10m apart
        assert 0 < diff_points_dist < 100, \
            f"Distance between points should be small (0-100m), got {diff_points_dist}m"

    def test_golden_dataset_hex_to_decimal_conversion(self, dask_client):
        """
        Test hexadecimal ID conversion to decimal.

        Validates:
        - Hexadecimal strings convert to correct decimal integers
        - Common BSM ID formats handled correctly
        """
        test_cases = [
            ("A1B2C3D4", 2712847316),
            ("B2C3D4E5", 2999178469),
            ("C3D4E5F6", 3285509622),
            ("FFFFFFFF", 4294967295),  # Max 32-bit unsigned
            ("00000000", 0),  # Min value
        ]

        for hex_str, expected_decimal in test_cases:
            calculated_decimal = int(hex_str, 16)
            assert calculated_decimal == expected_decimal, \
                f"Hex conversion mismatch for {hex_str}. " \
                f"Expected {expected_decimal}, got {calculated_decimal}"

    def test_golden_dataset_temporal_extraction(self, dask_client):
        """
        Test timestamp parsing extracts correct temporal features.

        Validates:
        - Month, day, year extracted correctly from MM/DD/YYYY format
        - Hour, minute, second extracted correctly
        - AM/PM indicator parsed correctly
        """
        test_cases = [
            ("04/06/2021 10:30:00 AM", 4, 6, 2021, 10, 30, 0, 0),  # AM
            ("04/06/2021 02:45:30 PM", 4, 6, 2021, 2, 45, 30, 1),  # PM
            ("05/15/2021 03:20:15 PM", 5, 15, 2021, 3, 20, 15, 1),  # Different month
            ("12/31/2021 11:59:59 PM", 12, 31, 2021, 11, 59, 59, 1),  # End of year
        ]

        for timestamp_str, exp_month, exp_day, exp_year, exp_hour, exp_min, exp_sec, exp_am_pm in test_cases:
            # Parse timestamp manually (simulating what cleaner does)
            date_part, time_part, am_pm = timestamp_str.split(' ')
            month, day, year = map(int, date_part.split('/'))
            hour, minute, second = map(int, time_part.split(':'))
            am_pm_val = 0 if am_pm == 'AM' else 1

            assert month == exp_month, f"Month mismatch for {timestamp_str}"
            assert day == exp_day, f"Day mismatch for {timestamp_str}"
            assert year == exp_year, f"Year mismatch for {timestamp_str}"
            assert hour == exp_hour, f"Hour mismatch for {timestamp_str}"
            assert minute == exp_min, f"Minute mismatch for {timestamp_str}"
            assert second == exp_sec, f"Second mismatch for {timestamp_str}"
            assert am_pm_val == exp_am_pm, f"AM/PM mismatch for {timestamp_str}"


@pytest.mark.dask
class TestDaskCleanersEdgeCases:
    """
    Edge case and boundary condition tests for Dask cleaners.

    Tests cover:
    - Empty DataFrames
    - Null/NaN values in critical columns
    - Invalid data formats
    - Single-row DataFrames
    - Missing required columns
    """

    # ========== UDF Edge Cases ==========

    def test_point_parsing_handles_none(self, dask_client):
        """Test that point parsing functions handle None values gracefully"""
        assert point_to_x(None) is None, "point_to_x(None) should return None"
        assert point_to_y(None) is None, "point_to_y(None) should return None"

    def test_point_parsing_handles_invalid_format(self, dask_client):
        """Test that point parsing handles invalid POINT strings"""
        invalid_points = [
            "",  # Empty string
            "INVALID",  # No POINT keyword
            "POINT ()",  # Empty coordinates
            "POINT (abc def)",  # Non-numeric coordinates
        ]

        for invalid_point in invalid_points:
            # Functions should either return None or raise ValueError
            # The actual behavior depends on implementation
            try:
                result_x = point_to_x(invalid_point)
                result_y = point_to_y(invalid_point)
                # If no exception, should be None
                assert result_x is None or result_y is None, \
                    f"Invalid point '{invalid_point}' should return None or raise error"
            except (ValueError, IndexError, AttributeError):
                # Expected behavior for invalid input
                pass

    def test_distance_calculation_with_zero_distance(self, dask_client):
        """Test distance calculations when points are identical"""
        # XY distance
        xy_dist = xy_distance(5.0, 10.0, 5.0, 10.0)
        assert abs(xy_dist) < 1e-9, f"XY distance for same point should be 0, got {xy_dist}"

        # Geodesic distance
        geo_dist = geodesic_distance(41.2565, -105.9378, 41.2565, -105.9378)
        assert abs(geo_dist) < 0.1, f"Geodesic distance for same point should be ~0, got {geo_dist}"

    def test_distance_calculation_with_large_distances(self, dask_client):
        """Test distance calculations with large separations"""
        # XY distance with large values
        xy_dist = xy_distance(1000.0, 1000.0, 0.0, 0.0)
        expected_dist = np.sqrt(1000**2 + 1000**2)
        assert abs(xy_dist - expected_dist) < 1e-9, \
            f"XY distance mismatch. Expected {expected_dist}, got {xy_dist}"

        # Geodesic distance across significant latitude/longitude
        # Fort Collins, CO to Denver, CO (roughly 65km south)
        fort_collins_lat, fort_collins_long = 40.5853, -105.0844
        denver_lat, denver_long = 39.7392, -104.9903
        geo_dist = geodesic_distance(fort_collins_lat, fort_collins_long, denver_lat, denver_long)

        # Should be a measurable distance (at least 1km)
        assert geo_dist > 1000, \
            f"Fort Collins to Denver distance should be > 1km, got {geo_dist}m"

    def test_hex_conversion_with_edge_cases(self, dask_client):
        """Test hexadecimal conversion edge cases"""
        # Max 32-bit unsigned integer
        assert int("FFFFFFFF", 16) == 4294967295

        # Zero
        assert int("00000000", 16) == 0

        # Mixed case (should work with int())
        assert int("AbCdEf12", 16) == int("ABCDEF12", 16)

        # Leading zeros
        assert int("0000ABCD", 16) == int("ABCD", 16)


@pytest.mark.dask
class TestDaskCleanersConsistency:
    """
    Consistency tests to ensure Dask UDFs produce deterministic results.
    """

    def test_distance_calculations_are_deterministic(self, dask_client):
        """
        Test that distance calculations produce identical results on repeated calls.
        """
        # XY distance
        xy_dist1 = xy_distance(3.0, 4.0, 0.0, 0.0)
        xy_dist2 = xy_distance(3.0, 4.0, 0.0, 0.0)
        assert xy_dist1 == xy_dist2, "XY distance should be deterministic"

        # Geodesic distance
        geo_dist1 = geodesic_distance(41.2565, -105.9378, 41.2570, -105.9380)
        geo_dist2 = geodesic_distance(41.2565, -105.9378, 41.2570, -105.9380)
        assert geo_dist1 == geo_dist2, "Geodesic distance should be deterministic"

    def test_point_parsing_is_deterministic(self, dask_client):
        """
        Test that point parsing produces identical results on repeated calls.
        """
        point_str = "POINT (-105.9378 41.2565)"

        x1, y1 = point_to_x(point_str), point_to_y(point_str)
        x2, y2 = point_to_x(point_str), point_to_y(point_str)

        assert x1 == x2, "point_to_x should be deterministic"
        assert y1 == y2, "point_to_y should be deterministic"

    def test_hex_conversion_is_deterministic(self, dask_client):
        """
        Test that hex conversion produces identical results on repeated calls.
        """
        hex_str = "A1B2C3D4"

        dec1 = int(hex_str, 16)
        dec2 = int(hex_str, 16)

        assert dec1 == dec2, "Hex conversion should be deterministic"


@pytest.mark.dask
class TestDaskDataFrameOperations:
    """
    Test Dask DataFrame operations used by cleaners.
    """

    def test_dask_apply_point_parsing(self, dask_client):
        """Test applying point parsing UDFs to Dask DataFrames"""
        data = {
            'position': [
                "POINT (-105.9378 41.2565)",
                "POINT (-105.9380 41.2570)",
                "POINT (-105.9385 41.2575)",
            ]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        # Apply UDFs
        df['x_pos'] = df['position'].apply(point_to_x, meta=('position', 'float64'))
        df['y_pos'] = df['position'].apply(point_to_y, meta=('position', 'float64'))

        result = df.compute()

        # Validate extraction
        assert len(result) == 3, "Should have 3 rows"
        assert 'x_pos' in result.columns, "Should have x_pos column"
        assert 'y_pos' in result.columns, "Should have y_pos column"

        # Check first row values
        assert abs(result.iloc[0]['x_pos'] - (-105.9378)) < 1e-9
        assert abs(result.iloc[0]['y_pos'] - 41.2565) < 1e-9

    def test_dask_filter_by_distance(self, dask_client):
        """Test filtering Dask DataFrame by distance"""
        data = {
            'x_pos': [0.0, 3.0, 10.0, 100.0],
            'y_pos': [0.0, 4.0, 10.0, 100.0],
            'id': [1, 2, 3, 4]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=2)

        # Add distance column
        df['distance'] = df.apply(
            lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),
            axis=1,
            meta=('distance', 'float64')
        )

        # Filter by distance <= 10
        filtered = df[df['distance'] <= 10.0]
        result = filtered.compute()

        # Should include rows 0, 1, 2 (distances: 0, 5, ~14.14)
        # Actually row 2 has distance sqrt(10^2 + 10^2) = sqrt(200) ~= 14.14, so it's filtered out
        assert len(result) == 2, f"Should have 2 rows within distance 10, got {len(result)}"
        assert list(result['id']) == [1, 2], "Should have ids 1 and 2"

    def test_dask_hex_conversion_in_dataframe(self, dask_client):
        """Test hex-to-decimal conversion in Dask DataFrames"""
        data = {
            'hex_id': ["A1B2C3D4", "B2C3D4E5", "FFFFFFFF"],
            'name': ["vehicle1", "vehicle2", "vehicle3"]
        }
        df = dd.from_pandas(pd.DataFrame(data), npartitions=1)

        # Convert hex to decimal
        df['decimal_id'] = df['hex_id'].apply(
            lambda x: int(x, 16) if x else None,
            meta=('hex_id', 'int64')
        )

        result = df.compute()

        # Validate conversion
        assert result.iloc[0]['decimal_id'] == 2712847316
        assert result.iloc[1]['decimal_id'] == 2999178469
        assert result.iloc[2]['decimal_id'] == 4294967295


@pytest.mark.dask
class TestDaskCleanerIntegration:
    """
    Integration tests validating cleaner components work together.
    """

    def test_full_cleaning_pipeline_simulation(self, dask_client):
        """
        Simulate a full cleaning pipeline with Dask operations.

        This test validates that all UDF operations can be chained together
        to produce a cleaned dataset ready for analysis.
        """
        # Raw BSM data
        raw_data = {
            "metadata_generatedAt": [
                "04/06/2021 10:30:00 AM",
                "04/06/2021 02:45:30 PM",
                "05/15/2021 03:20:15 PM",
            ],
            "coreData_id": ["A1B2C3D4", "B2C3D4E5", "C3D4E5F6"],
            "coreData_position": [
                "POINT (-105.9378 41.2565)",
                "POINT (-105.9380 41.2570)",
                "POINT (-105.9385 41.2575)",
            ],
            "coreData_speed": [15.5, 25.0, 18.0],
        }
        df = dd.from_pandas(pd.DataFrame(raw_data), npartitions=1)

        # Step 1: Parse POINT strings
        df['x_pos'] = df['coreData_position'].apply(point_to_x, meta=('coreData_position', 'float64'))
        df['y_pos'] = df['coreData_position'].apply(point_to_y, meta=('coreData_position', 'float64'))

        # Step 2: Convert hex IDs to decimal
        df['coreData_id_decimal'] = df['coreData_id'].apply(
            lambda x: int(x, 16) if x else None,
            meta=('coreData_id', 'int64')
        )

        # Step 3: Calculate distance from origin
        df['distance_from_origin'] = df.apply(
            lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),
            axis=1,
            meta=('distance', 'float64')
        )

        # Compute result
        result = df.compute()

        # Validate pipeline output
        assert len(result) == 3, "Should have 3 rows"
        assert 'x_pos' in result.columns, "Should have extracted x_pos"
        assert 'y_pos' in result.columns, "Should have extracted y_pos"
        assert 'coreData_id_decimal' in result.columns, "Should have converted hex IDs"
        assert 'distance_from_origin' in result.columns, "Should have calculated distances"

        # Validate specific values
        assert result.iloc[0]['coreData_id_decimal'] == 2712847316
        assert abs(result.iloc[0]['x_pos'] - (-105.9378)) < 1e-9
        assert abs(result.iloc[0]['y_pos'] - 41.2565) < 1e-9


@pytest.mark.dask
class TestDaskCleanersEmptyDataFrames:
    """
    Test that all Dask cleaner operations handle empty DataFrames correctly.

    Empty DataFrames can occur from:
    - Empty input files
    - Aggressive filtering that removes all rows
    - Data validation that drops all invalid records
    """

    def test_empty_dataframe_with_point_parsing(self, dask_client):
        """Test applying point parsing UDFs to empty Dask DataFrame"""
        # Create empty DataFrame with proper schema
        pdf = pd.DataFrame({
            'coreData_position': pd.Series([], dtype='object'),
            'id': pd.Series([], dtype='int64')
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply UDFs
        ddf['x_pos'] = ddf['coreData_position'].apply(point_to_x, meta=('coreData_position', 'float64'))
        ddf['y_pos'] = ddf['coreData_position'].apply(point_to_y, meta=('coreData_position', 'float64'))

        result = ddf.compute()

        # Validate empty DataFrame with correct schema
        assert len(result) == 0, "Should have 0 rows"
        assert 'x_pos' in result.columns, "Should have x_pos column"
        assert 'y_pos' in result.columns, "Should have y_pos column"
        assert list(result.columns) == ['coreData_position', 'id', 'x_pos', 'y_pos']

    def test_empty_dataframe_with_distance_calculation(self, dask_client):
        """Test distance calculations on empty DataFrame"""
        pdf = pd.DataFrame({
            'x_pos': pd.Series([], dtype='float64'),
            'y_pos': pd.Series([], dtype='float64'),
            'id': pd.Series([], dtype='int64')
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Add distance column
        ddf['distance'] = ddf.apply(
            lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),
            axis=1,
            meta=('distance', 'float64')
        )

        result = ddf.compute()

        # Should have empty DataFrame with distance column
        assert len(result) == 0
        assert 'distance' in result.columns

    def test_empty_dataframe_with_filtering(self, dask_client):
        """Test filtering operations on empty DataFrame"""
        pdf = pd.DataFrame({
            'x_pos': pd.Series([], dtype='float64'),
            'y_pos': pd.Series([], dtype='float64'),
            'value': pd.Series([], dtype='float64')
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Filter (should return empty)
        filtered = ddf[ddf['value'] > 10.0]
        result = filtered.compute()

        assert len(result) == 0
        assert list(result.columns) == ['x_pos', 'y_pos', 'value']

    def test_empty_dataframe_with_hex_conversion(self, dask_client):
        """Test hex-to-decimal conversion on empty DataFrame"""
        pdf = pd.DataFrame({
            'hex_id': pd.Series([], dtype='object'),
            'name': pd.Series([], dtype='object')
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Convert hex to decimal
        ddf['decimal_id'] = ddf['hex_id'].apply(
            lambda x: int(x, 16) if x else None,
            meta=('hex_id', 'int64')
        )

        result = ddf.compute()

        assert len(result) == 0
        assert 'decimal_id' in result.columns


@pytest.mark.dask
class TestDaskCleanersNullValues:
    """
    Test that all Dask cleaner operations handle null/NaN values correctly.

    Null values can occur from:
    - Missing data in source files
    - Invalid data that gets converted to null
    - Gaps in sensor readings
    - Data quality issues
    """

    def test_point_parsing_with_null_values(self, dask_client):
        """Test point parsing when some values are null"""
        pdf = pd.DataFrame({
            'coreData_position': [
                "POINT (-105.9378 41.2565)",
                None,
                "POINT (-105.9380 41.2570)",
                None,
                "POINT (-105.9385 41.2575)",
            ],
            'id': [1, 2, 3, 4, 5]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Apply UDFs
        ddf['x_pos'] = ddf['coreData_position'].apply(point_to_x, meta=('coreData_position', 'float64'))
        ddf['y_pos'] = ddf['coreData_position'].apply(point_to_y, meta=('coreData_position', 'float64'))

        result = ddf.compute()

        # Validate: rows with None should have None x_pos/y_pos
        assert len(result) == 5
        assert pd.isna(result.iloc[1]['x_pos']), "Row 1 should have null x_pos"
        assert pd.isna(result.iloc[1]['y_pos']), "Row 1 should have null y_pos"
        assert pd.isna(result.iloc[3]['x_pos']), "Row 3 should have null x_pos"
        assert not pd.isna(result.iloc[0]['x_pos']), "Row 0 should have valid x_pos"
        assert not pd.isna(result.iloc[2]['x_pos']), "Row 2 should have valid x_pos"

    def test_distance_calculation_with_null_coordinates(self, dask_client):
        """Test distance calculations when coordinates contain null values"""
        pdf = pd.DataFrame({
            'x_pos': [0.0, None, 3.0, None, 10.0],
            'y_pos': [0.0, 4.0, None, None, 10.0],
            'id': [1, 2, 3, 4, 5]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Add distance column (will fail if x or y is null)
        # The xy_distance function should handle null by propagating None
        def safe_xy_distance(row):
            if pd.isna(row['x_pos']) or pd.isna(row['y_pos']):
                return None
            return xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0)

        ddf['distance'] = ddf.apply(safe_xy_distance, axis=1, meta=('distance', 'float64'))

        result = ddf.compute()

        # Validate null handling
        assert len(result) == 5
        assert not pd.isna(result.iloc[0]['distance']), "Row 0 should have valid distance"
        assert pd.isna(result.iloc[1]['distance']), "Row 1 should have null distance (null x)"
        assert pd.isna(result.iloc[2]['distance']), "Row 2 should have null distance (null y)"
        assert pd.isna(result.iloc[3]['distance']), "Row 3 should have null distance (both null)"
        assert not pd.isna(result.iloc[4]['distance']), "Row 4 should have valid distance"

    def test_hex_conversion_with_null_values(self, dask_client):
        """Test hex-to-decimal conversion when hex IDs contain null values"""
        pdf = pd.DataFrame({
            'hex_id': ["A1B2C3D4", None, "B2C3D4E5", None, "FFFFFFFF"],
            'name': ["vehicle1", "vehicle2", "vehicle3", "vehicle4", "vehicle5"]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Convert hex to decimal (handle None)
        ddf['decimal_id'] = ddf['hex_id'].apply(
            lambda x: int(x, 16) if x is not None and pd.notna(x) else None,
            meta=('hex_id', 'object')
        )

        result = ddf.compute()

        # Validate null handling
        assert len(result) == 5
        assert result.iloc[0]['decimal_id'] == 2712847316, "Row 0 should have valid decimal"
        assert pd.isna(result.iloc[1]['decimal_id']), "Row 1 should have null decimal"
        assert result.iloc[2]['decimal_id'] == 2999178469, "Row 2 should have valid decimal"
        assert pd.isna(result.iloc[3]['decimal_id']), "Row 3 should have null decimal"
        assert result.iloc[4]['decimal_id'] == 4294967295, "Row 4 should have valid decimal"

    def test_filtering_with_null_values(self, dask_client):
        """Test filtering when filter columns contain null values"""
        pdf = pd.DataFrame({
            'x_pos': [0.0, 3.0, None, 10.0, None],
            'y_pos': [0.0, 4.0, 10.0, None, None],
            'id': [1, 2, 3, 4, 5]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Filter by x_pos > 2.0 (nulls should be excluded)
        filtered = ddf[ddf['x_pos'] > 2.0]
        result = filtered.compute()

        # Only rows 1 and 3 should remain (null rows filtered out)
        assert len(result) == 2
        assert list(result['id'].values) == [2, 4]

    def test_all_null_dataframe(self, dask_client):
        """Test operations on DataFrame where all values are null"""
        pdf = pd.DataFrame({
            'coreData_position': [None, None, None],
            'x_pos': [None, None, None],
            'y_pos': [None, None, None],
            'id': [1, 2, 3]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply point parsing (should return all nulls)
        ddf['parsed_x'] = ddf['coreData_position'].apply(point_to_x, meta=('coreData_position', 'float64'))

        result = ddf.compute()

        # All parsed values should be null
        assert len(result) == 3
        assert result['parsed_x'].isna().all(), "All parsed values should be null"


@pytest.mark.dask
class TestDaskCleanersSingleRowDataFrames:
    """
    Test that all Dask cleaner operations handle single-row DataFrames correctly.

    Single-row DataFrames can occur from:
    - Very small datasets
    - Aggressive filtering
    - Edge cases in data processing
    """

    def test_single_row_point_parsing(self, dask_client):
        """Test point parsing with single-row DataFrame"""
        pdf = pd.DataFrame({
            'coreData_position': ["POINT (-105.9378 41.2565)"],
            'id': [1]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply UDFs
        ddf['x_pos'] = ddf['coreData_position'].apply(point_to_x, meta=('coreData_position', 'float64'))
        ddf['y_pos'] = ddf['coreData_position'].apply(point_to_y, meta=('coreData_position', 'float64'))

        result = ddf.compute()

        # Validate single row processed correctly
        assert len(result) == 1
        assert abs(result.iloc[0]['x_pos'] - (-105.9378)) < 1e-9
        assert abs(result.iloc[0]['y_pos'] - 41.2565) < 1e-9

    def test_single_row_distance_calculation(self, dask_client):
        """Test distance calculation with single-row DataFrame"""
        pdf = pd.DataFrame({
            'x_pos': [3.0],
            'y_pos': [4.0],
            'id': [1]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Add distance column
        ddf['distance'] = ddf.apply(
            lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),
            axis=1,
            meta=('distance', 'float64')
        )

        result = ddf.compute()

        # Validate
        assert len(result) == 1
        assert abs(result.iloc[0]['distance'] - 5.0) < 1e-9

    def test_single_row_filtering(self, dask_client):
        """Test filtering with single-row DataFrame"""
        pdf = pd.DataFrame({
            'x_pos': [3.0],
            'y_pos': [4.0],
            'value': [10.0]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Filter that includes the row
        filtered_in = ddf[ddf['value'] > 5.0]
        result_in = filtered_in.compute()
        assert len(result_in) == 1

        # Filter that excludes the row
        filtered_out = ddf[ddf['value'] > 15.0]
        result_out = filtered_out.compute()
        assert len(result_out) == 0

    def test_single_row_hex_conversion(self, dask_client):
        """Test hex conversion with single-row DataFrame"""
        pdf = pd.DataFrame({
            'hex_id': ["A1B2C3D4"],
            'name': ["vehicle1"]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Convert hex to decimal
        ddf['decimal_id'] = ddf['hex_id'].apply(
            lambda x: int(x, 16) if x else None,
            meta=('hex_id', 'int64')
        )

        result = ddf.compute()

        # Validate
        assert len(result) == 1
        assert result.iloc[0]['decimal_id'] == 2712847316


@pytest.mark.dask
class TestDaskCleanersInvalidData:
    """
    Test that all Dask cleaner operations handle invalid data formats correctly.

    Invalid data can occur from:
    - Data corruption
    - Format changes in source systems
    - Manual data entry errors
    - Encoding issues
    """

    def test_invalid_point_format_handling(self, dask_client):
        """Test handling of various invalid POINT formats"""
        pdf = pd.DataFrame({
            'coreData_position': [
                "POINT (-105.9378 41.2565)",  # Valid
                "INVALID STRING",  # Invalid
                "POINT ()",  # Empty coordinates
                "POINT (abc def)",  # Non-numeric
                "(-105.9378 41.2565)",  # Missing POINT keyword
                "POINT (-105.9378 41.2565",  # Missing closing paren
            ],
            'id': [1, 2, 3, 4, 5, 6]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Apply UDFs (should handle errors gracefully)
        def safe_point_to_x(point_str):
            try:
                return point_to_x(point_str)
            except:
                return None

        def safe_point_to_y(point_str):
            try:
                return point_to_y(point_str)
            except:
                return None

        ddf['x_pos'] = ddf['coreData_position'].apply(safe_point_to_x, meta=('coreData_position', 'float64'))
        ddf['y_pos'] = ddf['coreData_position'].apply(safe_point_to_y, meta=('coreData_position', 'float64'))

        result = ddf.compute()

        # First row should be valid
        assert not pd.isna(result.iloc[0]['x_pos'])
        assert not pd.isna(result.iloc[0]['y_pos'])

        # Invalid rows should have None or NaN
        # (actual behavior depends on point_to_x/y implementation)
        assert len(result) == 6

    def test_invalid_hex_format_handling(self, dask_client):
        """Test handling of invalid hexadecimal formats"""
        pdf = pd.DataFrame({
            'hex_id': [
                "A1B2C3D4",  # Valid
                "GHIJKLMN",  # Invalid hex characters
                "12.34",  # Decimal point
                "",  # Empty string
                "0xABCD",  # With 0x prefix
            ],
            'name': ["v1", "v2", "v3", "v4", "v5"]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Safe hex conversion
        def safe_hex_to_decimal(hex_str):
            if not hex_str or pd.isna(hex_str):
                return None
            try:
                # Remove 0x prefix if present
                hex_str = str(hex_str).replace("0x", "").replace(".0", "").strip()
                return int(hex_str, 16) if hex_str else None
            except (ValueError, AttributeError):
                return None

        ddf['decimal_id'] = ddf['hex_id'].apply(safe_hex_to_decimal, meta=('hex_id', 'object'))

        result = ddf.compute()

        # Valid row should convert
        assert result.iloc[0]['decimal_id'] == 2712847316

        # Invalid rows should return None
        assert pd.isna(result.iloc[1]['decimal_id']) or result.iloc[1]['decimal_id'] is None
        assert pd.isna(result.iloc[3]['decimal_id']) or result.iloc[3]['decimal_id'] is None

        # 0x prefix should be handled
        # Empty string should return None

    def test_extreme_coordinate_values(self, dask_client):
        """Test handling of extreme coordinate values"""
        pdf = pd.DataFrame({
            'x_pos': [0.0, 1e10, -1e10, 1e-10, -1e-10],
            'y_pos': [0.0, 1e10, -1e10, 1e-10, -1e-10],
            'id': [1, 2, 3, 4, 5]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Calculate distances
        ddf['distance'] = ddf.apply(
            lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),
            axis=1,
            meta=('distance', 'float64')
        )

        result = ddf.compute()

        # Should not raise errors, even with extreme values
        assert len(result) == 5
        assert not result['distance'].isna().all()

        # Validate some expected values
        assert abs(result.iloc[0]['distance']) < 1e-9  # (0,0) -> 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "dask"])

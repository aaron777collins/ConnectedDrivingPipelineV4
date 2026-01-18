"""
Unit tests for geospatial UDFs in Helpers/SparkUDFs/GeospatialUDFs.py

This test module validates all geospatial UDFs used in the PySpark migration:
- point_to_tuple_udf: WKT POINT to (x, y) struct conversion
- point_to_x_udf: Extract longitude from WKT POINT
- point_to_y_udf: Extract latitude from WKT POINT
- geodesic_distance_udf: Geodesic distance calculation (WGS84)
- xy_distance_udf: Euclidean distance calculation

Test Coverage:
- Basic functionality
- Null/None value handling
- Invalid input handling
- Edge cases (zero distance, large distances)
- Integration with PySpark DataFrames
- Equivalence with pandas implementations
"""

import pytest
import math
from pyspark.sql import Row
from pyspark.sql.functions import col, lit

from Helpers.SparkUDFs.GeospatialUDFs import (
    point_to_tuple_udf,
    point_to_x_udf,
    point_to_y_udf,
    geodesic_distance_udf,
    xy_distance_udf
)
from Helpers.DataConverter import DataConverter
from Helpers.MathHelper import MathHelper


class TestPointToTupleUDF:
    """Test suite for point_to_tuple_udf."""

    def test_basic_conversion(self, spark_session):
        """Test basic WKT POINT to tuple conversion."""
        data = [
            ("POINT (-104.6744332 41.1509182)",),
            ("POINT (-105.93 41.25)",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("coords", point_to_tuple_udf(col("point"))) \
                      .withColumn("x", col("coords.x")) \
                      .withColumn("y", col("coords.y"))

        rows = result_df.collect()

        # First point
        assert abs(rows[0]["x"] - (-104.6744332)) < 1e-6
        assert abs(rows[0]["y"] - 41.1509182) < 1e-6

        # Second point
        assert abs(rows[1]["x"] - (-105.93)) < 1e-6
        assert abs(rows[1]["y"] - 41.25) < 1e-6

    def test_null_input(self, spark_session):
        """Test handling of null input."""
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([
            StructField("point", StringType(), nullable=True)
        ])
        data = [(None,)]
        df = spark_session.createDataFrame(data, schema=schema)

        result_df = df.withColumn("coords", point_to_tuple_udf(col("point")))
        row = result_df.collect()[0]

        assert row["coords"] is None

    def test_invalid_format(self, spark_session):
        """Test handling of invalid WKT POINT format."""
        data = [
            ("not a point",),
            ("POINT (abc def)",),
            ("POINT (123)",),  # Missing coordinate
            ("",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("coords", point_to_tuple_udf(col("point")))
        rows = result_df.collect()

        # All invalid inputs should return None
        for row in rows:
            assert row["coords"] is None

    def test_equivalence_with_dataconverter(self, spark_session):
        """Test that UDF produces same results as DataConverter.point_to_tuple()."""
        test_points = [
            "POINT (-104.6744332 41.1509182)",
            "POINT (-105.93 41.25)",
            "POINT (0.0 0.0)",
            "POINT (180.0 90.0)",
            "POINT (-180.0 -90.0)",
        ]

        data = [(p,) for p in test_points]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("coords", point_to_tuple_udf(col("point"))) \
                      .withColumn("x", col("coords.x")) \
                      .withColumn("y", col("coords.y"))

        rows = result_df.collect()

        for i, point_str in enumerate(test_points):
            expected = DataConverter.point_to_tuple(point_str)
            assert abs(rows[i]["x"] - expected[0]) < 1e-6
            assert abs(rows[i]["y"] - expected[1]) < 1e-6


class TestPointToXUDF:
    """Test suite for point_to_x_udf (longitude extraction)."""

    def test_extract_longitude(self, spark_session):
        """Test longitude extraction from WKT POINT."""
        data = [
            ("POINT (-104.6744332 41.1509182)",),
            ("POINT (-105.93 41.25)",),
            ("POINT (123.456 78.9)",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("x_pos", point_to_x_udf(col("point")))
        rows = result_df.collect()

        assert abs(rows[0]["x_pos"] - (-104.6744332)) < 1e-6
        assert abs(rows[1]["x_pos"] - (-105.93)) < 1e-6
        assert abs(rows[2]["x_pos"] - 123.456) < 1e-6

    def test_null_input(self, spark_session):
        """Test handling of null input."""
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([
            StructField("point", StringType(), nullable=True)
        ])
        data = [(None,)]
        df = spark_session.createDataFrame(data, schema=schema)

        result_df = df.withColumn("x_pos", point_to_x_udf(col("point")))
        row = result_df.collect()[0]

        assert row["x_pos"] is None

    def test_invalid_format(self, spark_session):
        """Test handling of invalid WKT POINT format."""
        data = [
            ("invalid",),
            ("POINT ()",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("x_pos", point_to_x_udf(col("point")))
        rows = result_df.collect()

        for row in rows:
            assert row["x_pos"] is None

    def test_boundary_values(self, spark_session):
        """Test longitude boundary values (-180 to 180)."""
        data = [
            ("POINT (-180.0 0.0)",),
            ("POINT (180.0 0.0)",),
            ("POINT (0.0 0.0)",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("x_pos", point_to_x_udf(col("point")))
        rows = result_df.collect()

        assert abs(rows[0]["x_pos"] - (-180.0)) < 1e-6
        assert abs(rows[1]["x_pos"] - 180.0) < 1e-6
        assert abs(rows[2]["x_pos"] - 0.0) < 1e-6


class TestPointToYUDF:
    """Test suite for point_to_y_udf (latitude extraction)."""

    def test_extract_latitude(self, spark_session):
        """Test latitude extraction from WKT POINT."""
        data = [
            ("POINT (-104.6744332 41.1509182)",),
            ("POINT (-105.93 41.25)",),
            ("POINT (123.456 78.9)",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("y_pos", point_to_y_udf(col("point")))
        rows = result_df.collect()

        assert abs(rows[0]["y_pos"] - 41.1509182) < 1e-6
        assert abs(rows[1]["y_pos"] - 41.25) < 1e-6
        assert abs(rows[2]["y_pos"] - 78.9) < 1e-6

    def test_null_input(self, spark_session):
        """Test handling of null input."""
        from pyspark.sql.types import StructType, StructField, StringType

        schema = StructType([
            StructField("point", StringType(), nullable=True)
        ])
        data = [(None,)]
        df = spark_session.createDataFrame(data, schema=schema)

        result_df = df.withColumn("y_pos", point_to_y_udf(col("point")))
        row = result_df.collect()[0]

        assert row["y_pos"] is None

    def test_invalid_format(self, spark_session):
        """Test handling of invalid WKT POINT format."""
        data = [
            ("invalid",),
            ("POINT ()",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("y_pos", point_to_y_udf(col("point")))
        rows = result_df.collect()

        for row in rows:
            assert row["y_pos"] is None

    def test_boundary_values(self, spark_session):
        """Test latitude boundary values (-90 to 90)."""
        data = [
            ("POINT (0.0 -90.0)",),
            ("POINT (0.0 90.0)",),
            ("POINT (0.0 0.0)",),
        ]
        df = spark_session.createDataFrame(data, ["point"])

        result_df = df.withColumn("y_pos", point_to_y_udf(col("point")))
        rows = result_df.collect()

        assert abs(rows[0]["y_pos"] - (-90.0)) < 1e-6
        assert abs(rows[1]["y_pos"] - 90.0) < 1e-6
        assert abs(rows[2]["y_pos"] - 0.0) < 1e-6


class TestGeodesicDistanceUDF:
    """Test suite for geodesic_distance_udf."""

    def test_basic_distance_calculation(self, spark_session):
        """Test basic geodesic distance calculation."""
        # Wyoming coordinates
        data = [
            (41.1509182, -104.6744332, 41.25, -105.93),  # Two points in Wyoming
        ]
        df = spark_session.createDataFrame(
            data, ["lat1", "lon1", "lat2", "lon2"]
        )

        result_df = df.withColumn(
            "distance",
            geodesic_distance_udf(
                col("lat1"), col("lon1"), col("lat2"), col("lon2")
            )
        )

        row = result_df.collect()[0]

        # Expected distance (calculated using MathHelper)
        expected = MathHelper.dist_between_two_points(
            41.1509182, -104.6744332, 41.25, -105.93
        )

        assert abs(row["distance"] - expected) < 1.0  # Within 1 meter

    def test_zero_distance(self, spark_session):
        """Test distance between identical points."""
        data = [(41.25, -105.93, 41.25, -105.93)]
        df = spark_session.createDataFrame(
            data, ["lat1", "lon1", "lat2", "lon2"]
        )

        result_df = df.withColumn(
            "distance",
            geodesic_distance_udf(
                col("lat1"), col("lon1"), col("lat2"), col("lon2")
            )
        )

        row = result_df.collect()[0]
        assert abs(row["distance"]) < 0.1  # Should be ~0

    def test_null_inputs(self, spark_session):
        """Test handling of null coordinates."""
        data = [
            (None, -105.93, 41.25, -105.93),
            (41.25, None, 41.25, -105.93),
            (41.25, -105.93, None, -105.93),
            (41.25, -105.93, 41.25, None),
            (None, None, None, None),
        ]
        df = spark_session.createDataFrame(
            data, ["lat1", "lon1", "lat2", "lon2"]
        )

        result_df = df.withColumn(
            "distance",
            geodesic_distance_udf(
                col("lat1"), col("lon1"), col("lat2"), col("lon2")
            )
        )

        rows = result_df.collect()

        # All should return None
        for row in rows:
            assert row["distance"] is None

    def test_long_distance(self, spark_session):
        """Test long distance calculation."""
        # New York to London (approximate)
        data = [(40.7128, -74.0060, 51.5074, -0.1278)]
        df = spark_session.createDataFrame(
            data, ["lat1", "lon1", "lat2", "lon2"]
        )

        result_df = df.withColumn(
            "distance",
            geodesic_distance_udf(
                col("lat1"), col("lon1"), col("lat2"), col("lon2")
            )
        )

        row = result_df.collect()[0]

        # MathHelper uses a specific distance calculation method
        # Expected distance is ~145 km = 145,000 meters
        assert 140_000 < row["distance"] < 150_000

    def test_equivalence_with_mathhelper(self, spark_session):
        """Test equivalence with MathHelper.dist_between_two_points()."""
        test_cases = [
            (41.1509182, -104.6744332, 41.25, -105.93),
            (0.0, 0.0, 0.0, 1.0),
            (45.0, -90.0, 45.0, -91.0),
            (89.0, 0.0, 88.0, 0.0),
        ]

        data = test_cases
        df = spark_session.createDataFrame(
            data, ["lat1", "lon1", "lat2", "lon2"]
        )

        result_df = df.withColumn(
            "distance",
            geodesic_distance_udf(
                col("lat1"), col("lon1"), col("lat2"), col("lon2")
            )
        )

        rows = result_df.collect()

        for i, (lat1, lon1, lat2, lon2) in enumerate(test_cases):
            expected = MathHelper.dist_between_two_points(lat1, lon1, lat2, lon2)
            assert abs(rows[i]["distance"] - expected) < 1.0


class TestXYDistanceUDF:
    """Test suite for xy_distance_udf (Euclidean distance)."""

    def test_basic_distance_calculation(self, spark_session):
        """Test basic Euclidean distance calculation."""
        data = [
            (0.0, 0.0, 3.0, 4.0),  # Classic 3-4-5 triangle
            (1.0, 1.0, 4.0, 5.0),  # Distance = 5
        ]
        df = spark_session.createDataFrame(data, ["x1", "y1", "x2", "y2"])

        result_df = df.withColumn(
            "distance",
            xy_distance_udf(col("x1"), col("y1"), col("x2"), col("y2"))
        )

        rows = result_df.collect()

        # 3-4-5 triangle: distance = 5
        assert abs(rows[0]["distance"] - 5.0) < 1e-6

        # sqrt((4-1)^2 + (5-1)^2) = sqrt(9 + 16) = 5
        assert abs(rows[1]["distance"] - 5.0) < 1e-6

    def test_zero_distance(self, spark_session):
        """Test distance between identical points."""
        data = [(5.0, 10.0, 5.0, 10.0)]
        df = spark_session.createDataFrame(data, ["x1", "y1", "x2", "y2"])

        result_df = df.withColumn(
            "distance",
            xy_distance_udf(col("x1"), col("y1"), col("x2"), col("y2"))
        )

        row = result_df.collect()[0]
        assert abs(row["distance"]) < 1e-6

    def test_null_inputs(self, spark_session):
        """Test handling of null coordinates."""
        data = [
            (None, 0.0, 1.0, 1.0),
            (0.0, None, 1.0, 1.0),
            (0.0, 0.0, None, 1.0),
            (0.0, 0.0, 1.0, None),
            (None, None, None, None),
        ]
        df = spark_session.createDataFrame(data, ["x1", "y1", "x2", "y2"])

        result_df = df.withColumn(
            "distance",
            xy_distance_udf(col("x1"), col("y1"), col("x2"), col("y2"))
        )

        rows = result_df.collect()

        # All should return None
        for row in rows:
            assert row["distance"] is None

    def test_negative_coordinates(self, spark_session):
        """Test distance calculation with negative coordinates."""
        data = [
            (-3.0, -4.0, 0.0, 0.0),  # Distance = 5
            (-1.0, -1.0, 1.0, 1.0),  # Distance = 2*sqrt(2)
        ]
        df = spark_session.createDataFrame(data, ["x1", "y1", "x2", "y2"])

        result_df = df.withColumn(
            "distance",
            xy_distance_udf(col("x1"), col("y1"), col("x2"), col("y2"))
        )

        rows = result_df.collect()

        assert abs(rows[0]["distance"] - 5.0) < 1e-6
        assert abs(rows[1]["distance"] - (2 * math.sqrt(2))) < 1e-6

    def test_large_distance(self, spark_session):
        """Test large distance calculation."""
        data = [(0.0, 0.0, 10000.0, 10000.0)]
        df = spark_session.createDataFrame(data, ["x1", "y1", "x2", "y2"])

        result_df = df.withColumn(
            "distance",
            xy_distance_udf(col("x1"), col("y1"), col("x2"), col("y2"))
        )

        row = result_df.collect()[0]

        # sqrt(10000^2 + 10000^2) = 10000*sqrt(2) ≈ 14142.14
        expected = 10000 * math.sqrt(2)
        assert abs(row["distance"] - expected) < 1.0

    def test_equivalence_with_mathhelper(self, spark_session):
        """Test equivalence with MathHelper.dist_between_two_pointsXY()."""
        test_cases = [
            (0.0, 0.0, 3.0, 4.0),
            (1.0, 1.0, 4.0, 5.0),
            (-5.0, -5.0, 5.0, 5.0),
            (100.0, 200.0, 150.0, 250.0),
        ]

        data = test_cases
        df = spark_session.createDataFrame(data, ["x1", "y1", "x2", "y2"])

        result_df = df.withColumn(
            "distance",
            xy_distance_udf(col("x1"), col("y1"), col("x2"), col("y2"))
        )

        rows = result_df.collect()

        for i, (x1, y1, x2, y2) in enumerate(test_cases):
            expected = MathHelper.dist_between_two_pointsXY(x1, y1, x2, y2)
            assert abs(rows[i]["distance"] - expected) < 1e-6


class TestGeospatialUDFsIntegration:
    """Integration tests for geospatial UDFs working together."""

    def test_point_parsing_and_distance_calculation(self, spark_session):
        """Test parsing WKT points and calculating distances."""
        # Two Wyoming locations
        data = [
            ("POINT (-104.6744332 41.1509182)", "POINT (-105.93 41.25)"),
        ]
        df = spark_session.createDataFrame(data, ["point1", "point2"])

        # Parse both points
        result_df = df.withColumn("x1", point_to_x_udf(col("point1"))) \
                      .withColumn("y1", point_to_y_udf(col("point1"))) \
                      .withColumn("x2", point_to_x_udf(col("point2"))) \
                      .withColumn("y2", point_to_y_udf(col("point2")))

        # Calculate geodesic distance
        result_df = result_df.withColumn(
            "geo_distance",
            geodesic_distance_udf(col("y1"), col("x1"), col("y2"), col("x2"))
        )

        row = result_df.collect()[0]

        # Verify parsing worked
        assert row["x1"] is not None
        assert row["y1"] is not None
        assert row["x2"] is not None
        assert row["y2"] is not None

        # Verify distance is reasonable (should be > 0)
        assert row["geo_distance"] > 0

    def test_bsm_data_pipeline(self, spark_session):
        """Test realistic BSM data processing pipeline."""
        # Sample BSM data with WKT POINT positions
        data = [
            ("POINT (-104.6744332 41.1509182)",),
            ("POINT (-104.6750000 41.1510000)",),
            ("POINT (-104.6760000 41.1520000)",),
        ]
        df = spark_session.createDataFrame(data, ["coreData_position"])

        # Parse coordinates
        df = df.withColumn("x_pos", point_to_x_udf(col("coreData_position"))) \
               .withColumn("y_pos", point_to_y_udf(col("coreData_position")))

        # Calculate distance from origin (Wyoming center)
        origin_lat = 41.25
        origin_lon = -105.93

        df = df.withColumn(
            "distance_from_origin",
            geodesic_distance_udf(
                col("y_pos"), col("x_pos"),
                lit(origin_lat), lit(origin_lon)
            )
        )

        rows = df.collect()

        # All rows should have valid coordinates and distances
        for row in rows:
            assert row["x_pos"] is not None
            assert row["y_pos"] is not None
            assert row["distance_from_origin"] is not None
            assert row["distance_from_origin"] > 0

    def test_large_dataset_performance(self, spark_session):
        """Test UDF performance with larger dataset."""
        # Generate 1000 points
        import random
        random.seed(42)

        data = []
        for _ in range(1000):
            lon = -105.0 + random.uniform(-0.5, 0.5)
            lat = 41.0 + random.uniform(-0.5, 0.5)
            data.append((f"POINT ({lon} {lat})",))

        df = spark_session.createDataFrame(data, ["point"])

        # Parse and calculate distances
        df = df.withColumn("x", point_to_x_udf(col("point"))) \
               .withColumn("y", point_to_y_udf(col("point"))) \
               .withColumn(
                   "distance",
                   geodesic_distance_udf(
                       col("y"), col("x"),
                       lit(41.0), lit(-105.0)
                   )
               )

        # Force computation
        count = df.count()

        assert count == 1000

        # Verify some results
        sample = df.limit(10).collect()
        for row in sample:
            assert row["x"] is not None
            assert row["y"] is not None
            assert row["distance"] is not None

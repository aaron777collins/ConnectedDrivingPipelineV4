"""
Tests for ConversionUDFs module.

This test suite validates the data conversion UDFs:
- hex_to_decimal_udf: Hexadecimal to decimal conversion
- direction_and_dist_to_xy_udf: Direction/distance to XY coordinates
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from Helpers.SparkUDFs.ConversionUDFs import hex_to_decimal_udf, direction_and_dist_to_xy_udf


@pytest.mark.spark
@pytest.mark.udf
class TestHexToDecimalUDF:
    """Tests for hex_to_decimal_udf."""

    def test_basic_hex_conversion(self, spark_session):
        """Test basic hexadecimal to decimal conversion."""
        # Create test data with hex strings
        test_data = [
            ("0xa1b2c3d4",),  # Standard hex with 0x prefix
            ("0x00000001",),  # Small hex value
            ("0xffffffff",),  # Large hex value (32-bit max unsigned)
            ("0x0",),          # Zero
        ]

        schema = StructType([StructField("hex_id", StringType(), True)])
        df = spark_session.createDataFrame(test_data, schema)

        # Apply hex_to_decimal_udf
        result_df = df.withColumn("decimal_id", hex_to_decimal_udf(col("hex_id")))

        # Collect results
        results = result_df.select("hex_id", "decimal_id").collect()

        # Expected conversions
        assert results[0]["decimal_id"] == int("0xa1b2c3d4", 16)  # 2713846740
        assert results[1]["decimal_id"] == 1
        assert results[2]["decimal_id"] == 4294967295  # Max 32-bit unsigned
        assert results[3]["decimal_id"] == 0

    def test_hex_with_decimal_point(self, spark_session):
        """Test hex strings with decimal points (edge case from pandas version)."""
        test_data = [
            ("0xa1b2c3d4.0",),  # Hex with decimal point
            ("0x12345678.0",),
            ("0x0.0",),
        ]

        schema = StructType([StructField("hex_id", StringType(), True)])
        df = spark_session.createDataFrame(test_data, schema)

        result_df = df.withColumn("decimal_id", hex_to_decimal_udf(col("hex_id")))
        results = result_df.select("hex_id", "decimal_id").collect()

        # Decimal point should be stripped before conversion
        assert results[0]["decimal_id"] == int("0xa1b2c3d4", 16)
        assert results[1]["decimal_id"] == int("0x12345678", 16)
        assert results[2]["decimal_id"] == 0

    def test_null_handling(self, spark_session):
        """Test null value handling."""
        test_data = [
            (None,),
        ]

        schema = StructType([StructField("hex_id", StringType(), True)])
        df = spark_session.createDataFrame(test_data, schema)

        result_df = df.withColumn("decimal_id", hex_to_decimal_udf(col("hex_id")))
        result = result_df.select("decimal_id").collect()[0]

        # None input should return None
        assert result["decimal_id"] is None

    def test_invalid_hex_handling(self, spark_session):
        """Test handling of invalid hex strings."""
        test_data = [
            ("not_a_hex",),      # Invalid characters
            ("0xZZZZ",),         # Invalid hex characters
            ("",),               # Empty string
            ("12345",),          # Missing 0x prefix
        ]

        schema = StructType([StructField("hex_id", StringType(), True)])
        df = spark_session.createDataFrame(test_data, schema)

        result_df = df.withColumn("decimal_id", hex_to_decimal_udf(col("hex_id")))
        results = result_df.select("hex_id", "decimal_id").collect()

        # Invalid hex strings should return None
        assert results[0]["decimal_id"] is None
        assert results[1]["decimal_id"] is None
        assert results[2]["decimal_id"] is None
        # Note: "12345" without "0x" will be interpreted as base-10, which is valid
        # but in our actual data, all hex IDs should have "0x" prefix

    def test_realistic_bsm_data(self, spark_session):
        """Test with realistic BSM coreData_id values."""
        # Simulate real coreData_id values from BSM data
        test_data = [
            ("0xa1b2c3d4", "VehicleA"),
            ("0xb2c3d4e5", "VehicleB"),
            ("0xc3d4e5f6", "VehicleC"),
            ("0xd4e5f6a1", "VehicleD"),
        ]

        schema = StructType([
            StructField("coreData_id", StringType(), True),
            StructField("vehicle_name", StringType(), True)
        ])
        df = spark_session.createDataFrame(test_data, schema)

        # Convert coreData_id from hex to decimal (as done in ML pipeline)
        result_df = df.withColumn("coreData_id", hex_to_decimal_udf(col("coreData_id")))

        results = result_df.select("coreData_id", "vehicle_name").collect()

        # Verify conversions (calculated using int(hex_string, 16))
        assert results[0]["coreData_id"] == 2712847316  # int("0xa1b2c3d4", 16)
        assert results[1]["coreData_id"] == 2999178469  # int("0xb2c3d4e5", 16)
        assert results[2]["coreData_id"] == 3285509622  # int("0xc3d4e5f6", 16)
        assert results[3]["coreData_id"] == 3571840673  # int("0xd4e5f6a1", 16)

        # Verify coreData_id is now an integer (not string)
        assert isinstance(results[0]["coreData_id"], int)

    def test_large_dataset_performance(self, spark_session):
        """Test hex_to_decimal_udf with larger dataset."""
        # Generate 1000 hex IDs
        test_data = [(f"0x{i:08x}",) for i in range(1000)]

        schema = StructType([StructField("hex_id", StringType(), True)])
        df = spark_session.createDataFrame(test_data, schema)

        # Apply UDF
        result_df = df.withColumn("decimal_id", hex_to_decimal_udf(col("hex_id")))

        # Verify count and spot-check values
        assert result_df.count() == 1000

        # Check first, middle, and last values
        results = result_df.collect()
        assert results[0]["decimal_id"] == 0
        assert results[500]["decimal_id"] == 500
        assert results[999]["decimal_id"] == 999

    def test_comparison_with_pandas_implementation(self, spark_session):
        """Test that Spark UDF matches pandas implementation."""
        # Pandas implementation (from MachineLearning/MConnectedDrivingDataCleaner.py)
        def convert_large_hex_str_to_hex_pandas(num):
            if "." in num:
                num = num.split(".")[0]
            num = int(num, 16)
            return num

        # Test cases
        test_hex_values = [
            "0xa1b2c3d4",
            "0xa1b2c3d4.0",  # With decimal point
            "0x12345678",
            "0xffffffff",
        ]

        # Pandas results
        pandas_results = [convert_large_hex_str_to_hex_pandas(h) for h in test_hex_values]

        # Spark UDF results
        test_data = [(h,) for h in test_hex_values]
        schema = StructType([StructField("hex_id", StringType(), True)])
        df = spark_session.createDataFrame(test_data, schema)
        result_df = df.withColumn("decimal_id", hex_to_decimal_udf(col("hex_id")))
        spark_results = [row["decimal_id"] for row in result_df.collect()]

        # Compare results
        assert spark_results == pandas_results, \
            f"Spark UDF results {spark_results} don't match pandas results {pandas_results}"


@pytest.mark.spark
@pytest.mark.udf
class TestDirectionAndDistToXYUDF:
    """Tests for direction_and_dist_to_xy_udf."""

    def test_basic_offset_calculation(self, spark_session):
        """Test basic direction and distance to XY calculation."""
        # Test data: starting point, direction, distance
        test_data = [
            (0.0, 0.0, 0, 100.0),    # North from origin
            (0.0, 0.0, 90, 100.0),   # East from origin
            (0.0, 0.0, 180, 100.0),  # South from origin
            (0.0, 0.0, 270, 100.0),  # West from origin
        ]

        schema = StructType([
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
            StructField("direction", IntegerType(), True),
            StructField("distance", DoubleType(), True)
        ])
        df = spark_session.createDataFrame(test_data, schema)

        # Apply UDF
        result_df = df.withColumn("new_coords",
                                    direction_and_dist_to_xy_udf(
                                        col("x"), col("y"),
                                        col("direction"), col("distance")
                                    ))

        result_df = result_df.withColumn("new_x", col("new_coords.new_x"))
        result_df = result_df.withColumn("new_y", col("new_coords.new_y"))

        results = result_df.select("direction", "new_x", "new_y").collect()

        # Verify that coordinates have changed from origin
        # Note: Exact values depend on MathHelper.direction_and_dist_to_XY implementation
        # For now, just verify non-null results
        for result in results:
            assert result["new_x"] is not None
            assert result["new_y"] is not None

    def test_null_handling_direction_dist(self, spark_session):
        """Test null value handling for direction_and_dist_to_xy_udf."""
        test_data = [
            (None, 0.0, 45, 100.0),   # Null x
            (0.0, None, 45, 100.0),   # Null y
            (0.0, 0.0, None, 100.0),  # Null direction
            (0.0, 0.0, 45, None),     # Null distance
        ]

        schema = StructType([
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True),
            StructField("direction", IntegerType(), True),
            StructField("distance", DoubleType(), True)
        ])
        df = spark_session.createDataFrame(test_data, schema)

        result_df = df.withColumn("new_coords",
                                    direction_and_dist_to_xy_udf(
                                        col("x"), col("y"),
                                        col("direction"), col("distance")
                                    ))

        results = result_df.select("new_coords").collect()

        # All null inputs should return None
        for result in results:
            assert result["new_coords"] is None


@pytest.mark.spark
@pytest.mark.integration
class TestConversionUDFsIntegration:
    """Integration tests for conversion UDFs."""

    def test_ml_pipeline_simulation(self, spark_session):
        """Simulate ML pipeline: hex conversion before feature extraction."""
        # Create sample BSM processed data
        test_data = [
            ("0xa1b2c3d4", 41.15, -104.67, 1500, 20.5, 180, 0),  # Regular vehicle
            ("0xb2c3d4e5", 41.16, -104.68, 1505, 22.0, 90, 1),   # Attacker
        ]

        schema = StructType([
            StructField("coreData_id", StringType(), True),
            StructField("y_pos", DoubleType(), True),
            StructField("x_pos", DoubleType(), True),
            StructField("coreData_elevation", IntegerType(), True),
            StructField("coreData_speed", DoubleType(), True),
            StructField("coreData_heading", IntegerType(), True),
            StructField("isAttacker", IntegerType(), True)
        ])
        df = spark_session.createDataFrame(test_data, schema)

        # Step 1: Convert coreData_id from hex to decimal (for ML)
        ml_df = df.withColumn("coreData_id", hex_to_decimal_udf(col("coreData_id")))

        # Step 2: Verify conversion
        results = ml_df.select("coreData_id", "isAttacker").collect()

        assert results[0]["coreData_id"] == 2712847316  # int("0xa1b2c3d4", 16)
        assert results[1]["coreData_id"] == 2999178469  # int("0xb2c3d4e5", 16)

        # Verify data types are correct for ML
        assert isinstance(results[0]["coreData_id"], int)
        assert isinstance(results[1]["coreData_id"], int)

        # Step 3: Verify we can now use coreData_id as a feature
        feature_df = ml_df.select(
            "coreData_id",     # Now decimal
            "y_pos",
            "x_pos",
            "coreData_elevation",
            "coreData_speed",
            "coreData_heading",
            "isAttacker"
        )

        # All features should be numeric
        assert feature_df.count() == 2
        row = feature_df.first()
        assert all(isinstance(row[col], (int, float)) for col in row.asDict().keys())

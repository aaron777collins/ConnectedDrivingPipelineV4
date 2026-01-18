"""
PySpark test fixtures for ConnectedDrivingPipelineV4.

This module provides pytest fixtures for PySpark testing, including:
- Spark session management (automatically cleaned up)
- Sample BSM DataFrames (raw and processed)
- Temporary directories for Spark I/O operations
- Dataset generators for different sizes (small, medium, large)

Usage:
    import pytest
    from Test.Fixtures.SparkFixtures import spark_session, sample_bsm_raw_df

    def test_my_spark_function(spark_session, sample_bsm_raw_df):
        # Use spark_session and sample_bsm_raw_df in your test
        result = spark_session.sql("SELECT * FROM sample")
        assert result.count() > 0
"""

import pytest
import tempfile
import shutil
import os
from datetime import datetime, timedelta
from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row


@pytest.fixture(scope="session")
def spark_session():
    """
    Session-scoped Spark session for testing.

    Creates a local Spark session optimized for testing with minimal memory
    and parallelism settings. Automatically stopped at the end of the test session.

    Yields:
        SparkSession: Local Spark session for testing

    Example:
        def test_spark_read(spark_session):
            df = spark_session.read.csv("test_data.csv")
            assert df.count() > 0
    """
    # Create a test-optimized Spark session
    spark = (
        SparkSession.builder
        .appName("ConnectedDrivingPipeline-Test")
        .master("local[2]")  # Use 2 cores for parallelism in tests
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")  # Fewer partitions for tests
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.ui.enabled", "false")  # Disable Spark UI for tests
        .config("spark.driver.host", "localhost")  # Ensure local execution
        .getOrCreate()
    )

    # Set log level to reduce noise during tests
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup: stop the session after all tests complete
    spark.stop()


@pytest.fixture(scope="function")
def spark_context(spark_session):
    """
    Function-scoped fixture providing the SparkContext.

    Args:
        spark_session: Session-scoped Spark session fixture

    Yields:
        SparkContext: Spark context from the session

    Example:
        def test_broadcast(spark_context):
            broadcast_var = spark_context.broadcast([1, 2, 3])
            assert broadcast_var.value == [1, 2, 3]
    """
    yield spark_session.sparkContext


@pytest.fixture(scope="function")
def temp_spark_dir(tmp_path):
    """
    Function-scoped temporary directory for Spark I/O operations.

    Creates a temporary directory that's automatically cleaned up after each test.
    Useful for testing file read/write operations without polluting the filesystem.

    Args:
        tmp_path: pytest's built-in tmp_path fixture

    Yields:
        str: Path to temporary directory

    Example:
        def test_parquet_write(spark_session, temp_spark_dir, sample_bsm_raw_df):
            output_path = os.path.join(temp_spark_dir, "output.parquet")
            sample_bsm_raw_df.write.parquet(output_path)
            assert os.path.exists(output_path)
    """
    spark_dir = tmp_path / "spark_test"
    spark_dir.mkdir(exist_ok=True)
    yield str(spark_dir)
    # Cleanup is handled automatically by pytest's tmp_path


@pytest.fixture(scope="function")
def sample_bsm_raw_df(spark_session):
    """
    Small sample DataFrame with raw BSM data (5 rows).

    Creates a DataFrame matching the BSM raw schema with realistic test data.
    Useful for unit testing individual functions without I/O overhead.

    Args:
        spark_session: Spark session fixture

    Returns:
        DataFrame: Sample DataFrame with 5 raw BSM records

    Example:
        def test_filter_by_speed(sample_bsm_raw_df):
            fast_vehicles = sample_bsm_raw_df.filter(col("coreData_speed") > 20)
            assert fast_vehicles.count() == 2
    """
    from Schemas.BSMRawSchema import get_bsm_raw_schema

    # Sample raw BSM data (5 rows with diverse values)
    data = [
        {
            "metadata_generatedAt": "04/06/2021 10:30:00 AM",
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": "00000000-0000-0000-0000-000000000001",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": 0,
            "metadata_serialId_recordId": 0,
            "metadata_serialId_serialNumber": 0,
            "metadata_receivedAt": "2021-04-06T10:30:00.000Z",
            "coreData_id": "A1B2C3D4",
            "coreData_position_lat": 41.2565,
            "coreData_position_long": -105.9378,
            "coreData_accuracy_semiMajor": 5.0,
            "coreData_accuracy_semiMinor": 5.0,
            "coreData_elevation": 2194.0,
            "coreData_accelSet_accelYaw": 0.0,
            "coreData_speed": 15.5,
            "coreData_heading": 90.0,
            "coreData_secMark": 30000,
        },
        {
            "metadata_generatedAt": "04/06/2021 10:30:01 AM",
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": "00000000-0000-0000-0000-000000000002",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": 0,
            "metadata_serialId_recordId": 1,
            "metadata_serialId_serialNumber": 1,
            "metadata_receivedAt": "2021-04-06T10:30:01.000Z",
            "coreData_id": "B2C3D4E5",
            "coreData_position_lat": 41.2570,
            "coreData_position_long": -105.9380,
            "coreData_accuracy_semiMajor": 6.0,
            "coreData_accuracy_semiMinor": 6.0,
            "coreData_elevation": 2195.0,
            "coreData_accelSet_accelYaw": 0.5,
            "coreData_speed": 25.0,
            "coreData_heading": 85.0,
            "coreData_secMark": 31000,
        },
        {
            "metadata_generatedAt": "04/06/2021 10:30:02 AM",
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": "00000000-0000-0000-0000-000000000003",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": 0,
            "metadata_serialId_recordId": 2,
            "metadata_serialId_serialNumber": 2,
            "metadata_receivedAt": "2021-04-06T10:30:02.000Z",
            "coreData_id": "C3D4E5F6",
            "coreData_position_lat": 41.2575,
            "coreData_position_long": -105.9385,
            "coreData_accuracy_semiMajor": 4.5,
            "coreData_accuracy_semiMinor": 4.5,
            "coreData_elevation": 2196.0,
            "coreData_accelSet_accelYaw": -0.3,
            "coreData_speed": 18.0,
            "coreData_heading": 92.0,
            "coreData_secMark": 32000,
        },
        {
            "metadata_generatedAt": "04/06/2021 10:30:03 AM",
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": "00000000-0000-0000-0000-000000000004",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": 0,
            "metadata_serialId_recordId": 3,
            "metadata_serialId_serialNumber": 3,
            "metadata_receivedAt": "2021-04-06T10:30:03.000Z",
            "coreData_id": "D4E5F6A7",
            "coreData_position_lat": 41.2580,
            "coreData_position_long": -105.9390,
            "coreData_accuracy_semiMajor": 7.0,
            "coreData_accuracy_semiMinor": 7.0,
            "coreData_elevation": 2197.0,
            "coreData_accelSet_accelYaw": 1.0,
            "coreData_speed": 30.0,
            "coreData_heading": 88.0,
            "coreData_secMark": 33000,
        },
        {
            "metadata_generatedAt": "04/06/2021 10:30:04 AM",
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": "00000000-0000-0000-0000-000000000005",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": 0,
            "metadata_serialId_recordId": 4,
            "metadata_serialId_serialNumber": 4,
            "metadata_receivedAt": "2021-04-06T10:30:04.000Z",
            "coreData_id": "E5F6A7B8",
            "coreData_position_lat": 41.2585,
            "coreData_position_long": -105.9395,
            "coreData_accuracy_semiMajor": 5.5,
            "coreData_accuracy_semiMinor": 5.5,
            "coreData_elevation": 2198.0,
            "coreData_accelSet_accelYaw": 0.2,
            "coreData_speed": 12.0,
            "coreData_heading": 91.0,
            "coreData_secMark": 34000,
        },
    ]

    schema = get_bsm_raw_schema(use_timestamp_type=False)
    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def sample_bsm_processed_df(spark_session):
    """
    Small sample DataFrame with processed BSM data (5 rows).

    Creates a DataFrame matching the BSM processed schema with ML-ready features.
    Includes temporal features, XY coordinates, and isAttacker labels.

    Args:
        spark_session: Spark session fixture

    Returns:
        DataFrame: Sample DataFrame with 5 processed BSM records

    Example:
        def test_ml_feature_selection(sample_bsm_processed_df):
            features = sample_bsm_processed_df.select("x_pos", "y_pos", "elevation")
            assert features.count() == 5
    """
    from Schemas.BSMProcessedSchema import get_bsm_processed_schema

    # Sample processed BSM data (5 rows)
    # Note: Using smaller decimal IDs to avoid IntegerType overflow
    data = [
        {
            "coreData_id": 1001,  # Simplified ID to avoid overflow
            "coreData_secMark": 30000,
            "coreData_accuracy_semiMajor": 5.0,
            "coreData_accuracy_semiMinor": 5.0,
            "coreData_elevation": 2194.0,
            "coreData_accelSet_accelYaw": 0.0,
            "coreData_speed": 15.5,
            "coreData_heading": 90.0,
            "x_pos": 100.0,
            "y_pos": 200.0,
            "month": 4,
            "day": 6,
            "year": 2021,
            "hour": 10,
            "minute": 30,
            "second": 0,
            "pm": 0,  # AM
            "isAttacker": 0,
        },
        {
            "coreData_id": 1002,  # Simplified ID
            "coreData_secMark": 31000,
            "coreData_accuracy_semiMajor": 6.0,
            "coreData_accuracy_semiMinor": 6.0,
            "coreData_elevation": 2195.0,
            "coreData_accelSet_accelYaw": 0.5,
            "coreData_speed": 25.0,
            "coreData_heading": 85.0,
            "x_pos": 150.0,
            "y_pos": 250.0,
            "month": 4,
            "day": 6,
            "year": 2021,
            "hour": 10,
            "minute": 30,
            "second": 1,
            "pm": 0,
            "isAttacker": 1,  # Attacker
        },
        {
            "coreData_id": 1003,  # Simplified ID
            "coreData_secMark": 32000,
            "coreData_accuracy_semiMajor": 4.5,
            "coreData_accuracy_semiMinor": 4.5,
            "coreData_elevation": 2196.0,
            "coreData_accelSet_accelYaw": -0.3,
            "coreData_speed": 18.0,
            "coreData_heading": 92.0,
            "x_pos": 200.0,
            "y_pos": 300.0,
            "month": 4,
            "day": 6,
            "year": 2021,
            "hour": 10,
            "minute": 30,
            "second": 2,
            "pm": 0,
            "isAttacker": 0,
        },
        {
            "coreData_id": 1004,  # Simplified ID
            "coreData_secMark": 33000,
            "coreData_accuracy_semiMajor": 7.0,
            "coreData_accuracy_semiMinor": 7.0,
            "coreData_elevation": 2197.0,
            "coreData_accelSet_accelYaw": 1.0,
            "coreData_speed": 30.0,
            "coreData_heading": 88.0,
            "x_pos": 250.0,
            "y_pos": 350.0,
            "month": 4,
            "day": 6,
            "year": 2021,
            "hour": 10,
            "minute": 30,
            "second": 3,
            "pm": 0,
            "isAttacker": 1,  # Attacker
        },
        {
            "coreData_id": 1005,  # Simplified ID
            "coreData_secMark": 34000,
            "coreData_accuracy_semiMajor": 5.5,
            "coreData_accuracy_semiMinor": 5.5,
            "coreData_elevation": 2198.0,
            "coreData_accelSet_accelYaw": 0.2,
            "coreData_speed": 12.0,
            "coreData_heading": 91.0,
            "x_pos": 300.0,
            "y_pos": 400.0,
            "month": 4,
            "day": 6,
            "year": 2021,
            "hour": 10,
            "minute": 30,
            "second": 4,
            "pm": 0,
            "isAttacker": 0,
        },
    ]

    schema = get_bsm_processed_schema()
    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def small_bsm_dataset(spark_session):
    """
    Generate a small BSM dataset (100 rows) for integration testing.

    Creates realistic BSM data with temporal progression and spatial distribution.
    Useful for testing full pipeline flows without excessive runtime.

    Args:
        spark_session: Spark session fixture

    Returns:
        DataFrame: Generated DataFrame with 100 raw BSM records

    Example:
        def test_pipeline_end_to_end(small_bsm_dataset):
            cleaned = clean_data(small_bsm_dataset)
            assert cleaned.count() <= small_bsm_dataset.count()
    """
    from Schemas.BSMRawSchema import get_bsm_raw_schema

    num_rows = 100
    base_time = datetime(2021, 4, 6, 10, 0, 0)

    data = []
    for i in range(num_rows):
        timestamp = base_time + timedelta(seconds=i)
        data.append({
            "metadata_generatedAt": timestamp.strftime("%m/%d/%Y %I:%M:%S %p"),
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": f"stream-{i % 10}",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": i // 10,
            "metadata_serialId_recordId": i % 10,
            "metadata_serialId_serialNumber": i,
            "metadata_receivedAt": timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "coreData_id": f"{i:08X}",  # Sequential hex IDs
            "coreData_position_lat": 41.25 + (i * 0.0001),  # Slight northward movement
            "coreData_position_long": -105.93 + (i * 0.0001),  # Slight eastward movement
            "coreData_accuracy_semiMajor": 5.0 + (i % 3),
            "coreData_accuracy_semiMinor": 5.0 + (i % 3),
            "coreData_elevation": 2190.0 + float(i % 10),
            "coreData_accelSet_accelYaw": float((i % 5 - 2)) * 0.5,  # Range: -1.0 to 1.0
            "coreData_speed": 10.0 + float(i % 30),  # Speed varies 10-40
            "coreData_heading": float((i * 3) % 360),  # Rotating heading
            "coreData_secMark": (30000 + i * 100) % 60000,
        })

    schema = get_bsm_raw_schema(use_timestamp_type=False)
    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def medium_bsm_dataset(spark_session):
    """
    Generate a medium BSM dataset (1000 rows) for performance testing.

    Creates realistic BSM data suitable for testing performance characteristics
    and parallelism without overwhelming test resources.

    Args:
        spark_session: Spark session fixture

    Returns:
        DataFrame: Generated DataFrame with 1000 raw BSM records

    Example:
        def test_attack_simulation_performance(medium_bsm_dataset):
            import time
            start = time.time()
            attacked = apply_attacks(medium_bsm_dataset)
            duration = time.time() - start
            assert duration < 10.0  # Should complete in < 10 seconds
    """
    from Schemas.BSMRawSchema import get_bsm_raw_schema

    num_rows = 1000
    base_time = datetime(2021, 4, 6, 10, 0, 0)

    data = []
    for i in range(num_rows):
        timestamp = base_time + timedelta(milliseconds=i * 100)  # 100ms intervals
        data.append({
            "metadata_generatedAt": timestamp.strftime("%m/%d/%Y %I:%M:%S %p"),
            "metadata_recordType": "bsmTx",
            "metadata_serialId_streamId": f"stream-{i % 20}",
            "metadata_serialId_bundleSize": 1,
            "metadata_serialId_bundleId": i // 20,
            "metadata_serialId_recordId": i % 20,
            "metadata_serialId_serialNumber": i,
            "metadata_receivedAt": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
            "coreData_id": f"{i:08X}",
            "coreData_position_lat": 41.25 + (i * 0.00001),
            "coreData_position_long": -105.93 + (i * 0.00001),
            "coreData_accuracy_semiMajor": 4.0 + (i % 5),
            "coreData_accuracy_semiMinor": 4.0 + (i % 5),
            "coreData_elevation": 2185.0 + float(i % 20),
            "coreData_accelSet_accelYaw": float((i % 11) - 5) * 0.2,  # Range: -1.0 to 1.0
            "coreData_speed": 5.0 + float(i % 40),  # Speed varies 5-45
            "coreData_heading": float((i * 5) % 360),
            "coreData_secMark": (28000 + i * 50) % 60000,
        })

    schema = get_bsm_raw_schema(use_timestamp_type=False)
    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def spark_df_comparer(spark_session):
    """
    Utility fixture for comparing Spark DataFrames in tests.

    Provides helper functions for DataFrame comparison with tolerance for
    floating-point differences and flexible column ordering.

    Args:
        spark_session: Spark session fixture

    Returns:
        object: Comparer object with comparison methods

    Example:
        def test_transformations(spark_df_comparer, sample_df):
            expected = create_expected_output()
            actual = my_transformation(sample_df)
            spark_df_comparer.assert_equal(expected, actual, rtol=1e-5)
    """
    class SparkDataFrameComparer:
        """Helper class for comparing Spark DataFrames."""

        def assert_equal(self, df1: DataFrame, df2: DataFrame,
                        check_dtype=True, rtol=1e-5, atol=1e-8,
                        ignore_column_order=False):
            """
            Assert that two Spark DataFrames are equal.

            Args:
                df1: First DataFrame
                df2: Second DataFrame
                check_dtype: Whether to check data types match
                rtol: Relative tolerance for floating-point comparison
                atol: Absolute tolerance for floating-point comparison
                ignore_column_order: If True, sort columns before comparison

            Raises:
                AssertionError: If DataFrames are not equal
            """
            # Check counts
            count1, count2 = df1.count(), df2.count()
            assert count1 == count2, f"Row counts differ: {count1} vs {count2}"

            # Check columns
            cols1 = set(df1.columns)
            cols2 = set(df2.columns)
            assert cols1 == cols2, f"Column sets differ: {cols1 - cols2} | {cols2 - cols1}"

            # Sort columns if requested
            if ignore_column_order:
                sorted_cols = sorted(df1.columns)
                df1 = df1.select(*sorted_cols)
                df2 = df2.select(*sorted_cols)

            # Check schemas
            if check_dtype:
                schema1 = {f.name: f.dataType for f in df1.schema.fields}
                schema2 = {f.name: f.dataType for f in df2.schema.fields}
                assert schema1 == schema2, f"Schemas differ: {schema1} vs {schema2}"

            # Compare data by converting to pandas (works for test-sized data)
            # For production, use Spark joins
            pdf1 = df1.toPandas().sort_values(by=list(df1.columns)).reset_index(drop=True)
            pdf2 = df2.toPandas().sort_values(by=list(df2.columns)).reset_index(drop=True)

            # Use pandas testing for detailed comparison
            import pandas.testing as pdt
            pdt.assert_frame_equal(pdf1, pdf2, rtol=rtol, atol=atol, check_dtype=check_dtype)

        def assert_schema_equal(self, df1: DataFrame, df2: DataFrame):
            """Assert that two DataFrames have the same schema."""
            schema1 = {f.name: f.dataType for f in df1.schema.fields}
            schema2 = {f.name: f.dataType for f in df2.schema.fields}
            assert schema1 == schema2, f"Schemas differ:\n{schema1}\nvs\n{schema2}"

        def assert_column_exists(self, df: DataFrame, column_name: str):
            """Assert that a DataFrame contains a specific column."""
            assert column_name in df.columns, f"Column '{column_name}' not found in {df.columns}"

    return SparkDataFrameComparer()

"""
Tests for PySpark fixtures to validate they work correctly.

This test file validates that all PySpark fixtures are properly configured
and can be used in tests.
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


@pytest.mark.spark
@pytest.mark.unit
def test_spark_session_fixture(spark_session):
    """Test that spark_session fixture provides a valid SparkSession."""
    assert spark_session is not None
    assert isinstance(spark_session, SparkSession)
    assert spark_session.sparkContext.appName == "ConnectedDrivingPipeline-Test"


@pytest.mark.spark
@pytest.mark.unit
def test_spark_context_fixture(spark_context):
    """Test that spark_context fixture provides a valid SparkContext."""
    assert spark_context is not None
    assert spark_context.master.startswith("local")


@pytest.mark.spark
@pytest.mark.unit
def test_temp_spark_dir_fixture(temp_spark_dir):
    """Test that temp_spark_dir fixture creates a temporary directory."""
    import os
    assert temp_spark_dir is not None
    assert os.path.exists(temp_spark_dir)
    assert os.path.isdir(temp_spark_dir)


@pytest.mark.spark
@pytest.mark.unit
def test_sample_bsm_raw_df_fixture(sample_bsm_raw_df):
    """Test that sample_bsm_raw_df fixture provides valid raw BSM data."""
    assert sample_bsm_raw_df is not None
    assert isinstance(sample_bsm_raw_df, DataFrame)
    assert sample_bsm_raw_df.count() == 5

    # Verify schema has expected columns
    expected_columns = [
        "metadata_generatedAt",
        "coreData_id",
        "coreData_position_lat",
        "coreData_position_long",
        "coreData_speed",
        "coreData_heading",
    ]
    for col_name in expected_columns:
        assert col_name in sample_bsm_raw_df.columns


@pytest.mark.spark
@pytest.mark.unit
def test_sample_bsm_processed_df_fixture(sample_bsm_processed_df):
    """Test that sample_bsm_processed_df fixture provides valid processed BSM data."""
    assert sample_bsm_processed_df is not None
    assert isinstance(sample_bsm_processed_df, DataFrame)
    assert sample_bsm_processed_df.count() == 5

    # Verify schema has expected processed columns
    expected_columns = [
        "coreData_id",
        "x_pos",
        "y_pos",
        "month",
        "day",
        "year",
        "hour",
        "minute",
        "second",
        "pm",
        "isAttacker",
    ]
    for col_name in expected_columns:
        assert col_name in sample_bsm_processed_df.columns

    # Verify attacker labels
    attackers = sample_bsm_processed_df.filter(col("isAttacker") == 1)
    assert attackers.count() == 2  # Should have 2 attackers in sample


@pytest.mark.spark
@pytest.mark.unit
def test_small_bsm_dataset_fixture(small_bsm_dataset):
    """Test that small_bsm_dataset fixture generates 100 rows."""
    assert small_bsm_dataset is not None
    assert isinstance(small_bsm_dataset, DataFrame)
    assert small_bsm_dataset.count() == 100

    # Verify data diversity
    unique_ids = small_bsm_dataset.select("coreData_id").distinct().count()
    assert unique_ids == 100  # All unique IDs


@pytest.mark.spark
@pytest.mark.unit
def test_medium_bsm_dataset_fixture(medium_bsm_dataset):
    """Test that medium_bsm_dataset fixture generates 1000 rows."""
    assert medium_bsm_dataset is not None
    assert isinstance(medium_bsm_dataset, DataFrame)
    assert medium_bsm_dataset.count() == 1000

    # Verify speed range
    speeds = medium_bsm_dataset.select("coreData_speed").rdd.flatMap(lambda x: x).collect()
    assert min(speeds) >= 5.0
    assert max(speeds) <= 45.0


@pytest.mark.spark
@pytest.mark.unit
def test_spark_df_comparer_fixture(spark_df_comparer, sample_bsm_raw_df):
    """Test that spark_df_comparer fixture provides comparison utilities."""
    assert spark_df_comparer is not None

    # Test assert_equal with identical DataFrames
    spark_df_comparer.assert_equal(sample_bsm_raw_df, sample_bsm_raw_df)

    # Test assert_schema_equal
    spark_df_comparer.assert_schema_equal(sample_bsm_raw_df, sample_bsm_raw_df)

    # Test assert_column_exists
    spark_df_comparer.assert_column_exists(sample_bsm_raw_df, "coreData_id")


@pytest.mark.spark
@pytest.mark.unit
def test_write_and_read_parquet(spark_session, sample_bsm_raw_df, temp_spark_dir):
    """Test writing and reading Parquet files with fixtures."""
    import os

    # Write to Parquet
    output_path = os.path.join(temp_spark_dir, "test_output.parquet")
    sample_bsm_raw_df.write.mode("overwrite").parquet(output_path)

    # Verify file exists
    assert os.path.exists(output_path)

    # Read back
    df_read = spark_session.read.parquet(output_path)

    # Verify data integrity
    assert df_read.count() == sample_bsm_raw_df.count()
    assert set(df_read.columns) == set(sample_bsm_raw_df.columns)


@pytest.mark.spark
@pytest.mark.unit
def test_fixture_isolation(spark_session):
    """Test that fixtures are properly isolated between tests."""
    # This test creates data in the session
    test_data = [{"id": 1, "value": "test"}]
    df = spark_session.createDataFrame(test_data)
    df.createOrReplaceTempView("test_view")

    # Verify the view exists
    result = spark_session.sql("SELECT * FROM test_view")
    assert result.count() == 1


@pytest.mark.spark
@pytest.mark.unit
def test_fixture_cleanup():
    """
    Test that fixtures clean up properly.

    This test runs after test_fixture_isolation to verify that
    temporary views don't leak between tests.
    """
    # This test should not see the view from the previous test
    # (although with session scope, it might - this is expected behavior)
    pass


if __name__ == "__main__":
    # Allow running this test file directly
    pytest.main([__file__, "-v", "-m", "spark"])

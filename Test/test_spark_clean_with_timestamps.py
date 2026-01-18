"""
Test suite for SparkCleanWithTimestamps.

This test suite validates the PySpark implementation of datetime parsing and
temporal feature extraction, ensuring it produces equivalent results to the
pandas-based CleanWithTimestamps implementation.

Tests cover:
- Timestamp parsing with native PySpark functions
- Temporal feature extraction (year, month, day, hour, minute, second, pm)
- Integration with SparkConnectedDrivingCleaner base class
- Equivalence with pandas implementation
- Edge cases (AM/PM boundary, month/year boundaries, null handling)
- Performance characteristics
"""

import pytest
import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, minute, second
from pyspark.sql.types import TimestampType

from Generator.Cleaners.SparkCleanWithTimestamps import SparkCleanWithTimestamps
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.PathProvider import PathProvider


@pytest.fixture
def setup_context_provider(temp_spark_dir, cache_dir):
    """Set up GeneratorContextProvider with test configuration for timestamp cleaning."""
    # Setup PathProvider for Logger (required by dependency injection)
    PathProvider(
        model="test_timestamps",
        contexts={
            "Logger.logpath": lambda model: os.path.join(temp_spark_dir, "logs", "test.log"),
        }
    )

    # Setup GeneratorPathProvider for cleaner paths
    GeneratorPathProvider(
        model="test_timestamps",
        contexts={
            "FileCache.filepath": lambda model: os.path.join(cache_dir, f"{model}_cache"),
        }
    )

    provider = GeneratorContextProvider()

    # Basic configuration
    provider.add("DataGatherer.numrows", 1000)
    provider.add("ConnectedDrivingCleaner.cleanParams", "test_timestamps")
    provider.add("ConnectedDrivingCleaner.filename", "test_timestamps_clean.parquet")
    provider.add("ConnectedDrivingCleaner.isXYCoords", False)
    provider.add("ConnectedDrivingCleaner.shouldGatherAutomatically", False)

    # Origin coordinates for XY conversion (Wyoming center)
    provider.add("ConnectedDrivingCleaner.x_pos", 41.25)
    provider.add("ConnectedDrivingCleaner.y_pos", -105.93)

    # Column selection for cleaning (must include metadata_generatedAt for timestamps)
    # Note: Using correct schema field names (accelset, not accelSet)
    provider.add("ConnectedDrivingCleaner.columns", [
        "metadata_generatedAt",
        "coreData_id",
        "coreData_secMark",
        "coreData_position_lat",
        "coreData_position_long",
        "coreData_accuracy_semiMajor",
        "coreData_accuracy_semiMinor",
        "coreData_elevation",
        "coreData_accelset_accelYaw",  # lowercase "set" to match schema
        "coreData_speed",
        "coreData_heading",
        "coreData_position"
    ])

    yield provider

    # Cleanup - reset singletons
    try:
        PathProvider._instance = None
        GeneratorPathProvider._instance = None
        GeneratorContextProvider._instance = None
    except AttributeError:
        pass


@pytest.fixture
def sample_timestamp_df(spark_session):
    """
    Create sample BSM DataFrame with diverse timestamps for testing.

    Includes:
    - AM and PM times
    - Different months, days, hours
    - Midnight and noon boundary cases
    - Sequential timestamps
    """
    data = [
        {
            "metadata_generatedAt": "04/06/2021 10:30:00 AM",
            "coreData_id": "A1B2C3D4",
            "coreData_secMark": 30000,
            "coreData_position_lat": 41.25,
            "coreData_position_long": -105.93,
            "coreData_accuracy_semiMajor": 5.0,
            "coreData_accuracy_semiMinor": 5.0,
            "coreData_elevation": 2194.0,
            "coreData_accelset_accelYaw": 0.0,
            "coreData_speed": 15.5,
            "coreData_heading": 90.0,
            "coreData_position": "POINT (-105.93 41.25)"
        },
        {
            "metadata_generatedAt": "04/06/2021 02:45:30 PM",
            "coreData_id": "B2C3D4E5",
            "coreData_secMark": 31000,
            "coreData_position_lat": 41.26,
            "coreData_position_long": -105.94,
            "coreData_accuracy_semiMajor": 6.0,
            "coreData_accuracy_semiMinor": 6.0,
            "coreData_elevation": 2195.0,
            "coreData_accelset_accelYaw": 0.5,
            "coreData_speed": 25.0,
            "coreData_heading": 85.0,
            "coreData_position": "POINT (-105.94 41.26)"
        },
        {
            "metadata_generatedAt": "12/31/2020 11:59:59 PM",
            "coreData_id": "C3D4E5F6",
            "coreData_secMark": 32000,
            "coreData_position_lat": 41.27,
            "coreData_position_long": -105.95,
            "coreData_accuracy_semiMajor": 4.5,
            "coreData_accuracy_semiMinor": 4.5,
            "coreData_elevation": 2196.0,
            "coreData_accelset_accelYaw": -0.3,
            "coreData_speed": 18.0,
            "coreData_heading": 92.0,
            "coreData_position": "POINT (-105.95 41.27)"
        },
        {
            "metadata_generatedAt": "01/01/2021 12:00:00 AM",
            "coreData_id": "D4E5F6A7",
            "coreData_secMark": 33000,
            "coreData_position_lat": 41.28,
            "coreData_position_long": -105.96,
            "coreData_accuracy_semiMajor": 7.0,
            "coreData_accuracy_semiMinor": 7.0,
            "coreData_elevation": 2197.0,
            "coreData_accelset_accelYaw": 1.0,
            "coreData_speed": 30.0,
            "coreData_heading": 88.0,
            "coreData_position": "POINT (-105.96 41.28)"
        },
        {
            "metadata_generatedAt": "07/31/2019 12:41:59 PM",
            "coreData_id": "E5F6A7B8",
            "coreData_secMark": 34000,
            "coreData_position_lat": 41.29,
            "coreData_position_long": -105.97,
            "coreData_accuracy_semiMajor": 5.5,
            "coreData_accuracy_semiMinor": 5.5,
            "coreData_elevation": 2198.0,
            "coreData_accelset_accelYaw": 0.2,
            "coreData_speed": 12.0,
            "coreData_heading": 91.0,
            "coreData_position": "POINT (-105.97 41.29)"
        },
    ]

    from Schemas.BSMRawSchema import get_bsm_raw_schema
    schema = get_bsm_raw_schema(use_timestamp_type=False)
    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture
def cache_dir(tmp_path):
    """Create a temporary cache directory for Parquet files."""
    cache_path = tmp_path / "cache"
    cache_path.mkdir(exist_ok=True)
    yield str(cache_path)


class TestSparkCleanWithTimestampsBasic:
    """Basic functionality tests for SparkCleanWithTimestamps."""

    def test_initialization(self, setup_context_provider, sample_timestamp_df):
        """Test that SparkCleanWithTimestamps initializes correctly."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)

        assert cleaner is not None
        assert isinstance(cleaner, SparkCleanWithTimestamps)
        assert cleaner.data is not None
        assert isinstance(cleaner.data, DataFrame)

    def test_clean_data_with_timestamps_returns_self(self, setup_context_provider, sample_timestamp_df):
        """Test that clean_data_with_timestamps returns self for method chaining."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        result = cleaner.clean_data_with_timestamps()

        assert result is cleaner

    def test_cleaned_data_is_dataframe(self, setup_context_provider, sample_timestamp_df):
        """Test that cleaned data is a valid PySpark DataFrame."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        assert cleaned is not None
        assert isinstance(cleaned, DataFrame)

    def test_drops_null_values(self, spark_session, setup_context_provider):
        """Test that null values are dropped during cleaning."""
        # Create DataFrame with nulls
        data = [
            {
                "metadata_generatedAt": "04/06/2021 10:30:00 AM",
                "coreData_id": "A1B2C3D4",
                "coreData_secMark": 30000,
                "coreData_position_lat": 41.25,
                "coreData_position_long": -105.93,
                "coreData_accuracy_semiMajor": 5.0,
                "coreData_accuracy_semiMinor": 5.0,
                "coreData_elevation": 2194.0,
                "coreData_accelset_accelYaw": 0.0,
                "coreData_speed": 15.5,
                "coreData_heading": 90.0,
                "coreData_position": "POINT (-105.93 41.25)"
            },
            {
                "metadata_generatedAt": None,  # NULL timestamp
                "coreData_id": "B2C3D4E5",
                "coreData_secMark": 31000,
                "coreData_position_lat": 41.26,
                "coreData_position_long": -105.94,
                "coreData_accuracy_semiMajor": None,  # NULL value
                "coreData_accuracy_semiMinor": 6.0,
                "coreData_elevation": 2195.0,
                "coreData_accelset_accelYaw": 0.5,
                "coreData_speed": 25.0,
                "coreData_heading": 85.0,
                "coreData_position": "POINT (-105.94 41.26)"
            },
        ]

        from Schemas.BSMRawSchema import get_bsm_raw_schema
        schema = get_bsm_raw_schema(use_timestamp_type=False)
        df_with_nulls = spark_session.createDataFrame(data, schema=schema)

        cleaner = SparkCleanWithTimestamps(data=df_with_nulls)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Only 1 row should remain (the one without nulls)
        assert cleaned.count() == 1


class TestDatetimeParsing:
    """Tests for timestamp parsing functionality."""

    def test_timestamp_column_converted_to_timestamp_type(self, setup_context_provider, sample_timestamp_df):
        """Test that metadata_generatedAt is converted to TimestampType."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Check that timestamp column exists and is TimestampType
        timestamp_field = [f for f in cleaned.schema.fields if f.name == "metadata_generatedAt"][0]
        assert isinstance(timestamp_field.dataType, TimestampType)

    def test_timestamp_parsing_am_format(self, setup_context_provider, sample_timestamp_df):
        """Test that AM timestamps are parsed correctly."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Filter to AM timestamp: "04/06/2021 10:30:00 AM"
        am_row = cleaned.filter(col("coreData_id") == "A1B2C3D4").collect()[0]

        assert am_row["year"] == 2021
        assert am_row["month"] == 4
        assert am_row["day"] == 6
        assert am_row["hour"] == 10
        assert am_row["minute"] == 30
        assert am_row["second"] == 0
        assert am_row["pm"] == 0  # AM = 0

    def test_timestamp_parsing_pm_format(self, setup_context_provider, sample_timestamp_df):
        """Test that PM timestamps are parsed correctly."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Filter to PM timestamp: "04/06/2021 02:45:30 PM" (14:45:30 in 24-hour)
        pm_row = cleaned.filter(col("coreData_id") == "B2C3D4E5").collect()[0]

        assert pm_row["year"] == 2021
        assert pm_row["month"] == 4
        assert pm_row["day"] == 6
        assert pm_row["hour"] == 14  # 2 PM = 14:00
        assert pm_row["minute"] == 45
        assert pm_row["second"] == 30
        assert pm_row["pm"] == 1  # PM = 1

    def test_midnight_boundary(self, setup_context_provider, sample_timestamp_df):
        """Test midnight (12:00 AM) is correctly parsed as hour 0."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Filter to midnight: "01/01/2021 12:00:00 AM"
        midnight_row = cleaned.filter(col("coreData_id") == "D4E5F6A7").collect()[0]

        assert midnight_row["year"] == 2021
        assert midnight_row["month"] == 1
        assert midnight_row["day"] == 1
        assert midnight_row["hour"] == 0  # Midnight = hour 0
        assert midnight_row["minute"] == 0
        assert midnight_row["second"] == 0
        assert midnight_row["pm"] == 0  # AM = 0

    def test_noon_boundary(self, setup_context_provider, sample_timestamp_df):
        """Test noon (12:XX PM) is correctly parsed as hour 12."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Filter to noon+: "07/31/2019 12:41:59 PM"
        noon_row = cleaned.filter(col("coreData_id") == "E5F6A7B8").collect()[0]

        assert noon_row["year"] == 2019
        assert noon_row["month"] == 7
        assert noon_row["day"] == 31
        assert noon_row["hour"] == 12  # Noon = hour 12
        assert noon_row["minute"] == 41
        assert noon_row["second"] == 59
        assert noon_row["pm"] == 1  # PM = 1

    def test_year_boundary(self, setup_context_provider, sample_timestamp_df):
        """Test year boundary (Dec 31 to Jan 1) is handled correctly."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Dec 31, 2020: "12/31/2020 11:59:59 PM"
        dec31_row = cleaned.filter(col("coreData_id") == "C3D4E5F6").collect()[0]
        assert dec31_row["year"] == 2020
        assert dec31_row["month"] == 12
        assert dec31_row["day"] == 31
        assert dec31_row["hour"] == 23
        assert dec31_row["pm"] == 1

        # Jan 1, 2021: "01/01/2021 12:00:00 AM"
        jan1_row = cleaned.filter(col("coreData_id") == "D4E5F6A7").collect()[0]
        assert jan1_row["year"] == 2021
        assert jan1_row["month"] == 1
        assert jan1_row["day"] == 1
        assert jan1_row["hour"] == 0
        assert jan1_row["pm"] == 0


class TestTemporalFeatures:
    """Tests for temporal feature extraction."""

    def test_all_temporal_columns_created(self, setup_context_provider, sample_timestamp_df):
        """Test that all temporal feature columns are created."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        expected_columns = ["month", "day", "year", "hour", "minute", "second", "pm"]
        for col_name in expected_columns:
            assert col_name in cleaned.columns, f"Missing temporal column: {col_name}"

    def test_temporal_feature_types(self, setup_context_provider, sample_timestamp_df):
        """Test that temporal features have correct data types (integers)."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # All temporal features should be IntegerType
        schema_dict = {f.name: str(f.dataType) for f in cleaned.schema.fields}

        temporal_columns = ["month", "day", "year", "hour", "minute", "second", "pm"]
        for col_name in temporal_columns:
            assert "IntegerType" in schema_dict[col_name], \
                f"{col_name} should be IntegerType, got {schema_dict[col_name]}"

    def test_month_range(self, setup_context_provider, sample_timestamp_df):
        """Test that month values are in valid range (1-12)."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        months = [row["month"] for row in cleaned.select("month").collect()]
        assert all(1 <= m <= 12 for m in months), f"Invalid month values: {months}"

    def test_day_range(self, setup_context_provider, sample_timestamp_df):
        """Test that day values are in valid range (1-31)."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        days = [row["day"] for row in cleaned.select("day").collect()]
        assert all(1 <= d <= 31 for d in days), f"Invalid day values: {days}"

    def test_hour_range(self, setup_context_provider, sample_timestamp_df):
        """Test that hour values are in valid range (0-23)."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        hours = [row["hour"] for row in cleaned.select("hour").collect()]
        assert all(0 <= h <= 23 for h in hours), f"Invalid hour values: {hours}"

    def test_pm_values(self, setup_context_provider, sample_timestamp_df):
        """Test that pm indicator is 0 for AM and 1 for PM."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Check that pm is binary (0 or 1)
        pm_values = [row["pm"] for row in cleaned.select("pm").collect()]
        assert all(pm in [0, 1] for pm in pm_values), f"pm should be 0 or 1: {pm_values}"

        # Verify specific cases
        am_rows = cleaned.filter(col("hour") < 12).collect()
        for row in am_rows:
            assert row["pm"] == 0, f"AM hour {row['hour']} should have pm=0"

        pm_rows = cleaned.filter(col("hour") >= 12).collect()
        for row in pm_rows:
            assert row["pm"] == 1, f"PM hour {row['hour']} should have pm=1"


class TestColumnOperations:
    """Tests for column selection and dropping."""

    def test_position_columns_dropped(self, setup_context_provider, sample_timestamp_df):
        """Test that original position columns are dropped after parsing."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        dropped_columns = ["coreData_position", "coreData_position_lat", "coreData_position_long"]
        for col_name in dropped_columns:
            assert col_name not in cleaned.columns, \
                f"Column {col_name} should be dropped but still exists"

    def test_xy_coordinates_extracted(self, setup_context_provider, sample_timestamp_df):
        """Test that x_pos and y_pos are extracted from POINT geometry."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        assert "x_pos" in cleaned.columns
        assert "y_pos" in cleaned.columns

        # Verify x_pos and y_pos are numeric
        sample_row = cleaned.select("x_pos", "y_pos").first()
        assert isinstance(sample_row["x_pos"], float)
        assert isinstance(sample_row["y_pos"], float)


class TestCachingBehavior:
    """Tests for ParquetCache decorator functionality."""

    def test_parquet_cache_creates_file(self, setup_context_provider, sample_timestamp_df, cache_dir):
        """Test that ParquetCache creates cached Parquet files."""
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()

        # Check that cache directory has Parquet files
        cache_files = os.listdir(cache_dir)
        parquet_files = [f for f in cache_files if "parquet" in f.lower()]

        # Should have at least one Parquet file or directory
        assert len(parquet_files) > 0, "ParquetCache should create cached files"

    def test_cache_reuse(self, setup_context_provider, sample_timestamp_df, cache_dir):
        """Test that cache is reused on subsequent calls."""
        # First call - creates cache
        cleaner1 = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner1.clean_data_with_timestamps()
        first_result = cleaner1.get_cleaned_data()
        first_count = first_result.count()

        # Second call - should use cache
        cleaner2 = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner2.clean_data_with_timestamps()
        second_result = cleaner2.get_cleaned_data()
        second_count = second_result.count()

        # Results should be identical
        assert first_count == second_count


class TestIntegration:
    """Integration tests with real-world scenarios."""

    def test_end_to_end_cleaning(self, setup_context_provider, sample_timestamp_df):
        """Test complete end-to-end cleaning workflow."""
        # Create cleaner and run full pipeline
        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Verify output
        assert cleaned.count() > 0
        assert "x_pos" in cleaned.columns
        assert "y_pos" in cleaned.columns
        assert "month" in cleaned.columns
        assert "metadata_generatedAt" in cleaned.columns

    def test_xy_coordinate_conversion(self, setup_context_provider, sample_timestamp_df):
        """Test XY coordinate conversion when isXYCoords=True."""
        # Reconfigure for XY conversion
        provider = GeneratorContextProvider()
        provider.add("ConnectedDrivingCleaner.isXYCoords", True)

        cleaner = SparkCleanWithTimestamps(data=sample_timestamp_df)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # x_pos and y_pos should be distances from origin (not raw lat/long)
        sample_row = cleaned.select("x_pos", "y_pos").first()

        # After conversion, values should be distance in meters (likely < 100000)
        # Original lat/long are ~41 and -105, which would be preserved if NOT converted
        # Converted values should be much smaller (meters from origin)
        assert sample_row["x_pos"] != -105.93, "x_pos should be converted to distance"
        assert sample_row["y_pos"] != 41.25, "y_pos should be converted to distance"


class TestPerformance:
    """Performance and scalability tests."""

    def test_performance_on_medium_dataset(self, spark_session, setup_context_provider, medium_bsm_dataset):
        """Test performance on 1000 row dataset."""
        import time

        start_time = time.time()

        cleaner = SparkCleanWithTimestamps(data=medium_bsm_dataset)
        cleaner.clean_data_with_timestamps()
        cleaned = cleaner.get_cleaned_data()

        # Force execution with action
        count = cleaned.count()

        duration = time.time() - start_time

        # Should complete in reasonable time (< 30 seconds on test hardware)
        assert duration < 30.0, f"Cleaning took too long: {duration:.2f}s"
        assert count > 0, "Should produce non-empty output"

"""
Tests for sample BSM datasets in Test/Data/.

Validates that generated sample datasets:
- Have correct number of rows
- Follow BSMRawSchema structure
- Can be loaded by both pandas and PySpark
- Contain realistic data values
"""

import pytest
import os
import pandas as pd
from pyspark.sql import SparkSession


# Dataset specifications
DATASETS = [
    ('sample_1k.csv', 1000),
    ('sample_10k.csv', 10000),
    ('sample_100k.csv', 100000)
]


@pytest.mark.data
@pytest.mark.parametrize("filename,expected_rows", DATASETS)
def test_dataset_exists(filename, expected_rows):
    """Verify sample dataset files exist."""
    data_dir = os.path.join('Test', 'Data')
    filepath = os.path.join(data_dir, filename)
    assert os.path.exists(filepath), f"Dataset {filename} not found"


@pytest.mark.data
@pytest.mark.pandas
@pytest.mark.parametrize("filename,expected_rows", DATASETS)
def test_pandas_load(filename, expected_rows):
    """Test loading datasets with pandas."""
    filepath = os.path.join('Test', 'Data', filename)

    df = pd.read_csv(filepath)

    # Check row count
    assert len(df) == expected_rows, f"Expected {expected_rows} rows, got {len(df)}"

    # Check column count (19 columns per BSMRawSchema)
    assert len(df.columns) == 19, f"Expected 19 columns, got {len(df.columns)}"

    # Check required columns exist
    required_cols = [
        'metadata_generatedAt',
        'coreData_id',
        'coreData_position_lat',
        'coreData_position_long',
        'coreData_speed',
        'coreData_heading'
    ]
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"

    # Check no null values in critical columns
    assert df['coreData_id'].notna().all(), "Null values in coreData_id"
    assert df['coreData_position_lat'].notna().all(), "Null values in latitude"
    assert df['coreData_position_long'].notna().all(), "Null values in longitude"


@pytest.mark.data
@pytest.mark.pyspark
@pytest.mark.parametrize("filename,expected_rows", DATASETS[:1])  # Only test 1k for speed
def test_pyspark_load(spark_session, filename, expected_rows):
    """Test loading datasets with PySpark."""
    from Schemas.BSMRawSchema import get_bsm_raw_schema

    filepath = os.path.join('Test', 'Data', filename)
    schema = get_bsm_raw_schema()

    df = spark_session.read.schema(schema).csv(filepath, header=True)

    # Check row count
    assert df.count() == expected_rows, f"Expected {expected_rows} rows, got {df.count()}"

    # Check column count
    assert len(df.columns) == 19, f"Expected 19 columns, got {len(df.columns)}"

    # Check required columns exist
    required_cols = [
        'metadata_generatedAt',
        'coreData_id',
        'coreData_position_lat',
        'coreData_position_long',
        'coreData_speed',
        'coreData_heading'
    ]
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"


@pytest.mark.data
@pytest.mark.validation
def test_data_quality_sample_1k():
    """Validate data quality of 1k sample dataset."""
    filepath = os.path.join('Test', 'Data', 'sample_1k.csv')
    df = pd.read_csv(filepath)

    # Check speed range (should be 0-45 m/s)
    assert df['coreData_speed'].min() >= 0, "Speed below 0"
    assert df['coreData_speed'].max() <= 45, "Speed above 45 m/s"

    # Check heading range (should be 0-359 degrees)
    assert df['coreData_heading'].min() >= 0, "Heading below 0"
    assert df['coreData_heading'].max() < 360, "Heading >= 360"

    # Check latitude/longitude in Wyoming range
    assert df['coreData_position_lat'].min() > 40, "Latitude too far south"
    assert df['coreData_position_lat'].max() < 45, "Latitude too far north"
    assert df['coreData_position_long'].min() > -108, "Longitude too far west"
    assert df['coreData_position_long'].max() < -104, "Longitude too far east"

    # Check elevation in Wyoming range (1000-2000m typical)
    assert df['coreData_elevation'].min() > 1000, "Elevation too low"
    assert df['coreData_elevation'].max() < 2000, "Elevation too high"

    # Check WKT POINT format
    sample_point = df['coreData_position'].iloc[0]
    assert sample_point.startswith('POINT ('), "Invalid WKT POINT format"
    assert sample_point.endswith(')'), "Invalid WKT POINT format"


@pytest.mark.data
@pytest.mark.validation
def test_temporal_progression():
    """Verify temporal progression is sequential."""
    filepath = os.path.join('Test', 'Data', 'sample_1k.csv')
    df = pd.read_csv(filepath)

    # Parse timestamps
    df['timestamp'] = pd.to_datetime(
        df['metadata_generatedAt'],
        format='%m/%d/%Y %I:%M:%S %p'
    )

    # Check timestamps are sequential (1 second intervals)
    time_diffs = df['timestamp'].diff()[1:]  # Skip first NaT
    assert (time_diffs == pd.Timedelta(seconds=1)).all(), "Timestamps not sequential"


@pytest.mark.data
@pytest.mark.validation
def test_vehicle_stream_distribution():
    """Verify vehicle streams are distributed correctly."""
    filepath = os.path.join('Test', 'Data', 'sample_1k.csv')
    df = pd.read_csv(filepath)

    # Count unique vehicle IDs
    unique_vehicles = df['coreData_id'].nunique()

    # For 1k dataset, expect ~10 unique streams
    assert unique_vehicles >= 5, "Too few unique vehicle streams"
    assert unique_vehicles <= 20, "Too many unique vehicle streams"

    # Each vehicle should have multiple messages
    messages_per_vehicle = df.groupby('coreData_id').size()
    assert messages_per_vehicle.min() >= 1, "Vehicles with no messages"
    assert messages_per_vehicle.max() <= 200, "Too many messages per vehicle"


if __name__ == '__main__':
    # Run tests for this module
    pytest.main([__file__, '-v', '-m', 'data'])

"""
PySpark schema definition for processed BSM (Basic Safety Message) data.

This module defines the StructType schema for the 18 processed/ML feature columns
that result from cleaning and feature engineering of raw BSM data.

The processed data includes:
- Retained raw columns (4): elevation, speed, heading, id (converted to decimal)
- Computed position columns (2): x_pos, y_pos
- Extracted temporal features (7): month, day, year, hour, minute, second, pm
- Target variable (1): isAttacker
- Removed: all metadata columns, lat/long, position WKT string, accuracy fields

Usage:
    from pyspark.sql import SparkSession
    from Schemas.BSMProcessedSchema import get_bsm_processed_schema

    spark = SparkSession.builder.getOrCreate()
    schema = get_bsm_processed_schema()
    df = spark.read.schema(schema).parquet("path/to/processed_data.parquet")
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
)


def get_bsm_processed_schema():
    """
    Get the PySpark StructType schema for processed BSM data.

    This schema defines the 18 columns used for ML training after cleaning,
    feature engineering, and attack simulation.

    Returns:
        StructType: PySpark schema definition for processed BSM data

    Column Details:
        Retained from Raw (8 columns):
        - coreData_id: Vehicle ID converted from hex to decimal (integer)
        - coreData_secMark: Second mark/timestamp marker (integer)
        - coreData_accuracy_semiMajor: Positional accuracy semi-major axis (double)
        - coreData_accuracy_semiMinor: Positional accuracy semi-minor axis (double)
        - coreData_elevation: Vehicle elevation/altitude in meters (double)
        - coreData_accelset_accelYaw: Acceleration yaw component (double)
        - coreData_speed: Vehicle speed in m/s (double)
        - coreData_heading: Vehicle heading in degrees (double)

        Computed Position (2 columns):
        - x_pos: X coordinate in meters (double)
        - y_pos: Y coordinate in meters (double)

        Temporal Features (7 columns):
        - month: Month extracted from timestamp (1-12, integer)
        - day: Day of month extracted from timestamp (1-31, integer)
        - year: Year extracted from timestamp (integer)
        - hour: Hour extracted from timestamp (0-23, integer)
        - minute: Minute extracted from timestamp (0-59, integer)
        - second: Second extracted from timestamp (0-59, integer)
        - pm: AM/PM indicator (0=AM, 1=PM, integer)

        Target Variable (1 column):
        - isAttacker: Binary label for attack simulation (0=legitimate, 1=attack, integer)

    Note:
        Total of 18 columns matching the "EXTTimestampsCols" pipeline configuration.
        Some pipelines use subsets of these columns (e.g., "ONLYXYELEVCols" uses only
        x_pos, y_pos, coreData_elevation, and isAttacker).
    """
    # TODO: Implement in Task 1.5
    # This is a placeholder - full implementation coming next

    schema = StructType([
        # Retained from raw data (7 columns)
        StructField("coreData_id", IntegerType(), nullable=False),  # Converted from hex
        StructField("coreData_secMark", IntegerType(), nullable=True),
        StructField("coreData_accuracy_semiMajor", DoubleType(), nullable=True),
        StructField("coreData_accuracy_semiMinor", DoubleType(), nullable=True),
        StructField("coreData_elevation", DoubleType(), nullable=True),
        StructField("coreData_accelset_accelYaw", DoubleType(), nullable=True),
        StructField("coreData_speed", DoubleType(), nullable=True),
        StructField("coreData_heading", DoubleType(), nullable=True),

        # Computed position columns (2 columns)
        StructField("x_pos", DoubleType(), nullable=True),
        StructField("y_pos", DoubleType(), nullable=True),

        # Temporal features extracted from metadata_generatedAt (7 columns)
        StructField("month", IntegerType(), nullable=True),
        StructField("day", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("hour", IntegerType(), nullable=True),
        StructField("minute", IntegerType(), nullable=True),
        StructField("second", IntegerType(), nullable=True),
        StructField("pm", IntegerType(), nullable=True),  # 0=AM, 1=PM

        # Target variable (1 column)
        StructField("isAttacker", IntegerType(), nullable=False),  # 0=legitimate, 1=attack
    ])

    return schema


def get_bsm_processed_column_names():
    """
    Get list of all processed BSM column names in order.

    Returns:
        list[str]: List of 18 column names
    """
    return [
        # Retained columns (8)
        "coreData_id",
        "coreData_secMark",
        "coreData_accuracy_semiMajor",
        "coreData_accuracy_semiMinor",
        "coreData_elevation",
        "coreData_accelset_accelYaw",
        "coreData_speed",
        "coreData_heading",
        # Position columns (2)
        "x_pos",
        "y_pos",
        # Temporal features (7)
        "month",
        "day",
        "year",
        "hour",
        "minute",
        "second",
        "pm",
        # Target variable (1)
        "isAttacker",
    ]


def get_feature_columns():
    """
    Get list of feature column names (excluding target variable).

    Returns:
        list[str]: List of 17 feature column names
    """
    return [col for col in get_bsm_processed_column_names() if col != "isAttacker"]


def get_ml_feature_columns():
    """
    Get list of ML feature column names commonly used for training.

    Based on analysis of pipeline files, common feature sets include:
    - all_features: All 17 features (used in EXTTimestampsCols pipelines)
    - xy_elev_heading_speed: x_pos, y_pos, elevation, heading, speed (ONLYXYELEVCOLS + heading/speed)
    - xy_elev: x_pos, y_pos, elevation (ONLYXYELEVCols pipelines)
    - position_only: x_pos, y_pos
    - temporal_only: month, day, year, hour, minute, second, pm

    Returns:
        dict: Dictionary with different feature column sets
    """
    return {
        "all_features": get_feature_columns(),
        "xy_elev_heading_speed": [
            "x_pos",
            "y_pos",
            "coreData_elevation",
            "coreData_heading",
            "coreData_speed",
        ],
        "xy_elev": [
            "x_pos",
            "y_pos",
            "coreData_elevation",
        ],
        "position_only": ["x_pos", "y_pos"],
        "temporal_only": ["month", "day", "year", "hour", "minute", "second", "pm"],
    }


if __name__ == "__main__":
    # Example usage and schema display
    schema = get_bsm_processed_schema()
    print("BSM Processed Data Schema:")
    print("=" * 80)
    print(schema)
    print()

    print("\nSchema Fields:")
    print("=" * 80)
    for field in schema.fields:
        print(f"{field.name:40s} {str(field.dataType):20s} nullable={field.nullable}")

    print("\nColumn Names:")
    print("=" * 80)
    for i, col in enumerate(get_bsm_processed_column_names(), 1):
        print(f"{i:2d}. {col}")

    print(f"\nTotal columns: {len(get_bsm_processed_column_names())}")
    print(f"Feature columns: {len(get_feature_columns())}")

    print("\nML Feature Sets:")
    print("=" * 80)
    ml_features = get_ml_feature_columns()
    for name, cols in ml_features.items():
        print(f"\n{name}: {len(cols)} columns")
        print(f"  {', '.join(cols)}")

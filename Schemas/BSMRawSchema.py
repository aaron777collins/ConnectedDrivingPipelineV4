"""
PySpark schema definition for raw BSM (Basic Safety Message) data.

This module defines the StructType schema for the 19 raw BSM data columns
as specified in the J2735 ASN.1 standard and Wyoming CV Pilot BSM datasets.

The raw BSM data includes:
- 8 metadata columns (generatedAt, recordType, serialId fields, receivedAt)
- 11 coreData columns (id, position, speed, heading, elevation, etc.)

Usage:
    from pyspark.sql import SparkSession
    from Schemas.BSMRawSchema import get_bsm_raw_schema

    spark = SparkSession.builder.getOrCreate()
    schema = get_bsm_raw_schema()
    df = spark.read.schema(schema).csv("path/to/bsm_data.csv", header=True)
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)


def get_bsm_raw_schema(use_timestamp_type=False):
    """
    Get the PySpark StructType schema for raw BSM data.

    This schema defines the 19 raw columns from Wyoming CV Pilot BSM datasets,
    including metadata and coreData fields from the J2735 BSM standard.

    Args:
        use_timestamp_type (bool): If True, use TimestampType for timestamp columns.
                                   If False (default), use StringType and convert later.
                                   Default is False to match pandas behavior and avoid
                                   parsing errors during initial CSV read.

    Returns:
        StructType: PySpark schema definition for raw BSM data

    Column Details:
        Metadata Fields (8 columns):
        - metadata_generatedAt: Timestamp when message was generated (format: "MM/dd/yyyy hh:mm:ss a")
        - metadata_recordType: Type of record (string)
        - metadata_serialId_streamId: Stream identifier (string)
        - metadata_serialId_bundleSize: Bundle size indicator (integer)
        - metadata_serialId_bundleId: Bundle identifier (integer)
        - metadata_serialId_recordId: Record ID within bundle (integer)
        - metadata_serialId_serialNumber: Serial number (integer)
        - metadata_receivedAt: Timestamp when message was received (format: "MM/dd/yyyy hh:mm:ss a")

        Core Data Fields (11 columns):
        - coreData_id: Vehicle temporary ID (hexadecimal string, e.g., "0x1a2b3c4d")
        - coreData_secMark: Second mark/timestamp marker (integer)
        - coreData_position_lat: GPS latitude (double)
        - coreData_position_long: GPS longitude (double)
        - coreData_accuracy_semiMajor: Positional accuracy semi-major axis (double)
        - coreData_accuracy_semiMinor: Positional accuracy semi-minor axis (double)
        - coreData_elevation: Vehicle elevation/altitude in meters (double)
        - coreData_accelset_accelYaw: Acceleration yaw component (double)
        - coreData_speed: Vehicle speed in m/s (double)
        - coreData_heading: Vehicle heading in degrees (double)
        - coreData_position: Combined position as WKT POINT string (format: "POINT (longitude latitude)")
    """

    # Choose timestamp type based on parameter
    timestamp_type = TimestampType() if use_timestamp_type else StringType()

    schema = StructType([
        # Metadata fields (8 columns)
        StructField("metadata_generatedAt", timestamp_type, nullable=True),
        StructField("metadata_recordType", StringType(), nullable=True),
        StructField("metadata_serialId_streamId", StringType(), nullable=True),
        StructField("metadata_serialId_bundleSize", IntegerType(), nullable=True),
        StructField("metadata_serialId_bundleId", IntegerType(), nullable=True),
        StructField("metadata_serialId_recordId", IntegerType(), nullable=True),
        StructField("metadata_serialId_serialNumber", IntegerType(), nullable=True),
        StructField("metadata_receivedAt", timestamp_type, nullable=True),

        # Core BSM data fields (11 columns)
        StructField("coreData_id", StringType(), nullable=True),  # Hex string, converted later
        StructField("coreData_secMark", IntegerType(), nullable=True),
        StructField("coreData_position_lat", DoubleType(), nullable=True),
        StructField("coreData_position_long", DoubleType(), nullable=True),
        StructField("coreData_accuracy_semiMajor", DoubleType(), nullable=True),
        StructField("coreData_accuracy_semiMinor", DoubleType(), nullable=True),
        StructField("coreData_elevation", DoubleType(), nullable=True),
        StructField("coreData_accelset_accelYaw", DoubleType(), nullable=True),
        StructField("coreData_speed", DoubleType(), nullable=True),
        StructField("coreData_heading", DoubleType(), nullable=True),
        StructField("coreData_position", StringType(), nullable=True),  # WKT POINT format
    ])

    return schema


def get_bsm_raw_column_names():
    """
    Get list of all raw BSM column names in order.

    Returns:
        list[str]: List of 19 column names
    """
    return [
        # Metadata columns
        "metadata_generatedAt",
        "metadata_recordType",
        "metadata_serialId_streamId",
        "metadata_serialId_bundleSize",
        "metadata_serialId_bundleId",
        "metadata_serialId_recordId",
        "metadata_serialId_serialNumber",
        "metadata_receivedAt",
        # Core data columns
        "coreData_id",
        "coreData_secMark",
        "coreData_position_lat",
        "coreData_position_long",
        "coreData_accuracy_semiMajor",
        "coreData_accuracy_semiMinor",
        "coreData_elevation",
        "coreData_accelset_accelYaw",
        "coreData_speed",
        "coreData_heading",
        "coreData_position",
    ]


def get_metadata_columns():
    """
    Get list of metadata column names.

    Returns:
        list[str]: List of 8 metadata column names
    """
    return [
        "metadata_generatedAt",
        "metadata_recordType",
        "metadata_serialId_streamId",
        "metadata_serialId_bundleSize",
        "metadata_serialId_bundleId",
        "metadata_serialId_recordId",
        "metadata_serialId_serialNumber",
        "metadata_receivedAt",
    ]


def get_coredata_columns():
    """
    Get list of coreData column names.

    Returns:
        list[str]: List of 11 coreData column names
    """
    return [
        "coreData_id",
        "coreData_secMark",
        "coreData_position_lat",
        "coreData_position_long",
        "coreData_accuracy_semiMajor",
        "coreData_accuracy_semiMinor",
        "coreData_elevation",
        "coreData_accelset_accelYaw",
        "coreData_speed",
        "coreData_heading",
        "coreData_position",
    ]


# Constants for timestamp format
TIMESTAMP_FORMAT = "%m/%d/%Y %I:%M:%S %p"  # Example: "07/31/2019 12:41:59 PM"

# Constants for WKT POINT format
WKT_POINT_PATTERN = r"POINT \((-?\d+\.?\d*) (-?\d+\.?\d*)\)"  # Regex pattern for parsing


if __name__ == "__main__":
    # Example usage and schema display
    schema = get_bsm_raw_schema()
    print("BSM Raw Data Schema:")
    print("=" * 80)
    print(schema)
    print()

    print("\nSchema Fields:")
    print("=" * 80)
    for field in schema.fields:
        print(f"{field.name:40s} {str(field.dataType):20s} nullable={field.nullable}")

    print("\nColumn Names:")
    print("=" * 80)
    for i, col in enumerate(get_bsm_raw_column_names(), 1):
        print(f"{i:2d}. {col}")

    print(f"\nTotal columns: {len(get_bsm_raw_column_names())}")
    print(f"Metadata columns: {len(get_metadata_columns())}")
    print(f"CoreData columns: {len(get_coredata_columns())}")

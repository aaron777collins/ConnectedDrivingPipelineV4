"""
Data Conversion UDFs for PySpark.

This module provides User-Defined Functions (UDFs) for data type conversions
used in the ConnectedDriving pipeline, including:
- Hexadecimal to decimal conversion (for coreData_id)
- Future: Direction and distance to XY coordinates
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, LongType, DoubleType, StructType, StructField


@udf(returnType=LongType())
def hex_to_decimal_udf(hex_str):
    """
    Convert a hexadecimal string to a decimal integer.

    This UDF is used to convert coreData_id from hexadecimal format to decimal
    for ML model training. It replicates the behavior of the pandas version's
    convert_large_hex_str_to_hex() function.

    Args:
        hex_str (str): Hexadecimal string, e.g., "0xa1b2c3d4" or "0x1a2b3c4d.0"

    Returns:
        long: Decimal integer, or None if conversion fails

    Handles edge cases:
        - Strips decimal point if present (e.g., "0x1a2b3c4d.0" -> "0x1a2b3c4d")
        - Returns None for None/null inputs
        - Returns None for invalid hex strings

    Example:
        >>> from pyspark.sql.functions import col
        >>> # Convert coreData_id from hex to decimal for ML training
        >>> df = df.withColumn("coreData_id", hex_to_decimal_udf(col("coreData_id")))

        # Before: "0xa1b2c3d4"
        # After:  2713846740

    Pandas Equivalent:
        >>> def convert_large_hex_str_to_hex(num):
        ...     if "." in num:
        ...         num = num.split(".")[0]
        ...     num = int(num, 16)
        ...     return num
        >>> df["coreData_id"] = df["coreData_id"].map(lambda x: convert_large_hex_str_to_hex(x))
    """
    if hex_str is None:
        return None

    try:
        # Handle decimal point in hex string (edge case)
        # Some data sources export hex with ".0" suffix
        if "." in hex_str:
            hex_str = hex_str.split(".")[0]

        # Convert hex string to decimal integer (base 16)
        # Python's int() automatically handles "0x" prefix
        decimal_value = int(hex_str, 16)

        return decimal_value
    except (ValueError, TypeError, AttributeError):
        # ValueError: Invalid hex format
        # TypeError: hex_str is not a string
        # AttributeError: hex_str doesn't support split()
        return None


@udf(returnType=StructType([
    StructField("new_x", DoubleType(), nullable=True),
    StructField("new_y", DoubleType(), nullable=True)
]))
def direction_and_dist_to_xy_udf(x, y, direction, distance):
    """
    Calculate new XY coordinates given a starting point, direction, and distance.

    This UDF is used for positional offset attacks in the attack simulation phase.
    It wraps MathHelper.direction_and_dist_to_XY() for use in PySpark.

    Args:
        x (float): Starting X coordinate (longitude)
        y (float): Starting Y coordinate (latitude)
        direction (int): Direction angle in degrees (0-360)
        distance (float): Distance in meters

    Returns:
        Row with fields:
            new_x (float): New X coordinate after offset
            new_y (float): New Y coordinate after offset
        Returns None if any input is None or invalid

    Example:
        >>> from pyspark.sql.functions import col, lit
        >>> # Apply random offset attack
        >>> df = df.withColumn("offset_coords",
        ...     direction_and_dist_to_xy_udf(
        ...         col("x_pos"),
        ...         col("y_pos"),
        ...         col("attack_direction"),  # Random 0-360
        ...         col("attack_distance")    # Random 100-200
        ...     )
        ... )
        >>> df = df.withColumn("x_pos",
        ...     when(col("isAttacker") == 1, col("offset_coords.new_x"))
        ...     .otherwise(col("x_pos"))
        ... )

    Note:
        This UDF requires MathHelper.direction_and_dist_to_XY() to be implemented.
        It will be completed in Task 3.6.
    """
    if x is None or y is None or direction is None or distance is None:
        return None

    try:
        # Import here to avoid circular dependency
        from Helpers.MathHelper import MathHelper

        # Calculate new coordinates
        new_x, new_y = MathHelper.direction_and_dist_to_XY(x, y, direction, distance)

        return (float(new_x), float(new_y))
    except (ValueError, TypeError, AttributeError, ImportError):
        return None

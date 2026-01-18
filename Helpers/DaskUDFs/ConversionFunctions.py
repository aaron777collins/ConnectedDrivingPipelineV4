"""
Data Conversion Functions for Dask.

This module provides vectorized functions for data type conversions
used in the ConnectedDriving pipeline, including:
- Hexadecimal to decimal conversion (for coreData_id)
- Direction and distance to XY coordinates

Unlike PySpark UDFs, these functions are designed to work directly with
pandas Series within Dask partitions for optimal performance.
"""

from typing import Optional, Tuple
from Helpers.MathHelper import MathHelper


def hex_to_decimal(hex_str: Optional[str]) -> Optional[int]:
    """
    Convert a hexadecimal string to a decimal integer.

    This function is used to convert coreData_id from hexadecimal format to decimal
    for ML model training. It replicates the behavior of the pandas version's
    convert_large_hex_str_to_hex() function.

    Args:
        hex_str (str): Hexadecimal string, e.g., "0xa1b2c3d4" or "0x1a2b3c4d.0"

    Returns:
        int: Decimal integer, or None if conversion fails

    Handles edge cases:
        - Strips decimal point if present (e.g., "0x1a2b3c4d.0" -> "0x1a2b3c4d")
        - Returns None for None/null inputs
        - Returns None for invalid hex strings

    Example:
        >>> import dask.dataframe as dd
        >>> # Convert coreData_id from hex to decimal for ML training
        >>> df['coreData_id'] = df['coreData_id'].apply(
        ...     hex_to_decimal, meta=('coreData_id', 'i8')
        ... )
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


def direction_and_dist_to_xy(
    x: Optional[float],
    y: Optional[float],
    direction: Optional[float],
    distance: Optional[float]
) -> Optional[Tuple[float, float]]:
    """
    Calculate new XY coordinates given a starting point, direction, and distance.

    This function is used for positional offset attacks in the attack simulation phase.
    It wraps MathHelper.direction_and_dist_to_XY() for use in Dask.

    Args:
        x (float): Starting X coordinate (longitude)
        y (float): Starting Y coordinate (latitude)
        direction (float): Direction angle in degrees (0-360)
        distance (float): Distance in meters

    Returns:
        tuple: (new_x, new_y) as (float, float), or None if any input is invalid

    Example:
        >>> import dask.dataframe as dd
        >>> # Apply random offset attack
        >>> df['offset_coords'] = df.apply(
        ...     lambda row: direction_and_dist_to_xy(
        ...         row['x_pos'],
        ...         row['y_pos'],
        ...         row['attack_direction'],  # Random 0-360
        ...         row['attack_distance']    # Random 100-200
        ...     ),
        ...     axis=1,
        ...     meta=('offset_coords', 'object')
        ... )
        >>> # Extract new coordinates for attackers
        >>> df['x_pos'] = df.apply(
        ...     lambda row: row['offset_coords'][0] if row['isAttacker'] == 1 and row['offset_coords'] else row['x_pos'],
        ...     axis=1,
        ...     meta=('x_pos', 'f8')
        ... )
    """
    if x is None or y is None or direction is None or distance is None:
        return None

    try:
        # Calculate new coordinates
        new_x, new_y = MathHelper.direction_and_dist_to_XY(x, y, direction, distance)

        return (float(new_x), float(new_y))
    except (ValueError, TypeError, AttributeError):
        return None

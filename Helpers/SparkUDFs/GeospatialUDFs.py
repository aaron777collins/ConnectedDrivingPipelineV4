"""
Geospatial UDFs for PySpark.

This module provides User-Defined Functions (UDFs) for geospatial operations
used in the ConnectedDriving pipeline, including:
- WKT POINT string parsing
- Geodesic distance calculations
- Euclidean distance calculations
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType, StructType, StructField
from Helpers.DataConverter import DataConverter
from Helpers.MathHelper import MathHelper


@udf(returnType=StructType([
    StructField("x", DoubleType(), nullable=True),
    StructField("y", DoubleType(), nullable=True)
]))
def point_to_tuple_udf(point_str):
    """
    Convert a WKT POINT string to a tuple of (x, y) coordinates.

    This UDF wraps the DataConverter.point_to_tuple() method for use in PySpark.

    Args:
        point_str (str): WKT POINT format string, e.g., "POINT (-104.6744332 41.1509182)"

    Returns:
        Row with fields:
            x (float): Longitude (first coordinate)
            y (float): Latitude (second coordinate)
        Returns None if point_str is None or invalid

    Example:
        >>> from pyspark.sql.functions import col
        >>> df = df.withColumn("coords", point_to_tuple_udf(col("coreData_position")))
        >>> df = df.withColumn("x_pos", col("coords.x"))
        >>> df = df.withColumn("y_pos", col("coords.y"))
    """
    if point_str is None:
        return None

    try:
        coords = DataConverter.point_to_tuple(point_str)
        return (float(coords[0]), float(coords[1]))
    except (ValueError, IndexError, AttributeError):
        return None


@udf(returnType=DoubleType())
def point_to_x_udf(point_str):
    """
    Extract the X coordinate (longitude) from a WKT POINT string.

    Args:
        point_str (str): WKT POINT format string, e.g., "POINT (-104.6744332 41.1509182)"

    Returns:
        float: Longitude (first coordinate), or None if invalid

    Example:
        >>> df = df.withColumn("x_pos", point_to_x_udf(col("coreData_position")))
    """
    if point_str is None:
        return None

    try:
        coords = DataConverter.point_to_tuple(point_str)
        return float(coords[0])
    except (ValueError, IndexError, AttributeError):
        return None


@udf(returnType=DoubleType())
def point_to_y_udf(point_str):
    """
    Extract the Y coordinate (latitude) from a WKT POINT string.

    Args:
        point_str (str): WKT POINT format string, e.g., "POINT (-104.6744332 41.1509182)"

    Returns:
        float: Latitude (second coordinate), or None if invalid

    Example:
        >>> df = df.withColumn("y_pos", point_to_y_udf(col("coreData_position")))
    """
    if point_str is None:
        return None

    try:
        coords = DataConverter.point_to_tuple(point_str)
        return float(coords[1])
    except (ValueError, IndexError, AttributeError):
        return None


@udf(returnType=DoubleType())
def geodesic_distance_udf(lat1, lon1, lat2, lon2):
    """
    Calculate geodesic distance between two lat/long points using WGS84 ellipsoid.

    This UDF wraps MathHelper.dist_between_two_points() for use in PySpark.

    Args:
        lat1 (float): Latitude of first point
        lon1 (float): Longitude of first point
        lat2 (float): Latitude of second point
        lon2 (float): Longitude of second point

    Returns:
        float: Distance in meters, or None if any input is None

    Example:
        >>> from pyspark.sql.functions import col, lit
        >>> # Calculate distance from origin point (41.25, -105.93)
        >>> df = df.withColumn("distance_from_origin",
        ...     geodesic_distance_udf(
        ...         col("coreData_position_lat"),
        ...         col("coreData_position_long"),
        ...         lit(41.25),
        ...         lit(-105.93)
        ...     )
        ... )
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    try:
        return float(MathHelper.dist_between_two_points(lat1, lon1, lat2, lon2))
    except (ValueError, TypeError):
        return None


@udf(returnType=DoubleType())
def xy_distance_udf(x1, y1, x2, y2):
    """
    Calculate Euclidean distance between two XY coordinate points.

    This UDF wraps MathHelper.dist_between_two_pointsXY() for use in PySpark.

    Args:
        x1 (float): X coordinate of first point
        y1 (float): Y coordinate of first point
        x2 (float): X coordinate of second point
        y2 (float): Y coordinate of second point

    Returns:
        float: Euclidean distance, or None if any input is None

    Example:
        >>> from pyspark.sql.functions import col, lit
        >>> # Calculate distance from origin (0, 0)
        >>> df = df.withColumn("distance_from_origin",
        ...     xy_distance_udf(col("x_pos"), col("y_pos"), lit(0.0), lit(0.0))
        ... )
    """
    if x1 is None or y1 is None or x2 is None or y2 is None:
        return None

    try:
        return float(MathHelper.dist_between_two_pointsXY(x1, y1, x2, y2))
    except (ValueError, TypeError):
        return None

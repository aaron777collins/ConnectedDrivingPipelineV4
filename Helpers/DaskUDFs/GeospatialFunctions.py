"""
Geospatial Functions for Dask.

This module provides vectorized functions for geospatial operations
used in the ConnectedDriving pipeline, including:
- WKT POINT string parsing
- Geodesic distance calculations
- Euclidean distance calculations

Unlike PySpark UDFs, these functions are designed to work directly with
pandas Series within Dask partitions for optimal performance.
"""

from typing import Optional, Tuple
from Helpers.DataConverter import DataConverter
from Helpers.MathHelper import MathHelper


def point_to_tuple(point_str: Optional[str]) -> Optional[Tuple[float, float]]:
    """
    Convert a WKT POINT string to a tuple of (x, y) coordinates.

    This function wraps the DataConverter.point_to_tuple() method for use in Dask.

    Args:
        point_str (str): WKT POINT format string, e.g., "POINT (-104.6744332 41.1509182)"

    Returns:
        tuple: (longitude, latitude) as (float, float), or None if invalid

    Example:
        >>> import dask.dataframe as dd
        >>> df['coords'] = df['coreData_position'].apply(
        ...     point_to_tuple, meta=('coords', 'object')
        ... )
        >>> df['x_pos'] = df['coords'].apply(lambda c: c[0] if c else None, meta=('x_pos', 'f8'))
        >>> df['y_pos'] = df['coords'].apply(lambda c: c[1] if c else None, meta=('y_pos', 'f8'))
    """
    if point_str is None:
        return None

    try:
        coords = DataConverter.point_to_tuple(point_str)
        return (float(coords[0]), float(coords[1]))
    except (ValueError, IndexError, AttributeError, TypeError):
        return None


def point_to_x(point_str: Optional[str]) -> Optional[float]:
    """
    Extract the X coordinate (longitude) from a WKT POINT string.

    Args:
        point_str (str): WKT POINT format string, e.g., "POINT (-104.6744332 41.1509182)"

    Returns:
        float: Longitude (first coordinate), or None if invalid

    Example:
        >>> import dask.dataframe as dd
        >>> df['x_pos'] = df['coreData_position'].apply(
        ...     point_to_x, meta=('x_pos', 'f8')
        ... )
    """
    if point_str is None:
        return None

    try:
        coords = DataConverter.point_to_tuple(point_str)
        return float(coords[0])
    except (ValueError, IndexError, AttributeError, TypeError):
        return None


def point_to_y(point_str: Optional[str]) -> Optional[float]:
    """
    Extract the Y coordinate (latitude) from a WKT POINT string.

    Args:
        point_str (str): WKT POINT format string, e.g., "POINT (-104.6744332 41.1509182)"

    Returns:
        float: Latitude (second coordinate), or None if invalid

    Example:
        >>> import dask.dataframe as dd
        >>> df['y_pos'] = df['coreData_position'].apply(
        ...     point_to_y, meta=('y_pos', 'f8')
        ... )
    """
    if point_str is None:
        return None

    try:
        coords = DataConverter.point_to_tuple(point_str)
        return float(coords[1])
    except (ValueError, IndexError, AttributeError, TypeError):
        return None


def geodesic_distance(
    lat1: Optional[float],
    lon1: Optional[float],
    lat2: Optional[float],
    lon2: Optional[float]
) -> Optional[float]:
    """
    Calculate geodesic distance between two lat/long points using WGS84 ellipsoid.

    This function wraps MathHelper.dist_between_two_points() for use in Dask.

    Args:
        lat1 (float): Latitude of first point
        lon1 (float): Longitude of first point
        lat2 (float): Latitude of second point
        lon2 (float): Longitude of second point

    Returns:
        float: Distance in meters, or None if any input is None

    Example:
        >>> import dask.dataframe as dd
        >>> # Calculate distance from origin point (41.25, -105.93)
        >>> df['distance_from_origin'] = df.apply(
        ...     lambda row: geodesic_distance(
        ...         row['coreData_position_lat'],
        ...         row['coreData_position_long'],
        ...         41.25,
        ...         -105.93
        ...     ),
        ...     axis=1,
        ...     meta=('distance_from_origin', 'f8')
        ... )
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    try:
        return float(MathHelper.dist_between_two_points(lat1, lon1, lat2, lon2))
    except (ValueError, TypeError):
        return None


def xy_distance(
    x1: Optional[float],
    y1: Optional[float],
    x2: Optional[float],
    y2: Optional[float]
) -> Optional[float]:
    """
    Calculate Euclidean distance between two XY coordinate points.

    This function wraps MathHelper.dist_between_two_pointsXY() for use in Dask.

    Args:
        x1 (float): X coordinate of first point
        y1 (float): Y coordinate of first point
        x2 (float): X coordinate of second point
        y2 (float): Y coordinate of second point

    Returns:
        float: Euclidean distance, or None if any input is None

    Example:
        >>> import dask.dataframe as dd
        >>> # Calculate distance from origin (0, 0)
        >>> df['distance_from_origin'] = df.apply(
        ...     lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),
        ...     axis=1,
        ...     meta=('distance_from_origin', 'f8')
        ... )
    """
    if x1 is None or y1 is None or x2 is None or y2 is None:
        return None

    try:
        return float(MathHelper.dist_between_two_pointsXY(x1, y1, x2, y2))
    except (ValueError, TypeError):
        return None

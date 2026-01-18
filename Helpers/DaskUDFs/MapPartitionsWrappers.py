"""
Map Partitions Wrappers for Dask UDFs.

This module provides optimized map_partitions() wrappers for common UDF operations
in the ConnectedDriving pipeline. Using map_partitions() is more efficient than
multiple separate apply() calls because it processes entire partitions at once.

Performance Benefits:
    - Reduces task graph overhead (one task vs multiple tasks per partition)
    - Minimizes data serialization/deserialization
    - Enables batch processing of related operations
    - Better memory locality

Usage:
    import dask.dataframe as dd
    from Helpers.DaskUDFs.MapPartitionsWrappers import extract_xy_coordinates

    # Apply wrapper to Dask DataFrame
    df = df.map_partitions(extract_xy_coordinates,
                          point_col='coreData_position',
                          meta=df._meta.assign(x_pos=0.0, y_pos=0.0))
"""

from typing import Callable, Dict, List, Optional, Tuple, Any
import pandas as pd
from Helpers.DaskUDFs.GeospatialFunctions import (
    point_to_x, point_to_y, point_to_tuple,
    geodesic_distance, xy_distance
)
from Helpers.DaskUDFs.ConversionFunctions import (
    hex_to_decimal, direction_and_dist_to_xy
)


# ============================================================================
# Single-Operation Wrappers
# ============================================================================

def extract_xy_coordinates(partition: pd.DataFrame,
                          point_col: str = 'coreData_position',
                          x_col: str = 'x_pos',
                          y_col: str = 'y_pos') -> pd.DataFrame:
    """
    Extract X and Y coordinates from WKT POINT column in one pass.

    This is more efficient than two separate apply() calls because it
    processes the entire partition at once.

    Args:
        partition: Pandas DataFrame partition
        point_col: Column name containing WKT POINT strings
        x_col: Output column name for X coordinate (longitude)
        y_col: Output column name for Y coordinate (latitude)

    Returns:
        Partition with new x_col and y_col columns added

    Example:
        >>> df = df.map_partitions(
        ...     extract_xy_coordinates,
        ...     point_col='coreData_position',
        ...     meta=df._meta.assign(x_pos=0.0, y_pos=0.0)
        ... )
    """
    partition[x_col] = partition[point_col].apply(point_to_x)
    partition[y_col] = partition[point_col].apply(point_to_y)
    return partition


def extract_coordinates_as_tuple(partition: pd.DataFrame,
                                 point_col: str = 'coreData_position',
                                 output_col: str = 'coords') -> pd.DataFrame:
    """
    Extract coordinates as (x, y) tuple from WKT POINT column.

    Useful when you need coordinate pairs for further processing.

    Args:
        partition: Pandas DataFrame partition
        point_col: Column name containing WKT POINT strings
        output_col: Output column name for (x, y) tuples

    Returns:
        Partition with new output_col containing (x, y) tuples

    Example:
        >>> df = df.map_partitions(
        ...     extract_coordinates_as_tuple,
        ...     point_col='coreData_position',
        ...     meta=df._meta.assign(coords=object)
        ... )
    """
    partition[output_col] = partition[point_col].apply(point_to_tuple)
    return partition


def convert_hex_id_column(partition: pd.DataFrame,
                          hex_col: str = 'coreData_id',
                          decimal_col: str = 'coreData_id_decimal') -> pd.DataFrame:
    """
    Convert hex string column to decimal integers.

    Args:
        partition: Pandas DataFrame partition
        hex_col: Column name containing hex strings
        decimal_col: Output column name for decimal integers

    Returns:
        Partition with new decimal_col column added

    Example:
        >>> df = df.map_partitions(
        ...     convert_hex_id_column,
        ...     hex_col='coreData_id',
        ...     meta=df._meta.assign(coreData_id_decimal=0)
        ... )
    """
    partition[decimal_col] = partition[hex_col].apply(hex_to_decimal)
    return partition


# ============================================================================
# Multi-Operation Wrappers
# ============================================================================

def parse_and_convert_coordinates(partition: pd.DataFrame,
                                  point_col: str = 'coreData_position',
                                  x_col: str = 'x_pos',
                                  y_col: str = 'y_pos',
                                  hex_col: Optional[str] = 'coreData_id',
                                  decimal_col: Optional[str] = 'coreData_id_decimal') -> pd.DataFrame:
    """
    Parse WKT POINT coordinates AND convert hex ID in one partition pass.

    Combines two common operations to reduce task graph overhead.

    Args:
        partition: Pandas DataFrame partition
        point_col: Column name containing WKT POINT strings
        x_col: Output column name for X coordinate
        y_col: Output column name for Y coordinate
        hex_col: Column name containing hex strings (None to skip)
        decimal_col: Output column name for decimal integers (None to skip)

    Returns:
        Partition with coordinate and ID columns added

    Example:
        >>> df = df.map_partitions(
        ...     parse_and_convert_coordinates,
        ...     meta=df._meta.assign(x_pos=0.0, y_pos=0.0, coreData_id_decimal=0)
        ... )
    """
    # Extract coordinates
    partition[x_col] = partition[point_col].apply(point_to_x)
    partition[y_col] = partition[point_col].apply(point_to_y)

    # Convert hex ID if specified
    if hex_col and decimal_col and hex_col in partition.columns:
        partition[decimal_col] = partition[hex_col].apply(hex_to_decimal)

    return partition


def calculate_distance_from_reference(partition: pd.DataFrame,
                                      lat_col: str,
                                      lon_col: str,
                                      ref_lat: float,
                                      ref_lon: float,
                                      output_col: str = 'distance_from_ref') -> pd.DataFrame:
    """
    Calculate geodesic distance from a reference point for all rows.

    Args:
        partition: Pandas DataFrame partition
        lat_col: Column name for latitude values
        lon_col: Column name for longitude values
        ref_lat: Reference latitude (degrees)
        ref_lon: Reference longitude (degrees)
        output_col: Output column name for distances (meters)

    Returns:
        Partition with distance column added

    Example:
        >>> # Calculate distance from Fort Collins, CO
        >>> df = df.map_partitions(
        ...     calculate_distance_from_reference,
        ...     lat_col='coreData_position_lat',
        ...     lon_col='coreData_position_long',
        ...     ref_lat=40.5853,
        ...     ref_lon=-105.0844,
        ...     meta=df._meta.assign(distance_from_ref=0.0)
        ... )
    """
    partition[output_col] = partition.apply(
        lambda row: geodesic_distance(row[lat_col], row[lon_col], ref_lat, ref_lon),
        axis=1
    )
    return partition


def calculate_pairwise_xy_distance(partition: pd.DataFrame,
                                   x1_col: str,
                                   y1_col: str,
                                   x2_col: str,
                                   y2_col: str,
                                   output_col: str = 'xy_distance') -> pd.DataFrame:
    """
    Calculate Euclidean distance between two XY points for all rows.

    Args:
        partition: Pandas DataFrame partition
        x1_col: Column name for first point X coordinate
        y1_col: Column name for first point Y coordinate
        x2_col: Column name for second point X coordinate
        y2_col: Column name for second point Y coordinate
        output_col: Output column name for distances

    Returns:
        Partition with distance column added

    Example:
        >>> # Calculate distance between current and previous positions
        >>> df = df.map_partitions(
        ...     calculate_pairwise_xy_distance,
        ...     x1_col='x_pos',
        ...     y1_col='y_pos',
        ...     x2_col='prev_x_pos',
        ...     y2_col='prev_y_pos',
        ...     meta=df._meta.assign(xy_distance=0.0)
        ... )
    """
    partition[output_col] = partition.apply(
        lambda row: xy_distance(row[x1_col], row[y1_col], row[x2_col], row[y2_col]),
        axis=1
    )
    return partition


def apply_positional_offset(partition: pd.DataFrame,
                           x_col: str = 'x_pos',
                           y_col: str = 'y_pos',
                           direction_col: str = 'attack_direction',
                           distance_col: str = 'attack_distance',
                           condition_col: str = 'isAttacker',
                           new_x_col: str = 'x_pos_offset',
                           new_y_col: str = 'y_pos_offset') -> pd.DataFrame:
    """
    Apply positional offset attack to rows matching condition.

    Calculates new XY coordinates based on direction and distance,
    but only updates rows where condition_col == 1 (attackers).

    Args:
        partition: Pandas DataFrame partition
        x_col: Column name for current X coordinate
        y_col: Column name for current Y coordinate
        direction_col: Column name for offset direction (degrees)
        distance_col: Column name for offset distance
        condition_col: Column name for attacker flag (1=attacker, 0=benign)
        new_x_col: Output column name for offset X coordinate
        new_y_col: Output column name for offset Y coordinate

    Returns:
        Partition with offset coordinate columns added

    Example:
        >>> df = df.map_partitions(
        ...     apply_positional_offset,
        ...     meta=df._meta.assign(x_pos_offset=0.0, y_pos_offset=0.0)
        ... )
    """
    # Calculate offset coordinates for all rows
    offset_coords = partition.apply(
        lambda row: direction_and_dist_to_xy(
            row[x_col], row[y_col], row[direction_col], row[distance_col]
        ),
        axis=1
    )

    # Apply offset only to attackers
    partition[new_x_col] = partition.apply(
        lambda row: offset_coords[row.name][0] if offset_coords[row.name] and row[condition_col] == 1 else row[x_col],
        axis=1
    )
    partition[new_y_col] = partition.apply(
        lambda row: offset_coords[row.name][1] if offset_coords[row.name] and row[condition_col] == 1 else row[y_col],
        axis=1
    )

    return partition


# ============================================================================
# Generic Utilities
# ============================================================================

def apply_udf_to_column(partition: pd.DataFrame,
                       udf_func: Callable,
                       input_col: str,
                       output_col: str,
                       **kwargs) -> pd.DataFrame:
    """
    Generic wrapper to apply any single-argument UDF to a column.

    Args:
        partition: Pandas DataFrame partition
        udf_func: Function to apply (must accept single value)
        input_col: Input column name
        output_col: Output column name
        **kwargs: Additional keyword arguments passed to udf_func

    Returns:
        Partition with output column added

    Example:
        >>> df = df.map_partitions(
        ...     apply_udf_to_column,
        ...     udf_func=point_to_x,
        ...     input_col='coreData_position',
        ...     output_col='x_pos',
        ...     meta=df._meta.assign(x_pos=0.0)
        ... )
    """
    if kwargs:
        partition[output_col] = partition[input_col].apply(lambda val: udf_func(val, **kwargs))
    else:
        partition[output_col] = partition[input_col].apply(udf_func)
    return partition


def batch_apply_udfs(partition: pd.DataFrame,
                    operations: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Apply multiple UDF operations to a partition in one pass.

    This is the most flexible wrapper - useful when you need to apply
    many different operations in a single map_partitions() call.

    Args:
        partition: Pandas DataFrame partition
        operations: List of operation dictionaries, each containing:
            - 'func': UDF function to apply
            - 'input_col': Input column name (for single-arg functions)
            - 'output_col': Output column name
            - 'apply_args': Optional dict of additional args for apply()
            - 'row_wise': Optional bool, True to use apply(axis=1)

    Returns:
        Partition with all output columns added

    Example:
        >>> operations = [
        ...     {'func': point_to_x, 'input_col': 'coreData_position', 'output_col': 'x_pos'},
        ...     {'func': point_to_y, 'input_col': 'coreData_position', 'output_col': 'y_pos'},
        ...     {'func': hex_to_decimal, 'input_col': 'coreData_id', 'output_col': 'id_decimal'},
        ... ]
        >>> df = df.map_partitions(
        ...     batch_apply_udfs,
        ...     operations=operations,
        ...     meta=df._meta.assign(x_pos=0.0, y_pos=0.0, id_decimal=0)
        ... )
    """
    for op in operations:
        func = op['func']
        output_col = op['output_col']

        # Handle row-wise operations (multiple columns as input)
        if op.get('row_wise', False):
            apply_args = op.get('apply_args', {})
            partition[output_col] = partition.apply(func, axis=1, **apply_args)
        else:
            # Column-wise operation
            input_col = op['input_col']
            partition[output_col] = partition[input_col].apply(func)

    return partition


# ============================================================================
# Advanced: Conditional Application
# ============================================================================

def apply_udf_conditionally(partition: pd.DataFrame,
                           udf_func: Callable,
                           input_col: str,
                           output_col: str,
                           condition_col: str,
                           condition_value: Any = 1,
                           default_value: Any = None) -> pd.DataFrame:
    """
    Apply UDF only to rows matching a condition.

    Useful for attack simulations where transformations only apply
    to attacker rows.

    Args:
        partition: Pandas DataFrame partition
        udf_func: Function to apply
        input_col: Input column name
        output_col: Output column name
        condition_col: Column name for condition check
        condition_value: Value to match in condition_col
        default_value: Value to use when condition not met (None = copy input)

    Returns:
        Partition with output column added

    Example:
        >>> # Only apply offset to attackers
        >>> df = df.map_partitions(
        ...     apply_udf_conditionally,
        ...     udf_func=some_attack_func,
        ...     input_col='x_pos',
        ...     output_col='x_pos_attacked',
        ...     condition_col='isAttacker',
        ...     condition_value=1,
        ...     meta=df._meta.assign(x_pos_attacked=0.0)
        ... )
    """
    # Apply UDF to all rows
    transformed = partition[input_col].apply(udf_func)

    # Use transformed value only where condition matches
    if default_value is None:
        # Default: copy original value
        partition[output_col] = partition.apply(
            lambda row: transformed[row.name] if row[condition_col] == condition_value else row[input_col],
            axis=1
        )
    else:
        # Use specified default value
        partition[output_col] = partition.apply(
            lambda row: transformed[row.name] if row[condition_col] == condition_value else default_value,
            axis=1
        )

    return partition

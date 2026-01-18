"""
Dask UDF (User-Defined Functions) module.

This module provides vectorized functions for common data transformations in the
ConnectedDriving pipeline Dask migration. Unlike PySpark UDFs which use @udf decorators,
Dask UDFs use map_partitions() with pandas-based vectorized operations.

Usage:
    import dask.dataframe as dd
    from Helpers.DaskUDFs import point_to_x, point_to_y, geodesic_distance

    # Apply functions to Dask DataFrame columns
    df = df.assign(
        x_pos=df['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8')),
        y_pos=df['coreData_position'].apply(point_to_y, meta=('y_pos', 'f8'))
    )
"""

from Helpers.DaskUDFs.GeospatialFunctions import (
    point_to_tuple,
    point_to_x,
    point_to_y,
    geodesic_distance,
    xy_distance
)

from Helpers.DaskUDFs.ConversionFunctions import (
    hex_to_decimal,
    direction_and_dist_to_xy
)

from Helpers.DaskUDFs.DaskUDFRegistry import (
    DaskUDFRegistry,
    FunctionCategory,
    FunctionMetadata,
    get_registry
)

from Helpers.DaskUDFs.RegisterDaskUDFs import (
    initialize_dask_udf_registry
)

__all__ = [
    # Geospatial Functions
    'point_to_tuple',
    'point_to_x',
    'point_to_y',
    'geodesic_distance',
    'xy_distance',
    # Conversion Functions
    'hex_to_decimal',
    'direction_and_dist_to_xy',
    # Registry Components
    'DaskUDFRegistry',
    'FunctionCategory',
    'FunctionMetadata',
    'get_registry',
    'initialize_dask_udf_registry',
]

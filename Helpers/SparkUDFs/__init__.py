"""
PySpark UDF (User-Defined Functions) module.

This module provides UDFs for common data transformations in the
ConnectedDriving pipeline PySpark migration.

Usage:
    # Option 1: Direct imports (legacy approach)
    from Helpers.SparkUDFs import hex_to_decimal_udf, point_to_x_udf
    df = df.withColumn('id_decimal', hex_to_decimal_udf(col('id_hex')))

    # Option 2: Registry-based (recommended for centralized management)
    from Helpers.SparkUDFs import get_registry
    registry = get_registry()
    hex_udf = registry.get('hex_to_decimal')
    df = df.withColumn('id_decimal', hex_udf(col('id_hex')))
"""

from Helpers.SparkUDFs.GeospatialUDFs import (
    point_to_tuple_udf,
    point_to_x_udf,
    point_to_y_udf,
    geodesic_distance_udf,
    xy_distance_udf
)

from Helpers.SparkUDFs.ConversionUDFs import (
    hex_to_decimal_udf,
    direction_and_dist_to_xy_udf
)

from Helpers.SparkUDFs.UDFRegistry import (
    UDFRegistry,
    UDFCategory,
    UDFMetadata,
    get_registry
)

from Helpers.SparkUDFs.RegisterUDFs import (
    initialize_udf_registry
)

__all__ = [
    # Geospatial UDFs
    'point_to_tuple_udf',
    'point_to_x_udf',
    'point_to_y_udf',
    'geodesic_distance_udf',
    'xy_distance_udf',
    # Conversion UDFs
    'hex_to_decimal_udf',
    'direction_and_dist_to_xy_udf',
    # UDF Registry
    'UDFRegistry',
    'UDFCategory',
    'UDFMetadata',
    'get_registry',
    'initialize_udf_registry',
]

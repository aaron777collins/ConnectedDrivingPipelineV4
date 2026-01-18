"""
Auto-registration of all UDFs into the UDF Registry

This module automatically registers all PySpark UDFs from GeospatialUDFs and ConversionUDFs
into the centralized UDF registry. It should be imported once during application initialization.

Usage:
    >>> from Helpers.SparkUDFs.RegisterUDFs import initialize_udf_registry
    >>> initialize_udf_registry()
    >>> # Now all UDFs are available via the registry
    >>> from Helpers.SparkUDFs.UDFRegistry import get_registry
    >>> registry = get_registry()
    >>> hex_udf = registry.get('hex_to_decimal')

Author: PySpark Migration Team
Created: 2026-01-17
"""

from Helpers.SparkUDFs.UDFRegistry import UDFRegistry, UDFCategory
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


def initialize_udf_registry() -> UDFRegistry:
    """
    Initialize the UDF registry with all available UDFs.

    This function registers all UDFs from GeospatialUDFs and ConversionUDFs modules
    with appropriate metadata. It should be called once during application initialization.

    Returns:
        The initialized UDFRegistry instance

    Note:
        This function is idempotent - calling it multiple times will not re-register UDFs
        if the registry is already initialized.
    """
    registry = UDFRegistry.get_instance()

    # Only register if not already initialized
    if registry.is_initialized():
        return registry

    # ===== GEOSPATIAL UDFs =====

    registry.register(
        name='point_to_tuple',
        udf_func=point_to_tuple_udf,
        description='Convert WKT POINT string to (x, y) coordinate struct',
        category=UDFCategory.GEOSPATIAL,
        input_types=['string'],
        output_type='struct<x:double,y:double>',
        example=(
            "from pyspark.sql.functions import col\n"
            "df = df.withColumn('coords', point_to_tuple_udf(col('coreData_position')))\n"
            "df = df.withColumn('x', col('coords.x'))\n"
            "df = df.withColumn('y', col('coords.y'))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='point_to_x',
        udf_func=point_to_x_udf,
        description='Extract X coordinate (longitude) from WKT POINT string',
        category=UDFCategory.GEOSPATIAL,
        input_types=['string'],
        output_type='double',
        example=(
            "from pyspark.sql.functions import col\n"
            "df = df.withColumn('x_pos', point_to_x_udf(col('coreData_position')))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='point_to_y',
        udf_func=point_to_y_udf,
        description='Extract Y coordinate (latitude) from WKT POINT string',
        category=UDFCategory.GEOSPATIAL,
        input_types=['string'],
        output_type='double',
        example=(
            "from pyspark.sql.functions import col\n"
            "df = df.withColumn('y_pos', point_to_y_udf(col('coreData_position')))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='geodesic_distance',
        udf_func=geodesic_distance_udf,
        description='Calculate geodesic distance between two lat/long points using WGS84 ellipsoid',
        category=UDFCategory.GEOSPATIAL,
        input_types=['double', 'double', 'double', 'double'],
        output_type='double',
        example=(
            "from pyspark.sql.functions import col, lit\n"
            "# Calculate distance from each point to a reference location\n"
            "df = df.withColumn('distance_meters', \n"
            "    geodesic_distance_udf(\n"
            "        col('latitude'), col('longitude'),\n"
            "        lit(41.25), lit(-105.93)  # Reference: Cheyenne, WY\n"
            "    )\n"
            ")"
        ),
        version='1.0.0'
    )

    registry.register(
        name='xy_distance',
        udf_func=xy_distance_udf,
        description='Calculate Euclidean distance between two XY coordinate points',
        category=UDFCategory.GEOSPATIAL,
        input_types=['double', 'double', 'double', 'double'],
        output_type='double',
        example=(
            "from pyspark.sql.functions import col, lit\n"
            "# Calculate XY distance from origin\n"
            "df = df.withColumn('distance_from_origin',\n"
            "    xy_distance_udf(\n"
            "        col('x_pos'), col('y_pos'),\n"
            "        lit(0.0), lit(0.0)\n"
            "    )\n"
            ")"
        ),
        version='1.0.0'
    )

    # ===== CONVERSION UDFs =====

    registry.register(
        name='hex_to_decimal',
        udf_func=hex_to_decimal_udf,
        description='Convert hexadecimal string to decimal integer (handles edge case with trailing .0)',
        category=UDFCategory.CONVERSION,
        input_types=['string'],
        output_type='long',
        example=(
            "from pyspark.sql.functions import col\n"
            "# Convert hex vehicle IDs to decimal for ML pipeline\n"
            "df = df.withColumn('id_decimal', hex_to_decimal_udf(col('coreData_id')))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='direction_and_dist_to_xy',
        udf_func=direction_and_dist_to_xy_udf,
        description='Calculate new XY coordinates given starting point, direction (degrees), and distance (meters)',
        category=UDFCategory.CONVERSION,
        input_types=['double', 'double', 'int', 'int'],
        output_type='struct<new_x:double,new_y:double>',
        example=(
            "from pyspark.sql.functions import col\n"
            "# Apply positional offset attack: move vehicle 100m at 45 degrees\n"
            "df = df.withColumn('offset_coords',\n"
            "    direction_and_dist_to_xy_udf(\n"
            "        col('x_pos'), col('y_pos'),\n"
            "        col('attack_direction'),  # 0-360 degrees\n"
            "        col('attack_distance')    # meters\n"
            "    )\n"
            ")\n"
            "df = df.withColumn('x_pos', col('offset_coords.new_x'))\n"
            "df = df.withColumn('y_pos', col('offset_coords.new_y'))"
        ),
        version='1.0.0'
    )

    # Mark registry as initialized
    registry.mark_initialized()

    return registry


# Auto-initialize on import (optional - can be disabled if explicit init is preferred)
def auto_initialize():
    """
    Auto-initialize the registry on module import.

    This function is called automatically when this module is imported,
    ensuring the registry is always ready to use.
    """
    initialize_udf_registry()


# Uncomment the line below to enable auto-initialization on import
# auto_initialize()

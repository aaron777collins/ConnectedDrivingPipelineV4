"""
Auto-registration of all Dask functions into the Function Registry

This module automatically registers all Dask functions from GeospatialFunctions and
ConversionFunctions into the centralized function registry. It should be imported once
during application initialization.

Unlike PySpark's RegisterUDFs which registers @udf-wrapped functions, this module
registers plain Python functions designed to work with pandas Series within Dask partitions.

Usage:
    >>> from Helpers.DaskUDFs.RegisterDaskUDFs import initialize_dask_udf_registry
    >>> initialize_dask_udf_registry()
    >>> # Now all functions are available via the registry
    >>> from Helpers.DaskUDFs.DaskUDFRegistry import get_registry
    >>> registry = get_registry()
    >>> hex_func = registry.get('hex_to_decimal')

Author: Dask Migration Team
Created: 2026-01-17
"""

from Helpers.DaskUDFs.DaskUDFRegistry import DaskUDFRegistry, FunctionCategory
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


def initialize_dask_udf_registry() -> DaskUDFRegistry:
    """
    Initialize the Dask function registry with all available functions.

    This function registers all functions from GeospatialFunctions and ConversionFunctions
    modules with appropriate metadata. It should be called once during application initialization.

    Returns:
        The initialized DaskUDFRegistry instance

    Note:
        This function is idempotent - calling it multiple times will not re-register functions
        if the registry is already initialized.
    """
    registry = DaskUDFRegistry.get_instance()

    # Only register if not already initialized
    if registry.is_initialized():
        return registry

    # ===== GEOSPATIAL FUNCTIONS =====

    registry.register(
        name='point_to_tuple',
        func=point_to_tuple,
        description='Convert WKT POINT string to (x, y) coordinate tuple',
        category=FunctionCategory.GEOSPATIAL,
        input_types=['str'],
        output_type='Tuple[float, float]',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "point_to_tuple = registry.get('point_to_tuple')\n"
            "# Extract coordinates\n"
            "df = df.assign(\n"
            "    coords=df['coreData_position'].apply(point_to_tuple, meta=('coords', 'object'))\n"
            ")\n"
            "df = df.assign(\n"
            "    x_pos=df['coords'].apply(lambda c: c[0] if c else None, meta=('x_pos', 'f8')),\n"
            "    y_pos=df['coords'].apply(lambda c: c[1] if c else None, meta=('y_pos', 'f8'))\n"
            ")"
        ),
        version='1.0.0'
    )

    registry.register(
        name='point_to_x',
        func=point_to_x,
        description='Extract X coordinate (longitude) from WKT POINT string',
        category=FunctionCategory.GEOSPATIAL,
        input_types=['str'],
        output_type='float',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "point_to_x = registry.get('point_to_x')\n"
            "df = df.assign(x_pos=df['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8')))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='point_to_y',
        func=point_to_y,
        description='Extract Y coordinate (latitude) from WKT POINT string',
        category=FunctionCategory.GEOSPATIAL,
        input_types=['str'],
        output_type='float',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "point_to_y = registry.get('point_to_y')\n"
            "df = df.assign(y_pos=df['coreData_position'].apply(point_to_y, meta=('y_pos', 'f8')))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='geodesic_distance',
        func=geodesic_distance,
        description='Calculate geodesic distance between two lat/long points using WGS84 ellipsoid',
        category=FunctionCategory.GEOSPATIAL,
        input_types=['float', 'float', 'float', 'float'],
        output_type='float',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "geodesic_distance = registry.get('geodesic_distance')\n"
            "# Calculate distance from each point to a reference location (Cheyenne, WY)\n"
            "ref_lat, ref_lon = 41.25, -105.93\n"
            "def calc_distance(partition):\n"
            "    partition['distance_meters'] = partition.apply(\n"
            "        lambda row: geodesic_distance(row['latitude'], row['longitude'], ref_lat, ref_lon),\n"
            "        axis=1\n"
            "    )\n"
            "    return partition\n"
            "df = df.map_partitions(calc_distance)"
        ),
        version='1.0.0'
    )

    registry.register(
        name='xy_distance',
        func=xy_distance,
        description='Calculate Euclidean distance between two XY coordinate points',
        category=FunctionCategory.GEOSPATIAL,
        input_types=['float', 'float', 'float', 'float'],
        output_type='float',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "xy_distance = registry.get('xy_distance')\n"
            "# Calculate XY distance from origin\n"
            "def calc_distance_from_origin(partition):\n"
            "    partition['distance_from_origin'] = partition.apply(\n"
            "        lambda row: xy_distance(row['x_pos'], row['y_pos'], 0.0, 0.0),\n"
            "        axis=1\n"
            "    )\n"
            "    return partition\n"
            "df = df.map_partitions(calc_distance_from_origin)"
        ),
        version='1.0.0'
    )

    # ===== CONVERSION FUNCTIONS =====

    registry.register(
        name='hex_to_decimal',
        func=hex_to_decimal,
        description='Convert hexadecimal string to decimal integer (handles edge case with trailing .0)',
        category=FunctionCategory.CONVERSION,
        input_types=['str'],
        output_type='int',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "hex_to_decimal = registry.get('hex_to_decimal')\n"
            "# Convert hex vehicle IDs to decimal for ML pipeline\n"
            "df = df.assign(id_decimal=df['coreData_id'].apply(hex_to_decimal, meta=('id_decimal', 'i8')))"
        ),
        version='1.0.0'
    )

    registry.register(
        name='direction_and_dist_to_xy',
        func=direction_and_dist_to_xy,
        description='Calculate new XY coordinates given starting point, direction (degrees), and distance (meters)',
        category=FunctionCategory.CONVERSION,
        input_types=['float', 'float', 'int', 'int'],
        output_type='Tuple[float, float]',
        example=(
            "import dask.dataframe as dd\n"
            "from Helpers.DaskUDFs.DaskUDFRegistry import get_registry\n"
            "registry = get_registry()\n"
            "direction_and_dist_to_xy = registry.get('direction_and_dist_to_xy')\n"
            "# Apply positional offset attack: move vehicle 100m at 45 degrees\n"
            "def apply_offset(partition):\n"
            "    partition['offset_coords'] = partition.apply(\n"
            "        lambda row: direction_and_dist_to_xy(\n"
            "            row['x_pos'], row['y_pos'],\n"
            "            row['attack_direction'],  # 0-360 degrees\n"
            "            row['attack_distance']    # meters\n"
            "        ),\n"
            "        axis=1\n"
            "    )\n"
            "    partition['x_pos'] = partition['offset_coords'].apply(lambda c: c[0] if c else None)\n"
            "    partition['y_pos'] = partition['offset_coords'].apply(lambda c: c[1] if c else None)\n"
            "    return partition\n"
            "df = df.map_partitions(apply_offset)"
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
    initialize_dask_udf_registry()


# Uncomment the line below to enable auto-initialization on import
# auto_initialize()

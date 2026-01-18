# Dask UDF Functions

This module provides vectorized functions for Dask DataFrame operations in the ConnectedDriving pipeline migration from pandas/PySpark to Dask.

## Key Differences from PySpark UDFs

| Aspect | PySpark UDFs | Dask Functions |
|--------|--------------|----------------|
| **Decorator** | `@udf(returnType=...)` | No decorator needed |
| **Execution** | Row-by-row across executors | Vectorized with pandas across workers |
| **Return Type** | PySpark types (DoubleType, etc.) | Python native types |
| **Usage Pattern** | `df.withColumn('col', udf(col('col')))` | `df['col'] = df['col'].apply(func)` |
| **Performance** | Slower (row-wise) | Faster (vectorized pandas) |

## Available Functions

### Geospatial Functions

Located in `GeospatialFunctions.py`:

1. **`point_to_tuple(point_str)`** - Convert WKT POINT to (x, y) tuple
2. **`point_to_x(point_str)`** - Extract X coordinate (longitude) from WKT POINT
3. **`point_to_y(point_str)`** - Extract Y coordinate (latitude) from WKT POINT
4. **`geodesic_distance(lat1, lon1, lat2, lon2)`** - Calculate geodesic distance (meters)
5. **`xy_distance(x1, y1, x2, y2)`** - Calculate Euclidean distance

### Conversion Functions

Located in `ConversionFunctions.py`:

1. **`hex_to_decimal(hex_str)`** - Convert hex string to decimal integer
2. **`direction_and_dist_to_xy(x, y, direction, distance)`** - Calculate new XY from direction/distance

## Usage Examples

### Example 1: Extract X/Y Coordinates from WKT POINT

**PySpark Version:**
```python
from pyspark.sql.functions import col
from Helpers.SparkUDFs import point_to_x_udf, point_to_y_udf

df = df.withColumn("x_pos", point_to_x_udf(col("coreData_position")))
df = df.withColumn("y_pos", point_to_y_udf(col("coreData_position")))
```

**Dask Version:**
```python
import dask.dataframe as dd
from Helpers.DaskUDFs import point_to_x, point_to_y

df['x_pos'] = df['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8'))
df['y_pos'] = df['coreData_position'].apply(point_to_y, meta=('y_pos', 'f8'))
```

### Example 2: Calculate Distance from Origin

**PySpark Version:**
```python
from pyspark.sql.functions import col, lit
from Helpers.SparkUDFs import geodesic_distance_udf

df = df.withColumn("distance_from_origin",
    geodesic_distance_udf(
        col("coreData_position_lat"),
        col("coreData_position_long"),
        lit(41.25),
        lit(-105.93)
    )
)
```

**Dask Version:**
```python
import dask.dataframe as dd
from Helpers.DaskUDFs import geodesic_distance

df['distance_from_origin'] = df.apply(
    lambda row: geodesic_distance(
        row['coreData_position_lat'],
        row['coreData_position_long'],
        41.25,
        -105.93
    ),
    axis=1,
    meta=('distance_from_origin', 'f8')
)
```

### Example 3: Convert Hex to Decimal

**PySpark Version:**
```python
from pyspark.sql.functions import col
from Helpers.SparkUDFs import hex_to_decimal_udf

df = df.withColumn("coreData_id", hex_to_decimal_udf(col("coreData_id")))
```

**Dask Version:**
```python
import dask.dataframe as dd
from Helpers.DaskUDFs import hex_to_decimal

df['coreData_id'] = df['coreData_id'].apply(hex_to_decimal, meta=('coreData_id', 'i8'))
```

### Example 4: Positional Offset Attack

**PySpark Version:**
```python
from pyspark.sql.functions import col, when
from Helpers.SparkUDFs import direction_and_dist_to_xy_udf

df = df.withColumn("offset_coords",
    direction_and_dist_to_xy_udf(
        col("x_pos"),
        col("y_pos"),
        col("attack_direction"),
        col("attack_distance")
    )
)
df = df.withColumn("x_pos",
    when(col("isAttacker") == 1, col("offset_coords.new_x"))
    .otherwise(col("x_pos"))
)
```

**Dask Version:**
```python
import dask.dataframe as dd
from Helpers.DaskUDFs import direction_and_dist_to_xy

# Calculate offset coordinates
df['offset_coords'] = df.apply(
    lambda row: direction_and_dist_to_xy(
        row['x_pos'],
        row['y_pos'],
        row['attack_direction'],
        row['attack_distance']
    ),
    axis=1,
    meta=('offset_coords', 'object')
)

# Apply to attackers only
df['x_pos'] = df.apply(
    lambda row: row['offset_coords'][0] if row['isAttacker'] == 1 and row['offset_coords'] else row['x_pos'],
    axis=1,
    meta=('x_pos', 'f8')
)
```

## Performance Optimization Tips

### 1. Use Vectorized Operations When Possible

Instead of `apply()` on each column separately, vectorize operations:

```python
# SLOW: Multiple apply() calls
df['x_pos'] = df['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8'))
df['y_pos'] = df['coreData_position'].apply(point_to_y, meta=('y_pos', 'f8'))

# FASTER: Single map_partitions() call
def extract_xy(partition):
    partition['x_pos'] = partition['coreData_position'].apply(point_to_x)
    partition['y_pos'] = partition['coreData_position'].apply(point_to_y)
    return partition

df = df.map_partitions(extract_xy)
```

### 2. Use `map_partitions()` for Multi-Column Operations

For operations involving multiple columns, use `map_partitions()` to process entire partitions at once:

```python
def calculate_distances(partition):
    """Calculate multiple distance metrics in one pass."""
    from Helpers.DaskUDFs import geodesic_distance, xy_distance

    # Extract coordinates once
    partition['x_pos'] = partition['coreData_position'].apply(point_to_x)
    partition['y_pos'] = partition['coreData_position'].apply(point_to_y)

    # Calculate distances
    partition['dist_origin'] = partition.apply(
        lambda row: geodesic_distance(row['y_pos'], row['x_pos'], 41.25, -105.93),
        axis=1
    )

    return partition

df = df.map_partitions(calculate_distances)
```

### 3. Specify `meta` Parameter

Always specify the `meta` parameter to avoid Dask having to infer types:

```python
# GOOD: Explicit meta
df['x_pos'] = df['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8'))

# BAD: Inferred meta (slower)
df['x_pos'] = df['coreData_position'].apply(point_to_x)
```

### 4. Persist After Heavy UDF Operations

If you'll reuse the result of UDF operations, persist to memory:

```python
# Apply UDFs
df['x_pos'] = df['coreData_position'].apply(point_to_x, meta=('x_pos', 'f8'))
df['y_pos'] = df['coreData_position'].apply(point_to_y, meta=('y_pos', 'f8'))

# Persist if reusing
df = df.persist()

# Now multiple operations use cached results
mean_x = df['x_pos'].mean().compute()
mean_y = df['y_pos'].mean().compute()
```

## Testing

All Dask functions are validated to match PySpark UDF outputs within `1e-5` tolerance:

```python
# Test that Dask function matches PySpark UDF
import pandas as pd
from Helpers.DaskUDFs import point_to_x
from Helpers.SparkUDFs import point_to_x_udf

test_point = "POINT (-104.6744332 41.1509182)"

# Dask version
dask_result = point_to_x(test_point)

# PySpark version (with PySpark session)
spark_df = spark.createDataFrame([(test_point,)], ["point"])
spark_result = spark_df.select(point_to_x_udf("point")).first()[0]

# Validate
assert abs(dask_result - spark_result) < 1e-5
```

## Migration Checklist

When converting code from PySpark to Dask:

- [ ] Replace `from Helpers.SparkUDFs import *_udf` with `from Helpers.DaskUDFs import *`
- [ ] Replace `df.withColumn(col, udf(col('col')))` with `df['col'] = df['col'].apply(func, meta=...)`
- [ ] Specify `meta` parameter for all `apply()` calls
- [ ] Consider using `map_partitions()` for multi-column operations
- [ ] Add `.persist()` after heavy UDF operations if results are reused
- [ ] Validate outputs match PySpark within 1e-5 tolerance

## Dependencies

These functions rely on:
- `Helpers.DataConverter.point_to_tuple()` - WKT POINT parsing
- `Helpers.MathHelper.dist_between_two_points()` - Geodesic distance (WGS84)
- `Helpers.MathHelper.dist_between_two_pointsXY()` - Euclidean distance
- `Helpers.MathHelper.direction_and_dist_to_XY()` - Direction/distance to XY

All dependencies are pure Python and work seamlessly with Dask.

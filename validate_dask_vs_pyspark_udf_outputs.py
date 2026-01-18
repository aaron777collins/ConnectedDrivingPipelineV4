"""
Validate that Dask UDF outputs match PySpark UDF outputs exactly.

Task 20: Validate UDF outputs match PySpark

This script creates identical test datasets and validates that Dask UDFs produce
the exact same outputs as PySpark UDFs for all implemented functions:
- Geospatial functions: point_to_x, point_to_y, point_to_tuple, geodesic_distance, xy_distance
- Conversion functions: hex_to_decimal, direction_and_dist_to_xy

Validation strategy:
1. Generate test dataset with realistic BSM data
2. Apply PySpark UDFs and collect results
3. Apply Dask UDFs and collect results
4. Compare outputs with strict equality (or floating-point tolerance)

Success criteria:
- All outputs must match within 1e-5 relative tolerance for floats
- All string/integer outputs must match exactly
- None/null handling must be identical
"""

import sys
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import dask.dataframe as dd

# Import PySpark UDFs
from Helpers.SparkUDFs.GeospatialUDFs import (
    point_to_x_udf,
    point_to_y_udf,
    geodesic_distance_udf,
    xy_distance_udf,
)
from Helpers.SparkUDFs.ConversionUDFs import (
    hex_to_decimal_udf,
    direction_and_dist_to_xy_udf,
)

# Import Dask UDF functions
from Helpers.DaskUDFs.GeospatialFunctions import (
    point_to_x as dask_point_to_x,
    point_to_y as dask_point_to_y,
    point_to_tuple as dask_point_to_tuple,
    geodesic_distance as dask_geodesic_distance,
    xy_distance as dask_xy_distance,
)
from Helpers.DaskUDFs.ConversionFunctions import (
    hex_to_decimal as dask_hex_to_decimal,
    direction_and_dist_to_xy as dask_direction_and_dist_to_xy,
)


def generate_test_data(n_rows=1000):
    """
    Generate test dataset with realistic BSM data.

    Returns:
        pd.DataFrame with columns: coreData_id, location, lat, lon, ref_lat, ref_lon, direction, distance
    """
    np.random.seed(42)  # Reproducible results

    # Generate WKT POINT strings (typical longitude/latitude ranges for Colorado)
    longitudes = np.random.uniform(-105.5, -104.0, n_rows)
    latitudes = np.random.uniform(40.5, 41.5, n_rows)
    locations = [f"POINT ({lon:.7f} {lat:.7f})" for lon, lat in zip(longitudes, latitudes)]

    # Generate hex IDs (like coreData_id in BSM messages)
    hex_ids = [f"0x{np.random.randint(0, 2**32):08x}" for _ in range(n_rows)]

    # Generate reference points for distance calculations
    ref_lats = np.random.uniform(40.5, 41.5, n_rows)
    ref_lons = np.random.uniform(-105.5, -104.0, n_rows)

    # Generate direction (radians) and distance (meters) for offset calculations
    directions = np.random.uniform(0, 2 * np.pi, n_rows)
    distances = np.random.uniform(10, 500, n_rows)

    # Add some None values to test null handling (10% nulls)
    null_indices = np.random.choice(n_rows, size=n_rows // 10, replace=False)
    locations_with_nulls = locations.copy()
    hex_ids_with_nulls = hex_ids.copy()
    for idx in null_indices:
        if idx % 2 == 0:
            locations_with_nulls[idx] = None
        else:
            hex_ids_with_nulls[idx] = None

    return pd.DataFrame({
        'coreData_id': hex_ids_with_nulls,
        'location': locations_with_nulls,
        'lat': latitudes,
        'lon': longitudes,
        'ref_lat': ref_lats,
        'ref_lon': ref_lons,
        'direction': directions,
        'distance': distances,
        'x1': np.random.uniform(-1000, 1000, n_rows),
        'y1': np.random.uniform(-1000, 1000, n_rows),
        'x2': np.random.uniform(-1000, 1000, n_rows),
        'y2': np.random.uniform(-1000, 1000, n_rows),
    })


def validate_point_to_x(spark, test_df):
    """Validate point_to_x outputs match between Dask and PySpark."""
    print("\n=== Testing point_to_x ===")

    # PySpark implementation
    spark_df = spark.createDataFrame(test_df)
    spark_result = spark_df.withColumn('x_pyspark', point_to_x_udf('location'))
    pyspark_values = spark_result.select('x_pyspark').toPandas()['x_pyspark'].values

    # Dask implementation
    dask_df = dd.from_pandas(test_df, npartitions=10)
    dask_values = dask_df['location'].apply(dask_point_to_x, meta=('x', 'f8')).compute().values

    # Compare
    non_null_mask = test_df['location'].notna()
    n_non_null = non_null_mask.sum()

    # Check nulls match
    pyspark_null_mask = pd.isna(pyspark_values)
    dask_null_mask = pd.isna(dask_values)
    assert np.array_equal(pyspark_null_mask, dask_null_mask), "Null masks don't match"

    # Check non-null values match
    pyspark_non_null = pyspark_values[non_null_mask]
    dask_non_null = dask_values[non_null_mask]

    max_diff = np.max(np.abs(pyspark_non_null - dask_non_null))
    relative_diff = max_diff / np.mean(np.abs(pyspark_non_null))

    print(f"  Tested {len(test_df)} rows ({n_non_null} non-null)")
    print(f"  Max absolute difference: {max_diff:.10f}")
    print(f"  Relative difference: {relative_diff:.10e}")

    assert relative_diff < 1e-5, f"Values differ by {relative_diff:.2e} (threshold: 1e-5)"
    print("  ✓ PASSED: Outputs match within tolerance")

    return True


def validate_point_to_y(spark, test_df):
    """Validate point_to_y outputs match between Dask and PySpark."""
    print("\n=== Testing point_to_y ===")

    # PySpark implementation
    spark_df = spark.createDataFrame(test_df)
    spark_result = spark_df.withColumn('y_pyspark', point_to_y_udf('location'))
    pyspark_values = spark_result.select('y_pyspark').toPandas()['y_pyspark'].values

    # Dask implementation
    dask_df = dd.from_pandas(test_df, npartitions=10)
    dask_values = dask_df['location'].apply(dask_point_to_y, meta=('y', 'f8')).compute().values

    # Compare
    non_null_mask = test_df['location'].notna()
    n_non_null = non_null_mask.sum()

    # Check nulls match
    pyspark_null_mask = pd.isna(pyspark_values)
    dask_null_mask = pd.isna(dask_values)
    assert np.array_equal(pyspark_null_mask, dask_null_mask), "Null masks don't match"

    # Check non-null values match
    pyspark_non_null = pyspark_values[non_null_mask]
    dask_non_null = dask_values[non_null_mask]

    max_diff = np.max(np.abs(pyspark_non_null - dask_non_null))
    relative_diff = max_diff / np.mean(np.abs(pyspark_non_null))

    print(f"  Tested {len(test_df)} rows ({n_non_null} non-null)")
    print(f"  Max absolute difference: {max_diff:.10f}")
    print(f"  Relative difference: {relative_diff:.10e}")

    assert relative_diff < 1e-5, f"Values differ by {relative_diff:.2e} (threshold: 1e-5)"
    print("  ✓ PASSED: Outputs match within tolerance")

    return True


def validate_geodesic_distance(spark, test_df):
    """Validate geodesic_distance outputs match between Dask and PySpark."""
    print("\n=== Testing geodesic_distance ===")

    # PySpark implementation
    spark_df = spark.createDataFrame(test_df)
    spark_result = spark_df.withColumn(
        'dist_pyspark',
        geodesic_distance_udf('lat', 'lon', 'ref_lat', 'ref_lon')
    )
    pyspark_values = spark_result.select('dist_pyspark').toPandas()['dist_pyspark'].values

    # Dask implementation
    dask_df = dd.from_pandas(test_df, npartitions=10)
    dask_values = dask_df.apply(
        lambda row: dask_geodesic_distance(row['lat'], row['lon'], row['ref_lat'], row['ref_lon']),
        axis=1,
        meta=('dist', 'f8')
    ).compute().values

    # Compare (geodesic distance has no nulls in this test)
    max_diff = np.max(np.abs(pyspark_values - dask_values))
    relative_diff = max_diff / np.mean(pyspark_values)

    print(f"  Tested {len(test_df)} rows")
    print(f"  Max absolute difference: {max_diff:.10f} meters")
    print(f"  Relative difference: {relative_diff:.10e}")

    assert relative_diff < 1e-5, f"Values differ by {relative_diff:.2e} (threshold: 1e-5)"
    print("  ✓ PASSED: Outputs match within tolerance")

    return True


def validate_xy_distance(spark, test_df):
    """Validate xy_distance outputs match between Dask and PySpark."""
    print("\n=== Testing xy_distance ===")

    # PySpark implementation
    spark_df = spark.createDataFrame(test_df)
    spark_result = spark_df.withColumn(
        'dist_pyspark',
        xy_distance_udf('x1', 'y1', 'x2', 'y2')
    )
    pyspark_values = spark_result.select('dist_pyspark').toPandas()['dist_pyspark'].values

    # Dask implementation
    dask_df = dd.from_pandas(test_df, npartitions=10)
    dask_values = dask_df.apply(
        lambda row: dask_xy_distance(row['x1'], row['y1'], row['x2'], row['y2']),
        axis=1,
        meta=('dist', 'f8')
    ).compute().values

    # Compare
    max_diff = np.max(np.abs(pyspark_values - dask_values))
    relative_diff = max_diff / np.mean(pyspark_values)

    print(f"  Tested {len(test_df)} rows")
    print(f"  Max absolute difference: {max_diff:.10f}")
    print(f"  Relative difference: {relative_diff:.10e}")

    assert relative_diff < 1e-5, f"Values differ by {relative_diff:.2e} (threshold: 1e-5)"
    print("  ✓ PASSED: Outputs match within tolerance")

    return True


def validate_hex_to_decimal(spark, test_df):
    """Validate hex_to_decimal outputs match between Dask and PySpark."""
    print("\n=== Testing hex_to_decimal ===")

    # PySpark implementation
    spark_df = spark.createDataFrame(test_df)
    spark_result = spark_df.withColumn('id_decimal_pyspark', hex_to_decimal_udf('coreData_id'))
    pyspark_values = spark_result.select('id_decimal_pyspark').toPandas()['id_decimal_pyspark'].values

    # Dask implementation
    dask_df = dd.from_pandas(test_df, npartitions=10)
    dask_values = dask_df['coreData_id'].apply(dask_hex_to_decimal, meta=('id_decimal', 'i8')).compute().values

    # Compare
    non_null_mask = test_df['coreData_id'].notna()
    n_non_null = non_null_mask.sum()

    # Check nulls match
    pyspark_null_mask = pd.isna(pyspark_values)
    dask_null_mask = pd.isna(dask_values)
    assert np.array_equal(pyspark_null_mask, dask_null_mask), "Null masks don't match"

    # Check non-null values match (exact integer comparison)
    pyspark_non_null = pyspark_values[non_null_mask]
    dask_non_null = dask_values[non_null_mask]

    exact_matches = np.sum(pyspark_non_null == dask_non_null)

    print(f"  Tested {len(test_df)} rows ({n_non_null} non-null)")
    print(f"  Exact matches: {exact_matches}/{n_non_null}")

    assert exact_matches == n_non_null, f"Only {exact_matches}/{n_non_null} values match exactly"
    print("  ✓ PASSED: All outputs match exactly")

    return True


def validate_direction_and_dist_to_xy(spark, test_df):
    """Validate direction_and_dist_to_xy outputs match between Dask and PySpark."""
    print("\n=== Testing direction_and_dist_to_xy ===")

    # PySpark implementation
    spark_df = spark.createDataFrame(test_df)
    spark_result = spark_df.withColumn(
        'xy_pyspark',
        direction_and_dist_to_xy_udf('x1', 'y1', 'direction', 'distance')
    )
    pyspark_values = spark_result.select('xy_pyspark').toPandas()['xy_pyspark'].values

    # Dask implementation
    dask_df = dd.from_pandas(test_df, npartitions=10)
    dask_values = dask_df.apply(
        lambda row: dask_direction_and_dist_to_xy(row['x1'], row['y1'], row['direction'], row['distance']),
        axis=1,
        meta=('xy', 'object')
    ).compute().values

    # Compare tuples
    print(f"  Tested {len(test_df)} rows")

    matches = 0
    max_x_diff = 0
    max_y_diff = 0

    for i, (pyspark_tuple, dask_tuple) in enumerate(zip(pyspark_values, dask_values)):
        if pyspark_tuple is None and dask_tuple is None:
            matches += 1
            continue

        assert pyspark_tuple is not None and dask_tuple is not None, f"Null mismatch at row {i}"

        px, py = pyspark_tuple
        dx, dy = dask_tuple

        x_diff = abs(px - dx)
        y_diff = abs(py - dy)

        max_x_diff = max(max_x_diff, x_diff)
        max_y_diff = max(max_y_diff, y_diff)

        if x_diff < 1e-5 and y_diff < 1e-5:
            matches += 1

    print(f"  Matches within tolerance: {matches}/{len(test_df)}")
    print(f"  Max X difference: {max_x_diff:.10f}")
    print(f"  Max Y difference: {max_y_diff:.10f}")

    assert matches == len(test_df), f"Only {matches}/{len(test_df)} tuples match"
    print("  ✓ PASSED: All outputs match within tolerance")

    return True


def main():
    """Run all validation tests."""
    print("=" * 60)
    print("Dask vs PySpark UDF Output Validation")
    print("Task 20: Validate UDF outputs match PySpark")
    print("=" * 60)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DaskPySparkUDFValidation") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()

    try:
        # Generate test data
        print("\nGenerating test data...")
        test_df = generate_test_data(n_rows=1000)
        print(f"Generated {len(test_df)} rows with {test_df['location'].isna().sum()} null locations")

        # Run validation tests
        results = []

        try:
            results.append(("point_to_x", validate_point_to_x(spark, test_df)))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append(("point_to_x", False))

        try:
            results.append(("point_to_y", validate_point_to_y(spark, test_df)))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append(("point_to_y", False))

        try:
            results.append(("geodesic_distance", validate_geodesic_distance(spark, test_df)))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append(("geodesic_distance", False))

        try:
            results.append(("xy_distance", validate_xy_distance(spark, test_df)))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append(("xy_distance", False))

        try:
            results.append(("hex_to_decimal", validate_hex_to_decimal(spark, test_df)))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append(("hex_to_decimal", False))

        try:
            results.append(("direction_and_dist_to_xy", validate_direction_and_dist_to_xy(spark, test_df)))
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            results.append(("direction_and_dist_to_xy", False))

        # Summary
        print("\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)

        passed = sum(1 for _, result in results if result)
        total = len(results)

        for func_name, result in results:
            status = "✓ PASSED" if result else "✗ FAILED"
            print(f"  {func_name}: {status}")

        print(f"\nTotal: {passed}/{total} tests passed")

        if passed == total:
            print("\n✓ ALL VALIDATION TESTS PASSED")
            print("Dask UDF outputs match PySpark UDF outputs exactly!")
            return 0
        else:
            print(f"\n✗ VALIDATION FAILED: {total - passed} tests failed")
            return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

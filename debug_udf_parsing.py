"""Debug script to test POINT parsing directly."""

import pandas as pd
from Helpers.DaskUDFs import point_to_x, point_to_y
from Helpers.SparkUDFs.GeospatialUDFs import point_to_x_udf, point_to_y_udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Test string
test_points = [
    "POINT(-105.479416 39.374540)",
    "POINT(-104.530090 39.950714)",
    "POINT(-104.667557 39.731994)"
]

print("Testing Dask UDFs:")
print("="*50)
for point_str in test_points:
    x = point_to_x(point_str)
    y = point_to_y(point_str)
    print(f"{point_str} -> x={x}, y={y}")

print("\n\nTesting Spark UDFs:")
print("="*50)

spark = SparkSession.builder \
    .appName("TestUDF") \
    .master("local[1]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create Spark DataFrame with test data
test_df = spark.createDataFrame(
    [(i, point) for i, point in enumerate(test_points)],
    ["id", "point_str"]
)

# Apply Spark UDFs
result_df = test_df.withColumn("x", point_to_x_udf(col("point_str"))) \
                   .withColumn("y", point_to_y_udf(col("point_str")))

result_pdf = result_df.toPandas()
print(result_pdf)

spark.stop()

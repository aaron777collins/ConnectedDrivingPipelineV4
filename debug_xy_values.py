"""Debug script to see what x_pos and y_pos values are produced by each cleaner."""

import pandas as pd
import numpy as np
import dask.dataframe as dd
from pyspark.sql import SparkSession
import os

from Generator.Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
from Generator.Cleaners.SparkConnectedDrivingCleaner import SparkConnectedDrivingCleaner
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.GeneratorPathProvider import GeneratorPathProvider
from ServiceProviders.PathProvider import PathProvider


# Generate small test dataset
np.random.seed(42)
latitudes = np.random.uniform(39.0, 40.0, 10)
longitudes = np.random.uniform(-105.5, -104.5, 10)
points = [f"POINT({lon:.6f} {lat:.6f})" for lon, lat in zip(longitudes, latitudes)]

test_data = pd.DataFrame({
    'coreData_id': [f"{i:08X}" for i in range(10)],
    'coreData_position': points,
    'coreData_speed': np.random.uniform(0, 120, 10),
})

print("Test data (first 5 rows):")
print(test_data.head())
print(f"\nSample POINT strings:")
for i in range(3):
    print(f"  {test_data['coreData_position'].iloc[i]}")

# Setup providers
PathProvider._instance = None
GeneratorPathProvider._instance = None
GeneratorContextProvider._instance = None

PathProvider(
    model="debug",
    contexts={"Logger.logpath": lambda model: "/tmp/debug.log"}
)
GeneratorPathProvider(
    model="debug",
    contexts={"FileCache.filepath": lambda model: "/tmp/debug_cache"}
)

context = GeneratorContextProvider()
context.add("DataGatherer.numrows", 10)
context.add("ConnectedDrivingCleaner.cleanParams", {})
context.add("ConnectedDrivingCleaner.filename", "debug.parquet")
context.add("ConnectedDrivingCleaner.isXYCoords", False)
context.add("ConnectedDrivingCleaner.shouldGatherAutomatically", False)
context.add("ConnectedDrivingCleaner.x_pos", 39.5)
context.add("ConnectedDrivingCleaner.y_pos", -105.0)
context.add("ConnectedDrivingCleaner.columns", ['coreData_id', 'coreData_position', 'coreData_speed'])

# Test Dask
print("\n" + "="*70)
print("DASK CLEANER")
print("="*70)
dask_df = dd.from_pandas(test_data, npartitions=2)
dask_cleaner = DaskConnectedDrivingCleaner(data=dask_df)
dask_cleaner.clean_data()
dask_result = dask_cleaner.get_cleaned_data().compute()

print("\nDask result columns:")
print(dask_result.columns.tolist())
print("\nDask result (first 5 rows):")
print(dask_result.head())
print("\nDask x_pos sample values:")
print(dask_result['x_pos'].head())
print("\nDask y_pos sample values:")
print(dask_result['y_pos'].head())

# Test Spark
print("\n" + "="*70)
print("SPARK CLEANER")
print("="*70)
spark = SparkSession.builder \
    .appName("Debug") \
    .master("local[2]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark_df = spark.createDataFrame(test_data)
spark_cleaner = SparkConnectedDrivingCleaner(data=spark_df)
spark_cleaner.clean_data()
spark_result = spark_cleaner.get_cleaned_data().toPandas()

print("\nSpark result columns:")
print(spark_result.columns.tolist())
print("\nSpark result (first 5 rows):")
print(spark_result.head())
print("\nSpark x_pos sample values:")
print(spark_result['x_pos'].head())
print("\nSpark y_pos sample values:")
print(spark_result['y_pos'].head())

# Compare
print("\n" + "="*70)
print("COMPARISON")
print("="*70)
print("\nAre x_pos values equal?")
print(np.allclose(dask_result['x_pos'].values, spark_result['x_pos'].values))
print("\nAre y_pos values equal?")
print(np.allclose(dask_result['y_pos'].values, spark_result['y_pos'].values))

print("\nDifferences in x_pos:")
print(dask_result['x_pos'].values - spark_result['x_pos'].values)
print("\nDifferences in y_pos:")
print(dask_result['y_pos'].values - spark_result['y_pos'].values)

spark.stop()

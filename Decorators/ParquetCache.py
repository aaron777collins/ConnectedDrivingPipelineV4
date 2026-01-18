import functools
import os

from Decorators.FileCache import FileCache
from Helpers.SparkSessionManager import SparkSessionManager

# Decorator to cache the return of a function in a Parquet file using PySpark
#
# This decorator is the PySpark equivalent of CSVCache, using Parquet format
# for efficient columnar storage and compression. It follows the same MD5-based
# hashing pattern as FileCache and CSVCache.
#
# KWARGS:
# cache_variables: list of variables to use as cache variables (default: all the arguments excluding the kwargs)
# full_file_cache_path: OVERRIDES the cache path and uses this path instead (do NOT include .parquet extension)
#
# NOTE:
# - The function being decorated must declare the return type in the function declaration
# - The return type should be pyspark.sql.DataFrame
# - Unlike CSV, Parquet is a directory-based format in Spark, so cache paths are directories
#
# Example usage:
#     @ParquetCache
#     def process_data(self, file_path) -> DataFrame:
#         spark = SparkSessionManager.get_session()
#         df = spark.read.csv(file_path, header=True)
#         # ... processing ...
#         return df


def ParquetCache(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        # Copy kwargs to avoid modifying original
        customkwargs = kwargs.copy()

        # Set file type to parquet (this becomes a directory extension)
        kwargs["cache_file_type"] = "parquet"

        def read_parquet(path, return_type):
            """
            Read Parquet file using Spark.

            Args:
                path (str): Path to the parquet directory
                return_type: The expected return type (ignored, always returns DataFrame)

            Returns:
                pyspark.sql.DataFrame: The cached DataFrame
            """
            # Get Spark session
            spark = SparkSessionManager.get_session()

            # Create path if it doesn't exist (should already exist if cached)
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Read parquet file
            # Note: path is a directory for Parquet format
            df = spark.read.parquet(path)

            return df

        def write_parquet(path, data):
            """
            Write DataFrame to Parquet file using Spark.

            Args:
                path (str): Path to write the parquet directory
                data (pyspark.sql.DataFrame): The DataFrame to cache

            Returns:
                pyspark.sql.DataFrame: The original DataFrame (unchanged)
            """
            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(path), exist_ok=True)

            # Write parquet file
            # Use overwrite mode to handle any partial writes
            # Use snappy compression for good balance of speed and size
            data.write.mode("overwrite").parquet(path)

            return data

        # Set custom reader and writer functions
        kwargs["cache_file_reader_function"] = lambda path, return_type: read_parquet(path, return_type)
        kwargs["cache_file_writer_function"] = lambda path, data: write_parquet(path, data)

        # Delegate to FileCache with our custom reader/writer
        return FileCache(f)(*args, **kwargs)

    return wrapper

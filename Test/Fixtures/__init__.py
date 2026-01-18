"""
Test fixtures for ConnectedDrivingPipelineV4 testing.

This package provides reusable test fixtures for pandas, PySpark, and Dask testing.
"""

from .SparkFixtures import (
    spark_session,
    spark_context,
    sample_bsm_raw_df,
    sample_bsm_processed_df,
    small_bsm_dataset,
    medium_bsm_dataset,
    temp_spark_dir,
)

from .DaskFixtures import (
    dask_cluster,
    dask_client,
    sample_bsm_raw_dask_df,
    sample_bsm_processed_dask_df,
    small_bsm_dask_dataset,
    medium_bsm_dask_dataset,
    temp_dask_dir,
    dask_df_comparer,
)

__all__ = [
    # Spark fixtures
    'spark_session',
    'spark_context',
    'sample_bsm_raw_df',
    'sample_bsm_processed_df',
    'small_bsm_dataset',
    'medium_bsm_dataset',
    'temp_spark_dir',
    # Dask fixtures
    'dask_cluster',
    'dask_client',
    'sample_bsm_raw_dask_df',
    'sample_bsm_processed_dask_df',
    'small_bsm_dask_dataset',
    'medium_bsm_dask_dataset',
    'temp_dask_dir',
    'dask_df_comparer',
]

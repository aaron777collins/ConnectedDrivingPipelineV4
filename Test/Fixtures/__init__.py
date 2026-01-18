"""
Test fixtures for ConnectedDrivingPipelineV4 testing.

This package provides reusable test fixtures for both pandas and PySpark testing.
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

__all__ = [
    'spark_session',
    'spark_context',
    'sample_bsm_raw_df',
    'sample_bsm_processed_df',
    'small_bsm_dataset',
    'medium_bsm_dataset',
    'temp_spark_dir',
]

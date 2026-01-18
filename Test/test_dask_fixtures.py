"""
Test suite for validating DaskFixtures.

This test file verifies that all Dask fixtures work correctly and produce
expected outputs.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


def test_dask_cluster_fixture(dask_cluster):
    """Test that dask_cluster fixture creates a valid cluster."""
    assert isinstance(dask_cluster, LocalCluster)
    assert dask_cluster.scheduler_address is not None


def test_dask_client_fixture(dask_client):
    """Test that dask_client fixture creates a valid client."""
    assert isinstance(dask_client, Client)
    assert dask_client.status == "running"


def test_temp_dask_dir_fixture(temp_dask_dir):
    """Test that temp_dask_dir fixture creates a valid temporary directory."""
    import os
    assert os.path.exists(temp_dask_dir)
    assert os.path.isdir(temp_dask_dir)


def test_sample_bsm_raw_dask_df_fixture(sample_bsm_raw_dask_df):
    """Test that sample_bsm_raw_dask_df fixture creates a valid Dask DataFrame."""
    assert isinstance(sample_bsm_raw_dask_df, dd.DataFrame)

    # Compute to pandas and check shape
    pdf = sample_bsm_raw_dask_df.compute()
    assert len(pdf) == 5
    assert "coreData_id" in pdf.columns
    assert "coreData_speed" in pdf.columns

    # Check data values
    assert pdf["coreData_speed"].max() == 30.0
    assert pdf["coreData_speed"].min() == 12.0


def test_sample_bsm_processed_dask_df_fixture(sample_bsm_processed_dask_df):
    """Test that sample_bsm_processed_dask_df fixture creates a valid Dask DataFrame."""
    assert isinstance(sample_bsm_processed_dask_df, dd.DataFrame)

    # Compute to pandas and check shape
    pdf = sample_bsm_processed_dask_df.compute()
    assert len(pdf) == 5
    assert "x_pos" in pdf.columns
    assert "y_pos" in pdf.columns
    assert "isAttacker" in pdf.columns

    # Check attacker labels
    assert pdf["isAttacker"].sum() == 2  # 2 attackers in the sample data


def test_small_bsm_dask_dataset_fixture(small_bsm_dask_dataset):
    """Test that small_bsm_dask_dataset fixture creates a valid dataset."""
    assert isinstance(small_bsm_dask_dataset, dd.DataFrame)

    # Compute to pandas and check shape
    pdf = small_bsm_dask_dataset.compute()
    assert len(pdf) == 100
    assert "coreData_id" in pdf.columns

    # Check that npartitions is as expected
    assert small_bsm_dask_dataset.npartitions == 2


def test_medium_bsm_dask_dataset_fixture(medium_bsm_dask_dataset):
    """Test that medium_bsm_dask_dataset fixture creates a valid dataset."""
    assert isinstance(medium_bsm_dask_dataset, dd.DataFrame)

    # Compute to pandas and check shape
    pdf = medium_bsm_dask_dataset.compute()
    assert len(pdf) == 1000
    assert "coreData_id" in pdf.columns

    # Check that npartitions is as expected
    assert medium_bsm_dask_dataset.npartitions == 4


def test_dask_df_comparer_assert_equal(dask_df_comparer, sample_bsm_raw_dask_df):
    """Test that dask_df_comparer.assert_equal works correctly."""
    # Create a copy of the DataFrame
    df_copy = sample_bsm_raw_dask_df.copy()

    # Should not raise an exception
    dask_df_comparer.assert_equal(sample_bsm_raw_dask_df, df_copy)


def test_dask_df_comparer_assert_pandas_dask_equal(dask_df_comparer, sample_bsm_raw_dask_df):
    """Test that dask_df_comparer.assert_pandas_dask_equal works correctly."""
    # Compute Dask DataFrame to pandas
    pdf = sample_bsm_raw_dask_df.compute()

    # Should not raise an exception
    dask_df_comparer.assert_pandas_dask_equal(pdf, sample_bsm_raw_dask_df)


def test_dask_df_comparer_assert_schema_equal(dask_df_comparer, sample_bsm_raw_dask_df):
    """Test that dask_df_comparer.assert_schema_equal works correctly."""
    # Create a copy of the DataFrame
    df_copy = sample_bsm_raw_dask_df.copy()

    # Should not raise an exception
    dask_df_comparer.assert_schema_equal(sample_bsm_raw_dask_df, df_copy)


def test_dask_df_comparer_assert_column_exists(dask_df_comparer, sample_bsm_raw_dask_df):
    """Test that dask_df_comparer.assert_column_exists works correctly."""
    # Should not raise an exception
    dask_df_comparer.assert_column_exists(sample_bsm_raw_dask_df, "coreData_id")

    # Should raise an exception for non-existent column
    with pytest.raises(AssertionError):
        dask_df_comparer.assert_column_exists(sample_bsm_raw_dask_df, "nonexistent_column")


def test_dask_computation_with_fixtures(dask_client, sample_bsm_raw_dask_df):
    """Test that we can perform Dask computations using the fixtures."""
    # Filter by speed
    fast_vehicles = sample_bsm_raw_dask_df[sample_bsm_raw_dask_df["coreData_speed"] > 20]
    result = fast_vehicles.compute()

    # Should have 2 vehicles with speed > 20 (25.0 and 30.0)
    assert len(result) == 2
    assert result["coreData_speed"].min() > 20


def test_dask_parquet_io(temp_dask_dir, sample_bsm_raw_dask_df):
    """Test that we can write and read Parquet files using Dask."""
    import os

    # Write to Parquet
    output_path = os.path.join(temp_dask_dir, "test_output.parquet")
    sample_bsm_raw_dask_df.to_parquet(output_path)

    # Verify file was created
    assert os.path.exists(output_path)

    # Read back from Parquet
    ddf_read = dd.read_parquet(output_path)

    # Verify data is the same
    pdf_original = sample_bsm_raw_dask_df.compute().sort_values(by="coreData_id").reset_index(drop=True)
    pdf_read = ddf_read.compute().sort_values(by="coreData_id").reset_index(drop=True)

    # Use pandas testing for comparison
    import pandas.testing as pdt
    pdt.assert_frame_equal(pdf_original, pdf_read)

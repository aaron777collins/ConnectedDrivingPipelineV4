"""
Dask test fixtures for ConnectedDrivingPipelineV4.

This module provides pytest fixtures for Dask testing, including:
- Dask client/cluster management (automatically cleaned up)
- Sample BSM Dask DataFrames (raw and processed)
- Temporary directories for Dask I/O operations
- Dataset generators for different sizes (small, medium, large)

Usage:
    import pytest
    from Test.Fixtures.DaskFixtures import dask_client, sample_bsm_raw_dask_df

    def test_my_dask_function(dask_client, sample_bsm_raw_dask_df):
        # Use dask_client and sample_bsm_raw_dask_df in your test
        result = sample_bsm_raw_dask_df.compute()
        assert len(result) > 0
"""

import pytest
import tempfile
import shutil
import os
from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster


@pytest.fixture(scope="session")
def dask_cluster():
    """
    Session-scoped Dask LocalCluster for testing.

    Creates a minimal Dask cluster optimized for testing with reduced memory
    and worker settings. Automatically closed at the end of the test session.

    Yields:
        LocalCluster: Local Dask cluster for testing

    Example:
        def test_dask_cluster(dask_cluster):
            assert dask_cluster.scheduler_address is not None
    """
    # Create a test-optimized Dask cluster
    cluster = LocalCluster(
        n_workers=2,  # Minimal workers for testing
        threads_per_worker=1,  # Single thread to avoid race conditions
        processes=True,  # Use processes for better isolation
        memory_limit='2GB',  # Small memory limit for tests
        dashboard_address=None,  # Disable dashboard in tests
        silence_logs=True  # Reduce log noise
    )

    yield cluster

    # Cleanup: close the cluster after all tests complete
    cluster.close()


@pytest.fixture(scope="session")
def dask_client(dask_cluster):
    """
    Session-scoped Dask client for testing.

    Creates a Dask client connected to the test cluster. Automatically closed
    at the end of the test session.

    Args:
        dask_cluster: Session-scoped Dask cluster fixture

    Yields:
        Client: Dask distributed client for testing

    Example:
        def test_dask_computation(dask_client):
            import dask.array as da
            x = da.ones((10, 10), chunks=(5, 5))
            result = x.sum().compute()
            assert result == 100
    """
    client = Client(dask_cluster)

    yield client

    # Cleanup: close the client after all tests complete
    client.close()


@pytest.fixture(scope="function")
def temp_dask_dir(tmp_path):
    """
    Function-scoped temporary directory for Dask I/O operations.

    Creates a temporary directory that's automatically cleaned up after each test.
    Useful for testing file read/write operations without polluting the filesystem.

    Args:
        tmp_path: pytest's built-in tmp_path fixture

    Yields:
        str: Path to temporary directory

    Example:
        def test_parquet_write(temp_dask_dir, sample_bsm_raw_dask_df):
            output_path = os.path.join(temp_dask_dir, "output.parquet")
            sample_bsm_raw_dask_df.to_parquet(output_path)
            assert os.path.exists(output_path)
    """
    dask_dir = tmp_path / "dask_test"
    dask_dir.mkdir(exist_ok=True)
    yield str(dask_dir)
    # Cleanup is handled automatically by pytest's tmp_path


@pytest.fixture(scope="function")
def sample_bsm_raw_dask_df(dask_client):
    """
    Small sample Dask DataFrame with raw BSM data (5 rows).

    Creates a Dask DataFrame matching the BSM raw schema with realistic test data.
    Useful for unit testing individual functions without I/O overhead.

    Args:
        dask_client: Dask client fixture

    Returns:
        dd.DataFrame: Sample Dask DataFrame with 5 raw BSM records

    Example:
        def test_filter_by_speed(sample_bsm_raw_dask_df):
            fast_vehicles = sample_bsm_raw_dask_df[sample_bsm_raw_dask_df["coreData_speed"] > 20]
            assert len(fast_vehicles.compute()) == 2
    """
    # Sample raw BSM data (5 rows with diverse values)
    data = {
        "metadata_generatedAt": [
            "04/06/2021 10:30:00 AM",
            "04/06/2021 10:30:01 AM",
            "04/06/2021 10:30:02 AM",
            "04/06/2021 10:30:03 AM",
            "04/06/2021 10:30:04 AM",
        ],
        "metadata_recordType": ["bsmTx"] * 5,
        "metadata_serialId_streamId": [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
            "00000000-0000-0000-0000-000000000004",
            "00000000-0000-0000-0000-000000000005",
        ],
        "metadata_serialId_bundleSize": [1] * 5,
        "metadata_serialId_bundleId": [0] * 5,
        "metadata_serialId_recordId": [0, 1, 2, 3, 4],
        "metadata_serialId_serialNumber": [0, 1, 2, 3, 4],
        "metadata_receivedAt": [
            "2021-04-06T10:30:00.000Z",
            "2021-04-06T10:30:01.000Z",
            "2021-04-06T10:30:02.000Z",
            "2021-04-06T10:30:03.000Z",
            "2021-04-06T10:30:04.000Z",
        ],
        "coreData_id": ["A1B2C3D4", "B2C3D4E5", "C3D4E5F6", "D4E5F6A7", "E5F6A7B8"],
        "coreData_position_lat": [41.2565, 41.2570, 41.2575, 41.2580, 41.2585],
        "coreData_position_long": [-105.9378, -105.9380, -105.9385, -105.9390, -105.9395],
        "coreData_accuracy_semiMajor": [5.0, 6.0, 4.5, 7.0, 5.5],
        "coreData_accuracy_semiMinor": [5.0, 6.0, 4.5, 7.0, 5.5],
        "coreData_elevation": [2194.0, 2195.0, 2196.0, 2197.0, 2198.0],
        "coreData_accelSet_accelYaw": [0.0, 0.5, -0.3, 1.0, 0.2],
        "coreData_speed": [15.5, 25.0, 18.0, 30.0, 12.0],
        "coreData_heading": [90.0, 85.0, 92.0, 88.0, 91.0],
        "coreData_secMark": [30000, 31000, 32000, 33000, 34000],
    }

    # Create pandas DataFrame first
    pdf = pd.DataFrame(data)

    # Convert to Dask DataFrame with 1 partition (small test data)
    ddf = dd.from_pandas(pdf, npartitions=1)

    return ddf


@pytest.fixture(scope="function")
def sample_bsm_processed_dask_df(dask_client):
    """
    Small sample Dask DataFrame with processed BSM data (5 rows).

    Creates a Dask DataFrame matching the BSM processed schema with ML-ready features.
    Includes temporal features, XY coordinates, and isAttacker labels.

    Args:
        dask_client: Dask client fixture

    Returns:
        dd.DataFrame: Sample Dask DataFrame with 5 processed BSM records

    Example:
        def test_ml_feature_selection(sample_bsm_processed_dask_df):
            features = sample_bsm_processed_dask_df[["x_pos", "y_pos", "elevation"]]
            assert len(features.compute()) == 5
    """
    # Sample processed BSM data (5 rows)
    data = {
        "coreData_id": [1001, 1002, 1003, 1004, 1005],
        "coreData_secMark": [30000, 31000, 32000, 33000, 34000],
        "coreData_accuracy_semiMajor": [5.0, 6.0, 4.5, 7.0, 5.5],
        "coreData_accuracy_semiMinor": [5.0, 6.0, 4.5, 7.0, 5.5],
        "coreData_elevation": [2194.0, 2195.0, 2196.0, 2197.0, 2198.0],
        "coreData_accelSet_accelYaw": [0.0, 0.5, -0.3, 1.0, 0.2],
        "coreData_speed": [15.5, 25.0, 18.0, 30.0, 12.0],
        "coreData_heading": [90.0, 85.0, 92.0, 88.0, 91.0],
        "x_pos": [100.0, 150.0, 200.0, 250.0, 300.0],
        "y_pos": [200.0, 250.0, 300.0, 350.0, 400.0],
        "month": [4, 4, 4, 4, 4],
        "day": [6, 6, 6, 6, 6],
        "year": [2021, 2021, 2021, 2021, 2021],
        "hour": [10, 10, 10, 10, 10],
        "minute": [30, 30, 30, 30, 30],
        "second": [0, 1, 2, 3, 4],
        "pm": [0, 0, 0, 0, 0],  # AM
        "isAttacker": [0, 1, 0, 1, 0],
    }

    # Create pandas DataFrame first
    pdf = pd.DataFrame(data)

    # Convert to Dask DataFrame with 1 partition (small test data)
    ddf = dd.from_pandas(pdf, npartitions=1)

    return ddf


@pytest.fixture(scope="function")
def small_bsm_dask_dataset(dask_client):
    """
    Generate a small BSM Dask dataset (100 rows) for integration testing.

    Creates realistic BSM data with temporal progression and spatial distribution.
    Useful for testing full pipeline flows without excessive runtime.

    Args:
        dask_client: Dask client fixture

    Returns:
        dd.DataFrame: Generated Dask DataFrame with 100 raw BSM records

    Example:
        def test_pipeline_end_to_end(small_bsm_dask_dataset):
            cleaned = clean_data(small_bsm_dask_dataset)
            assert len(cleaned.compute()) <= len(small_bsm_dask_dataset.compute())
    """
    num_rows = 100
    base_time = datetime(2021, 4, 6, 10, 0, 0)

    data = {
        "metadata_generatedAt": [],
        "metadata_recordType": [],
        "metadata_serialId_streamId": [],
        "metadata_serialId_bundleSize": [],
        "metadata_serialId_bundleId": [],
        "metadata_serialId_recordId": [],
        "metadata_serialId_serialNumber": [],
        "metadata_receivedAt": [],
        "coreData_id": [],
        "coreData_position_lat": [],
        "coreData_position_long": [],
        "coreData_accuracy_semiMajor": [],
        "coreData_accuracy_semiMinor": [],
        "coreData_elevation": [],
        "coreData_accelSet_accelYaw": [],
        "coreData_speed": [],
        "coreData_heading": [],
        "coreData_secMark": [],
    }

    for i in range(num_rows):
        timestamp = base_time + timedelta(seconds=i)
        data["metadata_generatedAt"].append(timestamp.strftime("%m/%d/%Y %I:%M:%S %p"))
        data["metadata_recordType"].append("bsmTx")
        data["metadata_serialId_streamId"].append(f"stream-{i % 10}")
        data["metadata_serialId_bundleSize"].append(1)
        data["metadata_serialId_bundleId"].append(i // 10)
        data["metadata_serialId_recordId"].append(i % 10)
        data["metadata_serialId_serialNumber"].append(i)
        data["metadata_receivedAt"].append(timestamp.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        data["coreData_id"].append(f"{i:08X}")  # Sequential hex IDs
        data["coreData_position_lat"].append(41.25 + (i * 0.0001))  # Slight northward movement
        data["coreData_position_long"].append(-105.93 + (i * 0.0001))  # Slight eastward movement
        data["coreData_accuracy_semiMajor"].append(5.0 + (i % 3))
        data["coreData_accuracy_semiMinor"].append(5.0 + (i % 3))
        data["coreData_elevation"].append(2190.0 + float(i % 10))
        data["coreData_accelSet_accelYaw"].append(float((i % 5 - 2)) * 0.5)  # Range: -1.0 to 1.0
        data["coreData_speed"].append(10.0 + float(i % 30))  # Speed varies 10-40
        data["coreData_heading"].append(float((i * 3) % 360))  # Rotating heading
        data["coreData_secMark"].append((30000 + i * 100) % 60000)

    # Create pandas DataFrame first
    pdf = pd.DataFrame(data)

    # Convert to Dask DataFrame with 2 partitions for parallelism
    ddf = dd.from_pandas(pdf, npartitions=2)

    return ddf


@pytest.fixture(scope="function")
def medium_bsm_dask_dataset(dask_client):
    """
    Generate a medium BSM Dask dataset (1000 rows) for performance testing.

    Creates realistic BSM data suitable for testing performance characteristics
    and parallelism without overwhelming test resources.

    Args:
        dask_client: Dask client fixture

    Returns:
        dd.DataFrame: Generated Dask DataFrame with 1000 raw BSM records

    Example:
        def test_attack_simulation_performance(medium_bsm_dask_dataset):
            import time
            start = time.time()
            attacked = apply_attacks(medium_bsm_dask_dataset)
            result = attacked.compute()
            duration = time.time() - start
            assert duration < 10.0  # Should complete in < 10 seconds
    """
    num_rows = 1000
    base_time = datetime(2021, 4, 6, 10, 0, 0)

    data = {
        "metadata_generatedAt": [],
        "metadata_recordType": [],
        "metadata_serialId_streamId": [],
        "metadata_serialId_bundleSize": [],
        "metadata_serialId_bundleId": [],
        "metadata_serialId_recordId": [],
        "metadata_serialId_serialNumber": [],
        "metadata_receivedAt": [],
        "coreData_id": [],
        "coreData_position_lat": [],
        "coreData_position_long": [],
        "coreData_accuracy_semiMajor": [],
        "coreData_accuracy_semiMinor": [],
        "coreData_elevation": [],
        "coreData_accelSet_accelYaw": [],
        "coreData_speed": [],
        "coreData_heading": [],
        "coreData_secMark": [],
    }

    for i in range(num_rows):
        timestamp = base_time + timedelta(milliseconds=i * 100)  # 100ms intervals
        data["metadata_generatedAt"].append(timestamp.strftime("%m/%d/%Y %I:%M:%S %p"))
        data["metadata_recordType"].append("bsmTx")
        data["metadata_serialId_streamId"].append(f"stream-{i % 20}")
        data["metadata_serialId_bundleSize"].append(1)
        data["metadata_serialId_bundleId"].append(i // 20)
        data["metadata_serialId_recordId"].append(i % 20)
        data["metadata_serialId_serialNumber"].append(i)
        data["metadata_receivedAt"].append(timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z")
        data["coreData_id"].append(f"{i:08X}")
        data["coreData_position_lat"].append(41.25 + (i * 0.00001))
        data["coreData_position_long"].append(-105.93 + (i * 0.00001))
        data["coreData_accuracy_semiMajor"].append(4.0 + (i % 5))
        data["coreData_accuracy_semiMinor"].append(4.0 + (i % 5))
        data["coreData_elevation"].append(2185.0 + float(i % 20))
        data["coreData_accelSet_accelYaw"].append(float((i % 11) - 5) * 0.2)  # Range: -1.0 to 1.0
        data["coreData_speed"].append(5.0 + float(i % 40))  # Speed varies 5-45
        data["coreData_heading"].append(float((i * 5) % 360))
        data["coreData_secMark"].append((28000 + i * 50) % 60000)

    # Create pandas DataFrame first
    pdf = pd.DataFrame(data)

    # Convert to Dask DataFrame with 4 partitions for better parallelism
    ddf = dd.from_pandas(pdf, npartitions=4)

    return ddf


@pytest.fixture(scope="function")
def dask_df_comparer(dask_client):
    """
    Utility fixture for comparing Dask DataFrames in tests.

    Provides helper functions for DataFrame comparison with tolerance for
    floating-point differences and flexible column ordering.

    Args:
        dask_client: Dask client fixture

    Returns:
        object: Comparer object with comparison methods

    Example:
        def test_transformations(dask_df_comparer, sample_dask_df):
            expected = create_expected_output()
            actual = my_transformation(sample_dask_df)
            dask_df_comparer.assert_equal(expected, actual, rtol=1e-5)
    """
    class DaskDataFrameComparer:
        """Helper class for comparing Dask DataFrames."""

        def assert_equal(self, df1: dd.DataFrame, df2: dd.DataFrame,
                        check_dtype=True, rtol=1e-5, atol=1e-8,
                        ignore_column_order=False):
            """
            Assert that two Dask DataFrames are equal.

            Args:
                df1: First Dask DataFrame
                df2: Second Dask DataFrame
                check_dtype: Whether to check data types match
                rtol: Relative tolerance for floating-point comparison
                atol: Absolute tolerance for floating-point comparison
                ignore_column_order: If True, sort columns before comparison

            Raises:
                AssertionError: If DataFrames are not equal
            """
            # Compute DataFrames to pandas for comparison
            pdf1 = df1.compute()
            pdf2 = df2.compute()

            # Check shapes
            assert pdf1.shape == pdf2.shape, f"Shapes differ: {pdf1.shape} vs {pdf2.shape}"

            # Check columns
            cols1 = set(pdf1.columns)
            cols2 = set(pdf2.columns)
            assert cols1 == cols2, f"Column sets differ: {cols1 - cols2} | {cols2 - cols1}"

            # Sort columns if requested
            if ignore_column_order:
                sorted_cols = sorted(pdf1.columns)
                pdf1 = pdf1[sorted_cols]
                pdf2 = pdf2[sorted_cols]

            # Sort rows for consistent comparison
            pdf1 = pdf1.sort_values(by=list(pdf1.columns)).reset_index(drop=True)
            pdf2 = pdf2.sort_values(by=list(pdf2.columns)).reset_index(drop=True)

            # Use pandas testing for detailed comparison
            import pandas.testing as pdt
            pdt.assert_frame_equal(pdf1, pdf2, rtol=rtol, atol=atol, check_dtype=check_dtype)

        def assert_pandas_dask_equal(self, pdf: pd.DataFrame, ddf: dd.DataFrame,
                                     check_dtype=True, rtol=1e-5, atol=1e-8,
                                     ignore_column_order=False):
            """
            Assert that a pandas DataFrame equals a Dask DataFrame (after compute).

            Args:
                pdf: Pandas DataFrame
                ddf: Dask DataFrame
                check_dtype: Whether to check data types match
                rtol: Relative tolerance for floating-point comparison
                atol: Absolute tolerance for floating-point comparison
                ignore_column_order: If True, sort columns before comparison

            Raises:
                AssertionError: If DataFrames are not equal
            """
            # Compute Dask DataFrame to pandas
            computed_pdf = ddf.compute()

            # Check shapes
            assert pdf.shape == computed_pdf.shape, f"Shapes differ: {pdf.shape} vs {computed_pdf.shape}"

            # Check columns
            cols1 = set(pdf.columns)
            cols2 = set(computed_pdf.columns)
            assert cols1 == cols2, f"Column sets differ: {cols1 - cols2} | {cols2 - cols1}"

            # Sort columns if requested
            if ignore_column_order:
                sorted_cols = sorted(pdf.columns)
                pdf = pdf[sorted_cols]
                computed_pdf = computed_pdf[sorted_cols]

            # Sort rows for consistent comparison
            pdf = pdf.sort_values(by=list(pdf.columns)).reset_index(drop=True)
            computed_pdf = computed_pdf.sort_values(by=list(computed_pdf.columns)).reset_index(drop=True)

            # Use pandas testing for detailed comparison
            import pandas.testing as pdt
            pdt.assert_frame_equal(pdf, computed_pdf, rtol=rtol, atol=atol, check_dtype=check_dtype)

        def assert_schema_equal(self, df1: dd.DataFrame, df2: dd.DataFrame):
            """Assert that two Dask DataFrames have the same schema."""
            dtypes1 = df1.dtypes.to_dict()
            dtypes2 = df2.dtypes.to_dict()
            assert dtypes1 == dtypes2, f"Schemas differ:\n{dtypes1}\nvs\n{dtypes2}"

        def assert_column_exists(self, df: dd.DataFrame, column_name: str):
            """Assert that a Dask DataFrame contains a specific column."""
            assert column_name in df.columns, f"Column '{column_name}' not found in {list(df.columns)}"

    return DaskDataFrameComparer()

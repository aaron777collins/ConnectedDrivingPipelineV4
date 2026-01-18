"""
Tests for DaskMConnectedDrivingDataCleaner.

This module tests the Dask implementation of the ML data cleaner.
Since DaskMConnectedDrivingDataCleaner uses @StandardDependencyInjection,
we focus on testing the underlying UDF (hex_to_decimal) and column selection logic.
"""

import pytest
import dask.dataframe as dd
import pandas as pd
from Helpers.DaskUDFs import hex_to_decimal


@pytest.mark.dask
class TestDaskMLCleanerHexConversion:
    """Test suite for hex_to_decimal UDF used by DaskMConnectedDrivingDataCleaner."""

    def test_hex_to_decimal_basic_conversion(self, dask_client):
        """Test basic hexadecimal to decimal conversion."""
        pdf = pd.DataFrame({
            'coreData_id': ['0xa1b2c3d4', '0x1a2b3c4d', '0xdeadbeef']
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply hex_to_decimal
        result = ddf['coreData_id'].apply(
            hex_to_decimal,
            meta=('coreData_id', 'i8')
        ).compute()

        # Verify conversions
        # 0xa1b2c3d4 = 2712847316
        # 0x1a2b3c4d = 439041101
        # 0xdeadbeef = 3735928559
        expected = [2712847316, 439041101, 3735928559]
        assert result.tolist() == expected

    def test_hex_with_decimal_point(self, dask_client):
        """Test hex conversion handles decimal points (edge case)."""
        pdf = pd.DataFrame({
            'coreData_id': ['0xa1b2c3d4.0', '0x1a2b3c4d.00', '0xdeadbeef.0']
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply hex_to_decimal
        result = ddf['coreData_id'].apply(
            hex_to_decimal,
            meta=('coreData_id', 'i8')
        ).compute()

        # Verify decimal point is stripped before conversion
        expected = [2712847316, 439041101, 3735928559]
        assert result.tolist() == expected

    def test_large_hex_values(self, dask_client):
        """Test conversion of large hexadecimal values."""
        pdf = pd.DataFrame({
            'coreData_id': [
                '0xffffffffffffffff',  # Max 64-bit value
                '0x8000000000000000',  # Min negative 64-bit (interpreted as positive)
                '0x123456789abcdef0'   # Large random value
            ]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply hex_to_decimal
        result = ddf['coreData_id'].apply(
            hex_to_decimal,
            meta=('coreData_id', 'i8')
        ).compute()

        # Verify conversion
        expected = [18446744073709551615, 9223372036854775808, 1311768467463790320]
        assert result.tolist() == expected

    def test_hex_none_values(self, dask_client):
        """Test hex conversion handles None values."""
        pdf = pd.DataFrame({
            'coreData_id': ['0xa1b2c3d4', None, '0xdeadbeef']
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Apply hex_to_decimal
        result = ddf['coreData_id'].apply(
            hex_to_decimal,
            meta=('coreData_id', 'float64')  # Use float64 to allow NaN
        ).compute()

        # Verify None is converted properly
        assert result[0] == 2712847316
        assert pd.isna(result[1])  # None becomes NaN
        assert result[2] == 3735928559

    def test_column_selection_pattern(self, dask_client):
        """Test column selection pattern used by cleaner."""
        # Create full DataFrame with extra columns
        pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0],
            'y_pos': [50.0, 150.0, 250.0],
            'speed': [30.0, 45.0, 60.0],
            'heading': [90.0, 180.0, 270.0],
            'isAttacker': [0, 1, 0],
            'timestamp': [1000, 2000, 3000],  # Extra column
            'extra_col': ['a', 'b', 'c']  # Extra column
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Select only ML features (pattern used by cleaner)
        feature_columns = ['x_pos', 'y_pos', 'speed', 'heading', 'isAttacker']
        result = ddf[feature_columns]

        # Verify only specified columns are present
        assert list(result.columns) == feature_columns

        # Verify data integrity
        result_pdf = result.compute()
        assert len(result_pdf) == 3
        assert result_pdf['x_pos'].tolist() == [100.0, 200.0, 300.0]

    def test_column_selection_with_hex_conversion(self, dask_client):
        """Test combined column selection and hex conversion pattern."""
        # Create DataFrame with hex IDs
        pdf = pd.DataFrame({
            'coreData_id': ['0xa1b2c3d4', '0x1a2b3c4d', '0xdeadbeef'],
            'x_pos': [100.0, 200.0, 300.0],
            'y_pos': [50.0, 150.0, 250.0],
            'speed': [30.0, 45.0, 60.0],
            'isAttacker': [0, 1, 0],
            'timestamp': [1000, 2000, 3000]  # Extra column
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Select columns (including coreData_id)
        feature_columns = ['coreData_id', 'x_pos', 'y_pos', 'speed', 'isAttacker']
        result = ddf[feature_columns]

        # Convert hex to decimal
        result = result.assign(
            coreData_id=result['coreData_id'].apply(
                hex_to_decimal,
                meta=('coreData_id', 'i8')
            )
        )

        # Verify result
        result_pdf = result.compute()
        assert list(result_pdf.columns) == feature_columns
        expected_ids = [2712847316, 439041101, 3735928559]
        assert result_pdf['coreData_id'].tolist() == expected_ids

    def test_empty_dataframe_pattern(self, dask_client):
        """Test column selection with empty DataFrame."""
        pdf = pd.DataFrame(columns=['x_pos', 'y_pos', 'speed', 'heading', 'isAttacker'])
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Select columns
        feature_columns = ['x_pos', 'y_pos', 'speed', 'heading', 'isAttacker']
        result = ddf[feature_columns]

        # Verify empty result
        result_pdf = result.compute()
        assert len(result_pdf) == 0
        assert list(result_pdf.columns) == feature_columns

    def test_lazy_evaluation_pattern(self, dask_client):
        """Test that operations maintain lazy evaluation."""
        pdf = pd.DataFrame({
            'coreData_id': ['0xa1b2c3d4', '0x1a2b3c4d'],
            'x_pos': [100.0, 200.0],
            'isAttacker': [0, 1]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Select columns
        result = ddf[['coreData_id', 'x_pos', 'isAttacker']]

        # Apply hex conversion
        result = result.assign(
            coreData_id=result['coreData_id'].apply(
                hex_to_decimal,
                meta=('coreData_id', 'i8')
            )
        )

        # Verify result is still lazy
        assert isinstance(result, dd.DataFrame)
        assert not isinstance(result, pd.DataFrame)

    def test_partition_preservation(self, dask_client):
        """Test that operations preserve partition count."""
        pdf = pd.DataFrame({
            'x_pos': list(range(100)),
            'y_pos': list(range(100, 200)),
            'isAttacker': [0] * 100
        })
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Select columns
        result = ddf[['x_pos', 'y_pos', 'isAttacker']]

        # Verify partitions preserved
        assert result.npartitions == ddf.npartitions

    def test_sklearn_compatibility_pattern(self, dask_client):
        """Test that cleaned data can be used with sklearn after compute()."""
        pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0],
            'isAttacker': [0, 1, 0, 1, 0]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Select columns
        feature_columns = ['x_pos', 'y_pos', 'speed', 'heading', 'isAttacker']
        result = ddf[feature_columns]

        # Compute to pandas for sklearn
        result_pandas = result.compute()

        # Verify it's a pandas DataFrame (required for sklearn)
        assert isinstance(result_pandas, pd.DataFrame)

        # Verify we can separate features and labels (common ML pattern)
        X = result_pandas.drop('isAttacker', axis=1)
        y = result_pandas['isAttacker']

        assert X.shape == (5, 4)  # 5 rows, 4 features
        assert y.shape == (5,)    # 5 labels
        assert list(y) == [0, 1, 0, 1, 0]

    def test_multiple_hex_columns(self, dask_client):
        """Test hex conversion with multiple ID columns."""
        pdf = pd.DataFrame({
            'id1': ['0xa1b2c3d4', '0x1a2b3c4d'],
            'id2': ['0xdeadbeef', '0xcafebabe'],
            'value': [100, 200]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Convert both hex columns
        result = ddf.assign(
            id1=ddf['id1'].apply(hex_to_decimal, meta=('id1', 'i8')),
            id2=ddf['id2'].apply(hex_to_decimal, meta=('id2', 'i8'))
        )

        result_pdf = result.compute()

        # Verify both conversions
        assert result_pdf['id1'].tolist() == [2712847316, 439041101]
        assert result_pdf['id2'].tolist() == [3735928559, 3405691582]
        assert result_pdf['value'].tolist() == [100, 200]

    def test_hex_conversion_preserves_other_columns(self, dask_client):
        """Test that hex conversion doesn't affect other columns."""
        pdf = pd.DataFrame({
            'coreData_id': ['0xa1b2c3d4', '0x1a2b3c4d'],
            'int_col': [1, 2],
            'float_col': [1.5, 2.5],
            'str_col': ['a', 'b'],
            'bool_col': [True, False]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Convert hex column
        result = ddf.assign(
            coreData_id=ddf['coreData_id'].apply(
                hex_to_decimal,
                meta=('coreData_id', 'i8')
            )
        )

        result_pdf = result.compute()

        # Verify other columns unchanged
        assert result_pdf['int_col'].tolist() == [1, 2]
        assert result_pdf['float_col'].tolist() == [1.5, 2.5]
        assert result_pdf['str_col'].tolist() == ['a', 'b']
        assert result_pdf['bool_col'].tolist() == [True, False]

        # Verify hex conversion worked
        assert result_pdf['coreData_id'].tolist() == [2712847316, 439041101]

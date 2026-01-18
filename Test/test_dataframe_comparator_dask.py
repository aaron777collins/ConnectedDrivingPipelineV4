"""
Tests for DataFrameComparator Dask methods.

This module validates that the Dask comparison methods in DataFrameComparator
work correctly for comparing Dask DataFrames and pandas vs Dask DataFrames.
"""

import pytest
import pandas as pd
import dask.dataframe as dd
from Test.Utils.DataFrameComparator import DataFrameComparator


class TestDataFrameComparatorDask:
    """Test suite for Dask comparison methods in DataFrameComparator."""

    @pytest.fixture
    def comparator(self):
        """Create a DataFrameComparator instance."""
        return DataFrameComparator()

    @pytest.fixture
    def sample_pandas_df(self):
        """Create a sample pandas DataFrame for testing."""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10.5, 20.3, 30.1, 40.9, 50.2],
            'category': ['A', 'B', 'A', 'C', 'B']
        })

    @pytest.fixture
    def sample_dask_df(self, sample_pandas_df):
        """Create a sample Dask DataFrame for testing."""
        return dd.from_pandas(sample_pandas_df, npartitions=2)

    def test_assert_dask_equal_identical(self, comparator, sample_dask_df):
        """Test that identical Dask DataFrames are equal."""
        ddf1 = sample_dask_df
        ddf2 = sample_dask_df.copy()

        # Should not raise any assertion
        comparator.assert_dask_equal(ddf1, ddf2)

    def test_assert_dask_equal_different_values(self, comparator, sample_pandas_df):
        """Test that different Dask DataFrames raise assertion error."""
        ddf1 = dd.from_pandas(sample_pandas_df, npartitions=2)

        modified_df = sample_pandas_df.copy()
        modified_df.loc[0, 'value'] = 999.9  # Change one value
        ddf2 = dd.from_pandas(modified_df, npartitions=2)

        with pytest.raises(AssertionError):
            comparator.assert_dask_equal(ddf1, ddf2)

    def test_assert_dask_equal_different_columns(self, comparator, sample_pandas_df):
        """Test that DataFrames with different columns raise assertion error."""
        ddf1 = dd.from_pandas(sample_pandas_df, npartitions=2)

        modified_df = sample_pandas_df.copy()
        modified_df['extra_column'] = [1, 2, 3, 4, 5]
        ddf2 = dd.from_pandas(modified_df, npartitions=2)

        # Will raise assertion about shapes first (columns counted in shape)
        with pytest.raises(AssertionError):
            comparator.assert_dask_equal(ddf1, ddf2)

    def test_assert_pandas_dask_equal_identical(self, comparator, sample_pandas_df, sample_dask_df):
        """Test that pandas and equivalent Dask DataFrames are equal."""
        # Should not raise any assertion
        comparator.assert_pandas_dask_equal(sample_pandas_df, sample_dask_df)

    def test_assert_pandas_dask_equal_different_values(self, comparator, sample_pandas_df):
        """Test that different pandas and Dask DataFrames raise assertion error."""
        modified_df = sample_pandas_df.copy()
        modified_df.loc[0, 'value'] = 999.9
        ddf = dd.from_pandas(modified_df, npartitions=2)

        with pytest.raises(AssertionError):
            comparator.assert_pandas_dask_equal(sample_pandas_df, ddf)

    def test_assert_pandas_dask_equal_different_row_counts(self, comparator, sample_pandas_df):
        """Test that DataFrames with different row counts raise assertion error."""
        truncated_df = sample_pandas_df.iloc[:3]  # Only 3 rows
        ddf = dd.from_pandas(truncated_df, npartitions=1)

        with pytest.raises(AssertionError, match="Row counts differ"):
            comparator.assert_pandas_dask_equal(sample_pandas_df, ddf)

    def test_assert_dask_schema_equal_identical(self, comparator, sample_dask_df):
        """Test that DataFrames with identical schemas are equal."""
        ddf1 = sample_dask_df
        ddf2 = sample_dask_df.copy()

        # Should not raise any assertion
        comparator.assert_dask_schema_equal(ddf1, ddf2)

    def test_assert_dask_schema_equal_different_types(self, comparator, sample_pandas_df):
        """Test that DataFrames with different types raise assertion error."""
        ddf1 = dd.from_pandas(sample_pandas_df, npartitions=2)

        modified_df = sample_pandas_df.copy()
        modified_df['id'] = modified_df['id'].astype(float)  # Change int to float
        ddf2 = dd.from_pandas(modified_df, npartitions=2)

        with pytest.raises(AssertionError, match="Schema data types differ"):
            comparator.assert_dask_schema_equal(ddf1, ddf2)

    def test_assert_dask_column_exists_present(self, comparator, sample_dask_df):
        """Test that existing columns are found."""
        # Should not raise any assertion
        comparator.assert_dask_column_exists(sample_dask_df, 'id')
        comparator.assert_dask_column_exists(sample_dask_df, 'value')
        comparator.assert_dask_column_exists(sample_dask_df, 'category')

    def test_assert_dask_column_exists_missing(self, comparator, sample_dask_df):
        """Test that missing columns raise assertion error."""
        with pytest.raises(AssertionError, match="Column 'missing_col' not found"):
            comparator.assert_dask_column_exists(sample_dask_df, 'missing_col')

    def test_assert_dask_columns_exist_all_present(self, comparator, sample_dask_df):
        """Test that all existing columns are found."""
        # Should not raise any assertion
        comparator.assert_dask_columns_exist(sample_dask_df, ['id', 'value', 'category'])

    def test_assert_dask_columns_exist_some_missing(self, comparator, sample_dask_df):
        """Test that missing columns in list raise assertion error."""
        with pytest.raises(AssertionError, match="Missing columns"):
            comparator.assert_dask_columns_exist(
                sample_dask_df,
                ['id', 'value', 'missing_col']
            )

    def test_assert_dask_equal_with_tolerance(self, comparator, sample_pandas_df):
        """Test floating-point comparison with tolerance."""
        ddf1 = dd.from_pandas(sample_pandas_df, npartitions=2)

        # Create nearly identical DataFrame with small floating-point differences
        modified_df = sample_pandas_df.copy()
        modified_df['value'] = modified_df['value'] + 1e-7  # Small difference
        ddf2 = dd.from_pandas(modified_df, npartitions=2)

        # Should pass with default tolerance
        comparator.assert_dask_equal(ddf1, ddf2, rtol=1e-5)

    def test_assert_dask_equal_ignore_column_order(self, comparator, sample_pandas_df):
        """Test comparison ignoring column order."""
        ddf1 = dd.from_pandas(sample_pandas_df, npartitions=2)

        # Reorder columns
        reordered_df = sample_pandas_df[['category', 'id', 'value']]
        ddf2 = dd.from_pandas(reordered_df, npartitions=2)

        # Should pass when ignoring column order
        comparator.assert_dask_equal(ddf1, ddf2, ignore_column_order=True)

    def test_assert_dask_equal_ignore_row_order(self, comparator, sample_pandas_df):
        """Test comparison ignoring row order."""
        ddf1 = dd.from_pandas(sample_pandas_df, npartitions=2)

        # Shuffle rows
        shuffled_df = sample_pandas_df.sample(frac=1, random_state=42).reset_index(drop=True)
        ddf2 = dd.from_pandas(shuffled_df, npartitions=2)

        # Should pass when ignoring row order (default behavior)
        comparator.assert_dask_equal(ddf1, ddf2, ignore_row_order=True)

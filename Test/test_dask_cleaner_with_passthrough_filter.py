"""
Tests for DaskCleanerWithPassthroughFilter.

This module tests the Dask implementation of the passthrough filter cleaner,
which should return DataFrames unchanged.
"""

import pytest
import dask.dataframe as dd
import pandas as pd
from Generator.Cleaners.CleanersWithFilters.DaskCleanerWithPassthroughFilter import DaskCleanerWithPassthroughFilter


@pytest.mark.dask
class TestDaskCleanerWithPassthroughFilter:
    """Test suite for DaskCleanerWithPassthroughFilter."""

    def test_passthrough_returns_same_dataframe(self, dask_client):
        """Test that passthrough filter returns the DataFrame unchanged."""
        # Create sample data
        pdf = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'label': ['a', 'b', 'c', 'd', 'e']
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Create cleaner instance (manual instantiation without DI)
        # Since we're testing just the passthrough method, we don't need full initialization
        from unittest.mock import Mock
        cleaner = DaskCleanerWithPassthroughFilter.__new__(DaskCleanerWithPassthroughFilter)
        cleaner.logger = Mock()

        # Apply passthrough filter
        result = cleaner.passthrough(ddf)

        # Verify the result is identical to input
        assert result is ddf, "Passthrough should return the same DataFrame object"

        # Verify data integrity (use check_dtype=False to handle string type differences)
        result_pdf = result.compute()
        pd.testing.assert_frame_equal(result_pdf.reset_index(drop=True),
                                      pdf.reset_index(drop=True),
                                      check_dtype=False)

    def test_passthrough_preserves_schema(self, dask_client):
        """Test that passthrough preserves DataFrame schema."""
        # Create sample data with various types
        pdf = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'str_col': ['x', 'y', 'z'],
            'bool_col': [True, False, True]
        })
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Create cleaner instance
        from unittest.mock import Mock
        cleaner = DaskCleanerWithPassthroughFilter.__new__(DaskCleanerWithPassthroughFilter)
        cleaner.logger = Mock()

        # Apply passthrough
        result = cleaner.passthrough(ddf)

        # Verify schema
        assert list(result.columns) == list(ddf.columns)
        assert result.dtypes.to_dict() == ddf.dtypes.to_dict()

    def test_passthrough_with_empty_dataframe(self, dask_client):
        """Test passthrough with empty DataFrame."""
        # Create empty DataFrame
        pdf = pd.DataFrame(columns=['a', 'b', 'c'])
        ddf = dd.from_pandas(pdf, npartitions=1)

        # Create cleaner instance
        from unittest.mock import Mock
        cleaner = DaskCleanerWithPassthroughFilter.__new__(DaskCleanerWithPassthroughFilter)
        cleaner.logger = Mock()

        # Apply passthrough
        result = cleaner.passthrough(ddf)

        # Verify empty DataFrame is returned
        assert len(result) == 0
        assert list(result.columns) == ['a', 'b', 'c']

    def test_passthrough_preserves_partitions(self, dask_client):
        """Test that passthrough preserves partition count."""
        # Create data with specific partition count
        pdf = pd.DataFrame({'x': range(100)})
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Create cleaner instance
        from unittest.mock import Mock
        cleaner = DaskCleanerWithPassthroughFilter.__new__(DaskCleanerWithPassthroughFilter)
        cleaner.logger = Mock()

        # Apply passthrough
        result = cleaner.passthrough(ddf)

        # Verify partitions preserved
        assert result.npartitions == ddf.npartitions

    def test_passthrough_lazy_evaluation(self, dask_client):
        """Test that passthrough maintains lazy evaluation."""
        # Create data
        pdf = pd.DataFrame({'x': range(10)})
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Create cleaner instance
        from unittest.mock import Mock
        cleaner = DaskCleanerWithPassthroughFilter.__new__(DaskCleanerWithPassthroughFilter)
        cleaner.logger = Mock()

        # Apply passthrough
        result = cleaner.passthrough(ddf)

        # Verify result is still lazy (not computed)
        assert isinstance(result, dd.DataFrame)
        assert not isinstance(result, pd.DataFrame)

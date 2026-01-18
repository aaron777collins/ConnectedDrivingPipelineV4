"""
DataFrame comparison utilities for testing pandas vs PySpark/Dask migrations.

This module provides utilities for comparing DataFrames during the migration
from pandas to PySpark and Dask, allowing validation that implementations
produce equivalent results to pandas implementations.

Usage:
    from Test.Utils.DataFrameComparator import DataFrameComparator

    comparator = DataFrameComparator()

    # Compare two PySpark DataFrames
    comparator.assert_spark_equal(df1, df2, rtol=1e-5)

    # Compare PySpark vs pandas DataFrames
    comparator.assert_pandas_spark_equal(pandas_df, spark_df, rtol=1e-5)

    # Compare two Dask DataFrames
    comparator.assert_dask_equal(ddf1, ddf2, rtol=1e-5)

    # Compare pandas vs Dask DataFrames
    comparator.assert_pandas_dask_equal(pandas_df, dask_df, rtol=1e-5)

    # Compare schemas only
    comparator.assert_schema_equal(df1, df2)
"""

from typing import List, Optional, Set, Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

try:
    import dask.dataframe as dd
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False


class DataFrameComparator:
    """
    Utility class for comparing DataFrames during pandas to PySpark migration.

    Provides methods for:
    - Comparing two PySpark DataFrames
    - Comparing pandas vs PySpark DataFrames
    - Schema comparison
    - Column existence checks
    - Tolerance-based comparison for floating-point values
    """

    def __init__(self):
        """Initialize the DataFrameComparator."""
        pass

    def assert_spark_equal(
        self,
        df1: SparkDataFrame,
        df2: SparkDataFrame,
        check_dtype: bool = True,
        rtol: float = 1e-5,
        atol: float = 1e-8,
        ignore_column_order: bool = False,
        ignore_row_order: bool = True
    ):
        """
        Assert that two PySpark DataFrames are equal.

        Args:
            df1: First PySpark DataFrame
            df2: Second PySpark DataFrame
            check_dtype: Whether to check data types match (default: True)
            rtol: Relative tolerance for floating-point comparison (default: 1e-5)
            atol: Absolute tolerance for floating-point comparison (default: 1e-8)
            ignore_column_order: If True, sort columns before comparison (default: False)
            ignore_row_order: If True, sort rows before comparison (default: True)

        Raises:
            AssertionError: If DataFrames are not equal

        Example:
            comparator = DataFrameComparator()
            comparator.assert_spark_equal(expected, actual, rtol=1e-5)
        """
        # Check counts
        count1, count2 = df1.count(), df2.count()
        assert count1 == count2, f"Row counts differ: {count1} vs {count2}"

        # Check columns
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        assert cols1 == cols2, (
            f"Column sets differ:\n"
            f"  Only in df1: {cols1 - cols2}\n"
            f"  Only in df2: {cols2 - cols1}"
        )

        # Sort columns if requested
        if ignore_column_order:
            sorted_cols = sorted(df1.columns)
            df1 = df1.select(*sorted_cols)
            df2 = df2.select(*sorted_cols)

        # Check schemas
        if check_dtype:
            self.assert_schema_equal(df1, df2)

        # Compare data by converting to pandas (works for test-sized data)
        # For production-sized data, use Spark joins instead
        pdf1 = df1.toPandas()
        pdf2 = df2.toPandas()

        # Sort rows if requested (for stable comparison)
        if ignore_row_order:
            pdf1 = pdf1.sort_values(by=list(df1.columns)).reset_index(drop=True)
            pdf2 = pdf2.sort_values(by=list(df2.columns)).reset_index(drop=True)

        # Use pandas testing for detailed comparison
        import pandas.testing as pdt
        pdt.assert_frame_equal(
            pdf1, pdf2,
            rtol=rtol,
            atol=atol,
            check_dtype=check_dtype
        )

    def assert_pandas_spark_equal(
        self,
        pandas_df: pd.DataFrame,
        spark_df: SparkDataFrame,
        rtol: float = 1e-5,
        atol: float = 1e-8,
        ignore_column_order: bool = False,
        check_dtype: bool = False  # Default to False for cross-platform comparison
    ):
        """
        Assert that a pandas DataFrame and PySpark DataFrame are equal.

        Useful for validating that PySpark implementations produce the same
        results as pandas implementations.

        Args:
            pandas_df: Pandas DataFrame (expected/baseline)
            spark_df: PySpark DataFrame (actual/new implementation)
            rtol: Relative tolerance for floating-point comparison (default: 1e-5)
            atol: Absolute tolerance for floating-point comparison (default: 1e-8)
            ignore_column_order: If True, sort columns before comparison (default: False)
            check_dtype: Whether to check data types match (default: False)
                        Set to False by default because pandas/PySpark have different type systems

        Raises:
            AssertionError: If DataFrames are not equal

        Example:
            comparator = DataFrameComparator()
            pandas_result = pandas_pipeline.run(data)
            pyspark_result = pyspark_pipeline.run(data)
            comparator.assert_pandas_spark_equal(pandas_result, pyspark_result)
        """
        # Convert PySpark DataFrame to pandas
        spark_as_pandas = spark_df.toPandas()

        # Check row counts
        assert len(pandas_df) == len(spark_as_pandas), (
            f"Row counts differ: pandas={len(pandas_df)} vs spark={len(spark_as_pandas)}"
        )

        # Check columns
        pandas_cols = set(pandas_df.columns)
        spark_cols = set(spark_as_pandas.columns)
        assert pandas_cols == spark_cols, (
            f"Column sets differ:\n"
            f"  Only in pandas: {pandas_cols - spark_cols}\n"
            f"  Only in spark: {spark_cols - pandas_cols}"
        )

        # Sort columns if requested
        if ignore_column_order:
            sorted_cols = sorted(pandas_df.columns)
            pandas_df = pandas_df[sorted_cols]
            spark_as_pandas = spark_as_pandas[sorted_cols]

        # Sort both DataFrames for stable comparison
        pandas_sorted = pandas_df.sort_values(by=list(pandas_df.columns)).reset_index(drop=True)
        spark_sorted = spark_as_pandas.sort_values(by=list(spark_as_pandas.columns)).reset_index(drop=True)

        # Use pandas testing for detailed comparison
        import pandas.testing as pdt
        pdt.assert_frame_equal(
            pandas_sorted,
            spark_sorted,
            rtol=rtol,
            atol=atol,
            check_dtype=check_dtype
        )

    def assert_schema_equal(
        self,
        df1: SparkDataFrame,
        df2: SparkDataFrame
    ):
        """
        Assert that two PySpark DataFrames have the same schema.

        Args:
            df1: First PySpark DataFrame
            df2: Second PySpark DataFrame

        Raises:
            AssertionError: If schemas differ

        Example:
            comparator = DataFrameComparator()
            comparator.assert_schema_equal(expected_schema_df, actual_schema_df)
        """
        schema1 = {f.name: f.dataType for f in df1.schema.fields}
        schema2 = {f.name: f.dataType for f in df2.schema.fields}

        # Check column names match
        cols1 = set(schema1.keys())
        cols2 = set(schema2.keys())
        assert cols1 == cols2, (
            f"Schema column names differ:\n"
            f"  Only in df1: {cols1 - cols2}\n"
            f"  Only in df2: {cols2 - cols1}"
        )

        # Check data types match for each column
        type_mismatches = []
        for col in schema1.keys():
            if schema1[col] != schema2[col]:
                type_mismatches.append(
                    f"  {col}: {schema1[col]} vs {schema2[col]}"
                )

        assert not type_mismatches, (
            f"Schema data types differ:\n" + "\n".join(type_mismatches)
        )

    def assert_column_exists(
        self,
        df: SparkDataFrame,
        column_name: str
    ):
        """
        Assert that a PySpark DataFrame contains a specific column.

        Args:
            df: PySpark DataFrame
            column_name: Name of column to check

        Raises:
            AssertionError: If column does not exist

        Example:
            comparator = DataFrameComparator()
            comparator.assert_column_exists(result_df, 'isAttacker')
        """
        assert column_name in df.columns, (
            f"Column '{column_name}' not found in DataFrame.\n"
            f"Available columns: {df.columns}"
        )

    def assert_columns_exist(
        self,
        df: SparkDataFrame,
        column_names: List[str]
    ):
        """
        Assert that a PySpark DataFrame contains multiple specific columns.

        Args:
            df: PySpark DataFrame
            column_names: List of column names to check

        Raises:
            AssertionError: If any column does not exist

        Example:
            comparator = DataFrameComparator()
            comparator.assert_columns_exist(
                result_df,
                ['x_pos', 'y_pos', 'isAttacker']
            )
        """
        missing_columns = [col for col in column_names if col not in df.columns]
        assert not missing_columns, (
            f"Missing columns: {missing_columns}\n"
            f"Available columns: {df.columns}"
        )

    def get_column_diff(
        self,
        df1: SparkDataFrame,
        df2: SparkDataFrame
    ) -> dict:
        """
        Get the difference between columns of two DataFrames.

        Args:
            df1: First PySpark DataFrame
            df2: Second PySpark DataFrame

        Returns:
            dict: Dictionary with keys:
                - 'only_in_df1': Set of columns only in df1
                - 'only_in_df2': Set of columns only in df2
                - 'common': Set of columns in both
                - 'type_mismatches': Dict of columns with different types

        Example:
            comparator = DataFrameComparator()
            diff = comparator.get_column_diff(df1, df2)
            if diff['only_in_df1']:
                print(f"Columns only in df1: {diff['only_in_df1']}")
        """
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        common = cols1 & cols2

        # Check type mismatches for common columns
        schema1 = {f.name: f.dataType for f in df1.schema.fields}
        schema2 = {f.name: f.dataType for f in df2.schema.fields}

        type_mismatches = {}
        for col in common:
            if schema1[col] != schema2[col]:
                type_mismatches[col] = {
                    'df1_type': schema1[col],
                    'df2_type': schema2[col]
                }

        return {
            'only_in_df1': cols1 - cols2,
            'only_in_df2': cols2 - cols1,
            'common': common,
            'type_mismatches': type_mismatches
        }

    def print_comparison_summary(
        self,
        df1: SparkDataFrame,
        df2: SparkDataFrame,
        name1: str = "df1",
        name2: str = "df2"
    ):
        """
        Print a summary comparison of two DataFrames.

        Useful for debugging test failures.

        Args:
            df1: First PySpark DataFrame
            df2: Second PySpark DataFrame
            name1: Name to display for df1 (default: "df1")
            name2: Name to display for df2 (default: "df2")

        Example:
            comparator = DataFrameComparator()
            comparator.print_comparison_summary(expected, actual, "Expected", "Actual")
        """
        print(f"\n{'='*60}")
        print(f"DataFrame Comparison: {name1} vs {name2}")
        print(f"{'='*60}")

        # Row counts
        count1 = df1.count()
        count2 = df2.count()
        print(f"\nRow Counts:")
        print(f"  {name1}: {count1}")
        print(f"  {name2}: {count2}")
        print(f"  Match: {'✓' if count1 == count2 else '✗'}")

        # Column diff
        diff = self.get_column_diff(df1, df2)
        print(f"\nColumn Comparison:")
        print(f"  Common columns: {len(diff['common'])}")

        if diff['only_in_df1']:
            print(f"  Only in {name1}: {diff['only_in_df1']}")
        if diff['only_in_df2']:
            print(f"  Only in {name2}: {diff['only_in_df2']}")

        if diff['type_mismatches']:
            print(f"\n  Type mismatches:")
            for col, types in diff['type_mismatches'].items():
                print(f"    {col}:")
                print(f"      {name1}: {types['df1_type']}")
                print(f"      {name2}: {types['df2_type']}")

        # Sample data
        print(f"\nSample Data ({name1}):")
        df1.show(5, truncate=50)

        print(f"\nSample Data ({name2}):")
        df2.show(5, truncate=50)

        print(f"{'='*60}\n")

    # ==================== DASK COMPARISON METHODS ====================

    def assert_dask_equal(
        self,
        df1,  # dd.DataFrame
        df2,  # dd.DataFrame
        check_dtype: bool = True,
        rtol: float = 1e-5,
        atol: float = 1e-8,
        ignore_column_order: bool = False,
        ignore_row_order: bool = True
    ):
        """
        Assert that two Dask DataFrames are equal.

        Args:
            df1: First Dask DataFrame
            df2: Second Dask DataFrame
            check_dtype: Whether to check data types match (default: True)
            rtol: Relative tolerance for floating-point comparison (default: 1e-5)
            atol: Absolute tolerance for floating-point comparison (default: 1e-8)
            ignore_column_order: If True, sort columns before comparison (default: False)
            ignore_row_order: If True, sort rows before comparison (default: True)

        Raises:
            AssertionError: If DataFrames are not equal
            ImportError: If Dask is not available

        Example:
            comparator = DataFrameComparator()
            comparator.assert_dask_equal(expected_ddf, actual_ddf, rtol=1e-9)
        """
        if not DASK_AVAILABLE:
            raise ImportError(
                "Dask is not available. Install with: pip install dask[complete]"
            )

        # Compute DataFrames to pandas for comparison
        pdf1 = df1.compute()
        pdf2 = df2.compute()

        # Check shapes
        assert pdf1.shape == pdf2.shape, (
            f"Shapes differ: {pdf1.shape} vs {pdf2.shape}"
        )

        # Check columns
        cols1 = set(pdf1.columns)
        cols2 = set(pdf2.columns)
        assert cols1 == cols2, (
            f"Column sets differ:\n"
            f"  Only in df1: {cols1 - cols2}\n"
            f"  Only in df2: {cols2 - cols1}"
        )

        # Sort columns if requested
        if ignore_column_order:
            sorted_cols = sorted(pdf1.columns)
            pdf1 = pdf1[sorted_cols]
            pdf2 = pdf2[sorted_cols]

        # Sort rows if requested (for stable comparison)
        if ignore_row_order:
            pdf1 = pdf1.sort_values(by=list(pdf1.columns)).reset_index(drop=True)
            pdf2 = pdf2.sort_values(by=list(pdf2.columns)).reset_index(drop=True)

        # Use pandas testing for detailed comparison
        import pandas.testing as pdt
        pdt.assert_frame_equal(
            pdf1, pdf2,
            rtol=rtol,
            atol=atol,
            check_dtype=check_dtype
        )

    def assert_pandas_dask_equal(
        self,
        pandas_df: pd.DataFrame,
        dask_df,  # dd.DataFrame
        rtol: float = 1e-5,
        atol: float = 1e-8,
        ignore_column_order: bool = False,
        check_dtype: bool = False  # Default to False for cross-platform comparison
    ):
        """
        Assert that a pandas DataFrame and Dask DataFrame are equal.

        Useful for validating that Dask implementations produce the same
        results as pandas implementations.

        Args:
            pandas_df: Pandas DataFrame (expected/baseline)
            dask_df: Dask DataFrame (actual/new implementation)
            rtol: Relative tolerance for floating-point comparison (default: 1e-5)
            atol: Absolute tolerance for floating-point comparison (default: 1e-8)
            ignore_column_order: If True, sort columns before comparison (default: False)
            check_dtype: Whether to check data types match (default: False)
                        Set to False by default because pandas/Dask may have different type systems

        Raises:
            AssertionError: If DataFrames are not equal
            ImportError: If Dask is not available

        Example:
            comparator = DataFrameComparator()
            pandas_result = pandas_pipeline.run(data)
            dask_result = dask_pipeline.run(data)
            comparator.assert_pandas_dask_equal(pandas_result, dask_result, rtol=1e-9)
        """
        if not DASK_AVAILABLE:
            raise ImportError(
                "Dask is not available. Install with: pip install dask[complete]"
            )

        # Compute Dask DataFrame to pandas
        dask_as_pandas = dask_df.compute()

        # Check row counts
        assert len(pandas_df) == len(dask_as_pandas), (
            f"Row counts differ: pandas={len(pandas_df)} vs dask={len(dask_as_pandas)}"
        )

        # Check columns
        pandas_cols = set(pandas_df.columns)
        dask_cols = set(dask_as_pandas.columns)
        assert pandas_cols == dask_cols, (
            f"Column sets differ:\n"
            f"  Only in pandas: {pandas_cols - dask_cols}\n"
            f"  Only in dask: {dask_cols - pandas_cols}"
        )

        # Sort columns if requested
        if ignore_column_order:
            sorted_cols = sorted(pandas_df.columns)
            pandas_df = pandas_df[sorted_cols]
            dask_as_pandas = dask_as_pandas[sorted_cols]

        # Sort both DataFrames for stable comparison
        pandas_sorted = pandas_df.sort_values(by=list(pandas_df.columns)).reset_index(drop=True)
        dask_sorted = dask_as_pandas.sort_values(by=list(dask_as_pandas.columns)).reset_index(drop=True)

        # Use pandas testing for detailed comparison
        import pandas.testing as pdt
        pdt.assert_frame_equal(
            pandas_sorted,
            dask_sorted,
            rtol=rtol,
            atol=atol,
            check_dtype=check_dtype
        )

    def assert_dask_schema_equal(
        self,
        df1,  # dd.DataFrame
        df2   # dd.DataFrame
    ):
        """
        Assert that two Dask DataFrames have the same schema.

        Args:
            df1: First Dask DataFrame
            df2: Second Dask DataFrame

        Raises:
            AssertionError: If schemas differ
            ImportError: If Dask is not available

        Example:
            comparator = DataFrameComparator()
            comparator.assert_dask_schema_equal(expected_schema_df, actual_schema_df)
        """
        if not DASK_AVAILABLE:
            raise ImportError(
                "Dask is not available. Install with: pip install dask[complete]"
            )

        # Get dtypes as dictionaries
        dtypes1 = df1.dtypes.to_dict()
        dtypes2 = df2.dtypes.to_dict()

        # Check column names match
        cols1 = set(dtypes1.keys())
        cols2 = set(dtypes2.keys())
        assert cols1 == cols2, (
            f"Schema column names differ:\n"
            f"  Only in df1: {cols1 - cols2}\n"
            f"  Only in df2: {cols2 - cols1}"
        )

        # Check data types match for each column
        type_mismatches = []
        for col in dtypes1.keys():
            if dtypes1[col] != dtypes2[col]:
                type_mismatches.append(
                    f"  {col}: {dtypes1[col]} vs {dtypes2[col]}"
                )

        assert not type_mismatches, (
            f"Schema data types differ:\n" + "\n".join(type_mismatches)
        )

    def assert_dask_column_exists(
        self,
        df,  # dd.DataFrame
        column_name: str
    ):
        """
        Assert that a Dask DataFrame contains a specific column.

        Args:
            df: Dask DataFrame
            column_name: Name of column to check

        Raises:
            AssertionError: If column does not exist
            ImportError: If Dask is not available

        Example:
            comparator = DataFrameComparator()
            comparator.assert_dask_column_exists(result_df, 'isAttacker')
        """
        if not DASK_AVAILABLE:
            raise ImportError(
                "Dask is not available. Install with: pip install dask[complete]"
            )

        assert column_name in df.columns, (
            f"Column '{column_name}' not found in DataFrame.\n"
            f"Available columns: {list(df.columns)}"
        )

    def assert_dask_columns_exist(
        self,
        df,  # dd.DataFrame
        column_names: List[str]
    ):
        """
        Assert that a Dask DataFrame contains multiple specific columns.

        Args:
            df: Dask DataFrame
            column_names: List of column names to check

        Raises:
            AssertionError: If any column does not exist
            ImportError: If Dask is not available

        Example:
            comparator = DataFrameComparator()
            comparator.assert_dask_columns_exist(
                result_df,
                ['x_pos', 'y_pos', 'isAttacker']
            )
        """
        if not DASK_AVAILABLE:
            raise ImportError(
                "Dask is not available. Install with: pip install dask[complete]"
            )

        missing_columns = [col for col in column_names if col not in df.columns]
        assert not missing_columns, (
            f"Missing columns: {missing_columns}\n"
            f"Available columns: {list(df.columns)}"
        )

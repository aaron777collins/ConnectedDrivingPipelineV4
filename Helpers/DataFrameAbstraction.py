"""
DataFrame abstraction layer for gradual pandas to PySpark migration.

This module provides a unified interface for DataFrame operations that works
with both pandas and PySpark DataFrames. This enables gradual migration by
allowing components to use either backend interchangeably.

Key Features:
- Adapter pattern for common DataFrame operations
- Feature flags to toggle between pandas/PySpark per component
- Type-safe wrapper classes
- Automatic backend detection
- Conversion utilities between pandas and PySpark

Usage:
    from Helpers.DataFrameAbstraction import DataFrameWrapper, create_wrapper

    # Auto-detect backend
    df_wrapper = create_wrapper(df)

    # Perform operations using unified interface
    filtered = df_wrapper.filter(lambda row: row['speed'] > 20)
    selected = df_wrapper.select(['x_pos', 'y_pos', 'speed'])
    result = df_wrapper.to_native()  # Get underlying DataFrame
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Union
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class DataFrameBackend:
    """Enumeration of supported DataFrame backends."""
    PANDAS = "pandas"
    PYSPARK = "pyspark"


class IDataFrame(ABC):
    """
    Abstract interface for DataFrame operations.

    Defines a common interface that can be implemented by both pandas
    and PySpark DataFrame adapters.
    """

    @abstractmethod
    def select(self, columns: List[str]) -> 'IDataFrame':
        """
        Select specific columns from the DataFrame.

        Args:
            columns: List of column names to select

        Returns:
            New IDataFrame with selected columns
        """
        pass

    @abstractmethod
    def filter(self, condition: Union[str, Callable]) -> 'IDataFrame':
        """
        Filter rows based on a condition.

        Args:
            condition: Filter condition (string expression or callable)

        Returns:
            New IDataFrame with filtered rows
        """
        pass

    @abstractmethod
    def drop(self, columns: List[str]) -> 'IDataFrame':
        """
        Drop specified columns.

        Args:
            columns: List of column names to drop

        Returns:
            New IDataFrame without specified columns
        """
        pass

    @abstractmethod
    def drop_na(self, subset: Optional[List[str]] = None) -> 'IDataFrame':
        """
        Remove rows with null/NA values.

        Args:
            subset: Optional list of columns to consider for NA removal

        Returns:
            New IDataFrame without NA rows
        """
        pass

    @abstractmethod
    def with_column(self, column_name: str, value: Any) -> 'IDataFrame':
        """
        Add or replace a column.

        Args:
            column_name: Name of the column to add/replace
            value: Column value or transformation

        Returns:
            New IDataFrame with the column added/replaced
        """
        pass

    @abstractmethod
    def count(self) -> int:
        """
        Count the number of rows.

        Returns:
            Number of rows in the DataFrame
        """
        pass

    @abstractmethod
    def collect(self) -> List[Any]:
        """
        Collect all rows to driver.

        Returns:
            List of rows
        """
        pass

    @abstractmethod
    def to_pandas(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame.

        Returns:
            Pandas DataFrame
        """
        pass

    @abstractmethod
    def to_spark(self, spark: SparkSession, schema: Optional[StructType] = None) -> SparkDataFrame:
        """
        Convert to PySpark DataFrame.

        Args:
            spark: SparkSession instance
            schema: Optional schema for the DataFrame

        Returns:
            PySpark DataFrame
        """
        pass

    @abstractmethod
    def to_native(self) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Get the underlying native DataFrame.

        Returns:
            The underlying pandas or PySpark DataFrame
        """
        pass

    @property
    @abstractmethod
    def columns(self) -> List[str]:
        """Get list of column names."""
        pass

    @property
    @abstractmethod
    def backend(self) -> str:
        """Get the backend type (pandas or pyspark)."""
        pass


class PandasDataFrameAdapter(IDataFrame):
    """
    Adapter for pandas DataFrames implementing the IDataFrame interface.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the adapter.

        Args:
            df: Pandas DataFrame to wrap
        """
        self._df = df
        self._backend = DataFrameBackend.PANDAS

    def select(self, columns: List[str]) -> 'PandasDataFrameAdapter':
        """Select specific columns."""
        return PandasDataFrameAdapter(self._df[columns])

    def filter(self, condition: Union[str, Callable]) -> 'PandasDataFrameAdapter':
        """
        Filter rows based on a condition.

        Args:
            condition: Can be a pandas query string or callable
        """
        if isinstance(condition, str):
            # Use pandas query syntax
            filtered = self._df.query(condition)
        elif callable(condition):
            # Use boolean indexing with callable
            filtered = self._df[self._df.apply(condition, axis=1)]
        else:
            raise ValueError("condition must be a string or callable")

        return PandasDataFrameAdapter(filtered)

    def drop(self, columns: List[str]) -> 'PandasDataFrameAdapter':
        """Drop specified columns."""
        return PandasDataFrameAdapter(self._df.drop(columns=columns, errors='ignore'))

    def drop_na(self, subset: Optional[List[str]] = None) -> 'PandasDataFrameAdapter':
        """Remove rows with null/NA values."""
        return PandasDataFrameAdapter(self._df.dropna(subset=subset))

    def with_column(self, column_name: str, value: Any) -> 'PandasDataFrameAdapter':
        """
        Add or replace a column.

        Args:
            column_name: Name of the column
            value: Can be a scalar, Series, or callable
        """
        df_copy = self._df.copy()
        if callable(value):
            df_copy[column_name] = df_copy.apply(value, axis=1)
        else:
            df_copy[column_name] = value
        return PandasDataFrameAdapter(df_copy)

    def count(self) -> int:
        """Count the number of rows."""
        return len(self._df)

    def collect(self) -> List[Any]:
        """Collect all rows as list of dicts."""
        return self._df.to_dict('records')

    def to_pandas(self) -> pd.DataFrame:
        """Return the underlying pandas DataFrame."""
        return self._df.copy()

    def to_spark(self, spark: SparkSession, schema: Optional[StructType] = None) -> SparkDataFrame:
        """Convert to PySpark DataFrame."""
        if schema:
            return spark.createDataFrame(self._df, schema=schema)
        return spark.createDataFrame(self._df)

    def to_native(self) -> pd.DataFrame:
        """Get the underlying pandas DataFrame."""
        return self._df

    @property
    def columns(self) -> List[str]:
        """Get list of column names."""
        return list(self._df.columns)

    @property
    def backend(self) -> str:
        """Get the backend type."""
        return self._backend


class PySparkDataFrameAdapter(IDataFrame):
    """
    Adapter for PySpark DataFrames implementing the IDataFrame interface.
    """

    def __init__(self, df: SparkDataFrame):
        """
        Initialize the adapter.

        Args:
            df: PySpark DataFrame to wrap
        """
        self._df = df
        self._backend = DataFrameBackend.PYSPARK

    def select(self, columns: List[str]) -> 'PySparkDataFrameAdapter':
        """Select specific columns."""
        return PySparkDataFrameAdapter(self._df.select(*columns))

    def filter(self, condition: Union[str, Callable]) -> 'PySparkDataFrameAdapter':
        """
        Filter rows based on a condition.

        Args:
            condition: SQL-like string condition or Column expression
        """
        if isinstance(condition, str):
            # Use SQL-like filter syntax
            filtered = self._df.filter(condition)
        elif callable(condition):
            # For PySpark, we need to convert callable to UDF
            # This is a simplified implementation
            raise NotImplementedError(
                "Callable filters not yet supported for PySpark adapter. "
                "Use string SQL expressions instead."
            )
        else:
            # Assume it's a Column expression
            filtered = self._df.filter(condition)

        return PySparkDataFrameAdapter(filtered)

    def drop(self, columns: List[str]) -> 'PySparkDataFrameAdapter':
        """Drop specified columns."""
        # Only drop columns that exist
        existing_columns = [col for col in columns if col in self._df.columns]
        if existing_columns:
            return PySparkDataFrameAdapter(self._df.drop(*existing_columns))
        return PySparkDataFrameAdapter(self._df)

    def drop_na(self, subset: Optional[List[str]] = None) -> 'PySparkDataFrameAdapter':
        """Remove rows with null/NA values."""
        if subset:
            return PySparkDataFrameAdapter(self._df.na.drop(subset=subset))
        return PySparkDataFrameAdapter(self._df.na.drop())

    def with_column(self, column_name: str, value: Any) -> 'PySparkDataFrameAdapter':
        """
        Add or replace a column.

        Args:
            column_name: Name of the column
            value: Column expression, literal, or UDF result
        """
        from pyspark.sql.functions import lit

        if callable(value):
            raise NotImplementedError(
                "Callable column transformations not yet supported for PySpark adapter. "
                "Use Column expressions or UDFs instead."
            )
        elif not hasattr(value, '_jc'):  # Not a Column object
            # Convert to literal
            value = lit(value)

        return PySparkDataFrameAdapter(self._df.withColumn(column_name, value))

    def count(self) -> int:
        """Count the number of rows."""
        return self._df.count()

    def collect(self) -> List[Any]:
        """Collect all rows as list of Row objects."""
        return self._df.collect()

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        return self._df.toPandas()

    def to_spark(self, spark: SparkSession, schema: Optional[StructType] = None) -> SparkDataFrame:
        """Return the underlying PySpark DataFrame."""
        return self._df

    def to_native(self) -> SparkDataFrame:
        """Get the underlying PySpark DataFrame."""
        return self._df

    @property
    def columns(self) -> List[str]:
        """Get list of column names."""
        return self._df.columns

    @property
    def backend(self) -> str:
        """Get the backend type."""
        return self._backend


class DataFrameWrapper:
    """
    High-level wrapper providing a unified DataFrame interface.

    This wrapper automatically detects the DataFrame type and delegates
    operations to the appropriate adapter.

    Example:
        # Works with both pandas and PySpark
        wrapper = DataFrameWrapper(df)
        result = wrapper.select(['col1', 'col2']).filter('col1 > 10')
        native_df = result.unwrap()
    """

    def __init__(self, df: Union[pd.DataFrame, SparkDataFrame, IDataFrame]):
        """
        Initialize the wrapper.

        Args:
            df: DataFrame to wrap (pandas, PySpark, or IDataFrame)
        """
        if isinstance(df, IDataFrame):
            self._adapter = df
        elif isinstance(df, pd.DataFrame):
            self._adapter = PandasDataFrameAdapter(df)
        elif isinstance(df, SparkDataFrame):
            self._adapter = PySparkDataFrameAdapter(df)
        else:
            raise TypeError(
                f"Unsupported DataFrame type: {type(df)}. "
                "Expected pandas.DataFrame, pyspark.sql.DataFrame, or IDataFrame."
            )

    def select(self, columns: List[str]) -> 'DataFrameWrapper':
        """Select specific columns."""
        return DataFrameWrapper(self._adapter.select(columns))

    def filter(self, condition: Union[str, Callable]) -> 'DataFrameWrapper':
        """Filter rows based on a condition."""
        return DataFrameWrapper(self._adapter.filter(condition))

    def drop(self, columns: List[str]) -> 'DataFrameWrapper':
        """Drop specified columns."""
        return DataFrameWrapper(self._adapter.drop(columns))

    def drop_na(self, subset: Optional[List[str]] = None) -> 'DataFrameWrapper':
        """Remove rows with null/NA values."""
        return DataFrameWrapper(self._adapter.drop_na(subset))

    def with_column(self, column_name: str, value: Any) -> 'DataFrameWrapper':
        """Add or replace a column."""
        return DataFrameWrapper(self._adapter.with_column(column_name, value))

    def count(self) -> int:
        """Count the number of rows."""
        return self._adapter.count()

    def collect(self) -> List[Any]:
        """Collect all rows to driver."""
        return self._adapter.collect()

    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        return self._adapter.to_pandas()

    def to_spark(self, spark: SparkSession, schema: Optional[StructType] = None) -> SparkDataFrame:
        """Convert to PySpark DataFrame."""
        return self._adapter.to_spark(spark, schema)

    def unwrap(self) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Get the underlying native DataFrame.

        Returns:
            The underlying pandas or PySpark DataFrame
        """
        return self._adapter.to_native()

    @property
    def columns(self) -> List[str]:
        """Get list of column names."""
        return self._adapter.columns

    @property
    def backend(self) -> str:
        """Get the backend type (pandas or pyspark)."""
        return self._adapter.backend

    def is_pandas(self) -> bool:
        """Check if this is a pandas DataFrame."""
        return self.backend == DataFrameBackend.PANDAS

    def is_pyspark(self) -> bool:
        """Check if this is a PySpark DataFrame."""
        return self.backend == DataFrameBackend.PYSPARK


def create_wrapper(df: Union[pd.DataFrame, SparkDataFrame]) -> DataFrameWrapper:
    """
    Factory function to create a DataFrameWrapper.

    Args:
        df: DataFrame to wrap (pandas or PySpark)

    Returns:
        DataFrameWrapper instance

    Example:
        wrapper = create_wrapper(df)
        result = wrapper.select(['col1']).filter('col1 > 10').unwrap()
    """
    return DataFrameWrapper(df)


def detect_backend(df: Any) -> str:
    """
    Detect the backend type of a DataFrame.

    Args:
        df: DataFrame object

    Returns:
        Backend type string ('pandas' or 'pyspark')

    Raises:
        TypeError: If DataFrame type is not recognized
    """
    if isinstance(df, pd.DataFrame):
        return DataFrameBackend.PANDAS
    elif isinstance(df, SparkDataFrame):
        return DataFrameBackend.PYSPARK
    elif isinstance(df, IDataFrame):
        return df.backend
    else:
        raise TypeError(f"Unsupported DataFrame type: {type(df)}")


def convert_dataframe(
    df: Union[pd.DataFrame, SparkDataFrame],
    target_backend: str,
    spark: Optional[SparkSession] = None,
    schema: Optional[StructType] = None
) -> Union[pd.DataFrame, SparkDataFrame]:
    """
    Convert a DataFrame between pandas and PySpark.

    Args:
        df: Source DataFrame (pandas or PySpark)
        target_backend: Target backend ('pandas' or 'pyspark')
        spark: SparkSession (required if converting to PySpark)
        schema: Optional schema for PySpark conversion

    Returns:
        Converted DataFrame

    Example:
        # Convert pandas to PySpark
        spark_df = convert_dataframe(pandas_df, 'pyspark', spark=spark)

        # Convert PySpark to pandas
        pandas_df = convert_dataframe(spark_df, 'pandas')
    """
    wrapper = create_wrapper(df)

    if target_backend == DataFrameBackend.PANDAS:
        return wrapper.to_pandas()
    elif target_backend == DataFrameBackend.PYSPARK:
        if spark is None:
            raise ValueError("SparkSession is required for conversion to PySpark")
        return wrapper.to_spark(spark, schema)
    else:
        raise ValueError(f"Unknown target backend: {target_backend}")

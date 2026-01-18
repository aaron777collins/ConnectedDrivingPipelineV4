"""
Unit tests for DataFrame abstraction layer.

Tests the unified DataFrame interface for both pandas and PySpark backends,
ensuring consistent behavior across both implementations.
"""

import pytest
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from Helpers.DataFrameAbstraction import (
    DataFrameWrapper,
    PandasDataFrameAdapter,
    PySparkDataFrameAdapter,
    DataFrameBackend,
    create_wrapper,
    detect_backend,
    convert_dataframe,
)


# Sample data for testing
SAMPLE_DATA = [
    {"id": 1, "name": "Alice", "age": 25, "score": 85.5},
    {"id": 2, "name": "Bob", "age": 30, "score": 92.0},
    {"id": 3, "name": "Charlie", "age": 35, "score": 78.5},
    {"id": 4, "name": "David", "age": 28, "score": 88.0},
    {"id": 5, "name": "Eve", "age": 32, "score": 95.5},
]


@pytest.fixture
def sample_pandas_df():
    """Create a sample pandas DataFrame for testing."""
    return pd.DataFrame(SAMPLE_DATA)


@pytest.fixture
def sample_spark_df(spark_session):
    """Create a sample PySpark DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), True),  # Nullable for testing
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", DoubleType(), True),  # Nullable for testing
    ])
    return spark_session.createDataFrame(SAMPLE_DATA, schema=schema)


class TestPandasDataFrameAdapter:
    """Tests for PandasDataFrameAdapter."""

    def test_select_columns(self, sample_pandas_df):
        """Test selecting columns from pandas adapter."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.select(["id", "name"])

        assert result.columns == ["id", "name"]
        assert result.count() == 5

    def test_filter_with_query_string(self, sample_pandas_df):
        """Test filtering with pandas query string."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.filter("age > 30")

        assert result.count() == 2
        result_df = result.to_native()
        assert all(result_df["age"] > 30)

    def test_filter_with_callable(self, sample_pandas_df):
        """Test filtering with callable."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.filter(lambda row: row["score"] >= 90)

        assert result.count() == 2
        result_df = result.to_native()
        assert all(result_df["score"] >= 90)

    def test_drop_columns(self, sample_pandas_df):
        """Test dropping columns."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.drop(["age", "score"])

        assert result.columns == ["id", "name"]
        assert result.count() == 5

    def test_drop_na(self, sample_pandas_df):
        """Test dropping NA values."""
        # Add some NA values
        df_with_na = sample_pandas_df.copy()
        df_with_na.loc[2, "score"] = None

        adapter = PandasDataFrameAdapter(df_with_na)
        result = adapter.drop_na()

        assert result.count() == 4

    def test_with_column_scalar(self, sample_pandas_df):
        """Test adding a column with scalar value."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.with_column("country", "USA")

        assert "country" in result.columns
        assert result.count() == 5
        result_df = result.to_native()
        assert all(result_df["country"] == "USA")

    def test_with_column_callable(self, sample_pandas_df):
        """Test adding a column with callable transformation."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.with_column("age_plus_10", lambda row: row["age"] + 10)

        assert "age_plus_10" in result.columns
        result_df = result.to_native()
        assert all(result_df["age_plus_10"] == result_df["age"] + 10)

    def test_count(self, sample_pandas_df):
        """Test counting rows."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        assert adapter.count() == 5

    def test_collect(self, sample_pandas_df):
        """Test collecting rows."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        rows = adapter.collect()

        assert len(rows) == 5
        assert all(isinstance(row, dict) for row in rows)
        assert rows[0]["name"] == "Alice"

    def test_to_pandas(self, sample_pandas_df):
        """Test conversion to pandas."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.to_pandas()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5

    def test_to_spark(self, sample_pandas_df, spark_session):
        """Test conversion to PySpark."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = adapter.to_spark(spark_session)

        assert result.count() == 5
        assert set(result.columns) == set(sample_pandas_df.columns)

    def test_backend_property(self, sample_pandas_df):
        """Test backend property."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        assert adapter.backend == DataFrameBackend.PANDAS

    def test_chaining_operations(self, sample_pandas_df):
        """Test chaining multiple operations."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        result = (
            adapter
            .filter("age > 25")
            .select(["name", "score"])
            .drop_na()
        )

        assert result.columns == ["name", "score"]
        assert result.count() == 4


class TestPySparkDataFrameAdapter:
    """Tests for PySparkDataFrameAdapter."""

    def test_select_columns(self, sample_spark_df):
        """Test selecting columns from PySpark adapter."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.select(["id", "name"])

        assert result.columns == ["id", "name"]
        assert result.count() == 5

    def test_filter_with_string(self, sample_spark_df):
        """Test filtering with SQL string."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.filter("age > 30")

        assert result.count() == 2
        result_df = result.to_native()
        rows = result_df.collect()
        assert all(row.age > 30 for row in rows)

    def test_filter_with_column_expression(self, sample_spark_df):
        """Test filtering with Column expression."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.filter(F.col("score") >= 90)

        assert result.count() == 2

    def test_drop_columns(self, sample_spark_df):
        """Test dropping columns."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.drop(["age", "score"])

        assert set(result.columns) == {"id", "name"}
        assert result.count() == 5

    def test_drop_nonexistent_columns(self, sample_spark_df):
        """Test dropping columns that don't exist."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.drop(["nonexistent"])

        # Should not raise error, just ignore
        assert result.count() == 5

    def test_drop_na(self, sample_spark_df, spark_session):
        """Test dropping NA values."""
        # Add some NA values - deep copy to avoid modifying shared SAMPLE_DATA
        import copy
        data_with_na = copy.deepcopy(SAMPLE_DATA)
        data_with_na[2]["score"] = None

        schema = sample_spark_df.schema
        df_with_na = spark_session.createDataFrame(data_with_na, schema=schema)

        adapter = PySparkDataFrameAdapter(df_with_na)
        result = adapter.drop_na()

        assert result.count() == 4

    def test_with_column_literal(self, sample_spark_df):
        """Test adding a column with literal value."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.with_column("country", F.lit("USA"))

        assert "country" in result.columns
        assert result.count() == 5
        rows = result.to_native().collect()
        assert all(row.country == "USA" for row in rows)

    def test_with_column_expression(self, sample_spark_df):
        """Test adding a column with expression."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.with_column("age_plus_10", F.col("age") + 10)

        assert "age_plus_10" in result.columns
        rows = result.to_native().collect()
        assert all(row.age_plus_10 == row.age + 10 for row in rows)

    def test_count(self, sample_spark_df):
        """Test counting rows."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        assert adapter.count() == 5

    def test_collect(self, sample_spark_df):
        """Test collecting rows."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        rows = adapter.collect()

        assert len(rows) == 5
        assert rows[0].name == "Alice"

    def test_to_pandas(self, sample_spark_df):
        """Test conversion to pandas."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.to_pandas()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5

    def test_to_spark(self, sample_spark_df, spark_session):
        """Test to_spark returns same DataFrame."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = adapter.to_spark(spark_session)

        assert result.count() == 5

    def test_backend_property(self, sample_spark_df):
        """Test backend property."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        assert adapter.backend == DataFrameBackend.PYSPARK

    def test_chaining_operations(self, sample_spark_df):
        """Test chaining multiple operations."""
        adapter = PySparkDataFrameAdapter(sample_spark_df)
        result = (
            adapter
            .filter("age > 25")
            .select(["name", "score"])
            .drop_na()
        )

        assert set(result.columns) == {"name", "score"}
        assert result.count() == 4


class TestDataFrameWrapper:
    """Tests for DataFrameWrapper."""

    def test_wrap_pandas_df(self, sample_pandas_df):
        """Test wrapping a pandas DataFrame."""
        wrapper = DataFrameWrapper(sample_pandas_df)

        assert wrapper.is_pandas()
        assert not wrapper.is_pyspark()
        assert wrapper.backend == DataFrameBackend.PANDAS

    def test_wrap_spark_df(self, sample_spark_df):
        """Test wrapping a PySpark DataFrame."""
        wrapper = DataFrameWrapper(sample_spark_df)

        assert wrapper.is_pyspark()
        assert not wrapper.is_pandas()
        assert wrapper.backend == DataFrameBackend.PYSPARK

    def test_wrap_adapter(self, sample_pandas_df):
        """Test wrapping an existing adapter."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        wrapper = DataFrameWrapper(adapter)

        assert wrapper.is_pandas()

    def test_select_pandas(self, sample_pandas_df):
        """Test select on pandas wrapper."""
        wrapper = DataFrameWrapper(sample_pandas_df)
        result = wrapper.select(["id", "name"])

        assert result.columns == ["id", "name"]
        assert result.count() == 5

    def test_select_spark(self, sample_spark_df):
        """Test select on PySpark wrapper."""
        wrapper = DataFrameWrapper(sample_spark_df)
        result = wrapper.select(["id", "name"])

        assert result.columns == ["id", "name"]
        assert result.count() == 5

    def test_unwrap_pandas(self, sample_pandas_df):
        """Test unwrapping pandas DataFrame."""
        wrapper = DataFrameWrapper(sample_pandas_df)
        result = wrapper.unwrap()

        assert isinstance(result, pd.DataFrame)

    def test_unwrap_spark(self, sample_spark_df):
        """Test unwrapping PySpark DataFrame."""
        wrapper = DataFrameWrapper(sample_spark_df)
        result = wrapper.unwrap()

        assert hasattr(result, 'rdd')  # PySpark DataFrame has rdd attribute

    def test_chaining_pandas(self, sample_pandas_df):
        """Test operation chaining on pandas wrapper."""
        wrapper = DataFrameWrapper(sample_pandas_df)
        result = (
            wrapper
            .filter("age > 25")
            .select(["name", "score"])
            .with_column("bonus", F.lit(10) if not wrapper.is_pandas() else 10)
        )

        assert "bonus" in result.columns
        assert result.count() == 4

    def test_chaining_spark(self, sample_spark_df):
        """Test operation chaining on PySpark wrapper."""
        wrapper = DataFrameWrapper(sample_spark_df)
        result = (
            wrapper
            .filter("age > 25")
            .select(["name", "score"])
            .with_column("bonus", F.lit(10))
        )

        assert "bonus" in result.columns
        assert result.count() == 4


class TestFactoryFunctions:
    """Tests for factory and utility functions."""

    def test_create_wrapper_pandas(self, sample_pandas_df):
        """Test create_wrapper with pandas DataFrame."""
        wrapper = create_wrapper(sample_pandas_df)

        assert isinstance(wrapper, DataFrameWrapper)
        assert wrapper.is_pandas()

    def test_create_wrapper_spark(self, sample_spark_df):
        """Test create_wrapper with PySpark DataFrame."""
        wrapper = create_wrapper(sample_spark_df)

        assert isinstance(wrapper, DataFrameWrapper)
        assert wrapper.is_pyspark()

    def test_detect_backend_pandas(self, sample_pandas_df):
        """Test detect_backend with pandas DataFrame."""
        backend = detect_backend(sample_pandas_df)
        assert backend == DataFrameBackend.PANDAS

    def test_detect_backend_spark(self, sample_spark_df):
        """Test detect_backend with PySpark DataFrame."""
        backend = detect_backend(sample_spark_df)
        assert backend == DataFrameBackend.PYSPARK

    def test_detect_backend_adapter(self, sample_pandas_df):
        """Test detect_backend with adapter."""
        adapter = PandasDataFrameAdapter(sample_pandas_df)
        backend = detect_backend(adapter)
        assert backend == DataFrameBackend.PANDAS

    def test_detect_backend_invalid(self):
        """Test detect_backend with invalid type."""
        with pytest.raises(TypeError):
            detect_backend("not a dataframe")

    def test_convert_pandas_to_spark(self, sample_pandas_df, spark_session):
        """Test converting pandas to PySpark."""
        result = convert_dataframe(
            sample_pandas_df,
            DataFrameBackend.PYSPARK,
            spark=spark_session
        )

        assert result.count() == 5
        assert set(result.columns) == set(sample_pandas_df.columns)

    def test_convert_spark_to_pandas(self, sample_spark_df):
        """Test converting PySpark to pandas."""
        result = convert_dataframe(
            sample_spark_df,
            DataFrameBackend.PANDAS
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5

    def test_convert_requires_spark_session(self, sample_pandas_df):
        """Test that conversion to PySpark requires SparkSession."""
        with pytest.raises(ValueError, match="SparkSession is required"):
            convert_dataframe(
                sample_pandas_df,
                DataFrameBackend.PYSPARK,
                spark=None
            )

    def test_convert_invalid_backend(self, sample_pandas_df):
        """Test conversion with invalid backend."""
        with pytest.raises(ValueError, match="Unknown target backend"):
            convert_dataframe(sample_pandas_df, "invalid_backend")


class TestConsistencyBetweenBackends:
    """Tests to ensure consistent behavior between pandas and PySpark."""

    def test_select_consistency(self, sample_pandas_df, sample_spark_df):
        """Test that select produces consistent results."""
        pandas_wrapper = create_wrapper(sample_pandas_df)
        spark_wrapper = create_wrapper(sample_spark_df)

        pandas_result = pandas_wrapper.select(["name", "score"]).to_pandas()
        spark_result = spark_wrapper.select(["name", "score"]).to_pandas()

        pd.testing.assert_frame_equal(
            pandas_result.sort_values("name").reset_index(drop=True),
            spark_result.sort_values("name").reset_index(drop=True),
            check_dtype=False
        )

    def test_filter_consistency(self, sample_pandas_df, sample_spark_df):
        """Test that filter produces consistent results."""
        pandas_wrapper = create_wrapper(sample_pandas_df)
        spark_wrapper = create_wrapper(sample_spark_df)

        pandas_result = pandas_wrapper.filter("age > 30").to_pandas()
        spark_result = spark_wrapper.filter("age > 30").to_pandas()

        pd.testing.assert_frame_equal(
            pandas_result.sort_values("id").reset_index(drop=True),
            spark_result.sort_values("id").reset_index(drop=True),
            check_dtype=False
        )

    def test_count_consistency(self, sample_pandas_df, sample_spark_df):
        """Test that count produces consistent results."""
        pandas_wrapper = create_wrapper(sample_pandas_df)
        spark_wrapper = create_wrapper(sample_spark_df)

        assert pandas_wrapper.count() == spark_wrapper.count()

    def test_drop_consistency(self, sample_pandas_df, sample_spark_df):
        """Test that drop produces consistent results."""
        pandas_wrapper = create_wrapper(sample_pandas_df)
        spark_wrapper = create_wrapper(sample_spark_df)

        pandas_result = pandas_wrapper.drop(["age"]).to_pandas()
        spark_result = spark_wrapper.drop(["age"]).to_pandas()

        assert set(pandas_result.columns) == set(spark_result.columns)

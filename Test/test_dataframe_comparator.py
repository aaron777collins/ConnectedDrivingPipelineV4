"""
Tests for DataFrameComparator utility.

Validates the DataFrame comparison utilities used for testing
pandas vs PySpark migration equivalence.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from Test.Utils.DataFrameComparator import DataFrameComparator
import pandas as pd


@pytest.mark.unit
@pytest.mark.utils
def test_comparator_init():
    """Test DataFrameComparator initialization."""
    comparator = DataFrameComparator()
    assert comparator is not None


@pytest.mark.unit
@pytest.mark.utils
def test_assert_column_exists(spark_session):
    """Test column existence check."""
    comparator = DataFrameComparator()

    # Create simple DataFrame
    data = [("Alice", 25), ("Bob", 30)]
    df = spark_session.createDataFrame(data, ["name", "age"])

    # Should pass
    comparator.assert_column_exists(df, "name")
    comparator.assert_column_exists(df, "age")

    # Should fail
    with pytest.raises(AssertionError, match="Column 'invalid' not found"):
        comparator.assert_column_exists(df, "invalid")


@pytest.mark.unit
@pytest.mark.utils
def test_assert_columns_exist(spark_session):
    """Test multiple column existence check."""
    comparator = DataFrameComparator()

    data = [("Alice", 25, "NYC"), ("Bob", 30, "LA")]
    df = spark_session.createDataFrame(data, ["name", "age", "city"])

    # Should pass
    comparator.assert_columns_exist(df, ["name", "age"])
    comparator.assert_columns_exist(df, ["name", "age", "city"])

    # Should fail
    with pytest.raises(AssertionError, match="Missing columns"):
        comparator.assert_columns_exist(df, ["name", "invalid"])


@pytest.mark.unit
@pytest.mark.utils
def test_assert_schema_equal(spark_session):
    """Test schema equality check."""
    comparator = DataFrameComparator()

    # Create two DataFrames with same schema
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    data1 = [("Alice", 25), ("Bob", 30)]
    data2 = [("Charlie", 35), ("Diana", 40)]

    df1 = spark_session.createDataFrame(data1, schema)
    df2 = spark_session.createDataFrame(data2, schema)

    # Should pass
    comparator.assert_schema_equal(df1, df2)

    # Create DataFrame with different schema
    schema2 = StructType([
        StructField("name", StringType(), True),
        StructField("age", DoubleType(), True)  # Different type
    ])
    df3 = spark_session.createDataFrame(data1, schema2)

    # Should fail
    with pytest.raises(AssertionError, match="Schema data types differ"):
        comparator.assert_schema_equal(df1, df3)


@pytest.mark.unit
@pytest.mark.utils
def test_assert_spark_equal_identical(spark_session):
    """Test comparing identical DataFrames."""
    comparator = DataFrameComparator()

    data = [("Alice", 25), ("Bob", 30)]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    df1 = spark_session.createDataFrame(data, schema)
    df2 = spark_session.createDataFrame(data, schema)

    # Should pass
    comparator.assert_spark_equal(df1, df2)


@pytest.mark.unit
@pytest.mark.utils
def test_assert_spark_equal_different_rows(spark_session):
    """Test comparing DataFrames with different row counts."""
    comparator = DataFrameComparator()

    data1 = [("Alice", 25), ("Bob", 30)]
    data2 = [("Alice", 25)]

    df1 = spark_session.createDataFrame(data1, ["name", "age"])
    df2 = spark_session.createDataFrame(data2, ["name", "age"])

    # Should fail
    with pytest.raises(AssertionError, match="Row counts differ"):
        comparator.assert_spark_equal(df1, df2)


@pytest.mark.unit
@pytest.mark.utils
def test_assert_spark_equal_floating_point(spark_session):
    """Test floating-point comparison with tolerance."""
    comparator = DataFrameComparator()

    # Create DataFrames with slight floating-point differences
    data1 = [(1.0000001,)]
    data2 = [(1.0000002,)]

    df1 = spark_session.createDataFrame(data1, ["value"])
    df2 = spark_session.createDataFrame(data2, ["value"])

    # Should pass with default tolerance
    comparator.assert_spark_equal(df1, df2, rtol=1e-5)

    # Should fail with strict tolerance
    with pytest.raises(AssertionError):
        comparator.assert_spark_equal(df1, df2, rtol=1e-10)


@pytest.mark.unit
@pytest.mark.utils
def test_assert_pandas_spark_equal(spark_session):
    """Test comparing pandas vs PySpark DataFrames."""
    comparator = DataFrameComparator()

    # Create pandas DataFrame
    pandas_df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30]
    })

    # Create equivalent PySpark DataFrame
    spark_df = spark_session.createDataFrame(
        [('Alice', 25), ('Bob', 30)],
        ['name', 'age']
    )

    # Should pass
    comparator.assert_pandas_spark_equal(pandas_df, spark_df, check_dtype=False)


@pytest.mark.unit
@pytest.mark.utils
def test_get_column_diff(spark_session):
    """Test column diff utility."""
    comparator = DataFrameComparator()

    # Create DataFrames with different columns
    df1 = spark_session.createDataFrame(
        [('Alice', 25, 'NYC')],
        ['name', 'age', 'city']
    )
    df2 = spark_session.createDataFrame(
        [('Bob', 30, 'Engineer')],
        ['name', 'age', 'job']
    )

    diff = comparator.get_column_diff(df1, df2)

    assert 'city' in diff['only_in_df1']
    assert 'job' in diff['only_in_df2']
    assert 'name' in diff['common']
    assert 'age' in diff['common']


@pytest.mark.unit
@pytest.mark.utils
def test_print_comparison_summary(spark_session, capsys):
    """Test comparison summary printing."""
    comparator = DataFrameComparator()

    df1 = spark_session.createDataFrame([('Alice', 25)], ['name', 'age'])
    df2 = spark_session.createDataFrame([('Bob', 30)], ['name', 'age'])

    comparator.print_comparison_summary(df1, df2, "Expected", "Actual")

    captured = capsys.readouterr()
    assert "DataFrame Comparison" in captured.out
    assert "Expected" in captured.out
    assert "Actual" in captured.out


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

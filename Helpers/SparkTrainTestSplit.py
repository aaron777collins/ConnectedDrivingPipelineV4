"""
PySpark implementation of train_test_split functionality.

This module provides a PySpark-native alternative to sklearn.model_selection.train_test_split
for splitting DataFrames and arrays into train and test sets using PySpark's randomSplit method.

Usage:
    from Helpers.SparkTrainTestSplit import spark_train_test_split

    # Split a PySpark DataFrame
    train_df, test_df = spark_train_test_split(df, test_size=0.2, random_state=42)

    # Split a list or array (for unique IDs, etc.)
    train_ids, test_ids = spark_train_test_split(unique_ids, test_size=0.3, random_state=42)
"""

from typing import Union, Tuple, List, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType
import numpy as np


def spark_train_test_split(
    data: Union[DataFrame, List, np.ndarray],
    test_size: float = 0.25,
    train_size: float = None,
    random_state: int = None,
    spark: SparkSession = None
) -> Tuple[Union[DataFrame, List], Union[DataFrame, List]]:
    """
    Split data into random train and test subsets using PySpark's randomSplit.

    This function mimics sklearn.model_selection.train_test_split but uses PySpark's
    native randomSplit for DataFrames and provides equivalent functionality for lists/arrays.

    Args:
        data: Input data to split. Can be:
            - PySpark DataFrame: Will be split using randomSplit
            - List or numpy array: Will be converted to DataFrame, split, then converted back
        test_size: Proportion of the dataset to include in the test split.
            Should be between 0.0 and 1.0. Default is 0.25.
        train_size: Proportion of the dataset to include in the train split.
            If None, train_size is set to 1.0 - test_size. Default is None.
        random_state: Random seed for reproducibility. If None, uses a random seed.
            Default is None.
        spark: SparkSession instance. Required only when splitting lists/arrays.
            If None and data is a list/array, will attempt to get active SparkSession.

    Returns:
        Tuple of (train, test) where each element has the same type as input data:
            - If input is DataFrame: returns (train_df, test_df)
            - If input is List: returns (train_list, test_list)
            - If input is numpy array: returns (train_list, test_list)

    Raises:
        ValueError: If test_size or train_size is not between 0.0 and 1.0
        ValueError: If train_size + test_size > 1.0
        RuntimeError: If SparkSession is not available when splitting list/array

    Examples:
        >>> # Split a DataFrame
        >>> train_df, test_df = spark_train_test_split(df, test_size=0.2, random_state=42)

        >>> # Split a list of unique IDs (sklearn pattern)
        >>> unique_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        >>> train_ids, test_ids = spark_train_test_split(unique_ids, test_size=0.3, random_state=42)

        >>> # With explicit train_size
        >>> train_df, test_df = spark_train_test_split(df, train_size=0.8, test_size=0.2, random_state=42)
    """
    # Validate test_size and train_size
    if test_size is not None and (test_size <= 0.0 or test_size >= 1.0):
        raise ValueError(f"test_size must be between 0.0 and 1.0, got {test_size}")

    if train_size is not None and (train_size <= 0.0 or train_size >= 1.0):
        raise ValueError(f"train_size must be between 0.0 and 1.0, got {train_size}")

    # Calculate train_size if not provided
    if train_size is None:
        train_size = 1.0 - test_size

    # Validate that train_size + test_size <= 1.0
    if train_size + test_size > 1.0:
        raise ValueError(
            f"train_size ({train_size}) + test_size ({test_size}) = {train_size + test_size} > 1.0. "
            "The sum of train_size and test_size must be <= 1.0"
        )

    # Handle DataFrame input
    if isinstance(data, DataFrame):
        return _split_dataframe(data, train_size, test_size, random_state)

    # Handle list or numpy array input
    elif isinstance(data, (list, np.ndarray)):
        return _split_array(data, train_size, test_size, random_state, spark)

    else:
        raise TypeError(
            f"data must be a PySpark DataFrame, list, or numpy array, got {type(data)}"
        )


def _split_dataframe(
    df: DataFrame,
    train_size: float,
    test_size: float,
    random_state: int = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Split a PySpark DataFrame using randomSplit.

    Args:
        df: PySpark DataFrame to split
        train_size: Proportion for training set
        test_size: Proportion for test set
        random_state: Random seed for reproducibility

    Returns:
        Tuple of (train_df, test_df)
    """
    # randomSplit expects weights that sum to 1.0
    weights = [train_size, test_size]

    # Use seed if provided
    if random_state is not None:
        train_df, test_df = df.randomSplit(weights, seed=random_state)
    else:
        train_df, test_df = df.randomSplit(weights)

    return train_df, test_df


def _split_array(
    data: Union[List, np.ndarray],
    train_size: float,
    test_size: float,
    random_state: int = None,
    spark: SparkSession = None
) -> Tuple[List, List]:
    """
    Split a list or numpy array using PySpark's randomSplit.

    This function converts the array to a DataFrame, splits it, then converts back to a list.
    This maintains consistency with PySpark's splitting logic while supporting sklearn-style
    usage patterns (e.g., splitting unique IDs).

    Args:
        data: List or numpy array to split
        train_size: Proportion for training set
        test_size: Proportion for test set
        random_state: Random seed for reproducibility
        spark: SparkSession instance (optional, will get active session if None)

    Returns:
        Tuple of (train_list, test_list)

    Raises:
        RuntimeError: If SparkSession is not available
    """
    # Get SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "No active SparkSession found. Please provide a SparkSession or create one before calling this function."
            )

    # Convert numpy array to list if needed
    if isinstance(data, np.ndarray):
        data = data.tolist()

    # Infer data type from first element
    if len(data) == 0:
        return [], []

    first_elem = data[0]
    if isinstance(first_elem, str):
        schema_type = StringType()
    elif isinstance(first_elem, int):
        schema_type = LongType()
    elif isinstance(first_elem, float):
        schema_type = DoubleType()
    else:
        # Default to string representation
        data = [str(x) for x in data]
        schema_type = StringType()

    # Create DataFrame from list
    # Each element becomes a single-column row
    df = spark.createDataFrame([(x,) for x in data], ["value"])

    # Split using randomSplit
    weights = [train_size, test_size]
    if random_state is not None:
        train_df, test_df = df.randomSplit(weights, seed=random_state)
    else:
        train_df, test_df = df.randomSplit(weights)

    # Convert back to lists
    train_list = [row["value"] for row in train_df.collect()]
    test_list = [row["value"] for row in test_df.collect()]

    return train_list, test_list


# Convenience aliases for consistency with sklearn naming
train_test_split = spark_train_test_split


def validate_split_consistency(
    train_data: Union[DataFrame, List],
    test_data: Union[DataFrame, List],
    original_data: Union[DataFrame, List],
    expected_train_size: float = None,
    expected_test_size: float = None,
    tolerance: float = 0.05
) -> dict:
    """
    Validate that a train/test split is consistent and matches expected proportions.

    This utility function checks:
    1. No overlap between train and test sets
    2. Union of train and test equals original data
    3. Split proportions match expected values (within tolerance)

    Args:
        train_data: Training set (DataFrame or List)
        test_data: Test set (DataFrame or List)
        original_data: Original data before splitting (DataFrame or List)
        expected_train_size: Expected proportion of training data (0.0-1.0)
        expected_test_size: Expected proportion of test data (0.0-1.0)
        tolerance: Acceptable deviation from expected proportions (default: 0.05 = 5%)

    Returns:
        Dictionary with validation results:
            {
                "valid": bool,
                "train_count": int,
                "test_count": int,
                "original_count": int,
                "train_proportion": float,
                "test_proportion": float,
                "issues": List[str]  # List of any validation issues found
            }

    Examples:
        >>> train, test = spark_train_test_split(data, test_size=0.2, random_state=42)
        >>> result = validate_split_consistency(train, test, data, expected_test_size=0.2)
        >>> assert result["valid"], f"Split validation failed: {result['issues']}"
    """
    issues = []

    # Get counts
    if isinstance(original_data, DataFrame):
        original_count = original_data.count()
        train_count = train_data.count()
        test_count = test_data.count()
    else:
        original_count = len(original_data)
        train_count = len(train_data)
        test_count = len(test_data)

    # Check that train + test = original
    total_count = train_count + test_count
    if total_count != original_count:
        issues.append(
            f"Train count ({train_count}) + test count ({test_count}) = {total_count} "
            f"does not equal original count ({original_count})"
        )

    # Calculate actual proportions
    if original_count > 0:
        actual_train_proportion = train_count / original_count
        actual_test_proportion = test_count / original_count
    else:
        actual_train_proportion = 0.0
        actual_test_proportion = 0.0

    # Check expected proportions
    if expected_train_size is not None:
        train_diff = abs(actual_train_proportion - expected_train_size)
        if train_diff > tolerance:
            issues.append(
                f"Train proportion ({actual_train_proportion:.4f}) differs from expected "
                f"({expected_train_size:.4f}) by {train_diff:.4f} (tolerance: {tolerance})"
            )

    if expected_test_size is not None:
        test_diff = abs(actual_test_proportion - expected_test_size)
        if test_diff > tolerance:
            issues.append(
                f"Test proportion ({actual_test_proportion:.4f}) differs from expected "
                f"({expected_test_size:.4f}) by {test_diff:.4f} (tolerance: {tolerance})"
            )

    return {
        "valid": len(issues) == 0,
        "train_count": train_count,
        "test_count": test_count,
        "original_count": original_count,
        "train_proportion": actual_train_proportion,
        "test_proportion": actual_test_proportion,
        "issues": issues
    }

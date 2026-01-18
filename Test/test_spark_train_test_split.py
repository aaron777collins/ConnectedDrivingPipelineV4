"""
Tests for SparkTrainTestSplit module.

This module tests the PySpark implementation of train_test_split functionality,
ensuring compatibility with sklearn patterns while using PySpark's randomSplit.
"""

import pytest
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

from Helpers.SparkTrainTestSplit import (
    spark_train_test_split,
    train_test_split,  # Alias
    validate_split_consistency
)


class TestDataFrameSplit:
    """Tests for splitting PySpark DataFrames."""

    def test_split_dataframe_basic(self, spark_session):
        """Test basic DataFrame splitting."""
        # Create sample DataFrame
        data = [(i, f"name_{i}", float(i * 10)) for i in range(100)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("value", DoubleType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Split with default test_size=0.25
        train_df, test_df = spark_train_test_split(df, test_size=0.25, random_state=42)

        assert isinstance(train_df, DataFrame)
        assert isinstance(test_df, DataFrame)
        assert train_df.count() + test_df.count() == 100

    def test_split_dataframe_proportions(self, spark_session):
        """Test that split proportions are approximately correct."""
        # Create larger DataFrame for better proportion accuracy
        data = [(i, f"value_{i}") for i in range(1000)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Test 80/20 split
        train_df, test_df = spark_train_test_split(df, test_size=0.2, random_state=42)

        train_count = train_df.count()
        test_count = test_df.count()
        total_count = train_count + test_count

        # Check proportions (allow 5% tolerance due to randomness)
        train_proportion = train_count / total_count
        test_proportion = test_count / total_count

        assert 0.75 <= train_proportion <= 0.85, f"Train proportion {train_proportion} out of range"
        assert 0.15 <= test_proportion <= 0.25, f"Test proportion {test_proportion} out of range"

    def test_split_dataframe_with_seed(self, spark_session):
        """Test that random_state produces consistent splits."""
        data = [(i, f"value_{i}") for i in range(100)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Split twice with same seed
        train1, test1 = spark_train_test_split(df, test_size=0.3, random_state=42)
        train2, test2 = spark_train_test_split(df, test_size=0.3, random_state=42)

        # Counts should be identical
        assert train1.count() == train2.count()
        assert test1.count() == test2.count()

        # Content should be identical
        train1_ids = set([row.id for row in train1.collect()])
        train2_ids = set([row.id for row in train2.collect()])
        assert train1_ids == train2_ids

    def test_split_dataframe_different_seeds(self, spark_session):
        """Test that different seeds produce different splits."""
        data = [(i, f"value_{i}") for i in range(100)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Split with different seeds
        train1, test1 = spark_train_test_split(df, test_size=0.3, random_state=42)
        train2, test2 = spark_train_test_split(df, test_size=0.3, random_state=99)

        # Counts should be similar but content different
        train1_ids = set([row.id for row in train1.collect()])
        train2_ids = set([row.id for row in train2.collect()])

        # With high probability, different seeds produce different splits
        assert train1_ids != train2_ids

    def test_split_dataframe_custom_train_size(self, spark_session):
        """Test splitting with explicit train_size."""
        data = [(i,) for i in range(1000)]
        schema = StructType([StructField("id", IntegerType(), False)])
        df = spark_session.createDataFrame(data, schema)

        # Explicit train/test sizes
        train_df, test_df = spark_train_test_split(
            df, train_size=0.7, test_size=0.3, random_state=42
        )

        total = train_df.count() + test_df.count()
        train_prop = train_df.count() / total

        # Check proportion is close to 0.7 (allow 5% tolerance)
        assert 0.65 <= train_prop <= 0.75


class TestArraySplit:
    """Tests for splitting lists and numpy arrays."""

    def test_split_list_integers(self, spark_session):
        """Test splitting a list of integers."""
        data = list(range(100))

        train, test = spark_train_test_split(data, test_size=0.2, random_state=42, spark=spark_session)

        assert isinstance(train, list)
        assert isinstance(test, list)
        assert len(train) + len(test) == 100
        assert set(train).isdisjoint(set(test))  # No overlap

    def test_split_list_strings(self, spark_session):
        """Test splitting a list of strings."""
        data = [f"id_{i}" for i in range(100)]

        train, test = spark_train_test_split(data, test_size=0.3, random_state=42, spark=spark_session)

        assert isinstance(train, list)
        assert isinstance(test, list)
        assert len(train) + len(test) == 100
        assert set(train).isdisjoint(set(test))

    def test_split_numpy_array(self, spark_session):
        """Test splitting a numpy array."""
        data = np.arange(100)

        train, test = spark_train_test_split(data, test_size=0.25, random_state=42, spark=spark_session)

        # Results should be lists (not numpy arrays)
        assert isinstance(train, list)
        assert isinstance(test, list)
        assert len(train) + len(test) == 100

    def test_split_list_reproducibility(self, spark_session):
        """Test that splitting lists is reproducible with same seed."""
        data = list(range(100))

        train1, test1 = spark_train_test_split(data, test_size=0.3, random_state=42, spark=spark_session)
        train2, test2 = spark_train_test_split(data, test_size=0.3, random_state=42, spark=spark_session)

        # Same seed should produce identical splits
        assert set(train1) == set(train2)
        assert set(test1) == set(test2)

    def test_split_empty_list(self, spark_session):
        """Test splitting an empty list."""
        data = []

        train, test = spark_train_test_split(data, test_size=0.2, random_state=42, spark=spark_session)

        assert train == []
        assert test == []

    def test_split_list_proportions(self, spark_session):
        """Test that list split proportions are approximately correct."""
        data = list(range(1000))

        train, test = spark_train_test_split(data, test_size=0.2, random_state=42, spark=spark_session)

        total = len(train) + len(test)
        test_prop = len(test) / total

        # Allow 5% tolerance
        assert 0.15 <= test_prop <= 0.25


class TestSklearnCompatibility:
    """Tests for sklearn.model_selection.train_test_split compatibility."""

    def test_attacker_selection_pattern(self, spark_session):
        """
        Test the pattern used in ConnectedDrivingAttacker.add_attackers().

        This mimics the sklearn pattern:
        regular, attackers = train_test_split(uniqueIDs, test_size=0.3, random_state=42)
        """
        # Simulate unique IDs from a dataset
        unique_ids = list(range(1, 101))  # IDs 1-100

        # Split into regular and attackers (30% attackers)
        regular, attackers = spark_train_test_split(
            unique_ids, test_size=0.30, random_state=42, spark=spark_session
        )

        # Verify split properties
        assert len(regular) + len(attackers) == 100
        assert set(regular).isdisjoint(set(attackers))
        assert set(regular).union(set(attackers)) == set(unique_ids)

        # Verify proportion (allow 15% tolerance for small datasets)
        attacker_ratio = len(attackers) / 100
        assert 0.15 <= attacker_ratio <= 0.45

    def test_alias_function(self, spark_session):
        """Test that the train_test_split alias works."""
        data = list(range(50))

        # Use alias instead of full name
        train, test = train_test_split(data, test_size=0.2, random_state=42, spark=spark_session)

        assert len(train) + len(test) == 50


class TestValidation:
    """Tests for input validation and error handling."""

    def test_invalid_test_size_too_small(self, spark_session):
        """Test that test_size <= 0.0 raises ValueError."""
        data = list(range(10))

        with pytest.raises(ValueError, match="test_size must be between 0.0 and 1.0"):
            spark_train_test_split(data, test_size=0.0, spark=spark_session)

    def test_invalid_test_size_too_large(self, spark_session):
        """Test that test_size >= 1.0 raises ValueError."""
        data = list(range(10))

        with pytest.raises(ValueError, match="test_size must be between 0.0 and 1.0"):
            spark_train_test_split(data, test_size=1.0, spark=spark_session)

    def test_invalid_train_size(self, spark_session):
        """Test that invalid train_size raises ValueError."""
        data = list(range(10))

        with pytest.raises(ValueError, match="train_size must be between 0.0 and 1.0"):
            spark_train_test_split(data, train_size=1.5, test_size=0.2, spark=spark_session)

    def test_train_plus_test_exceeds_one(self, spark_session):
        """Test that train_size + test_size > 1.0 raises ValueError."""
        data = list(range(10))

        with pytest.raises(ValueError, match="The sum of train_size and test_size must be"):
            spark_train_test_split(data, train_size=0.8, test_size=0.5, spark=spark_session)

    def test_unsupported_data_type(self, spark_session):
        """Test that unsupported data types raise TypeError."""
        data = "not a valid type"

        with pytest.raises(TypeError, match="data must be a PySpark DataFrame"):
            spark_train_test_split(data, test_size=0.2, spark=spark_session)


class TestValidateSplitConsistency:
    """Tests for the validate_split_consistency utility function."""

    def test_validate_dataframe_split(self, spark_session):
        """Test validation of a DataFrame split."""
        data = [(i,) for i in range(1000)]
        schema = StructType([StructField("id", IntegerType(), False)])
        df = spark_session.createDataFrame(data, schema)

        train_df, test_df = spark_train_test_split(df, test_size=0.2, random_state=42)

        result = validate_split_consistency(
            train_df, test_df, df,
            expected_test_size=0.2,
            tolerance=0.05
        )

        assert result["valid"], f"Validation failed: {result['issues']}"
        assert result["original_count"] == 1000
        assert result["train_count"] + result["test_count"] == 1000

    def test_validate_list_split(self, spark_session):
        """Test validation of a list split."""
        data = list(range(1000))

        train, test = spark_train_test_split(data, test_size=0.3, random_state=42, spark=spark_session)

        result = validate_split_consistency(
            train, test, data,
            expected_test_size=0.3,
            tolerance=0.05
        )

        assert result["valid"], f"Validation failed: {result['issues']}"
        assert result["original_count"] == 1000

    def test_validate_incorrect_split(self, spark_session):
        """Test that validation detects incorrect splits."""
        data = list(range(100))

        # Create intentionally wrong split
        train = list(range(50))
        test = list(range(40))  # Only 40 items instead of 50

        result = validate_split_consistency(
            train, test, data,
            expected_test_size=0.5,
            tolerance=0.05
        )

        assert not result["valid"]
        assert len(result["issues"]) > 0
        assert "does not equal original count" in result["issues"][0]

    def test_validate_proportion_mismatch(self, spark_session):
        """Test that validation detects proportion mismatches."""
        data = list(range(100))

        train, test = spark_train_test_split(data, test_size=0.2, random_state=42, spark=spark_session)

        # Validate with wrong expected proportion
        result = validate_split_consistency(
            train, test, data,
            expected_test_size=0.5,  # Actual is ~0.2
            tolerance=0.05
        )

        assert not result["valid"]
        assert any("Test proportion" in issue for issue in result["issues"])


class TestIntegration:
    """Integration tests for real-world usage patterns."""

    def test_end_to_end_attacker_simulation(self, spark_session):
        """
        Test complete attacker selection workflow.

        This simulates the ConnectedDrivingAttacker.add_attackers() pattern.
        """
        # Create mock BSM data with unique vehicle IDs
        data = [(i, f"vehicle_{i}", 10.5 + i * 0.1, 20.3 + i * 0.1)
                for i in range(1, 201)]  # 200 vehicles
        schema = StructType([
            StructField("coreData_id", IntegerType(), False),
            StructField("vehicle_name", StringType(), False),
            StructField("x_pos", DoubleType(), False),
            StructField("y_pos", DoubleType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Extract unique IDs
        unique_ids = [row.coreData_id for row in df.select("coreData_id").distinct().collect()]
        assert len(unique_ids) == 200

        # Split into regular and attackers (30% attackers)
        attack_ratio = 0.30
        seed = 42
        regular, attackers = spark_train_test_split(
            unique_ids, test_size=attack_ratio, random_state=seed, spark=spark_session
        )

        # Validate split
        assert len(regular) + len(attackers) == 200
        assert set(regular).isdisjoint(set(attackers))

        # Create isAttacker column (PySpark pattern)
        from pyspark.sql.functions import col, when, lit

        attacker_set = set(attackers)
        df = df.withColumn(
            "isAttacker",
            when(col("coreData_id").isin(attacker_set), 1).otherwise(0)
        )

        # Verify attack ratio
        attacker_count = df.filter(col("isAttacker") == 1).count()
        actual_ratio = attacker_count / 200

        # Allow 5% tolerance
        assert 0.25 <= actual_ratio <= 0.35

    def test_ml_pipeline_split(self, spark_session):
        """
        Test train/test split for ML pipeline.

        This simulates splitting a dataset for training and testing ML models.
        """
        # Create mock feature dataset
        data = [(i, float(i * 2), float(i * 3), i % 2) for i in range(1000)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("feature1", DoubleType(), False),
            StructField("feature2", DoubleType(), False),
            StructField("label", IntegerType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Split into train (80%) and test (20%)
        train_df, test_df = spark_train_test_split(df, test_size=0.2, random_state=42)

        # Validate split
        result = validate_split_consistency(
            train_df, test_df, df,
            expected_train_size=0.8,
            expected_test_size=0.2,
            tolerance=0.05
        )

        assert result["valid"], f"Split validation failed: {result['issues']}"

        # Verify both splits have both label classes (stratification check)
        train_labels = set([row.label for row in train_df.select("label").distinct().collect()])
        test_labels = set([row.label for row in test_df.select("label").distinct().collect()])

        assert train_labels == {0, 1}
        assert test_labels == {0, 1}

    def test_performance_large_dataset(self, spark_session):
        """Test performance on a larger dataset."""
        # Create larger dataset (10k rows)
        data = [(i, f"value_{i}", float(i)) for i in range(10000)]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("value", DoubleType(), False)
        ])
        df = spark_session.createDataFrame(data, schema)

        # Split should complete quickly
        import time
        start = time.time()
        train_df, test_df = spark_train_test_split(df, test_size=0.2, random_state=42)
        duration = time.time() - start

        # Verify split completed
        assert train_df.count() + test_df.count() == 10000

        # Should complete in reasonable time (< 5 seconds on most systems)
        assert duration < 5.0, f"Split took too long: {duration:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

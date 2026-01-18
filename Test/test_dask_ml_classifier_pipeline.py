"""
Tests for DaskMClassifierPipeline core conversion logic.

This module tests the Dask→pandas conversion behavior that DaskMClassifierPipeline implements.
Since the full class has dependency injection dependencies, we test the core conversion pattern
that the pipeline uses.

The key behavior: DaskMClassifierPipeline must convert Dask DataFrames to pandas before
passing to sklearn classifiers (sklearn only supports pandas).
"""

import pytest
import dask.dataframe as dd
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier


@pytest.mark.dask
class TestDaskToPandasConversionPattern:
    """Test the Dask→pandas conversion pattern used by DaskMClassifierPipeline."""

    def test_dask_dataframe_to_pandas_conversion(self, dask_client):
        """Test that Dask DataFrames convert to pandas correctly."""
        # Create Dask DataFrame
        pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Verify it's a Dask DataFrame
        assert isinstance(ddf, dd.DataFrame)

        # Convert to pandas (pattern used by DaskMClassifierPipeline)
        result = ddf.compute()

        # Verify conversion
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert list(result.columns) == ['x_pos', 'y_pos', 'speed', 'heading']

    def test_dask_series_to_pandas_conversion(self, dask_client):
        """Test that Dask Series convert to pandas correctly."""
        # Create Dask Series
        ps = pd.Series([0, 1, 0, 1, 0], name='isAttacker')
        ds = dd.from_pandas(ps, npartitions=2)

        # Verify it's a Dask Series
        assert isinstance(ds, dd.Series)

        # Convert to pandas
        result = ds.compute()

        # Verify conversion
        assert isinstance(result, pd.Series)
        assert len(result) == 5
        assert result.name == 'isAttacker'

    def test_conditional_conversion_dask_dataframe(self, dask_client):
        """Test conditional conversion pattern: only convert if Dask."""
        # Create Dask DataFrame
        pdf = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Convert if Dask (pattern from DaskMClassifierPipeline.__init__)
        if isinstance(ddf, dd.DataFrame):
            result = ddf.compute()
        else:
            result = ddf

        # Verify result is pandas
        assert isinstance(result, pd.DataFrame)

    def test_conditional_conversion_pandas_dataframe(self, dask_client):
        """Test conditional conversion pattern: skip if already pandas."""
        # Create pandas DataFrame
        pdf = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})

        # Convert if Dask (pattern from DaskMClassifierPipeline.__init__)
        if isinstance(pdf, dd.DataFrame):
            result = pdf.compute()
        else:
            result = pdf

        # Verify result is still pandas (no conversion needed)
        assert isinstance(result, pd.DataFrame)
        assert result is pdf  # Same object


@pytest.mark.dask
class TestSklearnCompatibility:
    """Test that converted data works with sklearn classifiers."""

    def test_sklearn_random_forest_with_dask_data(self, dask_client):
        """Test RandomForestClassifier with Dask-converted data."""
        # Create training data as Dask
        train_pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0, 550.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0, 55.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0, 135.0],
            'isAttacker': [0, 1, 0, 1, 0, 1]
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)

        # Convert to pandas (DaskMClassifierPipeline pattern)
        train_X = train_ddf.drop('isAttacker', axis=1).compute()
        train_Y = train_ddf['isAttacker'].compute()

        # Train sklearn classifier
        clf = RandomForestClassifier(random_state=42)
        clf.fit(train_X, train_Y)

        # Verify training succeeded
        assert clf is not None
        assert hasattr(clf, 'n_estimators')

    def test_sklearn_decision_tree_with_dask_data(self, dask_client):
        """Test DecisionTreeClassifier with Dask-converted data."""
        # Create training data
        train_pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0, 550.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0, 55.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0, 135.0],
            'isAttacker': [0, 1, 0, 1, 0, 1]
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)

        # Convert to pandas
        train_X = train_ddf.drop('isAttacker', axis=1).compute()
        train_Y = train_ddf['isAttacker'].compute()

        # Train sklearn classifier
        clf = DecisionTreeClassifier(random_state=42)
        clf.fit(train_X, train_Y)

        # Verify training succeeded
        assert clf is not None

    def test_sklearn_kneighbors_with_dask_data(self, dask_client):
        """Test KNeighborsClassifier with Dask-converted data."""
        # Create training data
        train_pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0, 550.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0, 55.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0, 135.0],
            'isAttacker': [0, 1, 0, 1, 0, 1]
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)

        # Convert to pandas
        train_X = train_ddf.drop('isAttacker', axis=1).compute()
        train_Y = train_ddf['isAttacker'].compute()

        # Train sklearn classifier
        clf = KNeighborsClassifier()
        clf.fit(train_X, train_Y)

        # Verify training succeeded
        assert clf is not None


@pytest.mark.dask
class TestFullMLWorkflowPattern:
    """Test complete ML workflow pattern that DaskMClassifierPipeline implements."""

    def test_full_train_test_workflow_with_dask(self, dask_client):
        """Test complete train→test workflow with Dask data."""
        # Create training data (Dask)
        train_pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0, 550.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0, 55.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0, 135.0],
            'isAttacker': [0, 1, 0, 1, 0, 1]
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)

        # Create test data (Dask)
        test_pdf = pd.DataFrame({
            'x_pos': [150.0, 250.0, 350.0],
            'y_pos': [75.0, 175.0, 275.0],
            'speed': [35.0, 50.0, 65.0],
            'heading': [100.0, 200.0, 280.0],
            'isAttacker': [0, 1, 0]
        })
        test_ddf = dd.from_pandas(test_pdf, npartitions=1)

        # Convert to pandas (DaskMClassifierPipeline pattern)
        train_X = train_ddf.drop('isAttacker', axis=1).compute()
        train_Y = train_ddf['isAttacker'].compute()
        test_X = test_ddf.drop('isAttacker', axis=1).compute()
        test_Y = test_ddf['isAttacker'].compute()

        # Train classifier
        clf = RandomForestClassifier(random_state=42)
        clf.fit(train_X, train_Y)

        # Test classifier
        predictions = clf.predict(test_X)

        # Verify predictions
        assert len(predictions) == 3
        assert all(p in [0, 1] for p in predictions)

    def test_multiple_classifiers_workflow(self, dask_client):
        """Test workflow with multiple classifiers (DaskMClassifierPipeline pattern)."""
        # Create data
        train_pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0, 550.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0, 55.0],
            'heading': [90.0, 180.0, 270.0, 0.0, 45.0, 135.0],
            'isAttacker': [0, 1, 0, 1, 0, 1]
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)

        # Convert to pandas
        train_X = train_ddf.drop('isAttacker', axis=1).compute()
        train_Y = train_ddf['isAttacker'].compute()

        # Create multiple classifiers (default set from DaskMClassifierPipeline)
        classifiers = [
            RandomForestClassifier(random_state=42),
            DecisionTreeClassifier(random_state=42),
            KNeighborsClassifier()
        ]

        # Train all classifiers
        for clf in classifiers:
            clf.fit(train_X, train_Y)

        # Verify all trained
        assert len(classifiers) == 3
        for clf in classifiers:
            assert hasattr(clf, 'predict')

    def test_metrics_calculation_pattern(self, dask_client):
        """Test metrics calculation pattern (used in DaskMClassifierPipeline.calc_classifier_results)."""
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

        # Create separable data for better metrics
        np.random.seed(42)
        train_pdf = pd.DataFrame({
            'x_pos': np.concatenate([np.random.uniform(0, 100, 10), np.random.uniform(200, 300, 10)]),
            'y_pos': np.concatenate([np.random.uniform(0, 100, 10), np.random.uniform(200, 300, 10)]),
            'speed': np.concatenate([np.random.uniform(20, 40, 10), np.random.uniform(50, 70, 10)]),
            'heading': np.concatenate([np.random.uniform(0, 180, 10), np.random.uniform(180, 360, 10)]),
            'isAttacker': [0] * 10 + [1] * 10
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)

        # Convert to pandas
        train_X = train_ddf.drop('isAttacker', axis=1).compute()
        train_Y = train_ddf['isAttacker'].compute()

        # Train and predict
        clf = RandomForestClassifier(random_state=42)
        clf.fit(train_X, train_Y)
        predictions = clf.predict(train_X)

        # Calculate metrics (DaskMClassifierPipeline.calc_classifier_results pattern)
        accuracy = accuracy_score(train_Y, predictions)
        precision = precision_score(train_Y, predictions)
        recall = recall_score(train_Y, predictions)
        f1 = f1_score(train_Y, predictions)

        # Verify metrics are valid
        assert 0.0 <= accuracy <= 1.0
        assert 0.0 <= precision <= 1.0
        assert 0.0 <= recall <= 1.0
        assert 0.0 <= f1 <= 1.0


@pytest.mark.dask
class TestMixedInputTypes:
    """Test handling of mixed Dask and pandas inputs."""

    def test_mixed_dask_and_pandas_inputs(self, dask_client):
        """Test workflow with mixed Dask and pandas DataFrames."""
        # Create mixed inputs
        train_pdf = pd.DataFrame({
            'x': [1, 2, 3, 4, 5, 6],
            'y': [2, 3, 4, 5, 6, 7],
            'label': [0, 1, 0, 1, 0, 1]
        })
        train_ddf = dd.from_pandas(train_pdf, npartitions=2)  # Dask

        test_pdf = pd.DataFrame({
            'x': [1.5, 2.5, 3.5],
            'y': [2.5, 3.5, 4.5],
            'label': [0, 1, 0]
        })
        # test_pdf is already pandas

        # Convert pattern (handles both Dask and pandas)
        train_X = train_ddf.drop('label', axis=1)
        if isinstance(train_X, dd.DataFrame):
            train_X = train_X.compute()

        train_Y = train_ddf['label']
        if isinstance(train_Y, (dd.DataFrame, dd.Series)):
            train_Y = train_Y.compute()

        test_X = test_pdf.drop('label', axis=1)
        if isinstance(test_X, dd.DataFrame):
            test_X = test_X.compute()

        test_Y = test_pdf['label']
        if isinstance(test_Y, (dd.DataFrame, dd.Series)):
            test_Y = test_Y.compute()

        # Train classifier
        clf = DecisionTreeClassifier(random_state=42)
        clf.fit(train_X, train_Y)
        predictions = clf.predict(test_X)

        # Verify predictions
        assert len(predictions) == 3

    def test_all_pandas_inputs_no_conversion(self, dask_client):
        """Test that pandas inputs don't get unnecessarily converted."""
        # All pandas inputs
        train_X = pd.DataFrame({'x': [1, 2, 3], 'y': [2, 3, 4]})
        train_Y = pd.Series([0, 1, 0])

        # Conditional conversion (should skip)
        original_train_X = train_X
        original_train_Y = train_Y

        if isinstance(train_X, dd.DataFrame):
            train_X = train_X.compute()

        if isinstance(train_Y, (dd.DataFrame, dd.Series)):
            train_Y = train_Y.compute()

        # Verify no conversion occurred (same objects)
        assert train_X is original_train_X
        assert train_Y is original_train_Y

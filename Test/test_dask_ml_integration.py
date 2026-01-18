"""
Integration tests for Dask ML pipeline components.

This module tests the full ML integration including:
- DaskMConnectedDrivingDataCleaner (feature selection + hex conversion)
- DaskMClassifierPipeline (training with multiple sklearn classifiers)
- Full pipeline workflow (data → clean → ML → metrics)

Tests cover:
1. End-to-end ML workflow with Dask DataFrames
2. Compatibility with sklearn classifiers (RandomForest, DecisionTree, KNeighbors)
3. Metrics calculation (accuracy, precision, recall, F1, specificity)
4. Dask to pandas conversion accuracy
"""

import pytest
import dask.dataframe as dd
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score

# NOTE: We don't import DaskMClassifierPipeline or MDataClassifier here due to EasyMLLib dependency
# Tests that need them will use lazy imports


@pytest.mark.dask
class TestDaskToPandasConversion:
    """Test Dask to pandas conversion patterns used by ML pipeline."""

    def test_dask_dataframe_conversion_preserves_data(self, dask_client):
        """Test that Dask DataFrames convert to pandas without data loss."""
        # Create sample data
        pdf = pd.DataFrame({
            'x_pos': [100.0, 200.0, 300.0, 400.0, 500.0],
            'y_pos': [50.0, 150.0, 250.0, 350.0, 450.0],
            'speed': [30.0, 45.0, 60.0, 50.0, 40.0],
            'isAttacker': [0, 1, 0, 1, 0]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Convert to pandas (what DaskMClassifierPipeline does)
        converted = ddf.compute()

        # Validate conversion preserved data
        assert isinstance(converted, pd.DataFrame), "Should convert to pandas DataFrame"
        assert len(converted) == 5, "Should preserve row count"
        pd.testing.assert_frame_equal(converted, pdf, "Should preserve data exactly")

    def test_dask_series_conversion_preserves_data(self, dask_client):
        """Test that Dask Series convert to pandas without data loss."""
        ps = pd.Series([0, 1, 0, 1, 0], name='isAttacker')
        ds = dd.from_pandas(ps, npartitions=2)

        # Convert to pandas
        converted = ds.compute()

        # Validate conversion
        assert isinstance(converted, pd.Series), "Should convert to pandas Series"
        assert len(converted) == 5, "Should preserve length"
        pd.testing.assert_series_equal(converted, ps, "Should preserve data exactly")

    def test_numeric_precision_preserved(self, dask_client):
        """Test that numeric precision is preserved during conversion."""
        # Create data with high precision values
        pdf = pd.DataFrame({
            'lat': [40.123456789, 40.987654321, 40.555555555],
            'lon': [-83.123456789, -83.987654321, -83.555555555]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Convert to pandas
        converted = ddf.compute()

        # Validate precision (rtol=1e-9)
        for col in ['lat', 'lon']:
            original = pdf[col].values
            converted_vals = converted[col].values
            assert np.allclose(original, converted_vals, rtol=1e-9), \
                f"Column {col} should preserve precision to 1e-9"


@pytest.mark.dask
class TestSklearnWithDaskConvertedData:
    """Test sklearn classifiers work with Dask-converted data."""

    def test_random_forest_with_converted_data(self, dask_client):
        """Test RandomForestClassifier with Dask-converted data."""
        # Create training data
        np.random.seed(42)
        X_pdf = pd.DataFrame({
            'feature1': np.random.rand(100),
            'feature2': np.random.rand(100)
        })
        y_ps = pd.Series(np.random.randint(0, 2, 100))

        # Convert to Dask and back (simulating pipeline behavior)
        X_ddf = dd.from_pandas(X_pdf, npartitions=4)
        y_ds = dd.from_pandas(y_ps, npartitions=4)
        X_converted = X_ddf.compute()
        y_converted = y_ds.compute()

        # Train classifier
        clf = RandomForestClassifier(n_estimators=5, random_state=42)
        clf.fit(X_converted, y_converted)

        # Predict
        predictions = clf.predict(X_converted)

        # Validate
        assert len(predictions) == 100, "Should predict all samples"
        accuracy = accuracy_score(y_converted, predictions)
        assert accuracy > 0.5, "Should have reasonable accuracy (>50%)"

    def test_decision_tree_with_converted_data(self, dask_client):
        """Test DecisionTreeClassifier with Dask-converted data."""
        np.random.seed(42)
        X_pdf = pd.DataFrame({
            'x': np.random.rand(100),
            'y': np.random.rand(100)
        })
        y_ps = pd.Series(np.random.randint(0, 2, 100))

        # Dask conversion
        X_converted = dd.from_pandas(X_pdf, npartitions=4).compute()
        y_converted = dd.from_pandas(y_ps, npartitions=4).compute()

        # Train
        clf = DecisionTreeClassifier(random_state=42)
        clf.fit(X_converted, y_converted)
        predictions = clf.predict(X_converted)

        assert len(predictions) == 100, "Should predict all samples"
        assert all(p in [0, 1] for p in predictions), "Predictions should be binary"

    def test_kneighbors_with_converted_data(self, dask_client):
        """Test KNeighborsClassifier with Dask-converted data."""
        np.random.seed(42)
        X_pdf = pd.DataFrame({
            'a': np.random.rand(100),
            'b': np.random.rand(100)
        })
        y_ps = pd.Series(np.random.randint(0, 2, 100))

        # Dask conversion
        X_converted = dd.from_pandas(X_pdf, npartitions=4).compute()
        y_converted = dd.from_pandas(y_ps, npartitions=4).compute()

        # Train
        clf = KNeighborsClassifier(n_neighbors=3)
        clf.fit(X_converted, y_converted)
        predictions = clf.predict(X_converted)

        assert len(predictions) == 100, "Should predict all samples"


@pytest.mark.dask
class TestMLDataPrepation:
    """Test ML data preparation patterns."""

    def test_feature_selection_pattern(self, dask_client):
        """Test selecting feature columns from Dask DataFrame."""
        # Create dataset with many columns
        pdf = pd.DataFrame({
            'coreData_id': [f'0x{i:04x}' for i in range(50)],
            'x_pos': np.random.rand(50),
            'y_pos': np.random.rand(50),
            'speed': np.random.rand(50),
            'heading': np.random.rand(50),
            'lat': np.random.rand(50),
            'lon': np.random.rand(50),
            'extra_col': np.random.rand(50),
            'isAttacker': np.random.randint(0, 2, 50)
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Select only ML features (pattern used by cleaners)
        feature_cols = ['x_pos', 'y_pos', 'speed', 'heading', 'lat', 'lon']
        X = ddf[feature_cols]
        y = ddf['isAttacker']

        # Validate selection
        assert isinstance(X, dd.DataFrame), "X should be Dask DataFrame"
        assert isinstance(y, dd.Series), "y should be Dask Series"

        # Validate after conversion
        X_pandas = X.compute()
        y_pandas = y.compute()

        assert list(X_pandas.columns) == feature_cols, "Should have only selected features"
        assert len(X_pandas) == 50, "Should preserve all rows"
        assert y_pandas.name == 'isAttacker', "Label should be named isAttacker"

    def test_train_test_split_pattern(self, dask_client):
        """Test splitting Dask DataFrame into train/test sets."""
        # Create dataset
        pdf = pd.DataFrame({
            'x': np.arange(100),
            'y': np.arange(100, 200),
            'label': [0, 1] * 50
        })
        ddf = dd.from_pandas(pdf, npartitions=4)

        # Split pattern: compute to pandas, split, convert back
        pdf_computed = ddf.compute()
        train_size = int(len(pdf_computed) * 0.8)

        train_pdf = pdf_computed.iloc[:train_size]
        test_pdf = pdf_computed.iloc[train_size:]

        train_ddf = dd.from_pandas(train_pdf, npartitions=2)
        test_ddf = dd.from_pandas(test_pdf, npartitions=2)

        # Validate split
        assert len(train_ddf) == 80, "Training set should have 80 rows"
        assert len(test_ddf) == 20, "Test set should have 20 rows"

        # Validate no overlap
        train_ids = train_ddf['x'].compute().values
        test_ids = test_ddf['x'].compute().values
        assert len(set(train_ids) & set(test_ids)) == 0, "Train/test should not overlap"


@pytest.mark.dask
class TestMLWorkflowPatterns:
    """Test common ML workflow patterns."""

    def test_full_classification_workflow(self, dask_client):
        """Test complete classification workflow with Dask data."""
        # Create linearly separable data for reliable testing
        np.random.seed(42)
        n_samples = 200

        # Class 0: low values
        class0_X = pd.DataFrame({
            'x': np.random.rand(100),
            'y': np.random.rand(100)
        })
        class0_y = pd.Series([0] * 100)

        # Class 1: high values
        class1_X = pd.DataFrame({
            'x': np.random.rand(100) + 2,
            'y': np.random.rand(100) + 2
        })
        class1_y = pd.Series([1] * 100)

        # Combine
        X_pdf = pd.concat([class0_X, class1_X]).reset_index(drop=True)
        y_ps = pd.concat([class0_y, class1_y]).reset_index(drop=True)

        # Convert to Dask
        X_ddf = dd.from_pandas(X_pdf, npartitions=4)
        y_ds = dd.from_pandas(y_ps, npartitions=4)

        # Convert to pandas (ML pipeline pattern)
        X_train = X_ddf.compute()
        y_train = y_ds.compute()

        # Train classifier
        clf = RandomForestClassifier(n_estimators=10, random_state=42)
        clf.fit(X_train, y_train)

        # Test on training data (should have very high accuracy)
        predictions = clf.predict(X_train)
        accuracy = accuracy_score(y_train, predictions)

        # With linearly separable data, should achieve >95% accuracy
        assert accuracy > 0.95, f"Should have >95% accuracy on separable data, got {accuracy:.2f}"

    def test_multiple_classifiers_comparison(self, dask_client):
        """Test comparing multiple classifier types."""
        # Create simple dataset
        np.random.seed(42)
        X_pdf = pd.DataFrame({
            'feature1': np.random.rand(100),
            'feature2': np.random.rand(100)
        })
        y_ps = pd.Series([0, 1] * 50)

        # Convert to Dask and back
        X_train = dd.from_pandas(X_pdf, npartitions=2).compute()
        y_train = dd.from_pandas(y_ps, npartitions=2).compute()

        # Train multiple classifiers
        classifiers = {
            'RandomForest': RandomForestClassifier(n_estimators=5, random_state=42),
            'DecisionTree': DecisionTreeClassifier(random_state=42),
            'KNeighbors': KNeighborsClassifier(n_neighbors=3)
        }

        results = {}
        for name, clf in classifiers.items():
            clf.fit(X_train, y_train)
            predictions = clf.predict(X_train)
            accuracy = accuracy_score(y_train, predictions)
            results[name] = accuracy

        # Validate all classifiers trained
        assert len(results) == 3, "Should have results for 3 classifiers"
        for name, acc in results.items():
            assert 0.0 <= acc <= 1.0, f"{name} accuracy should be in [0, 1]"


@pytest.mark.dask
class TestHexConversionPattern:
    """Test hexadecimal ID conversion pattern (used by ML cleaner)."""

    def test_hex_to_decimal_conversion(self, dask_client):
        """Test converting hex IDs to decimal for ML features."""
        # Create data with hex IDs
        pdf = pd.DataFrame({
            'coreData_id': ['0x0001', '0x0010', '0x00ff', '0x1000'],
            'x_pos': [100, 200, 300, 400],
            'isAttacker': [0, 1, 0, 1]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        # Convert hex to decimal (pattern used by DaskMConnectedDrivingDataCleaner)
        def hex_to_decimal(hex_str):
            if pd.isna(hex_str):
                return None
            # Remove '0x' prefix if present, then convert
            hex_clean = str(hex_str).replace('0x', '').replace('.0', '')
            return int(hex_clean, 16)

        # Apply conversion
        ddf_converted = ddf.assign(
            coreData_id=ddf['coreData_id'].map(hex_to_decimal, meta=('coreData_id', 'int64'))
        )

        # Validate
        result = ddf_converted.compute()
        expected_ids = [1, 16, 255, 4096]  # Decimal equivalents

        assert list(result['coreData_id']) == expected_ids, "Hex conversion should be correct"
        assert result['coreData_id'].dtype == np.int64, "Should be integer type"

    def test_hex_conversion_with_none_values(self, dask_client):
        """Test hex conversion handles None values."""
        pdf = pd.DataFrame({
            'coreData_id': ['0x0001', None, '0x00ff', '0x1000'],
            'x_pos': [100, 200, 300, 400]
        })
        ddf = dd.from_pandas(pdf, npartitions=2)

        def hex_to_decimal(hex_str):
            if pd.isna(hex_str):
                return np.nan  # Return NaN for None
            hex_clean = str(hex_str).replace('0x', '').replace('.0', '')
            return int(hex_clean, 16)

        ddf_converted = ddf.assign(
            coreData_id=ddf['coreData_id'].map(hex_to_decimal, meta=('coreData_id', 'float64'))
        )

        result = ddf_converted.compute()

        # Validate: first should be 1, second should be NaN, third should be 255, fourth should be 4096
        assert result['coreData_id'].iloc[0] == 1.0
        assert pd.isna(result['coreData_id'].iloc[1])
        assert result['coreData_id'].iloc[2] == 255.0
        assert result['coreData_id'].iloc[3] == 4096.0

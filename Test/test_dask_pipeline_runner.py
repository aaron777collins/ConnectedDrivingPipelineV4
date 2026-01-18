"""
Test Suite for DaskPipelineRunner

Tests the parameterized ML pipeline runner with various configurations.

Author: Claude (Anthropic)
Date: 2026-01-18
"""

import pytest
import json
import os
import tempfile
from unittest.mock import Mock, MagicMock, patch
import dask.dataframe as dd
import pandas as pd

from MachineLearning.DaskPipelineRunner import DaskPipelineRunner


@pytest.fixture
def sample_config():
    """Sample pipeline configuration for testing."""
    return {
        "pipeline_name": "test_pipeline_2000m_rand_offset",
        "data": {
            "source_file": "data/data.csv",
            "num_subsection_rows": 1000,
            "lines_per_file": 10000,
            "filtering": {
                "type": "xy_offset_position",
                "distance_meters": 2000,
                "center_x": -106.0,
                "center_y": 41.0,
                "use_xy_coords": True
            },
            "date_range": {
                "start": "2021-04-01",
                "end": "2021-04-30"
            },
            "columns": [
                "coreData_id", "coreData_position_lat", "coreData_position_long",
                "coreData_elevation", "coreData_position"
            ]
        },
        "features": {
            "column_set": "xy_elev",
            "columns": ["x_pos", "y_pos", "coreData_elevation", "isAttacker"]
        },
        "attacks": {
            "enabled": True,
            "ratio": 0.30,
            "type": "rand_offset",
            "min_distance": 100,
            "max_distance": 200,
            "seed": 42
        },
        "ml": {
            "train_test_split": {
                "type": "random",
                "train_ratio": 0.80,
                "test_ratio": 0.20,
                "seed": 42
            },
            "classifiers": ["RandomForest", "DecisionTree", "KNeighbors"]
        },
        "cache": {
            "enabled": True,
            "format": "parquet"
        }
    }


@pytest.fixture
def minimal_config():
    """Minimal configuration with defaults."""
    return {
        "pipeline_name": "minimal_test_pipeline",
        "data": {},
        "features": {},
        "attacks": {"enabled": False},
        "ml": {}
    }


class TestDaskPipelineRunnerInitialization:
    """Test DaskPipelineRunner initialization and configuration."""

    def test_init_with_valid_config(self, sample_config):
        """Test initialization with valid configuration."""
        runner = DaskPipelineRunner(sample_config)

        assert runner.pipeline_name == "test_pipeline_2000m_rand_offset"
        assert runner.config == sample_config
        assert runner.csvWriter is not None
        assert runner.logger is not None

    def test_init_with_minimal_config(self, minimal_config):
        """Test initialization with minimal configuration."""
        runner = DaskPipelineRunner(minimal_config)

        assert runner.pipeline_name == "minimal_test_pipeline"
        assert runner.config == minimal_config

    def test_from_config_file(self, sample_config, tmp_path):
        """Test loading configuration from JSON file."""
        config_file = tmp_path / "test_config.json"
        with open(config_file, 'w') as f:
            json.dump(sample_config, f)

        runner = DaskPipelineRunner.from_config(str(config_file))

        assert runner.pipeline_name == "test_pipeline_2000m_rand_offset"
        assert runner.config["data"]["filtering"]["distance_meters"] == 2000

    def test_config_hash_generation(self, sample_config):
        """Test configuration hash generation for caching."""
        runner = DaskPipelineRunner(sample_config)
        hash1 = runner._get_config_hash()

        # Hash should be consistent
        hash2 = runner._get_config_hash()
        assert hash1 == hash2

        # Different pipeline name should produce different hash
        sample_config["pipeline_name"] = "different_pipeline"
        runner2 = DaskPipelineRunner(sample_config)
        hash3 = runner2._get_config_hash()
        assert hash1 != hash3


class TestProviderSetup:
    """Test context and path provider setup."""

    def test_generator_context_provider_setup(self, sample_config):
        """Test GeneratorContextProvider is configured correctly."""
        runner = DaskPipelineRunner(sample_config)

        assert runner.generatorContextProvider is not None
        assert runner.generatorContextProvider.get("ConnectedDrivingCleaner.x_pos") == -106.0
        assert runner.generatorContextProvider.get("ConnectedDrivingCleaner.y_pos") == 41.0
        assert runner.generatorContextProvider.get("ConnectedDrivingLargeDataCleaner.max_dist") == 2000
        assert runner.generatorContextProvider.get("ConnectedDrivingAttacker.attack_ratio") == 0.30
        assert runner.generatorContextProvider.get("ConnectedDrivingAttacker.SEED") == 42

    def test_ml_context_provider_setup(self, sample_config):
        """Test MLContextProvider is configured correctly."""
        runner = DaskPipelineRunner(sample_config)

        assert runner.MLContextProvider is not None
        columns = runner.MLContextProvider.get("MConnectedDrivingDataCleaner.columns")
        assert "x_pos" in columns
        assert "y_pos" in columns
        assert "isAttacker" in columns

    def test_path_providers_setup(self, sample_config):
        """Test all path providers are initialized."""
        runner = DaskPipelineRunner(sample_config)

        assert runner._pathprovider is not None
        assert runner._initialGathererPathProvider is not None
        assert runner._generatorPathProvider is not None
        assert runner._mlPathProvider is not None

    def test_date_range_parsing(self, sample_config):
        """Test date range configuration is parsed correctly."""
        runner = DaskPipelineRunner(sample_config)

        assert runner.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.startyear") == 2021
        assert runner.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.startmonth") == 4
        assert runner.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.startday") == 1
        assert runner.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.endyear") == 2021
        assert runner.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.endmonth") == 4
        assert runner.generatorContextProvider.get("CleanerWithFilterWithinRangeXYAndDay.endday") == 30


class TestAttackApplication:
    """Test attack configuration and application."""

    def test_apply_attacks_disabled(self, sample_config):
        """Test that attacks can be disabled."""
        runner = DaskPipelineRunner(sample_config)

        # Create sample data
        data = dd.from_pandas(pd.DataFrame({
            'x_pos': [1.0, 2.0, 3.0],
            'y_pos': [1.0, 2.0, 3.0],
            'isAttacker': [0, 0, 0]
        }), npartitions=1)

        attack_config = {"enabled": False}
        result = runner._apply_attacks(data, attack_config, "test")

        # Should return data unchanged (same object reference)
        assert result is data

    def test_apply_rand_offset_attack(self, sample_config):
        """Test random offset attack configuration."""
        runner = DaskPipelineRunner(sample_config)

        attack_config = {
            "enabled": True,
            "type": "rand_offset",
            "min_distance": 100,
            "max_distance": 200
        }

        # Mock the attacker to verify method calls
        with patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingAttacker') as MockAttacker:
            mock_attacker_instance = Mock()
            mock_attacker_instance.add_attackers.return_value = mock_attacker_instance
            mock_attacker_instance.add_attacks_positional_offset_rand.return_value = mock_attacker_instance
            mock_attacker_instance.get_data.return_value = "attacked_data"
            MockAttacker.return_value = mock_attacker_instance

            data = Mock()
            result = runner._apply_attacks(data, attack_config, "train")

            # Verify attack methods were called
            mock_attacker_instance.add_attackers.assert_called_once()
            mock_attacker_instance.add_attacks_positional_offset_rand.assert_called_once_with(
                min_dist=100, max_dist=200
            )
            assert result == "attacked_data"

    def test_apply_const_offset_attack(self, sample_config):
        """Test constant offset attack configuration."""
        runner = DaskPipelineRunner(sample_config)

        attack_config = {
            "enabled": True,
            "type": "const_offset",
            "direction_angle": 45,
            "distance_meters": 100
        }

        with patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingAttacker') as MockAttacker:
            mock_attacker_instance = Mock()
            mock_attacker_instance.add_attackers.return_value = mock_attacker_instance
            mock_attacker_instance.add_attacks_positional_offset_const.return_value = mock_attacker_instance
            mock_attacker_instance.get_data.return_value = "attacked_data"
            MockAttacker.return_value = mock_attacker_instance

            data = Mock()
            result = runner._apply_attacks(data, attack_config, "train")

            mock_attacker_instance.add_attacks_positional_offset_const.assert_called_once_with(
                direction_angle=45, distance_meters=100
            )

    def test_apply_swap_rand_attack(self, sample_config):
        """Test random swap attack configuration."""
        runner = DaskPipelineRunner(sample_config)

        attack_config = {
            "enabled": True,
            "type": "swap_rand"
        }

        with patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingAttacker') as MockAttacker:
            mock_attacker_instance = Mock()
            mock_attacker_instance.add_attackers.return_value = mock_attacker_instance
            mock_attacker_instance.add_attacks_positional_swap_rand.return_value = mock_attacker_instance
            mock_attacker_instance.get_data.return_value = "attacked_data"
            MockAttacker.return_value = mock_attacker_instance

            data = Mock()
            result = runner._apply_attacks(data, attack_config, "train")

            mock_attacker_instance.add_attacks_positional_swap_rand.assert_called_once()

    def test_apply_override_const_attack(self, sample_config):
        """Test constant override attack configuration."""
        runner = DaskPipelineRunner(sample_config)

        attack_config = {
            "enabled": True,
            "type": "override_const",
            "direction_angle": 90,
            "distance_meters": 150
        }

        with patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingAttacker') as MockAttacker:
            mock_attacker_instance = Mock()
            mock_attacker_instance.add_attackers.return_value = mock_attacker_instance
            mock_attacker_instance.add_attacks_positional_override_const.return_value = mock_attacker_instance
            mock_attacker_instance.get_data.return_value = "attacked_data"
            MockAttacker.return_value = mock_attacker_instance

            data = Mock()
            result = runner._apply_attacks(data, attack_config, "train")

            mock_attacker_instance.add_attacks_positional_override_const.assert_called_once_with(
                direction_angle=90, distance_meters=150
            )

    def test_apply_override_rand_attack(self, sample_config):
        """Test random override attack configuration."""
        runner = DaskPipelineRunner(sample_config)

        attack_config = {
            "enabled": True,
            "type": "override_rand",
            "min_distance": 50,
            "max_distance": 150
        }

        with patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingAttacker') as MockAttacker:
            mock_attacker_instance = Mock()
            mock_attacker_instance.add_attackers.return_value = mock_attacker_instance
            mock_attacker_instance.add_attacks_positional_override_rand.return_value = mock_attacker_instance
            mock_attacker_instance.get_data.return_value = "attacked_data"
            MockAttacker.return_value = mock_attacker_instance

            data = Mock()
            result = runner._apply_attacks(data, attack_config, "train")

            mock_attacker_instance.add_attacks_positional_override_rand.assert_called_once_with(
                min_dist=50, max_dist=150
            )


class TestUtilityMethods:
    """Test utility methods."""

    def test_get_default_columns(self, sample_config):
        """Test default column list generation."""
        runner = DaskPipelineRunner(sample_config)
        columns = runner._get_default_columns()

        assert isinstance(columns, list)
        assert "coreData_id" in columns
        assert "coreData_position_lat" in columns
        assert "coreData_elevation" in columns

    def test_get_cleaner_and_filter_xy_offset(self, sample_config):
        """Test cleaner/filter selection for xy_offset_position type."""
        runner = DaskPipelineRunner(sample_config)
        cleaner_class, filter_func = runner._get_cleaner_and_filter("xy_offset_position")

        assert cleaner_class is not None
        assert filter_func is not None

    def test_get_cleaner_and_filter_passthrough(self, sample_config):
        """Test cleaner/filter selection for passthrough type."""
        runner = DaskPipelineRunner(sample_config)
        cleaner_class, filter_func = runner._get_cleaner_and_filter("passthrough")

        assert cleaner_class is not None
        assert filter_func is not None

    def test_write_entire_row(self, sample_config):
        """Test CSV row writing."""
        runner = DaskPipelineRunner(sample_config)

        result_dict = {
            "Model": "RandomForest",
            "train_accuracy": 0.95,
            "test_accuracy": 0.92
        }

        # Should not raise exception
        runner.write_entire_row(result_dict)


class TestPipelineExecution:
    """Test full pipeline execution (with mocking)."""

    @pytest.mark.skip(reason="Integration test - requires full Dask cluster and data files")
    def test_run_full_pipeline(self, sample_config):
        """Test full pipeline execution (integration test)."""
        # This would require actual data files and full Dask setup
        # Skipped in unit tests, but useful as integration test
        runner = DaskPipelineRunner(sample_config)
        results = runner.run()

        assert len(results) > 0
        for classifier, train_results, test_results in results:
            assert len(train_results) == 5  # accuracy, precision, recall, f1, specificity
            assert len(test_results) == 5

    def test_run_pipeline_mock(self, sample_config):
        """Test pipeline execution with mocked components."""
        runner = DaskPipelineRunner(sample_config)

        # Mock all the major components
        with patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingLargeDataCleaner') as MockCleaner, \
             patch('MachineLearning.DaskPipelineRunner.DaskConnectedDrivingAttacker') as MockAttacker, \
             patch('MachineLearning.DaskPipelineRunner.DaskMConnectedDrivingDataCleaner') as MockMLCleaner, \
             patch('MachineLearning.DaskPipelineRunner.DaskMClassifierPipeline') as MockPipeline:

            # Setup mock cleaner
            mock_cleaner = Mock()
            mock_data = dd.from_pandas(pd.DataFrame({
                'x_pos': [1.0, 2.0, 3.0, 4.0],
                'y_pos': [1.0, 2.0, 3.0, 4.0],
                'coreData_elevation': [100, 110, 120, 130],
                'isAttacker': [0, 1, 0, 1]
            }), npartitions=1)
            mock_cleaner.getAllRows.return_value = mock_data
            mock_cleaner.getNumOfRows.return_value = 4
            MockCleaner.return_value = mock_cleaner

            # Setup mock attacker
            mock_attacker = Mock()
            mock_attacker.add_attackers.return_value = mock_attacker
            mock_attacker.add_attacks_positional_offset_rand.return_value = mock_attacker
            mock_attacker.get_data.return_value = mock_data
            MockAttacker.return_value = mock_attacker

            # Setup mock ML cleaner
            mock_ml_cleaner = Mock()
            mock_ml_cleaner.clean_data.return_value = mock_ml_cleaner
            mock_ml_cleaner.get_cleaned_data.return_value = mock_data
            MockMLCleaner.return_value = mock_ml_cleaner

            # Setup mock pipeline
            mock_pipeline = Mock()
            mock_pipeline.train.return_value = mock_pipeline
            mock_pipeline.test.return_value = mock_pipeline
            mock_pipeline.calc_classifier_results.return_value = mock_pipeline
            mock_pipeline.get_classifier_results.return_value = [
                ("RandomForest", (0.95, 0.93, 0.94, 0.935, 0.92), (0.90, 0.88, 0.89, 0.885, 0.87))
            ]
            MockPipeline.return_value = mock_pipeline

            # Run pipeline
            results = runner.run()

            # Verify results
            assert len(results) == 1
            assert results[0][0] == "RandomForest"
            assert len(results[0][1]) == 5  # train metrics
            assert len(results[0][2]) == 5  # test metrics


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

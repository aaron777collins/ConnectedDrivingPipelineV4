"""
Test Suite for DaskPipelineRunner with Real Config Files

Tests DaskPipelineRunner with actual generated pipeline configs from MClassifierPipelines/configs/

Author: Claude (Anthropic)
Date: 2026-01-18
"""

import pytest
import json
import os
from pathlib import Path
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner


# Get path to configs directory
CONFIGS_DIR = Path(__file__).parent.parent / "MClassifierPipelines" / "configs"


def get_all_config_files():
    """Get all JSON config files from configs directory."""
    if not CONFIGS_DIR.exists():
        return []
    return sorted([f for f in CONFIGS_DIR.glob("*.json") if f.name != ".gitkeep"])


def get_sample_config_files():
    """Get a representative sample of config files for testing."""
    all_configs = get_all_config_files()
    if not all_configs:
        return []

    # Select diverse configs to test different features
    sample_names = [
        "MClassifierLargePipelineUserNoXYOffsetPosNoMaxDistMapCreator.json",  # Passthrough filter, simple
        "MClassifierLargePipelineUserJNoCacheWithXYOffsetPos2000mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVCols30attackersRandOffset100To200xN106y41d01to30m04y2021.json",  # XY filter, date range
        "MClassifierLargePipelineUserWithXYOffsetPos1000mDist80kTrain20kTestRowsEXTTimestampsCols30attackersRandOffset50To100xN106y41d02m04y2021.json",  # Fixed split
        "MClassifierLargePipelineUserWithXYOffsetPos500mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVHeadingSpeedCols30attackersRandOffset100To200xN106y41d01to30m04y2021.json",  # 500m filter
        "MClassifierLargePipelineUserJNoCacheWithXYOffsetPos2000mDistRandSplit80PercentTrain20PercentTestAllRowsONLYXYELEVCols30attackersConstPosPerCarOffset100To200xN106y41d01to30m04y2021.json",  # const_offset_per_id attack
    ]

    sample_configs = []
    for name in sample_names:
        config_path = CONFIGS_DIR / name
        if config_path.exists():
            sample_configs.append(config_path)

    return sample_configs


class TestConfigFileLoading:
    """Test loading and parsing of real config files."""

    def test_configs_directory_exists(self):
        """Test that configs directory exists."""
        assert CONFIGS_DIR.exists(), f"Configs directory not found at {CONFIGS_DIR}"
        assert CONFIGS_DIR.is_dir(), f"{CONFIGS_DIR} is not a directory"

    def test_all_55_configs_exist(self):
        """Test that all 55 config files are present."""
        config_files = get_all_config_files()
        assert len(config_files) == 55, f"Expected 55 configs, found {len(config_files)}"

    def test_all_configs_are_valid_json(self):
        """Test that all config files contain valid JSON."""
        config_files = get_all_config_files()
        invalid_files = []

        for config_file in config_files:
            try:
                with open(config_file, 'r') as f:
                    json.load(f)
            except json.JSONDecodeError as e:
                invalid_files.append((config_file.name, str(e)))

        assert len(invalid_files) == 0, f"Invalid JSON in files: {invalid_files}"

    def test_all_configs_have_required_fields(self):
        """Test that all configs have required top-level fields."""
        config_files = get_all_config_files()
        required_fields = ["pipeline_name", "data", "features", "attacks", "ml", "cache"]
        missing_fields = []

        for config_file in config_files:
            with open(config_file, 'r') as f:
                config = json.load(f)

            for field in required_fields:
                if field not in config:
                    missing_fields.append((config_file.name, field))

        assert len(missing_fields) == 0, f"Missing required fields: {missing_fields}"


class TestDaskPipelineRunnerWithConfigs:
    """Test DaskPipelineRunner initialization with real configs."""

    @pytest.mark.parametrize("config_file", get_sample_config_files(), ids=lambda f: f.name)
    def test_load_sample_configs(self, config_file):
        """Test loading sample configs with DaskPipelineRunner.from_config()."""
        runner = DaskPipelineRunner.from_config(str(config_file))

        # Verify runner is initialized correctly
        assert runner is not None
        assert runner.config is not None
        assert runner.pipeline_name is not None
        assert len(runner.pipeline_name) > 0

    @pytest.mark.parametrize("config_file", get_sample_config_files(), ids=lambda f: f.name)
    def test_config_providers_setup(self, config_file):
        """Test that context providers are setup correctly for each config."""
        runner = DaskPipelineRunner.from_config(str(config_file))

        # Verify providers exist
        assert runner.generatorContextProvider is not None
        assert runner.MLContextProvider is not None
        assert runner._pathprovider is not None

    @pytest.mark.parametrize("config_file", get_sample_config_files(), ids=lambda f: f.name)
    def test_config_hash_uniqueness(self, config_file):
        """Test that each config produces a unique hash."""
        runner = DaskPipelineRunner.from_config(str(config_file))
        hash1 = runner._get_config_hash()

        # Hash should be consistent
        hash2 = runner._get_config_hash()
        assert hash1 == hash2

        # Hash should be non-empty
        assert len(hash1) > 0

    def test_different_configs_produce_different_hashes(self):
        """Test that different configs produce different hashes."""
        sample_configs = get_sample_config_files()
        if len(sample_configs) < 2:
            pytest.skip("Need at least 2 sample configs")

        hashes = {}
        for config_file in sample_configs[:5]:  # Test first 5
            runner = DaskPipelineRunner.from_config(str(config_file))
            config_hash = runner._get_config_hash()
            hashes[config_file.name] = config_hash

        # All hashes should be unique
        unique_hashes = set(hashes.values())
        assert len(unique_hashes) == len(hashes), f"Duplicate hashes found: {hashes}"


class TestConfigFilterTypes:
    """Test different filter type configurations."""

    def test_passthrough_filter_config(self):
        """Test config with passthrough filter."""
        config_file = CONFIGS_DIR / "MClassifierLargePipelineUserNoXYOffsetPosNoMaxDistMapCreator.json"
        if not config_file.exists():
            pytest.skip("Passthrough config not found")

        runner = DaskPipelineRunner.from_config(str(config_file))
        assert runner.config["data"]["filtering"]["type"] == "passthrough"

    def test_xy_offset_filter_config(self):
        """Test config with xy_offset_position filter."""
        # Find a config with xy_offset_position
        config_files = get_all_config_files()
        xy_configs = [f for f in config_files if "XYOffsetPos" in f.name]

        if not xy_configs:
            pytest.skip("No XY offset configs found")

        runner = DaskPipelineRunner.from_config(str(xy_configs[0]))
        assert runner.config["data"]["filtering"]["type"] == "xy_offset_position"
        assert "distance_meters" in runner.config["data"]["filtering"]
        assert "center_x" in runner.config["data"]["filtering"]
        assert "center_y" in runner.config["data"]["filtering"]


class TestConfigAttackTypes:
    """Test different attack type configurations."""

    def test_rand_offset_attack_config(self):
        """Test config with rand_offset attack."""
        config_files = get_all_config_files()
        rand_offset_configs = [f for f in config_files if "RandOffset" in f.name]

        if not rand_offset_configs:
            pytest.skip("No rand_offset configs found")

        runner = DaskPipelineRunner.from_config(str(rand_offset_configs[0]))
        assert runner.config["attacks"]["enabled"] is True
        assert runner.config["attacks"]["type"] == "rand_offset"
        assert "min_distance" in runner.config["attacks"]
        assert "max_distance" in runner.config["attacks"]

    def test_const_offset_per_id_attack_config(self):
        """Test config with const_offset_per_id attack."""
        config_files = get_all_config_files()
        const_configs = [f for f in config_files if "ConstPosPerCarOffset" in f.name]

        if not const_configs:
            pytest.skip("No const_offset_per_id configs found")

        runner = DaskPipelineRunner.from_config(str(const_configs[0]))
        assert runner.config["attacks"]["enabled"] is True
        assert runner.config["attacks"]["type"] == "const_offset_per_id"
        assert "min_distance" in runner.config["attacks"]
        assert "max_distance" in runner.config["attacks"]


class TestConfigTrainTestSplit:
    """Test different train/test split configurations."""

    def test_percentage_split_config(self):
        """Test config with percentage-based train/test split."""
        config_files = get_all_config_files()
        percent_configs = [f for f in config_files if "PercentTrain" in f.name]

        if not percent_configs:
            pytest.skip("No percentage split configs found")

        runner = DaskPipelineRunner.from_config(str(percent_configs[0]))
        assert runner.config["ml"]["train_test_split"]["type"] == "random"
        assert runner.config["ml"]["train_test_split"]["train_ratio"] == 0.8
        assert runner.config["ml"]["train_test_split"]["test_ratio"] == 0.2

    def test_fixed_size_split_config(self):
        """Test config with fixed-size train/test split."""
        config_files = get_all_config_files()
        fixed_configs = [f for f in config_files if "80kTrain20kTest" in f.name]

        if not fixed_configs:
            pytest.skip("No fixed-size split configs found")

        runner = DaskPipelineRunner.from_config(str(fixed_configs[0]))
        assert runner.config["ml"]["train_test_split"]["type"] == "fixed_size"
        assert runner.config["ml"]["train_test_split"]["train_size"] == 80000
        assert runner.config["ml"]["train_test_split"]["test_size"] == 20000


class TestConfigColumnSets:
    """Test different column set configurations."""

    def test_minimal_columns_config(self):
        """Test config with minimal_xy_elev columns."""
        config_files = get_all_config_files()
        minimal_configs = [f for f in config_files if "ONLYXYELEVCols" in f.name and "Heading" not in f.name]

        if not minimal_configs:
            pytest.skip("No minimal column configs found")

        runner = DaskPipelineRunner.from_config(str(minimal_configs[0]))
        assert runner.config["features"]["columns"] == "minimal_xy_elev"

    def test_extended_columns_config(self):
        """Test config with extended_with_timestamps columns."""
        config_files = get_all_config_files()
        ext_configs = [f for f in config_files if "EXTTimestampsCols" in f.name]

        if not ext_configs:
            pytest.skip("No extended column configs found")

        runner = DaskPipelineRunner.from_config(str(ext_configs[0]))
        assert runner.config["features"]["columns"] == "extended_with_timestamps"

    def test_heading_speed_columns_config(self):
        """Test config with heading/speed columns."""
        config_files = get_all_config_files()
        heading_configs = [f for f in config_files if "HeadingSpeedCols" in f.name]

        if not heading_configs:
            pytest.skip("No heading/speed column configs found")

        runner = DaskPipelineRunner.from_config(str(heading_configs[0]))
        assert runner.config["features"]["columns"] == "minimal_xy_elev_heading_speed"


class TestConfigValidation:
    """Test validation of config structure."""

    def test_all_configs_load_without_errors(self):
        """Test that all 55 configs can be loaded without errors."""
        config_files = get_all_config_files()
        failed_configs = []

        for config_file in config_files:
            try:
                runner = DaskPipelineRunner.from_config(str(config_file))
                assert runner is not None
            except Exception as e:
                failed_configs.append((config_file.name, str(e)))

        assert len(failed_configs) == 0, f"Failed to load configs: {failed_configs}"

    def test_all_configs_have_valid_providers(self):
        """Test that all configs setup providers correctly."""
        config_files = get_all_config_files()
        failed_configs = []

        for config_file in config_files:
            try:
                runner = DaskPipelineRunner.from_config(str(config_file))
                assert runner.generatorContextProvider is not None
                assert runner.MLContextProvider is not None
                assert runner._pathprovider is not None
            except Exception as e:
                failed_configs.append((config_file.name, str(e)))

        assert len(failed_configs) == 0, f"Provider setup failed for: {failed_configs}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

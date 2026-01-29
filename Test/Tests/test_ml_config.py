"""
Tests for ML pipeline configuration files.
"""

import pytest
import yaml
from pathlib import Path


class TestMLConfig32GB:
    """Tests for the 32GB ML configuration."""
    
    @pytest.fixture
    def config(self):
        """Load the 32GB ML config."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "ml" / "32gb-production.yml"
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def test_config_file_exists(self):
        """Verify the config file exists."""
        config_path = Path(__file__).parent.parent.parent / "configs" / "ml" / "32gb-production.yml"
        assert config_path.exists(), f"Config not found at {config_path}"
    
    def test_config_valid_yaml(self, config):
        """Verify config is valid YAML."""
        assert config is not None
        assert isinstance(config, dict)
    
    def test_has_required_sections(self, config):
        """Verify all required sections are present."""
        required_sections = [
            'preprocessing',
            'features', 
            'models',
            'training',
            'attack_detection',
            'output',
            'memory',
            'dask'
        ]
        for section in required_sections:
            assert section in config, f"Missing section: {section}"
    
    def test_preprocessing_scaler_valid(self, config):
        """Test preprocessing scaler configuration."""
        scaler = config['preprocessing']['scaler']
        assert scaler['type'] in ['StandardScaler', 'MinMaxScaler', 'RobustScaler']
        assert isinstance(scaler['with_mean'], bool)
        assert isinstance(scaler['with_std'], bool)
    
    def test_preprocessing_split_valid(self, config):
        """Test train/test split configuration."""
        split = config['preprocessing']['split']
        assert 0 < split['test_size'] < 1
        assert 0 < split['validation_size'] < 1
        assert split['test_size'] + split['validation_size'] < 1
    
    def test_features_core_not_empty(self, config):
        """Test core features are defined."""
        core = config['features']['core_features']
        assert len(core) > 0
        # Standard BSM features
        expected = ['latitude', 'longitude', 'speed']
        for feat in expected:
            assert feat in core, f"Missing core feature: {feat}"
    
    def test_models_at_least_one_enabled(self, config):
        """Test at least one model is enabled."""
        models = config['models']
        enabled = [name for name, cfg in models.items() if cfg.get('enabled', False)]
        assert len(enabled) > 0, "No models enabled"
    
    def test_random_forest_params_valid(self, config):
        """Test RandomForest parameters."""
        rf = config['models']['random_forest']
        assert rf['enabled'] == True
        params = rf['params']
        assert params['n_estimators'] > 0
        assert params['max_depth'] is None or params['max_depth'] > 0
        assert params['n_jobs'] == -1  # Use all cores
    
    def test_training_batch_size_appropriate(self, config):
        """Test batch size is appropriate for 32GB."""
        batch_size = config['training']['batch_size']
        # Should be reasonable for 32GB RAM
        assert 1000 <= batch_size <= 100000
    
    def test_cross_validation_config_valid(self, config):
        """Test cross-validation configuration."""
        cv = config['training']['cross_validation']
        assert cv['enabled'] == True
        assert cv['folds'] >= 2
        assert isinstance(cv['shuffle'], bool)
    
    def test_memory_config_for_32gb(self, config):
        """Test memory settings are appropriate for 32GB."""
        mem = config['memory']
        
        # Max concurrent models should be low for 32GB
        assert mem['max_concurrent_models'] <= 4
        
        # Chunk size should be manageable
        assert 10000 <= mem['chunk_size'] <= 200000
        
        # GC should be enabled
        assert mem['gc_frequency'] > 0
    
    def test_dask_config_for_32gb(self, config):
        """Test Dask settings are appropriate for 32GB."""
        dask = config['dask']
        assert dask['enabled'] == True
        
        cluster = dask['cluster']
        n_workers = cluster['n_workers']
        mem_limit = cluster['memory_limit']
        
        # Parse memory limit
        mem_gb = float(mem_limit.replace('GB', ''))
        
        # Total Dask memory should be <= 28GB (leaving room for OS)
        total_dask_mem = n_workers * mem_gb
        assert total_dask_mem <= 28, f"Dask memory {total_dask_mem}GB too high for 32GB system"
    
    def test_attack_detection_config_valid(self, config):
        """Test attack detection configuration."""
        attack = config['attack_detection']
        
        assert attack['task'] in ['classification', 'anomaly_detection', 'regression']
        assert 'target_column' in attack
        assert len(attack['attack_types']) > 0
    
    def test_class_balance_config_valid(self, config):
        """Test class balancing configuration."""
        balance = config['attack_detection']['class_balance']
        
        assert isinstance(balance['enabled'], bool)
        assert balance['method'] in ['smote', 'random_oversample', 'random_undersample', 'class_weight', 'none']
    
    def test_output_paths_defined(self, config):
        """Test output paths are defined."""
        output = config['output']
        
        assert 'model_dir' in output
        assert 'predictions_dir' in output
        assert 'reports_dir' in output
    
    def test_metrics_defined(self, config):
        """Test evaluation metrics are defined."""
        metrics = config['training']['metrics']
        
        # Should have classification metrics
        assert len(metrics['classification']) > 0
        assert 'accuracy' in metrics['classification']
        
        # Should have regression metrics
        assert len(metrics['regression']) > 0


class TestMLConfigLoader:
    """Tests for ML config loading utilities."""
    
    def test_load_all_ml_configs(self):
        """Test that all ML configs can be loaded."""
        configs_dir = Path(__file__).parent.parent.parent / "configs" / "ml"
        
        if not configs_dir.exists():
            pytest.skip("ML configs directory not found")
        
        for config_file in configs_dir.glob("*.yml"):
            with open(config_file) as f:
                config = yaml.safe_load(f)
            assert config is not None, f"Failed to load {config_file}"
    
    def test_configs_have_version(self):
        """Test that configs have version info."""
        configs_dir = Path(__file__).parent.parent.parent / "configs" / "ml"
        
        if not configs_dir.exists():
            pytest.skip("ML configs directory not found")
        
        for config_file in configs_dir.glob("*.yml"):
            if config_file.name.startswith("README"):
                continue
            with open(config_file) as f:
                config = yaml.safe_load(f)
            
            # Version should be present
            assert 'version' in config or 'preprocessing' in config, \
                f"Config {config_file} missing version or preprocessing section"

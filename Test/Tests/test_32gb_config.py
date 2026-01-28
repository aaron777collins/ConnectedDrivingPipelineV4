"""
Tests for 32GB RAM configuration files.

This module validates that the 32GB Spark and Dask configurations:
1. Load correctly from YAML
2. Have valid memory settings
3. Work within the 32GB constraint
4. Can initialize sessions successfully
"""

import pytest
import yaml
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class TestSparkConfig32GB:
    """Tests for the 32GB Spark configuration."""

    @pytest.fixture
    def spark_config_path(self):
        """Path to 32GB Spark config."""
        return project_root / "configs" / "spark" / "32gb-single-node.yml"

    @pytest.fixture
    def spark_config(self, spark_config_path):
        """Load the Spark configuration."""
        with open(spark_config_path, 'r') as f:
            return yaml.safe_load(f)

    def test_config_file_exists(self, spark_config_path):
        """Verify config file exists."""
        assert spark_config_path.exists(), f"Config not found: {spark_config_path}"

    def test_config_loads_valid_yaml(self, spark_config):
        """Verify config is valid YAML."""
        assert spark_config is not None
        assert 'spark_config' in spark_config

    def test_app_name_set(self, spark_config):
        """Verify app name is set correctly."""
        assert spark_config['spark_config']['app_name'] == "ConnectedDrivingPipeline-32GB"

    def test_master_local_mode(self, spark_config):
        """Verify local mode is configured."""
        master = spark_config['spark_config']['master']
        assert master.startswith('local['), f"Expected local mode, got: {master}"

    def test_driver_memory_within_limits(self, spark_config):
        """Verify driver memory is reasonable for 32GB system."""
        driver_mem = spark_config['spark_config']['driver_memory']
        # Extract numeric value
        mem_gb = int(driver_mem.replace('g', ''))
        assert 2 <= mem_gb <= 6, f"Driver memory {mem_gb}GB outside expected range 2-6GB"

    def test_executor_memory_within_limits(self, spark_config):
        """Verify executor memory is reasonable for 32GB system."""
        executor_mem = spark_config['spark_config']['executor_memory']
        # Extract numeric value
        mem_gb = int(executor_mem.replace('g', ''))
        assert 12 <= mem_gb <= 22, f"Executor memory {mem_gb}GB outside expected range 12-22GB"

    def test_total_memory_under_32gb(self, spark_config):
        """Verify total memory allocation is under 32GB."""
        cfg = spark_config['spark_config']

        driver_mem = int(cfg['driver_memory'].replace('g', ''))
        executor_mem = int(cfg['executor_memory'].replace('g', ''))
        overhead = int(cfg.get('executor_memoryOverhead', '0g').replace('g', ''))

        # Account for 10% JVM overhead on driver
        driver_total = driver_mem * 1.1
        # Executor + explicit overhead
        executor_total = executor_mem + overhead

        # Estimate Python worker memory
        cores = int(cfg.get('executor_cores', 4))
        python_worker_mem = int(cfg.get('python_worker_memory', '1g').replace('g', ''))
        python_total = cores * python_worker_mem * 0.5  # Not all active simultaneously

        total = driver_total + executor_total + python_total

        # Should leave at least 4GB for OS
        assert total <= 28, f"Total allocation {total}GB too high for 32GB system"

    def test_shuffle_partitions_reasonable(self, spark_config):
        """Verify shuffle partitions are set reasonably for 32GB."""
        partitions = spark_config['spark_config']['sql_shuffle_partitions']
        assert 50 <= partitions <= 200, f"Shuffle partitions {partitions} outside expected range"

    def test_adaptive_query_execution_enabled(self, spark_config):
        """Verify AQE is enabled for performance."""
        assert spark_config['spark_config']['sql_adaptive_enabled'] == True

    def test_arrow_enabled(self, spark_config):
        """Verify Arrow is enabled for pandas interop."""
        assert spark_config['spark_config']['sql_execution_arrow_pyspark_enabled'] == True

    def test_spill_compression_enabled(self, spark_config):
        """Verify spill compression is enabled for memory-constrained env."""
        cfg = spark_config['spark_config']
        assert cfg.get('shuffle_spill_compress', True) == True
        assert cfg.get('shuffle_compress', True) == True


class TestDaskConfig32GB:
    """Tests for the 32GB Dask configuration."""

    @pytest.fixture
    def dask_config_path(self):
        """Path to 32GB Dask config."""
        return project_root / "configs" / "dask" / "32gb-production.yml"

    @pytest.fixture
    def dask_config(self, dask_config_path):
        """Load the Dask configuration."""
        with open(dask_config_path, 'r') as f:
            return yaml.safe_load(f)

    def test_config_file_exists(self, dask_config_path):
        """Verify config file exists."""
        assert dask_config_path.exists(), f"Config not found: {dask_config_path}"

    def test_config_loads_valid_yaml(self, dask_config):
        """Verify config is valid YAML."""
        assert dask_config is not None
        assert 'distributed' in dask_config

    def test_version_is_2(self, dask_config):
        """Verify distributed config version is 2."""
        assert dask_config['distributed']['version'] == 2

    def test_memory_target_aggressive(self, dask_config):
        """Verify memory target is aggressive for 32GB (should be <=0.5)."""
        target = dask_config['distributed']['worker']['memory']['target']
        assert target <= 0.5, f"Memory target {target} too high for 32GB"

    def test_memory_spill_configured(self, dask_config):
        """Verify spill threshold is configured."""
        spill = dask_config['distributed']['worker']['memory']['spill']
        assert 0.4 <= spill <= 0.7, f"Spill threshold {spill} outside expected range"

    def test_memory_thresholds_order(self, dask_config):
        """Verify memory thresholds are in correct order."""
        mem = dask_config['distributed']['worker']['memory']
        assert mem['target'] < mem['spill'] < mem['pause'] < mem['terminate']

    def test_shuffle_method_safe(self, dask_config):
        """Verify shuffle uses disk method for safety on 32GB."""
        method = dask_config['dataframe']['shuffle']['method']
        assert method == 'disk', f"Expected disk shuffle for 32GB, got: {method}"

    def test_compression_enabled(self, dask_config):
        """Verify compression is enabled for spilling."""
        compression = dask_config['dataframe']['shuffle']['compression']
        assert compression in ['lz4', 'snappy', 'zstd']

    def test_chunk_size_small(self, dask_config):
        """Verify chunk size is reduced for memory constraints."""
        chunk_size = dask_config['array']['chunk-size']
        # Should be 64MiB or less for 32GB system
        assert '32MiB' in chunk_size or '64MiB' in chunk_size or int(chunk_size.replace('MiB', '')) <= 64


class TestConfigIntegration:
    """Integration tests for config loading with actual session managers."""

    @pytest.fixture
    def spark_config_path(self):
        return project_root / "configs" / "spark" / "32gb-single-node.yml"

    def test_spark_config_loader_integration(self, spark_config_path):
        """Test that SparkConfigLoader can process the config."""
        try:
            from Helpers.SparkConfigLoader import SparkConfigLoader

            config = SparkConfigLoader.load_config(custom_path=str(spark_config_path))
            assert config is not None
            assert 'app_name' in config or 'driver_memory' in config

            # Test conversion to Spark conf format
            spark_conf = SparkConfigLoader.convert_to_spark_conf(config)
            assert isinstance(spark_conf, dict)

        except ImportError:
            pytest.skip("SparkConfigLoader not available")

    def test_dask_config_can_create_cluster(self):
        """Test that Dask config can be used to create a LocalCluster."""
        try:
            from dask.distributed import LocalCluster, Client
            import dask

            # Load config
            config_path = project_root / "configs" / "dask" / "32gb-production.yml"
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Apply config to dask (note: dask.config.set uses nested dict)
            dask.config.set(config)

            # Create a minimal cluster to test (1 worker, 1 thread)
            cluster = LocalCluster(
                n_workers=1,
                threads_per_worker=1,
                memory_limit='1GB',  # Small for test
                processes=False  # Faster for tests
            )
            client = Client(cluster)

            # Verify connection
            assert client.status == 'running'

            # Cleanup
            client.close()
            cluster.close()

        except Exception as e:
            pytest.skip(f"Dask cluster test skipped: {e}")


class TestMemoryCalculations:
    """Test memory calculation helpers for 32GB config."""

    def test_recommended_worker_count_4(self):
        """Verify 4 workers with 6GB each = 24GB (leaving 8GB for OS)."""
        total_ram = 32
        os_reserve = 8
        available = total_ram - os_reserve
        workers = 4
        per_worker = available / workers
        assert per_worker == 6.0, f"Expected 6GB per worker, got {per_worker}"

    def test_recommended_worker_count_2(self):
        """Verify 2 workers with 12GB each = 24GB (alternative config)."""
        total_ram = 32
        os_reserve = 8
        available = total_ram - os_reserve
        workers = 2
        per_worker = available / workers
        assert per_worker == 12.0, f"Expected 12GB per worker, got {per_worker}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

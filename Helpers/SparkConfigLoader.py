"""
Spark Configuration Loader

Utility for loading Spark configuration from YAML files and applying them to SparkSession.
Supports local, cluster, and large-dataset configurations.

Usage:
    from Helpers.SparkConfigLoader import SparkConfigLoader
    from Helpers.SparkSessionManager import SparkSessionManager

    # Load configuration
    config = SparkConfigLoader.load_config('local')

    # Create SparkSession with loaded config
    spark = SparkSessionManager.get_session(
        app_name=config.get('app_name', 'ConnectedDrivingPipeline'),
        config=config
    )

Author: ConnectedDrivingPipelineV4 Migration Team
Date: 2026-01-17
"""

import os
import yaml
from typing import Dict, Any, Optional


class SparkConfigLoader:
    """Loads and processes Spark configuration from YAML files."""

    # Default configuration directory
    CONFIG_DIR = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'configs',
        'spark'
    )

    # Available configuration presets
    PRESETS = {
        'local': 'local.yml',
        'cluster': 'cluster.yml',
        'large-dataset': 'large-dataset.yml',
        'large': 'large-dataset.yml',  # Alias
    }

    @classmethod
    def load_config(cls, preset: str = 'local', custom_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load Spark configuration from YAML file.

        Args:
            preset: Configuration preset name ('local', 'cluster', 'large-dataset')
            custom_path: Optional custom path to YAML configuration file

        Returns:
            Dictionary of Spark configuration parameters

        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ValueError: If preset name is invalid
        """
        if custom_path:
            config_path = custom_path
        else:
            if preset not in cls.PRESETS:
                raise ValueError(
                    f"Invalid preset '{preset}'. "
                    f"Available presets: {', '.join(cls.PRESETS.keys())}"
                )
            config_path = os.path.join(cls.CONFIG_DIR, cls.PRESETS[preset])

        if not os.path.exists(config_path):
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )

        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        # Extract spark_config section
        if 'spark_config' in config_data:
            return config_data['spark_config']

        return config_data

    @classmethod
    def convert_to_spark_conf(cls, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Convert configuration dict to Spark conf format.

        Converts short-form keys (e.g., 'driver_memory') to Spark conf format
        (e.g., 'spark.driver.memory').

        Args:
            config: Configuration dictionary

        Returns:
            Dictionary with Spark conf keys
        """
        spark_conf = {}

        # Mapping of short keys to Spark conf keys
        key_mappings = {
            'app_name': None,  # Not a conf key, used separately
            'master': 'spark.master',
            'driver_memory': 'spark.driver.memory',
            'executor_memory': 'spark.executor.memory',
            'executor_cores': 'spark.executor.cores',
            'cores_max': 'spark.cores.max',
            'sql_shuffle_partitions': 'spark.sql.shuffle.partitions',
            'default_parallelism': 'spark.default.parallelism',
            'sql_adaptive_enabled': 'spark.sql.adaptive.enabled',
            'sql_adaptive_coalesce_partitions_enabled': 'spark.sql.adaptive.coalescePartitions.enabled',
            'sql_adaptive_skewJoin_enabled': 'spark.sql.adaptive.skewJoin.enabled',
            'sql_adaptive_skewJoin_skewedPartitionFactor': 'spark.sql.adaptive.skewJoin.skewedPartitionFactor',
            'sql_adaptive_skewJoin_skewedPartitionThresholdInBytes': 'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes',
            'sql_adaptive_advisoryPartitionSizeInBytes': 'spark.sql.adaptive.advisoryPartitionSizeInBytes',
            'sql_execution_arrow_pyspark_enabled': 'spark.sql.execution.arrow.pyspark.enabled',
            'sql_execution_arrow_pyspark_fallback_enabled': 'spark.sql.execution.arrow.pyspark.fallback.enabled',
            'sql_execution_arrow_maxRecordsPerBatch': 'spark.sql.execution.arrow.maxRecordsPerBatch',
            'sql_parquet_compression_codec': 'spark.sql.parquet.compression.codec',
            'io_compression_codec': 'spark.io.compression.codec',
            'memory_fraction': 'spark.memory.fraction',
            'memory_storage_fraction': 'spark.memory.storageFraction',
            'memory_offHeap_enabled': 'spark.memory.offHeap.enabled',
            'memory_offHeap_size': 'spark.memory.offHeap.size',
            'dynamicAllocation_enabled': 'spark.dynamicAllocation.enabled',
            'dynamicAllocation_minExecutors': 'spark.dynamicAllocation.minExecutors',
            'dynamicAllocation_maxExecutors': 'spark.dynamicAllocation.maxExecutors',
            'dynamicAllocation_initialExecutors': 'spark.dynamicAllocation.initialExecutors',
            'dynamicAllocation_executorIdleTimeout': 'spark.dynamicAllocation.executorIdleTimeout',
            'shuffle_service_enabled': 'spark.shuffle.service.enabled',
            'shuffle_service_port': 'spark.shuffle.service.port',
            'shuffle_file_buffer': 'spark.shuffle.file.buffer',
            'shuffle_unsafe_file_output_buffer': 'spark.shuffle.unsafe.file.output.buffer',
            'network_timeout': 'spark.network.timeout',
            'executor_heartbeatInterval': 'spark.executor.heartbeatInterval',
            'sql_autoBroadcastJoinThreshold': 'spark.sql.autoBroadcastJoinThreshold',
            'speculation': 'spark.speculation',
            'speculation_multiplier': 'spark.speculation.multiplier',
            'speculation_quantile': 'spark.speculation.quantile',
            'ui_port': 'spark.ui.port',
            'ui_retainedJobs': 'spark.ui.retainedJobs',
            'ui_retainedStages': 'spark.ui.retainedStages',
            'ui_showConsoleProgress': 'spark.ui.showConsoleProgress',
            'eventLog_enabled': 'spark.eventLog.enabled',
            'eventLog_dir': 'spark.eventLog.dir',
            'log_level': None,  # Handled separately via spark.sparkContext.setLogLevel()
            'serializer': 'spark.serializer',
            'kryoserializer_buffer_max': 'spark.kryoserializer.buffer.max',
            'locality_wait': 'spark.locality.wait',
            'python_worker_memory': 'spark.python.worker.memory',
            'python_worker_reuse': 'spark.python.worker.reuse',
            'sql_files_maxPartitionBytes': 'spark.sql.files.maxPartitionBytes',
            'sql_files_maxRecordsPerFile': 'spark.sql.files.maxRecordsPerFile',
            'storage_memoryMapThreshold': 'spark.storage.memoryMapThreshold',
            'sql_optimizer_maxIterations': 'spark.sql.optimizer.maxIterations',
            'local_dir': 'spark.local.dir',
        }

        for key, value in config.items():
            # Skip comments and None values
            if key == 'comments' or value is None:
                continue

            # Get the Spark conf key
            spark_key = key_mappings.get(key)

            if spark_key is None:
                # Skip keys that don't map to Spark conf (like app_name, log_level)
                continue

            # Convert boolean to string
            if isinstance(value, bool):
                value = str(value).lower()
            else:
                value = str(value)

            spark_conf[spark_key] = value

        return spark_conf

    @classmethod
    def load_and_convert(cls, preset: str = 'local', custom_path: Optional[str] = None) -> Dict[str, str]:
        """
        Load configuration and convert to Spark conf format.

        Convenience method that combines load_config() and convert_to_spark_conf().

        Args:
            preset: Configuration preset name
            custom_path: Optional custom path to YAML file

        Returns:
            Dictionary with Spark conf keys ready for SparkSession.Builder.config()
        """
        config = cls.load_config(preset, custom_path)
        return cls.convert_to_spark_conf(config)

    @classmethod
    def get_app_name(cls, preset: str = 'local', custom_path: Optional[str] = None) -> str:
        """
        Get application name from configuration.

        Args:
            preset: Configuration preset name
            custom_path: Optional custom path to YAML file

        Returns:
            Application name string
        """
        config = cls.load_config(preset, custom_path)
        return config.get('app_name', 'ConnectedDrivingPipeline')

    @classmethod
    def get_log_level(cls, preset: str = 'local', custom_path: Optional[str] = None) -> str:
        """
        Get log level from configuration.

        Args:
            preset: Configuration preset name
            custom_path: Optional custom path to YAML file

        Returns:
            Log level string (e.g., 'WARN', 'INFO')
        """
        config = cls.load_config(preset, custom_path)
        return config.get('log_level', 'WARN')

    @classmethod
    def merge_configs(cls, base_config: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge configuration dictionaries.

        Args:
            base_config: Base configuration
            overrides: Configuration overrides

        Returns:
            Merged configuration dictionary
        """
        merged = base_config.copy()
        merged.update(overrides)
        return merged


# Example usage
if __name__ == "__main__":
    # Test loading configurations
    print("Testing SparkConfigLoader...")
    print()

    for preset in ['local', 'cluster', 'large-dataset']:
        print(f"Loading {preset} configuration...")
        config = SparkConfigLoader.load_config(preset)
        print(f"  App Name: {config.get('app_name')}")
        print(f"  Master: {config.get('master')}")
        print(f"  Driver Memory: {config.get('driver_memory')}")
        print(f"  Executor Memory: {config.get('executor_memory')}")
        print(f"  Shuffle Partitions: {config.get('sql_shuffle_partitions')}")
        print()

    print("Converting to Spark conf format...")
    spark_conf = SparkConfigLoader.load_and_convert('local')
    print(f"  Total conf keys: {len(spark_conf)}")
    print(f"  Sample keys: {list(spark_conf.keys())[:5]}")
    print()

    print("Testing configuration merge...")
    base = SparkConfigLoader.load_config('local')
    overrides = {'executor_memory': '8g', 'sql_shuffle_partitions': 16}
    merged = SparkConfigLoader.merge_configs(base, overrides)
    print(f"  Base executor memory: {base['executor_memory']}")
    print(f"  Merged executor memory: {merged['executor_memory']}")
    print(f"  Merged shuffle partitions: {merged['sql_shuffle_partitions']}")
    print()

    print("All tests passed!")

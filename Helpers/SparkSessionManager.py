from pyspark.sql import SparkSession
import os
from typing import Optional, Dict, Any


class SparkSessionManager:
    """
    Singleton manager for PySpark SparkSession.
    Handles creation, configuration, and lifecycle management of Spark sessions.
    """
    _instance = None
    _spark_session = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkSessionManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_session(cls, app_name="ConnectedDrivingPipeline", config=None):
        """
        Get or create a SparkSession with specified configuration.

        Args:
            app_name (str): Name of the Spark application
            config (dict): Optional dictionary of Spark configuration key-value pairs

        Returns:
            SparkSession: Configured Spark session instance
        """
        if cls._spark_session is None:
            cls._spark_session = cls._create_session(app_name, config)
        return cls._spark_session

    @classmethod
    def _create_session(cls, app_name, config=None):
        """
        Create a new SparkSession with default or custom configuration.

        Args:
            app_name (str): Name of the Spark application
            config (dict): Optional configuration overrides

        Returns:
            SparkSession: New Spark session instance
        """
        builder = SparkSession.builder.appName(app_name)

        # Default configuration optimized for data processing
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.execution.arrow.pyspark.fallback.enabled": "true",
        }

        # Merge default config with user-provided config
        final_config = {**default_config, **(config or {})}

        # Apply all configurations
        for key, value in final_config.items():
            builder = builder.config(key, value)

        # Create and return the session
        spark = builder.getOrCreate()

        # Set log level to reduce noise (can be overridden)
        spark.sparkContext.setLogLevel("WARN")

        return spark

    @classmethod
    def stop_session(cls):
        """
        Stop the current SparkSession and clean up resources.
        """
        if cls._spark_session is not None:
            cls._spark_session.stop()
            cls._spark_session = None

    @classmethod
    def get_local_session(cls, app_name="ConnectedDrivingPipeline", cores="*"):
        """
        Get a SparkSession configured for local mode testing.

        Args:
            app_name (str): Name of the Spark application
            cores (str): Number of cores to use ("*" for all, or specific number)

        Returns:
            SparkSession: Spark session configured for local mode
        """
        local_config = {
            "spark.master": f"local[{cores}]",
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "8",
        }
        return cls.get_session(app_name, local_config)

    @classmethod
    def get_cluster_session(cls, app_name="ConnectedDrivingPipeline",
                           driver_memory="16g", executor_memory="32g",
                           executor_cores=4, num_executors=None):
        """
        Get a SparkSession configured for cluster mode.

        Args:
            app_name (str): Name of the Spark application
            driver_memory (str): Driver memory allocation (e.g., "16g")
            executor_memory (str): Executor memory allocation (e.g., "32g")
            executor_cores (int): Number of cores per executor
            num_executors (int): Number of executors (None for dynamic allocation)

        Returns:
            SparkSession: Spark session configured for cluster mode
        """
        cluster_config = {
            "spark.driver.memory": driver_memory,
            "spark.executor.memory": executor_memory,
            "spark.executor.cores": str(executor_cores),
            "spark.sql.shuffle.partitions": "200",
        }

        # Enable dynamic allocation if num_executors not specified
        if num_executors is None:
            cluster_config["spark.dynamicAllocation.enabled"] = "true"
            cluster_config["spark.dynamicAllocation.minExecutors"] = "1"
            cluster_config["spark.dynamicAllocation.maxExecutors"] = "100"
        else:
            cluster_config["spark.executor.instances"] = str(num_executors)

        return cls.get_session(app_name, cluster_config)

    @classmethod
    def get_session_from_config_file(cls, preset: str = 'local',
                                     custom_path: Optional[str] = None,
                                     overrides: Optional[Dict[str, Any]] = None):
        """
        Get a SparkSession configured from a YAML configuration file.

        This method integrates with SparkConfigLoader to load configuration
        from YAML templates in configs/spark/ directory.

        Args:
            preset (str): Configuration preset name ('local', 'cluster', 'large-dataset')
            custom_path (str): Optional custom path to YAML configuration file
            overrides (dict): Optional configuration overrides to apply

        Returns:
            SparkSession: Spark session configured from YAML file

        Example:
            # Load local configuration
            spark = SparkSessionManager.get_session_from_config_file('local')

            # Load cluster configuration with overrides
            spark = SparkSessionManager.get_session_from_config_file(
                'cluster',
                overrides={'executor_memory': '64g'}
            )

            # Load custom configuration file
            spark = SparkSessionManager.get_session_from_config_file(
                custom_path='/path/to/custom-config.yml'
            )
        """
        try:
            from Helpers.SparkConfigLoader import SparkConfigLoader
        except ImportError:
            raise ImportError(
                "SparkConfigLoader not found. "
                "Ensure SparkConfigLoader.py is in the Helpers directory."
            )

        # Load configuration from YAML
        config = SparkConfigLoader.load_config(preset, custom_path)

        # Apply overrides if provided
        if overrides:
            config = SparkConfigLoader.merge_configs(config, overrides)

        # Convert to Spark conf format
        spark_conf = SparkConfigLoader.convert_to_spark_conf(config)

        # Get app name and log level from config
        app_name = config.get('app_name', 'ConnectedDrivingPipeline')
        log_level = config.get('log_level', 'WARN')

        # Create session
        spark = cls.get_session(app_name, spark_conf)

        # Set log level
        spark.sparkContext.setLogLevel(log_level)

        return spark

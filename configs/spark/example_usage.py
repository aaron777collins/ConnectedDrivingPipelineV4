"""
Example Usage of Spark Configuration Templates

This script demonstrates how to use the Spark configuration templates
with SparkSessionManager for the ConnectedDrivingPipelineV4 migration.

Run this script to see examples of different configuration approaches.

Usage:
    python configs/spark/example_usage.py
"""

import sys
import os

# Add parent directory to path to import from Helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from Helpers.SparkSessionManager import SparkSessionManager
from Helpers.SparkConfigLoader import SparkConfigLoader


def example_1_local_development():
    """Example 1: Local development using built-in presets."""
    print("=" * 60)
    print("Example 1: Local Development (Built-in Preset)")
    print("=" * 60)

    # Create a local SparkSession using built-in preset
    spark = SparkSessionManager.get_local_session(cores="4")

    print(f"Spark Version: {spark.version}")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")

    # Test basic operation
    df = spark.createDataFrame([(1, "test"), (2, "example")], ["id", "value"])
    print(f"Created test DataFrame with {df.count()} rows")

    SparkSessionManager.stop_session()
    print("Session stopped.\n")


def example_2_yaml_local():
    """Example 2: Local development using YAML configuration."""
    print("=" * 60)
    print("Example 2: Local Development (YAML Configuration)")
    print("=" * 60)

    # Create a local SparkSession from YAML config
    spark = SparkSessionManager.get_session_from_config_file('local')

    print(f"Spark Version: {spark.version}")
    print(f"App Name: {spark.sparkContext.appName}")
    print(f"Master: {spark.sparkContext.master}")

    # Show some configuration values
    conf = spark.sparkContext.getConf()
    print(f"Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions')}")
    print(f"Driver Memory: {conf.get('spark.driver.memory')}")
    print(f"Adaptive Enabled: {conf.get('spark.sql.adaptive.enabled')}")

    SparkSessionManager.stop_session()
    print("Session stopped.\n")


def example_3_cluster_with_overrides():
    """Example 3: Cluster configuration with custom overrides."""
    print("=" * 60)
    print("Example 3: Cluster Configuration with Overrides")
    print("=" * 60)

    # Load cluster config and override some settings
    spark = SparkSessionManager.get_session_from_config_file(
        'cluster',
        overrides={
            'executor_memory': '64g',  # Increase memory
            'sql_shuffle_partitions': 300,  # More partitions
        }
    )

    print(f"App Name: {spark.sparkContext.appName}")

    # Show configuration values
    conf = spark.sparkContext.getConf()
    print(f"Executor Memory: {conf.get('spark.executor.memory')}")
    print(f"Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions')}")
    print(f"Dynamic Allocation: {conf.get('spark.dynamicAllocation.enabled')}")

    SparkSessionManager.stop_session()
    print("Session stopped.\n")


def example_4_manual_yaml_loading():
    """Example 4: Manual YAML loading and customization."""
    print("=" * 60)
    print("Example 4: Manual YAML Loading and Customization")
    print("=" * 60)

    # Load configuration manually
    config = SparkConfigLoader.load_config('large-dataset')

    print(f"Loaded configuration: {config['app_name']}")
    print(f"Original executor memory: {config['executor_memory']}")

    # Customize configuration
    config['executor_memory'] = '128g'
    config['sql_shuffle_partitions'] = 800

    print(f"Modified executor memory: {config['executor_memory']}")
    print(f"Modified shuffle partitions: {config['sql_shuffle_partitions']}")

    # Convert to Spark conf format
    spark_conf = SparkConfigLoader.convert_to_spark_conf(config)

    print(f"Spark conf keys: {len(spark_conf)}")

    # Create session with custom config
    spark = SparkSessionManager.get_session(
        app_name=config['app_name'],
        config=spark_conf
    )

    print(f"Session created successfully")

    SparkSessionManager.stop_session()
    print("Session stopped.\n")


def example_5_merge_configurations():
    """Example 5: Merging multiple configurations."""
    print("=" * 60)
    print("Example 5: Merging Configurations")
    print("=" * 60)

    # Load base configuration
    base_config = SparkConfigLoader.load_config('cluster')

    # Define custom overrides for specific use case
    attack_simulation_overrides = {
        'python_worker_memory': '4g',  # More memory for Python UDFs
        'executor_cores': 5,  # More cores for parallelism
        'sql_shuffle_partitions': 400,  # Higher for large shuffles
    }

    # Merge configurations
    merged_config = SparkConfigLoader.merge_configs(
        base_config,
        attack_simulation_overrides
    )

    print(f"Base python_worker_memory: {base_config.get('python_worker_memory', 'not set')}")
    print(f"Merged python_worker_memory: {merged_config['python_worker_memory']}")
    print(f"Merged executor_cores: {merged_config['executor_cores']}")
    print(f"Merged shuffle_partitions: {merged_config['sql_shuffle_partitions']}")

    # Create session
    spark_conf = SparkConfigLoader.convert_to_spark_conf(merged_config)
    spark = SparkSessionManager.get_session(
        app_name="ConnectedDrivingPipeline-AttackSim",
        config=spark_conf
    )

    print(f"Session created for attack simulation")

    SparkSessionManager.stop_session()
    print("Session stopped.\n")


def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("Spark Configuration Templates - Usage Examples")
    print("=" * 60 + "\n")

    examples = [
        example_1_local_development,
        example_2_yaml_local,
        example_3_cluster_with_overrides,
        example_4_manual_yaml_loading,
        example_5_merge_configurations,
    ]

    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"Error in {example.__name__}: {e}\n")

    print("=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()

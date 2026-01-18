"""
DaskSessionManager - Centralized Dask cluster and client management.

This module provides singleton access to a configured Dask distributed cluster
and client for the ConnectedDrivingPipelineV4 project.

Key Features:
- Singleton pattern for cluster management (one cluster per application)
- Automatic cluster configuration from YAML files
- Support for production and development environments
- Memory-safe defaults for 64GB systems
- Dashboard integration for monitoring

Usage:
    from Helpers.DaskSessionManager import DaskSessionManager

    # Get or create Dask client (singleton)
    client = DaskSessionManager.get_client()

    # Access cluster dashboard
    print(f"Dashboard: {client.dashboard_link}")

    # Get cluster instance
    cluster = DaskSessionManager.get_cluster()

    # Shutdown cluster (typically at end of application)
    DaskSessionManager.shutdown()
"""

import os
import logging
from typing import Optional
from dask.distributed import Client, LocalCluster
import dask
import dask.config


class DaskSessionManager:
    """
    Singleton manager for Dask distributed cluster and client.

    Provides centralized access to a configured Dask cluster with memory-safe
    defaults for 64GB systems. Supports both production and development configurations.
    """

    _cluster: Optional[LocalCluster] = None
    _client: Optional[Client] = None
    _logger = logging.getLogger("DaskSessionManager")

    @classmethod
    def get_cluster(cls,
                   n_workers: int = 5,
                   threads_per_worker: int = 1,
                   memory_limit: str = '8GB',
                   config_path: Optional[str] = None,
                   env: str = 'production') -> LocalCluster:
        """
        Get or create Dask LocalCluster (singleton).

        Args:
            n_workers: Number of worker processes (default: 5 for 64GB system, leaves 24GB for OS/kernel)
            threads_per_worker: Threads per worker (default: 1 to avoid GIL contention)
            memory_limit: Memory limit per worker (default: 8GB = 5 workers × 8GB = 40GB total)
            config_path: Path to custom Dask config YAML file (optional)
            env: Environment ('production' or 'development')

        Returns:
            LocalCluster: The singleton Dask cluster instance
        """
        if cls._cluster is None:
            cls._logger.info(f"Creating new Dask cluster ({env} mode)...")

            # Load configuration from YAML if specified
            if config_path and os.path.exists(config_path):
                cls._logger.info(f"Loading Dask config from: {config_path}")
                dask.config.set(config=dask.config.config)
                with dask.config.set(config=dask.config.config):
                    dask.config.refresh(paths=[config_path])
            else:
                # Use default config path based on environment
                default_config = f"/tmp/original-repo/configs/dask/{'64gb-production' if env == 'production' else 'development'}.yml"
                if os.path.exists(default_config):
                    cls._logger.info(f"Loading default Dask config: {default_config}")
                    with open(default_config, 'r') as f:
                        import yaml
                        config_dict = yaml.safe_load(f)
                        dask.config.update(dask.config.config, config_dict)

            # Create LocalCluster with specified configuration
            cls._cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                processes=True,  # Use processes (not threads) for better memory isolation
                memory_limit=memory_limit,

                # Scheduler options
                scheduler_port=8786,
                dashboard_address=':8787',

                # Silence excessive logs
                silence_logs=logging.ERROR
            )

            cls._logger.info(f"Dask cluster created with {n_workers} workers × {memory_limit} memory")
            cls._logger.info(f"Total worker memory: {n_workers * int(memory_limit[:-2])}GB")

        return cls._cluster

    @classmethod
    def get_client(cls, **cluster_kwargs) -> Client:
        """
        Get or create Dask Client connected to the singleton cluster.

        Args:
            **cluster_kwargs: Arguments passed to get_cluster() if cluster doesn't exist

        Returns:
            Client: The Dask client instance
        """
        if cls._client is None:
            # Create cluster if it doesn't exist
            cluster = cls.get_cluster(**cluster_kwargs)

            # Create client connected to cluster
            cls._client = Client(cluster)

            cls._logger.info(f"Dask client created")
            cls._logger.info(f"Dashboard available at: {cls._client.dashboard_link}")

            # Log memory configuration
            worker_info = cls._client.scheduler_info()['workers']
            for worker_addr, info in worker_info.items():
                memory_limit_gb = info['memory_limit'] / 1e9
                cls._logger.info(f"Worker {worker_addr}: {memory_limit_gb:.1f}GB memory limit")

        return cls._client

    @classmethod
    def shutdown(cls):
        """
        Shutdown the Dask cluster and client.

        Call this at the end of your application to clean up resources.
        """
        if cls._client is not None:
            cls._logger.info("Shutting down Dask client...")
            cls._client.close()
            cls._client = None

        if cls._cluster is not None:
            cls._logger.info("Shutting down Dask cluster...")
            cls._cluster.close()
            cls._cluster = None

    @classmethod
    def restart(cls):
        """
        Restart the Dask cluster (useful for clearing memory).
        """
        if cls._client is not None:
            cls._logger.info("Restarting Dask cluster...")
            cls._client.restart()

    @classmethod
    def get_dashboard_link(cls) -> Optional[str]:
        """
        Get the URL for the Dask dashboard.

        Returns:
            str: Dashboard URL or None if client not initialized
        """
        if cls._client is not None:
            return cls._client.dashboard_link
        return None

    @classmethod
    def get_worker_info(cls) -> dict:
        """
        Get information about all workers in the cluster.

        Returns:
            dict: Worker information including memory usage and status
        """
        if cls._client is not None:
            return cls._client.scheduler_info()['workers']
        return {}

    @classmethod
    def get_memory_usage(cls) -> dict:
        """
        Get current memory usage across all workers.

        Returns:
            dict: Memory usage information (used, limit, percentage)
        """
        memory_info = {}

        if cls._client is not None:
            worker_info = cls._client.scheduler_info()['workers']

            total_used = 0
            total_limit = 0

            for worker_addr, info in worker_info.items():
                memory_used = info['metrics']['memory']
                memory_limit = info['memory_limit']

                total_used += memory_used
                total_limit += memory_limit

                memory_info[worker_addr] = {
                    'used_gb': memory_used / 1e9,
                    'limit_gb': memory_limit / 1e9,
                    'percent': (memory_used / memory_limit) * 100
                }

            memory_info['total'] = {
                'used_gb': total_used / 1e9,
                'limit_gb': total_limit / 1e9,
                'percent': (total_used / total_limit) * 100 if total_limit > 0 else 0
            }

        return memory_info


# Convenience function for getting client
def get_dask_client(**kwargs) -> Client:
    """
    Convenience function to get Dask client.

    Args:
        **kwargs: Arguments passed to get_cluster()

    Returns:
        Client: Dask client instance
    """
    return DaskSessionManager.get_client(**kwargs)

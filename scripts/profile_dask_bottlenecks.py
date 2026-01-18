#!/usr/bin/env python3
"""
Dask Dashboard Profiling Script for Bottleneck Identification
Task 47: Identify bottlenecks with Dask dashboard profiling

This script profiles the Dask pipeline components using the Dask dashboard
and scheduler metrics to identify performance bottlenecks.

Usage:
    python scripts/profile_dask_bottlenecks.py

Features:
- Profiles all core pipeline operations (gather, clean, attack, ML)
- Monitors task execution times, worker utilization, and memory usage
- Collects scheduler metrics and task graph statistics
- Identifies slow operations and bottlenecks
- Generates detailed profiling report
"""

import sys
import os
import time
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass, asdict
import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from Helpers.DaskSessionManager import DaskSessionManager
from Gatherer.DaskDataGatherer import DaskDataGatherer
from Decorators.DaskParquetCache import DaskParquetCache
from Generator.Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner
from Generator.Cleaners.DaskCleanWithTimestamps import DaskCleanWithTimestamps
from Generator.Attackers.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker
from MachineLearning.DaskMConnectedDrivingDataCleaner import DaskMConnectedDrivingDataCleaner
from MachineLearning.DaskMClassifierPipeline import DaskMClassifierPipeline
from ServiceProviders.PathProvider import PathProvider
from ServiceProviders.InitialGathererPathProvider import InitialGathererPathProvider
from ServiceProviders.GeneratorContextProvider import GeneratorContextProvider
from ServiceProviders.MLContextProvider import MLContextProvider


@dataclass
class ProfilingMetrics:
    """Metrics collected during profiling."""
    operation_name: str
    dataset_size: int
    execution_time_seconds: float
    throughput_rows_per_sec: float
    time_per_row_ms: float

    # Worker metrics
    num_workers: int
    avg_worker_cpu_percent: float
    avg_worker_memory_percent: float
    peak_memory_gb: float

    # Task graph metrics
    num_tasks_total: int
    num_tasks_completed: int
    num_tasks_failed: int
    avg_task_duration_ms: float

    # Scheduler metrics
    scheduler_processing_time_ms: float
    scheduler_queue_depth: int

    # Bottleneck indicators
    is_compute_bound: bool  # High CPU, low memory
    is_memory_bound: bool   # High memory, low CPU
    is_io_bound: bool       # Low CPU, low memory
    is_scheduler_bound: bool  # High queue depth


class DaskProfiler:
    """Profiles Dask operations to identify bottlenecks."""

    def __init__(self, dataset_sizes: List[int] = None):
        """
        Initialize profiler.

        Args:
            dataset_sizes: List of dataset row counts to profile.
                          Default: [10_000, 50_000, 100_000, 500_000]
        """
        self.dataset_sizes = dataset_sizes or [10_000, 50_000, 100_000, 500_000]
        self.metrics: List[ProfilingMetrics] = []
        self.session_manager = None
        self.client = None

    def _setup_dependency_injection(self, tmp_path: Path):
        """Setup dependency injection providers."""
        # Setup base path provider for Logger
        PathProvider(
            model="profiling",
            contexts={
                "Logger.logpath": lambda model: str(tmp_path / "logs" / "profiling.log"),
            }
        )

        # Setup gatherer-specific path provider
        InitialGathererPathProvider(
            model="profiling",
            contexts={
                "DataGatherer.filepath": lambda model: str(tmp_path / "data.csv"),
                "DataGatherer.subsectionpath": lambda model: str(tmp_path / "cache.csv"),
                "DataGatherer.splitfilespath": lambda model: str(tmp_path / "split.csv"),
            }
        )

        # Setup generator context provider (for cleaners/attackers)
        GeneratorContextProvider(contexts={
            "DataGatherer.numrows": None,
            "DataGatherer.lines_per_file": 100000,
        })

        # Setup ML context provider (for ML components)
        MLContextProvider(contexts={
            "Classifier.name": "profiling",
            "Classifier.type": "RandomForest",
        })

    def __enter__(self):
        """Start Dask cluster."""
        # Setup dependency injection
        tmp_path = Path("/tmp/dask_profiling")
        tmp_path.mkdir(exist_ok=True)
        self._setup_dependency_injection(tmp_path)

        # Get Dask cluster and client (singleton pattern)
        cluster = DaskSessionManager.get_cluster(n_workers=6, threads_per_worker=2)
        self.client = DaskSessionManager.get_client()
        print(f"\n{'='*80}")
        print(f"Dask Dashboard: {DaskSessionManager.get_dashboard_link()}")
        print(f"{'='*80}\n")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop Dask cluster."""
        DaskSessionManager.shutdown()

    def _get_worker_metrics(self) -> Dict[str, Any]:
        """Get current worker metrics from scheduler."""
        workers = self.client.scheduler_info()['workers']

        cpu_percents = []
        memory_percents = []
        memory_used_gb = []

        for worker_addr, worker_info in workers.items():
            # CPU metrics
            metrics = worker_info.get('metrics', {})
            cpu_percents.append(metrics.get('cpu', 0))

            # Memory metrics
            memory_limit = worker_info.get('memory_limit', 1)
            memory_used = metrics.get('memory', 0)
            memory_percents.append(100 * memory_used / memory_limit if memory_limit > 0 else 0)
            memory_used_gb.append(memory_used / 1e9)

        return {
            'num_workers': len(workers),
            'avg_cpu_percent': np.mean(cpu_percents) if cpu_percents else 0,
            'avg_memory_percent': np.mean(memory_percents) if memory_percents else 0,
            'peak_memory_gb': max(memory_used_gb) if memory_used_gb else 0,
        }

    def _get_task_metrics(self) -> Dict[str, Any]:
        """Get task graph metrics from scheduler."""
        # Get scheduler info
        info = self.client.scheduler_info()

        # Task stream metrics (simplified - full metrics require dashboard API)
        tasks = info.get('tasks', {})

        return {
            'num_tasks_total': len(tasks),
            'num_tasks_completed': sum(1 for t in tasks.values() if t.get('state') == 'memory'),
            'num_tasks_failed': sum(1 for t in tasks.values() if t.get('state') == 'erred'),
            'avg_task_duration_ms': 0,  # Requires task stream data
        }

    def _get_scheduler_metrics(self) -> Dict[str, Any]:
        """Get scheduler performance metrics."""
        # Note: Full scheduler metrics require distributed.diagnostics.performance_report
        # This is a simplified version
        return {
            'scheduler_processing_time_ms': 0,  # Requires performance report
            'scheduler_queue_depth': 0,  # Requires task stream data
        }

    def _classify_bottleneck(self, worker_metrics: Dict, task_metrics: Dict,
                            scheduler_metrics: Dict) -> Tuple[bool, bool, bool, bool]:
        """
        Classify the bottleneck type.

        Returns:
            (is_compute_bound, is_memory_bound, is_io_bound, is_scheduler_bound)
        """
        cpu = worker_metrics['avg_cpu_percent']
        memory = worker_metrics['avg_memory_percent']
        queue = scheduler_metrics['scheduler_queue_depth']

        # Thresholds
        HIGH_CPU = 70
        HIGH_MEMORY = 60
        HIGH_QUEUE = 100

        is_compute_bound = cpu > HIGH_CPU and memory < HIGH_MEMORY
        is_memory_bound = memory > HIGH_MEMORY
        is_io_bound = cpu < 30 and memory < HIGH_MEMORY and queue < HIGH_QUEUE
        is_scheduler_bound = queue > HIGH_QUEUE

        return is_compute_bound, is_memory_bound, is_io_bound, is_scheduler_bound

    def profile_operation(self, operation_name: str, operation_func,
                         dataset_size: int, *args, **kwargs) -> ProfilingMetrics:
        """
        Profile a single operation.

        Args:
            operation_name: Name of the operation
            operation_func: Function to profile
            dataset_size: Number of rows in dataset
            *args, **kwargs: Arguments to pass to operation_func

        Returns:
            ProfilingMetrics object
        """
        print(f"\nProfiling: {operation_name} ({dataset_size:,} rows)")
        print("-" * 80)

        # Baseline metrics before operation
        baseline_worker = self._get_worker_metrics()

        # Execute operation
        start_time = time.time()
        result = operation_func(*args, **kwargs)

        # Force computation if Dask DataFrame
        if hasattr(result, 'compute'):
            result = result.compute()

        elapsed = time.time() - start_time

        # Post-operation metrics
        worker_metrics = self._get_worker_metrics()
        task_metrics = self._get_task_metrics()
        scheduler_metrics = self._get_scheduler_metrics()

        # Calculate derived metrics
        throughput = dataset_size / elapsed if elapsed > 0 else 0
        time_per_row_ms = (elapsed * 1000) / dataset_size if dataset_size > 0 else 0

        # Classify bottleneck
        is_compute, is_memory, is_io, is_scheduler = self._classify_bottleneck(
            worker_metrics, task_metrics, scheduler_metrics
        )

        # Create metrics object
        metrics = ProfilingMetrics(
            operation_name=operation_name,
            dataset_size=dataset_size,
            execution_time_seconds=elapsed,
            throughput_rows_per_sec=throughput,
            time_per_row_ms=time_per_row_ms,
            num_workers=worker_metrics['num_workers'],
            avg_worker_cpu_percent=worker_metrics['avg_cpu_percent'],
            avg_worker_memory_percent=worker_metrics['avg_memory_percent'],
            peak_memory_gb=worker_metrics['peak_memory_gb'],
            num_tasks_total=task_metrics['num_tasks_total'],
            num_tasks_completed=task_metrics['num_tasks_completed'],
            num_tasks_failed=task_metrics['num_tasks_failed'],
            avg_task_duration_ms=task_metrics['avg_task_duration_ms'],
            scheduler_processing_time_ms=scheduler_metrics['scheduler_processing_time_ms'],
            scheduler_queue_depth=scheduler_metrics['scheduler_queue_depth'],
            is_compute_bound=is_compute,
            is_memory_bound=is_memory,
            is_io_bound=is_io,
            is_scheduler_bound=is_scheduler,
        )

        # Print summary
        print(f"  Execution Time: {elapsed:.2f}s")
        print(f"  Throughput: {throughput:,.0f} rows/sec")
        print(f"  Time per Row: {time_per_row_ms:.3f}ms")
        print(f"  Avg CPU: {worker_metrics['avg_cpu_percent']:.1f}%")
        print(f"  Avg Memory: {worker_metrics['avg_memory_percent']:.1f}%")
        print(f"  Peak Memory: {worker_metrics['peak_memory_gb']:.2f} GB")

        bottleneck_types = []
        if is_compute: bottleneck_types.append("COMPUTE")
        if is_memory: bottleneck_types.append("MEMORY")
        if is_io: bottleneck_types.append("I/O")
        if is_scheduler: bottleneck_types.append("SCHEDULER")

        if bottleneck_types:
            print(f"  Bottleneck: {', '.join(bottleneck_types)}")
        else:
            print(f"  Bottleneck: BALANCED")

        self.metrics.append(metrics)
        return metrics

    def _create_test_data(self, n_rows: int, n_vehicles: int,
                         tmp_path: Path) -> Tuple[Path, str]:
        """
        Create test CSV data.

        Args:
            n_rows: Number of rows to generate
            n_vehicles: Number of unique vehicle IDs
            tmp_path: Temporary directory path

        Returns:
            (csv_file_path, cache_key)
        """
        print(f"  Creating test data: {n_rows:,} rows, {n_vehicles:,} vehicles...")

        # Generate synthetic BSM data
        np.random.seed(42)

        # Create realistic distribution of vehicles
        vehicle_ids = np.random.choice(range(1, n_vehicles + 1), size=n_rows)

        # Timestamp range: 1 hour of data
        timestamps = np.linspace(0, 3600, n_rows)

        # Coordinates: Area around (0, 0)
        latitudes = np.random.uniform(-0.01, 0.01, n_rows)
        longitudes = np.random.uniform(-0.01, 0.01, n_rows)

        # Other BSM fields
        speeds = np.random.uniform(0, 30, n_rows)  # m/s
        headings = np.random.uniform(0, 360, n_rows)

        df = pd.DataFrame({
            'VehicleID': vehicle_ids,
            'Timestamp': timestamps,
            'Latitude': latitudes,
            'Longitude': longitudes,
            'Speed': speeds,
            'Heading': headings,
            'Acceleration': np.random.uniform(-3, 3, n_rows),
            'YawRate': np.random.uniform(-10, 10, n_rows),
        })

        # Write to CSV
        csv_file = tmp_path / f"bsm_data_{n_rows}.csv"
        df.to_csv(csv_file, index=False)

        cache_key = f"profile_{n_rows}"

        print(f"  Created: {csv_file} ({csv_file.stat().st_size / 1e6:.1f} MB)")
        return csv_file, cache_key

    def run_full_profile(self):
        """Run complete profiling suite across all dataset sizes."""
        print("\n" + "="*80)
        print("DASK PIPELINE BOTTLENECK PROFILING")
        print("="*80)
        print(f"\nDataset Sizes: {', '.join(f'{s:,}' for s in self.dataset_sizes)}")
        print(f"Workers: {self.client.scheduler_info()['workers'].__len__()}")
        print(f"Dashboard: {DaskSessionManager.get_dashboard_link()}")

        # Create temporary directory for test data
        tmp_path = Path("/tmp/dask_profiling")
        tmp_path.mkdir(exist_ok=True)

        # Disable cache for pure profiling
        DaskParquetCache.enabled = False

        for dataset_size in self.dataset_sizes:
            print(f"\n{'='*80}")
            print(f"PROFILING DATASET SIZE: {dataset_size:,} rows")
            print(f"{'='*80}")

            # Determine vehicle count (1:20 vehicle-to-row ratio)
            n_vehicles = max(100, dataset_size // 20)

            # Create test data
            csv_file, cache_key = self._create_test_data(dataset_size, n_vehicles, tmp_path)

            # Profile 1: Data Gathering (CSV → Dask DataFrame)
            def gather_operation():
                # Update provider with current CSV path
                InitialGathererPathProvider(
                    model="profiling",
                    contexts={
                        "DataGatherer.filepath": lambda model: str(csv_file),
                        "DataGatherer.subsectionpath": lambda model: str(tmp_path / f"cache_{dataset_size}.csv"),
                        "DataGatherer.splitfilespath": lambda model: str(tmp_path / f"split_{dataset_size}.csv"),
                    }
                )
                gatherer = DaskDataGatherer()
                return gatherer.gather_data()

            self.profile_operation(
                "Data Gathering (CSV Read)",
                gather_operation,
                dataset_size
            )

            # Profile 2: Basic Cleaning (Row filtering)
            def clean_operation():
                # Update provider
                InitialGathererPathProvider(
                    model="profiling",
                    contexts={
                        "DataGatherer.filepath": lambda model: str(csv_file),
                        "DataGatherer.subsectionpath": lambda model: str(tmp_path / f"cache_{dataset_size}.csv"),
                        "DataGatherer.splitfilespath": lambda model: str(tmp_path / f"split_{dataset_size}.csv"),
                    }
                )
                gatherer = DaskDataGatherer()
                df = gatherer.gather_data()
                cleaner = DaskConnectedDrivingCleaner()
                return cleaner.clean_data(df)

            self.profile_operation(
                "Basic Cleaning (ConnectedDrivingCleaner)",
                clean_operation,
                dataset_size
            )

            # Profile 3: Large Data Cleaning (Spatial filtering)
            def large_clean_operation():
                # Update provider
                InitialGathererPathProvider(
                    model="profiling",
                    contexts={
                        "DataGatherer.filepath": lambda model: str(csv_file),
                        "DataGatherer.subsectionpath": lambda model: str(tmp_path / f"cache_{dataset_size}.csv"),
                        "DataGatherer.splitfilespath": lambda model: str(tmp_path / f"split_{dataset_size}.csv"),
                    }
                )
                gatherer = DaskDataGatherer()
                df = gatherer.gather_data()
                cleaner = DaskConnectedDrivingLargeDataCleaner(
                    distance=100.0,
                    start_lat=0.0,
                    start_lon=0.0
                )
                return cleaner.clean_data(df)

            self.profile_operation(
                "Large Data Cleaning (Spatial Filter)",
                large_clean_operation,
                dataset_size
            )

            # Profile 4: Timestamp Cleaning (Temporal filtering)
            def timestamp_clean_operation():
                # Update provider
                InitialGathererPathProvider(
                    model="profiling",
                    contexts={
                        "DataGatherer.filepath": lambda model: str(csv_file),
                        "DataGatherer.subsectionpath": lambda model: str(tmp_path / f"cache_{dataset_size}.csv"),
                        "DataGatherer.splitfilespath": lambda model: str(tmp_path / f"split_{dataset_size}.csv"),
                    }
                )
                gatherer = DaskDataGatherer()
                df = gatherer.gather_data()
                cleaner = DaskCleanWithTimestamps(
                    start_timestamp=0.0,
                    end_timestamp=1800.0  # First 30 minutes
                )
                return cleaner.clean_data(df)

            self.profile_operation(
                "Timestamp Cleaning (Temporal Filter)",
                timestamp_clean_operation,
                dataset_size
            )

            # Profile 5: Attack Simulation (Positional Offset)
            def attack_operation():
                # Update provider
                InitialGathererPathProvider(
                    model="profiling",
                    contexts={
                        "DataGatherer.filepath": lambda model: str(csv_file),
                        "DataGatherer.subsectionpath": lambda model: str(tmp_path / f"cache_{dataset_size}.csv"),
                        "DataGatherer.splitfilespath": lambda model: str(tmp_path / f"split_{dataset_size}.csv"),
                    }
                )
                gatherer = DaskDataGatherer()
                df = gatherer.gather_data()
                attacker = DaskConnectedDrivingAttacker()
                # Select 30% as attackers
                df_with_attackers = attacker.add_attackers(df, attacker_percentage=0.3)
                # Apply random offset attack
                return attacker.positional_offset_rand(
                    df_with_attackers,
                    min_offset=10.0,
                    max_offset=20.0
                )

            self.profile_operation(
                "Attack Simulation (Random Offset)",
                attack_operation,
                dataset_size
            )

            # Profile 6: ML Data Preparation (Feature Engineering)
            # Only for smaller datasets (ML is expensive)
            if dataset_size <= 100_000:
                def ml_prep_operation():
                    # Update provider
                    InitialGathererPathProvider(
                        model="profiling",
                        contexts={
                            "DataGatherer.filepath": lambda model: str(csv_file),
                            "DataGatherer.subsectionpath": lambda model: str(tmp_path / f"cache_{dataset_size}.csv"),
                            "DataGatherer.splitfilespath": lambda model: str(tmp_path / f"split_{dataset_size}.csv"),
                        }
                    )
                    gatherer = DaskDataGatherer()
                    df = gatherer.gather_data()
                    ml_cleaner = DaskMConnectedDrivingDataCleaner()
                    return ml_cleaner.clean_data(df)

                self.profile_operation(
                    "ML Data Preparation (Feature Engineering)",
                    ml_prep_operation,
                    dataset_size
                )

            # Cleanup
            csv_file.unlink()

        print(f"\n{'='*80}")
        print("PROFILING COMPLETE")
        print(f"{'='*80}\n")

    def generate_report(self, output_path: str = "DASK_BOTTLENECK_PROFILING_REPORT.md"):
        """
        Generate detailed profiling report.

        Args:
            output_path: Path to save markdown report
        """
        print(f"\nGenerating profiling report: {output_path}")

        # Convert metrics to DataFrame for analysis
        df = pd.DataFrame([asdict(m) for m in self.metrics])

        # Calculate statistics
        bottleneck_summary = {
            'Compute Bound': df['is_compute_bound'].sum(),
            'Memory Bound': df['is_memory_bound'].sum(),
            'I/O Bound': df['is_io_bound'].sum(),
            'Scheduler Bound': df['is_scheduler_bound'].sum(),
        }

        # Find slowest operations
        df_sorted = df.sort_values('time_per_row_ms', ascending=False)

        # Generate markdown report
        report = []
        report.append("# Dask Pipeline Bottleneck Profiling Report")
        report.append("")
        report.append("**Generated:** " + time.strftime("%Y-%m-%d %H:%M:%S"))
        report.append("**Task:** Task 47 - Identify bottlenecks with Dask dashboard profiling")
        report.append("")
        report.append("---")
        report.append("")

        # Executive Summary
        report.append("## Executive Summary")
        report.append("")
        report.append(f"Profiled {len(self.metrics)} operations across {len(self.dataset_sizes)} dataset sizes.")
        report.append("")
        report.append("**Bottleneck Classification:**")
        for bottleneck_type, count in bottleneck_summary.items():
            percentage = 100 * count / len(self.metrics) if len(self.metrics) > 0 else 0
            report.append(f"- {bottleneck_type}: {count} operations ({percentage:.1f}%)")
        report.append("")

        # Top Bottlenecks
        report.append("## Top Performance Bottlenecks")
        report.append("")
        report.append("Operations ranked by time per row (slowest first):")
        report.append("")
        report.append("| Rank | Operation | Dataset Size | Time/Row (ms) | Throughput (rows/s) | Bottleneck Type |")
        report.append("|------|-----------|--------------|---------------|---------------------|-----------------|")

        for idx, row in df_sorted.head(10).iterrows():
            bottleneck_types = []
            if row['is_compute_bound']: bottleneck_types.append("Compute")
            if row['is_memory_bound']: bottleneck_types.append("Memory")
            if row['is_io_bound']: bottleneck_types.append("I/O")
            if row['is_scheduler_bound']: bottleneck_types.append("Scheduler")
            bottleneck_str = ", ".join(bottleneck_types) if bottleneck_types else "Balanced"

            report.append(
                f"| {len(df_sorted) - len(df_sorted) + df_sorted.index.get_loc(idx) + 1} | "
                f"{row['operation_name']} | "
                f"{row['dataset_size']:,} | "
                f"{row['time_per_row_ms']:.3f} | "
                f"{row['throughput_rows_per_sec']:,.0f} | "
                f"{bottleneck_str} |"
            )
        report.append("")

        # Detailed Metrics by Operation
        report.append("## Detailed Metrics by Operation")
        report.append("")

        for operation_name in df['operation_name'].unique():
            operation_df = df[df['operation_name'] == operation_name]
            report.append(f"### {operation_name}")
            report.append("")

            report.append("| Dataset Size | Time (s) | Throughput (rows/s) | CPU (%) | Memory (%) | Peak Memory (GB) |")
            report.append("|--------------|----------|---------------------|---------|------------|------------------|")

            for _, row in operation_df.iterrows():
                report.append(
                    f"| {row['dataset_size']:,} | "
                    f"{row['execution_time_seconds']:.2f} | "
                    f"{row['throughput_rows_per_sec']:,.0f} | "
                    f"{row['avg_worker_cpu_percent']:.1f} | "
                    f"{row['avg_worker_memory_percent']:.1f} | "
                    f"{row['peak_memory_gb']:.2f} |"
                )
            report.append("")

        # Scaling Analysis
        report.append("## Scaling Analysis")
        report.append("")

        for operation_name in df['operation_name'].unique():
            operation_df = df[df['operation_name'] == operation_name].sort_values('dataset_size')

            if len(operation_df) >= 2:
                # Calculate scaling factor
                first_size = operation_df.iloc[0]['dataset_size']
                last_size = operation_df.iloc[-1]['dataset_size']
                first_time = operation_df.iloc[0]['execution_time_seconds']
                last_time = operation_df.iloc[-1]['execution_time_seconds']

                size_ratio = last_size / first_size
                time_ratio = last_time / first_time

                # O(n) would have time_ratio == size_ratio
                # O(n log n) would have time_ratio slightly > size_ratio
                # O(n^2) would have time_ratio >> size_ratio

                if time_ratio < size_ratio * 0.8:
                    scaling = "Sub-linear (O(n) or better)"
                elif time_ratio < size_ratio * 1.2:
                    scaling = "Linear (O(n))"
                elif time_ratio < size_ratio * 2:
                    scaling = "Super-linear (O(n log n))"
                else:
                    scaling = "Quadratic or worse (O(n^2))"

                report.append(f"**{operation_name}:**")
                report.append(f"- Size ratio: {size_ratio:.1f}x ({first_size:,} → {last_size:,})")
                report.append(f"- Time ratio: {time_ratio:.1f}x ({first_time:.2f}s → {last_time:.2f}s)")
                report.append(f"- Scaling: {scaling}")
                report.append("")

        # Recommendations
        report.append("## Optimization Recommendations")
        report.append("")

        # Group by bottleneck type
        compute_bound = df[df['is_compute_bound']]
        memory_bound = df[df['is_memory_bound']]
        io_bound = df[df['is_io_bound']]

        if len(compute_bound) > 0:
            report.append("### Compute-Bound Operations")
            report.append("")
            report.append("Operations limited by CPU:")
            for _, row in compute_bound.iterrows():
                report.append(f"- {row['operation_name']} ({row['dataset_size']:,} rows)")
            report.append("")
            report.append("**Recommendations:**")
            report.append("- Increase number of workers (currently 6)")
            report.append("- Optimize computation algorithms (vectorization, Numba)")
            report.append("- Increase partition sizes to reduce overhead")
            report.append("")

        if len(memory_bound) > 0:
            report.append("### Memory-Bound Operations")
            report.append("")
            report.append("Operations limited by memory:")
            for _, row in memory_bound.iterrows():
                report.append(f"- {row['operation_name']} ({row['dataset_size']:,} rows, "
                            f"{row['peak_memory_gb']:.2f} GB peak)")
            report.append("")
            report.append("**Recommendations:**")
            report.append("- Reduce partition sizes to lower memory per task")
            report.append("- Use incremental processing for large operations")
            report.append("- Enable spill-to-disk for memory-heavy operations")
            report.append("- Consider distributed cluster if >40GB peak at 15M rows")
            report.append("")

        if len(io_bound) > 0:
            report.append("### I/O-Bound Operations")
            report.append("")
            report.append("Operations limited by disk I/O:")
            for _, row in io_bound.iterrows():
                report.append(f"- {row['operation_name']} ({row['dataset_size']:,} rows)")
            report.append("")
            report.append("**Recommendations:**")
            report.append("- Use Parquet format instead of CSV (60% size reduction)")
            report.append("- Enable DaskParquetCache for repeated operations")
            report.append("- Use faster storage (NVMe SSD)")
            report.append("- Optimize compression settings (lz4 or snappy)")
            report.append("")

        # Overall recommendations
        report.append("## Overall Recommendations for Tasks 48-50")
        report.append("")
        report.append("### Task 48: Optimize Slow Operations")
        report.append("")

        # Find slowest operation
        slowest = df_sorted.iloc[0]
        report.append(f"**Priority 1:** Optimize {slowest['operation_name']}")
        report.append(f"- Current: {slowest['time_per_row_ms']:.3f} ms/row")
        report.append(f"- Target: {slowest['time_per_row_ms'] / 2:.3f} ms/row (2x speedup)")
        report.append("")

        report.append("### Task 49: Reduce Memory Usage")
        report.append("")
        max_memory = df['peak_memory_gb'].max()
        report.append(f"**Current Peak Memory:** {max_memory:.2f} GB")
        report.append(f"**Target:** <40 GB at 15M rows")
        if max_memory < 40:
            report.append("✅ Already within target")
        else:
            report.append("❌ Needs optimization")
        report.append("")

        report.append("### Task 50: Optimize Cache Hit Rates")
        report.append("")
        report.append("**Current:** DaskParquetCache disabled for profiling")
        report.append("**Target:** >85% cache hit rate in production")
        report.append("")
        report.append("**Recommendations:**")
        report.append("- Enable DaskParquetCache globally")
        report.append("- Use deterministic cache keys")
        report.append("- Monitor cache hit/miss rates with logging")
        report.append("- Increase cache size if needed (monitor disk usage)")
        report.append("")

        # Appendix: Raw Data
        report.append("## Appendix: Raw Profiling Data")
        report.append("")
        report.append("```json")
        report.append(json.dumps([asdict(m) for m in self.metrics], indent=2))
        report.append("```")
        report.append("")

        # Write report
        output_file = Path(output_path)
        output_file.write_text("\n".join(report))

        print(f"✅ Report saved: {output_file}")
        print(f"   Size: {output_file.stat().st_size / 1024:.1f} KB")
        print(f"   Operations profiled: {len(self.metrics)}")


def main():
    """Main entry point."""
    print("\n" + "="*80)
    print("DASK BOTTLENECK PROFILING - TASK 47")
    print("="*80)

    # Configuration
    dataset_sizes = [10_000, 50_000, 100_000, 500_000]

    print(f"\nConfiguration:")
    print(f"  Dataset Sizes: {', '.join(f'{s:,}' for s in dataset_sizes)}")
    print(f"  Workers: 6 (2 threads each)")
    print(f"  Memory: Auto-detect")
    print(f"\nNote: Check Dask dashboard at http://localhost:8787 during profiling")
    print(f"      for real-time task graph, worker utilization, and memory usage.")

    # Run profiling
    with DaskProfiler(dataset_sizes=dataset_sizes) as profiler:
        profiler.run_full_profile()

        # Generate report
        report_path = "/tmp/original-repo/DASK_BOTTLENECK_PROFILING_REPORT.md"
        profiler.generate_report(output_path=report_path)

    print("\n" + "="*80)
    print("PROFILING COMPLETE")
    print("="*80)
    print(f"\n✅ Report generated: {report_path}")
    print(f"\nNext Steps:")
    print(f"  1. Review report for bottleneck identification")
    print(f"  2. Implement optimizations (Task 48)")
    print(f"  3. Validate memory usage <40GB at 15M rows (Task 49)")
    print(f"  4. Optimize cache hit rates >85% (Task 50)")
    print()


if __name__ == "__main__":
    main()

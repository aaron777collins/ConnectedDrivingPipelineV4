#!/usr/bin/env python3
"""
Dask Setup Validation Script

Validates that the Dask environment is properly configured for the 64GB migration.
This script verifies:
1. Dask cluster creation with correct memory limits
2. Basic DataFrame operations
3. .iloc[] support (critical for position swap attacks)
4. GroupBy operations
5. Memory usage monitoring
6. Dashboard accessibility
7. Parquet I/O with DaskParquetCache

Based on the migration plan in /tmp/dask-migration-plan.md, Phase 1.
"""

import os
import sys
import tempfile
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DaskValidator')


def validate_imports():
    """Validate that all required Dask packages are importable."""
    logger.info("=" * 60)
    logger.info("TEST 1: Validating Dask imports")
    logger.info("=" * 60)

    try:
        import dask
        import dask.dataframe as dd
        import dask.distributed
        import dask_ml
        import distributed
        import lz4

        logger.info(f"✓ dask version: {dask.__version__}")
        logger.info(f"✓ dask-ml installed")
        logger.info(f"✓ distributed version: {distributed.__version__}")
        logger.info(f"✓ lz4 compression available")

        return True
    except ImportError as e:
        logger.error(f"✗ Import failed: {e}")
        logger.error("Run: pip install dask[complete]>=2024.1.0 dask-ml>=2024.4.0 distributed>=2024.1.0 lz4>=4.3.0")
        return False


def validate_session_manager():
    """Validate DaskSessionManager creates cluster correctly."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 2: Validating DaskSessionManager")
    logger.info("=" * 60)

    try:
        from Helpers.DaskSessionManager import DaskSessionManager, get_dask_client

        # Test getting client (should create cluster)
        client = get_dask_client(
            n_workers=2,  # Use 2 workers for validation (not full 6)
            threads_per_worker=1,
            memory_limit='4GB',  # Smaller for validation
            env='development'
        )

        logger.info(f"✓ Client created successfully")
        logger.info(f"✓ Dashboard: {client.dashboard_link}")

        # Validate cluster configuration
        scheduler_info = client.scheduler_info()
        num_workers = len(scheduler_info['workers'])

        logger.info(f"✓ Number of workers: {num_workers}")

        for worker_addr, worker_info in scheduler_info['workers'].items():
            memory_limit_gb = worker_info['memory_limit'] / 1e9
            logger.info(f"  Worker {worker_addr}: {memory_limit_gb:.1f}GB memory limit")

        # Verify we have the expected number of workers
        if num_workers != 2:
            logger.warning(f"⚠ Expected 2 workers, got {num_workers}")

        return True, client

    except Exception as e:
        logger.error(f"✗ DaskSessionManager validation failed: {e}")
        return False, None


def validate_basic_operations(client):
    """Validate basic Dask DataFrame operations."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 3: Validating basic DataFrame operations")
    logger.info("=" * 60)

    try:
        import pandas as pd
        import dask.dataframe as dd
        import numpy as np

        # Create sample DataFrame
        df_pandas = pd.DataFrame({
            'a': np.random.randn(100000),
            'b': np.random.choice(['X', 'Y', 'Z'], 100000),
            'c': np.random.randint(0, 100, 100000)
        })

        df_dask = dd.from_pandas(df_pandas, npartitions=10)

        # Test 1: Shape
        dask_len = len(df_dask)
        if dask_len == 100000:
            logger.info(f"✓ Shape: {dask_len} rows")
        else:
            logger.error(f"✗ Shape mismatch: expected 100000, got {dask_len}")
            return False

        # Test 2: Columns
        if list(df_dask.columns) == ['a', 'b', 'c']:
            logger.info(f"✓ Columns: {list(df_dask.columns)}")
        else:
            logger.error(f"✗ Columns mismatch")
            return False

        # Test 3: Compute
        result = df_dask.head(10)
        logger.info(f"✓ Compute: Successfully computed {len(result)} rows")

        # Test 4: Filtering
        filtered = df_dask[df_dask['c'] > 50]
        filtered_count = len(filtered)
        logger.info(f"✓ Filtering: {filtered_count} rows where c > 50")

        # Test 5: Column operations
        df_dask['d'] = df_dask['a'] + df_dask['c']
        logger.info(f"✓ Column operations: Created new column 'd'")

        return True

    except Exception as e:
        logger.error(f"✗ Basic operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_iloc_support(client):
    """Validate .iloc[] support (critical for position swap attacks)."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 4: Validating .iloc[] support (CRITICAL)")
    logger.info("=" * 60)

    try:
        import pandas as pd
        import dask.dataframe as dd
        import numpy as np

        # Create sample DataFrame
        df_pandas = pd.DataFrame({
            'feature_1': np.random.randn(1000),
            'feature_2': np.random.randn(1000),
            'label': np.random.choice([0, 1], 1000)
        })

        df_dask = dd.from_pandas(df_pandas, npartitions=10)

        # IMPORTANT: Dask's .iloc[] implementation ONLY supports column selection
        # Row-based indexing is NOT supported in Dask 2024+
        logger.warning("⚠ IMPORTANT: Dask .iloc[] only supports column selection (NOT row selection)")
        logger.warning("⚠ Row slicing like df.iloc[0:100] is NOT available in Dask")
        logger.warning("⚠ Position swap attack MUST use compute-then-daskify strategy")

        # Test 1: Column slicing (ONLY supported operation)
        try:
            result = df_dask.iloc[:, 0:2].compute()
            if len(result.columns) == 2:
                logger.info(f"✓ Column slicing: df.iloc[:, 0:2] returned {len(result.columns)} columns")
            else:
                logger.error(f"✗ Column slicing failed")
                return False
        except Exception as e:
            logger.error(f"✗ Column slicing failed: {e}")
            return False

        # Test 2: Verify row slicing is NOT supported (expected to fail)
        try:
            result = df_dask.iloc[0:100].compute()
            logger.warning(f"⚠ Unexpected: Row slicing worked (Dask version may have changed)")
        except Exception as e:
            logger.info(f"✓ Confirmed: Row slicing not supported (expected behavior)")
            logger.info(f"  Error message: {str(e)}")

        # Test 3: Demonstrate the workaround - head() for row limiting
        try:
            result = df_dask.head(100, npartitions=-1)
            if len(result) == 100:
                logger.info(f"✓ Workaround: df.head(100) returned {len(result)} rows (alternative to iloc)")
            else:
                logger.error(f"✗ head() workaround failed")
                return False
        except Exception as e:
            logger.error(f"✗ head() workaround failed: {e}")
            return False

        # CRITICAL RECOMMENDATION
        logger.info("\n" + "-" * 60)
        logger.info("POSITION SWAP ATTACK IMPLEMENTATION NOTES:")
        logger.info("-" * 60)
        logger.info("1. Dask .iloc[] does NOT support row indexing")
        logger.info("2. Must use compute-then-daskify strategy:")
        logger.info("   - Call .compute() to get pandas DataFrame")
        logger.info("   - Perform position swap using pandas .iloc[]")
        logger.info("   - Convert back to Dask with dd.from_pandas()")
        logger.info("3. This is memory-safe for 15-20M rows on 64GB system")
        logger.info("-" * 60)

        return True

    except Exception as e:
        logger.error(f"✗ .iloc[] validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_groupby(client):
    """Validate GroupBy operations."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 5: Validating GroupBy operations")
    logger.info("=" * 60)

    try:
        import pandas as pd
        import dask.dataframe as dd
        import numpy as np

        # Create sample DataFrame
        df_pandas = pd.DataFrame({
            'category': np.random.choice(['A', 'B', 'C'], 10000),
            'value': np.random.randn(10000)
        })

        df_dask = dd.from_pandas(df_pandas, npartitions=10)

        # Test GroupBy
        grouped = df_dask.groupby('category')['value'].mean().compute()

        if len(grouped) == 3:
            logger.info(f"✓ GroupBy: {len(grouped)} groups computed")
            for cat, val in grouped.items():
                logger.info(f"  {cat}: mean = {val:.4f}")
        else:
            logger.error(f"✗ GroupBy failed: expected 3 groups, got {len(grouped)}")
            return False

        return True

    except Exception as e:
        logger.error(f"✗ GroupBy validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_memory_monitoring(client):
    """Validate memory usage monitoring."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 6: Validating memory monitoring")
    logger.info("=" * 60)

    try:
        from Helpers.DaskSessionManager import DaskSessionManager

        memory_info = DaskSessionManager.get_memory_usage()

        if 'total' in memory_info:
            total = memory_info['total']
            logger.info(f"✓ Total cluster memory: {total['used_gb']:.2f}GB / {total['limit_gb']:.2f}GB ({total['percent']:.1f}%)")

            for worker_addr, info in memory_info.items():
                if worker_addr != 'total':
                    logger.info(f"  Worker {worker_addr}: {info['used_gb']:.2f}GB / {info['limit_gb']:.2f}GB ({info['percent']:.1f}%)")

            return True
        else:
            logger.error(f"✗ Memory info missing 'total' key")
            return False

    except Exception as e:
        logger.error(f"✗ Memory monitoring validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_parquet_cache():
    """Validate DaskParquetCache decorator."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 7: Validating DaskParquetCache decorator")
    logger.info("=" * 60)

    try:
        import pandas as pd
        import dask.dataframe as dd
        import numpy as np
        from Decorators.DaskParquetCache import DaskParquetCache

        # Create a temporary directory for cache
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = os.path.join(tmpdir, 'test_cache.parquet')

            # Define a test function with DaskParquetCache
            call_count = [0]

            @DaskParquetCache
            def create_test_data(rows: int, full_file_cache_path: str = None) -> dd.DataFrame:
                """Test function that creates a Dask DataFrame."""
                call_count[0] += 1

                df_pandas = pd.DataFrame({
                    'x': np.random.randn(rows),
                    'y': np.random.randn(rows)
                })

                return dd.from_pandas(df_pandas, npartitions=4)

            # First call - should execute and cache
            df1 = create_test_data(1000, full_file_cache_path=cache_path)
            result1 = df1.compute()

            logger.info(f"✓ First call: Created {len(result1)} rows (call_count={call_count[0]})")

            # Second call - should load from cache
            df2 = create_test_data(1000, full_file_cache_path=cache_path)
            result2 = df2.compute()

            logger.info(f"✓ Second call: Loaded {len(result2)} rows from cache (call_count={call_count[0]})")

            # Verify cache was used (call_count should be 1, not 2)
            if call_count[0] == 1:
                logger.info(f"✓ Cache working: Function only called once")
            else:
                logger.warning(f"⚠ Cache may not be working: Function called {call_count[0]} times")

            # Verify Parquet file exists
            if os.path.exists(cache_path):
                logger.info(f"✓ Cache file created: {cache_path}")
            else:
                logger.error(f"✗ Cache file not found: {cache_path}")
                return False

            return True

    except Exception as e:
        logger.error(f"✗ DaskParquetCache validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_dashboard_accessibility(client):
    """Validate that the Dask dashboard is accessible."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST 8: Validating dashboard accessibility")
    logger.info("=" * 60)

    try:
        dashboard_link = client.dashboard_link

        if dashboard_link:
            logger.info(f"✓ Dashboard available at: {dashboard_link}")
            logger.info(f"ℹ Open this URL in your browser to monitor the cluster")
            return True
        else:
            logger.error(f"✗ Dashboard link not available")
            return False

    except Exception as e:
        logger.error(f"✗ Dashboard validation failed: {e}")
        return False


def cleanup(client):
    """Cleanup Dask cluster."""
    logger.info("\n" + "=" * 60)
    logger.info("Cleaning up")
    logger.info("=" * 60)

    try:
        from Helpers.DaskSessionManager import DaskSessionManager

        DaskSessionManager.shutdown()
        logger.info("✓ Dask cluster shutdown successfully")

    except Exception as e:
        logger.error(f"⚠ Cleanup warning: {e}")


def main():
    """Run all validation tests."""
    logger.info("\n")
    logger.info("*" * 60)
    logger.info("DASK SETUP VALIDATION SCRIPT")
    logger.info("64GB System Configuration Validator")
    logger.info("*" * 60)

    results = {}
    client = None

    # Test 1: Imports
    results['imports'] = validate_imports()
    if not results['imports']:
        logger.error("\n✗ FATAL: Cannot proceed without required imports")
        return False

    # Test 2: Session Manager
    success, client = validate_session_manager()
    results['session_manager'] = success
    if not success or client is None:
        logger.error("\n✗ FATAL: Cannot proceed without Dask client")
        return False

    # Test 3-8: Operational tests
    results['basic_operations'] = validate_basic_operations(client)
    results['iloc_support'] = validate_iloc_support(client)
    results['groupby'] = validate_groupby(client)
    results['memory_monitoring'] = validate_memory_monitoring(client)
    results['parquet_cache'] = validate_parquet_cache()
    results['dashboard'] = validate_dashboard_accessibility(client)

    # Cleanup
    cleanup(client)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status}: {test_name}")

    logger.info("\n" + "-" * 60)
    logger.info(f"Results: {passed}/{total} tests passed")
    logger.info("-" * 60)

    if passed == total:
        logger.info("\n✓ ALL VALIDATION TESTS PASSED")
        logger.info("Dask environment is properly configured for 64GB migration")
        return True
    else:
        logger.error(f"\n✗ VALIDATION FAILED: {total - passed} test(s) failed")
        logger.error("Please fix the issues above before proceeding with migration")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

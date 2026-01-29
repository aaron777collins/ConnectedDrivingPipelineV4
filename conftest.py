"""
Pytest configuration and shared fixtures for ConnectedDrivingPipelineV4

This file provides global pytest fixtures that can be used across all test files.
"""

import pytest
import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import PySpark and Dask fixtures to make them available globally
# These fixtures are defined in Test/Fixtures/SparkFixtures.py and Test/Fixtures/DaskFixtures.py
# Made optional to allow running tests without heavy dependencies
pytest_plugins = []

try:
    import pyspark
    pytest_plugins.append('Test.Fixtures.SparkFixtures')
except ImportError:
    pass  # PySpark not available, skip Spark fixtures

try:
    import dask
    pytest_plugins.append('Test.Fixtures.DaskFixtures')
except ImportError:
    pass  # Dask not available, skip Dask fixtures


@pytest.fixture(scope="session")
def project_root_path():
    """Fixture that provides the project root directory path."""
    return project_root


@pytest.fixture(scope="function")
def temp_dir():
    """
    Fixture that provides a temporary directory for test file operations.
    Automatically cleaned up after each test.
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    # Cleanup after test
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="function")
def cache_dir(temp_dir):
    """
    Fixture that provides a temporary cache directory.
    Useful for testing caching decorators without polluting the real cache.
    """
    cache_path = os.path.join(temp_dir, "cache")
    os.makedirs(cache_path, exist_ok=True)

    # Override the default cache path for testing
    original_cache = os.environ.get("CACHE_DIR", None)
    os.environ["CACHE_DIR"] = cache_path

    yield cache_path

    # Restore original cache path
    if original_cache:
        os.environ["CACHE_DIR"] = original_cache
    else:
        os.environ.pop("CACHE_DIR", None)


@pytest.fixture(scope="session")
def sample_bsm_data_dict():
    """
    Fixture that provides a sample BSM data dictionary.
    Useful for creating test DataFrames.
    """
    return {
        "metadata_generatedAt": "04/06/2021 10:30:00 AM",
        "metadata_recordType": "bsmTx",
        "metadata_serialId_streamId": "00000000-0000-0000-0000-000000000001",
        "metadata_serialId_bundleSize": 1,
        "metadata_serialId_bundleId": 0,
        "metadata_serialId_recordId": 0,
        "metadata_serialId_serialNumber": 0,
        "metadata_receivedAt": "2021-04-06T10:30:00.000Z",
        "coreData_id": "A1B2C3D4",
        "coreData_position_lat": 41.2565,
        "coreData_position_long": -105.9378,
        "coreData_accuracy_semiMajor": 5.0,
        "coreData_accuracy_semiMinor": 5.0,
        "coreData_elevation": 2194.0,
        "coreData_accelSet_accelYaw": 0.0,
        "coreData_speed": 15.5,
        "coreData_heading": 90.0,
        "coreData_secMark": 30000,
    }


@pytest.fixture(scope="function")
def cleanup_cache_files():
    """
    Fixture that ensures cache files are cleaned up after tests.
    Use this when tests create cache files that need cleanup.
    """
    created_files = []

    def register_file(filepath):
        """Register a file for cleanup."""
        created_files.append(filepath)

    yield register_file

    # Cleanup after test
    for filepath in created_files:
        try:
            if os.path.isfile(filepath):
                os.remove(filepath)
            elif os.path.isdir(filepath):
                shutil.rmtree(filepath)
        except Exception:
            pass  # Ignore cleanup errors


@pytest.fixture
def someDict():
    """Fixture providing a simple dictionary for pass-by-reference tests."""
    return {
        'key1': 'value1',
        'key2': 'value2'
    }


# Hook to customize test collection
def pytest_collection_modifyitems(config, items):
    """
    Automatically add markers to tests based on their location or name.
    """
    for item in items:
        # Add 'spark' marker to tests with 'spark' in their name
        if "spark" in item.nodeid.lower():
            item.add_marker(pytest.mark.spark)

        # Add 'dask' marker to tests with 'dask' in their name
        if "dask" in item.nodeid.lower():
            item.add_marker(pytest.mark.dask)

        # Add 'integration' marker to integration test files
        if "integration" in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)

        # Add 'unit' marker to unit test files (default for Test/Tests/)
        if "/Test/Tests/" in item.nodeid or "/tests/unit/" in item.nodeid:
            item.add_marker(pytest.mark.unit)


# Configure test output
def pytest_configure(config):
    """
    Configure pytest with custom settings.
    """
    # Set custom terminal report header
    config.option.verbose = max(config.option.verbose, 1)

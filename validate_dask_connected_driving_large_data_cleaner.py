"""
Validation script for DaskConnectedDrivingLargeDataCleaner.

This script tests the Dask implementation of ConnectedDrivingLargeDataCleaner
to ensure it works correctly for large dataset processing.

Test cases:
1. Initialization test - verify all paths and configuration
2. Path validation test - _is_valid_parquet_directory() method
3. Mock clean_data() test - test cleaning logic with mock data
4. getNRows() test - verify row sampling
5. getNumOfRows() test - verify row counting
6. getAllRows() test - verify full dataset retrieval
7. combine_data() test - verify no-op behavior
8. Error handling test - verify proper exceptions
9. Method chaining test - verify fluent interface
10. Configuration integration test - verify dependency injection
"""

import os
import shutil
import tempfile
import dask.dataframe as dd
import pandas as pd
import numpy as np
from unittest.mock import Mock, MagicMock, patch

# Set up test environment
TEST_DIR = tempfile.mkdtemp(prefix="dask_large_cleaner_test_")
print(f"Test directory: {TEST_DIR}")

# Mock service providers
class MockGeneratorPathProvider:
    def __init__(self):
        self.paths = {
            "ConnectedDrivingLargeDataCleaner.cleanedfilespath": os.path.join(TEST_DIR, "cleaned/cleaned.parquet"),
            "ConnectedDrivingLargeDataCleaner.combinedcleandatapath": os.path.join(TEST_DIR, "combined/combined.parquet")
        }

    def getPathWithModelName(self, key):
        return self.paths.get(key, os.path.join(TEST_DIR, "default.parquet"))

class MockInitialGathererPathProvider:
    def __init__(self):
        self.paths = {
            "DataGatherer.splitfilespath": os.path.join(TEST_DIR, "split/split.parquet")
        }

    def getPathWithModelName(self, key):
        return self.paths.get(key, os.path.join(TEST_DIR, "default.parquet"))

class MockGeneratorContextProvider:
    def __init__(self):
        self.context = {
            "ConnectedDrivingCleaner.x_pos": -105.0,
            "ConnectedDrivingCleaner.y_pos": 39.5,
            "ConnectedDrivingLargeDataCleaner.max_dist": 500000,  # 500 km
            "ConnectedDrivingLargeDataCleaner.cleanerClass": Mock,
            "ConnectedDrivingLargeDataCleaner.cleanFunc": lambda x: x,
            "ConnectedDrivingLargeDataCleaner.filterFunc": None
        }

    def get(self, key, default=None):
        return self.context.get(key, default)

# Create test data
def create_test_data(num_rows=1000):
    """Create test BSM data."""
    np.random.seed(42)

    data = {
        'coreData_id': [f"ID_{i:06d}" for i in range(num_rows)],
        'coreData_position': [f"POINT ({-105 + np.random.randn()*0.1:.6f} {39 + np.random.randn()*0.1:.6f})"
                               for _ in range(num_rows)],
        'coreData_speed': np.random.randint(0, 120, num_rows),
        'coreData_heading': np.random.randint(0, 360, num_rows),
        'coreData_lat': 39 + np.random.randn(num_rows) * 0.1,
        'coreData_long': -105 + np.random.randn(num_rows) * 0.1,
        'timestamp': pd.date_range('2024-01-01', periods=num_rows, freq='1s')
    }

    return pd.DataFrame(data)

# Mock DaskSessionManager
class MockDaskSessionManager:
    @staticmethod
    def get_instance():
        mock_instance = Mock()
        mock_instance.get_client.return_value = Mock()
        return mock_instance

# Test 1: Initialization test
print("\n" + "="*70)
print("TEST 1: Initialization")
print("="*70)

# Mock the StandardDependencyInjection decorator to use our mock providers
with patch('Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner.DaskSessionManager', MockDaskSessionManager):
    with patch('Decorators.StandardDependencyInjection.SDI_DEPENDENCIES', {
        'IGeneratorPathProvider': MockGeneratorPathProvider,
        'IInitialGathererPathProvider': MockInitialGathererPathProvider,
        'IGeneratorContextProvider': MockGeneratorContextProvider,
        'IPathProvider': MockGeneratorPathProvider  # Logger needs this
    }):
        from Generator.Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner

        cleaner = DaskConnectedDrivingLargeDataCleaner()

    # Verify paths
    assert cleaner.splitfilespath == os.path.join(TEST_DIR, "split/split.parquet")
    assert cleaner.cleanedfilespath == os.path.join(TEST_DIR, "cleaned/cleaned.parquet")
    assert cleaner.combinedcleandatapath == os.path.join(TEST_DIR, "combined/combined.parquet")

    # Verify configuration
    assert cleaner.x_pos == -105.0
    assert cleaner.y_pos == 39.5
    assert cleaner.max_dist == 500000

    # Verify column names
    assert cleaner.pos_lat_col == "y_pos"
    assert cleaner.pos_long_col == "x_pos"
    assert cleaner.x_col == "x_pos"
    assert cleaner.y_col == "y_pos"

    print("✓ Cleaner initialized successfully")
    print(f"  - splitfilespath: {cleaner.splitfilespath}")
    print(f"  - cleanedfilespath: {cleaner.cleanedfilespath}")
    print(f"  - combinedcleandatapath: {cleaner.combinedcleandatapath}")
    print(f"  - x_pos: {cleaner.x_pos}, y_pos: {cleaner.y_pos}")
    print(f"  - max_dist: {cleaner.max_dist}")

# Test 2: Path validation test
print("\n" + "="*70)
print("TEST 2: Path Validation (_is_valid_parquet_directory)")
print("="*70)

# Create valid Parquet directory
valid_parquet_dir = os.path.join(TEST_DIR, "valid_parquet")
os.makedirs(valid_parquet_dir, exist_ok=True)

# Create some .parquet files
test_df = create_test_data(100)
dask_df = dd.from_pandas(test_df, npartitions=2)
dask_df.to_parquet(valid_parquet_dir, engine='pyarrow', compression='snappy', write_index=False)

# Test valid directory
assert cleaner._is_valid_parquet_directory(valid_parquet_dir) == True
print(f"✓ Valid Parquet directory detected: {valid_parquet_dir}")

# Test invalid directory (empty)
empty_dir = os.path.join(TEST_DIR, "empty_dir")
os.makedirs(empty_dir, exist_ok=True)
assert cleaner._is_valid_parquet_directory(empty_dir) == False
print(f"✓ Empty directory rejected: {empty_dir}")

# Test non-existent directory
non_existent = os.path.join(TEST_DIR, "does_not_exist")
assert cleaner._is_valid_parquet_directory(non_existent) == False
print(f"✓ Non-existent directory rejected: {non_existent}")

# Test file (not directory)
test_file = os.path.join(TEST_DIR, "test_file.txt")
with open(test_file, 'w') as f:
    f.write("test")
assert cleaner._is_valid_parquet_directory(test_file) == False
print(f"✓ File rejected (not a directory): {test_file}")

# Test 3: getNRows() test
print("\n" + "="*70)
print("TEST 3: getNRows() Method")
print("="*70)

# Create combined data for testing
combined_dir = cleaner.combinedcleandatapath
os.makedirs(os.path.dirname(combined_dir), exist_ok=True)

test_df_large = create_test_data(1000)
dask_df_large = dd.from_pandas(test_df_large, npartitions=5)
dask_df_large.to_parquet(combined_dir, engine='pyarrow', compression='snappy', write_index=False)

# Test getNRows()
n_rows = 100
df_sample = cleaner.getNRows(n_rows)
assert isinstance(df_sample, pd.DataFrame)
assert len(df_sample) == n_rows
print(f"✓ getNRows({n_rows}) returned {len(df_sample)} rows")
print(f"  - Sample columns: {list(df_sample.columns)[:5]}")
print(f"  - Sample IDs: {df_sample['coreData_id'].head(3).tolist()}")

# Test 4: getNumOfRows() test
print("\n" + "="*70)
print("TEST 4: getNumOfRows() Method")
print("="*70)

total_rows = cleaner.getNumOfRows()
assert total_rows == 1000
print(f"✓ getNumOfRows() returned {total_rows} rows (expected 1000)")

# Test 5: getAllRows() test
print("\n" + "="*70)
print("TEST 5: getAllRows() Method")
print("="*70)

df_all = cleaner.getAllRows()
assert isinstance(df_all, dd.DataFrame)
row_count = len(df_all)
assert row_count == 1000
print(f"✓ getAllRows() returned Dask DataFrame with {row_count} rows")
print(f"  - Type: {type(df_all)}")
print(f"  - Partitions: {df_all.npartitions}")

# Test 6: combine_data() test
print("\n" + "="*70)
print("TEST 6: combine_data() Method (No-op)")
print("="*70)

# Should be no-op since data already exists
result = cleaner.combine_data()
assert result is cleaner  # Method chaining
print("✓ combine_data() executed (no-op for Dask)")
print("  - Returned self for method chaining")

# Test 7: Error handling test
print("\n" + "="*70)
print("TEST 7: Error Handling")
print("="*70)

# Create new cleaner with non-existent data (reuse existing mocks)
cleaner_no_data = cleaner  # Use same cleaner instance

# Override combinedcleandatapath to non-existent path
cleaner_no_data.combinedcleandatapath = os.path.join(TEST_DIR, "nonexistent/data.parquet")

# Test FileNotFoundError for getNRows()
try:
    cleaner_no_data.getNRows(10)
    assert False, "Expected FileNotFoundError"
except FileNotFoundError as e:
    assert "Combined cleaned data not found" in str(e)
    print(f"✓ getNRows() raised FileNotFoundError: {str(e)[:60]}...")

# Test FileNotFoundError for getNumOfRows()
try:
    cleaner_no_data.getNumOfRows()
    assert False, "Expected FileNotFoundError"
except FileNotFoundError as e:
    assert "Combined cleaned data not found" in str(e)
    print(f"✓ getNumOfRows() raised FileNotFoundError: {str(e)[:60]}...")

# Test FileNotFoundError for getAllRows()
try:
    cleaner_no_data.getAllRows()
    assert False, "Expected FileNotFoundError"
except FileNotFoundError as e:
    assert "Combined cleaned data not found" in str(e)
    print(f"✓ getAllRows() raised FileNotFoundError: {str(e)[:60]}...")

# Test 8: Method chaining test
print("\n" + "="*70)
print("TEST 8: Method Chaining")
print("="*70)

result = cleaner.combine_data()
assert result is cleaner
print("✓ combine_data() returns self for method chaining")

# Test 9: clean_data() with mock cleaner
print("\n" + "="*70)
print("TEST 9: clean_data() with Mock Cleaner")
print("="*70)

# Create mock cleaner class
class MockCleaner:
    def __init__(self, data):
        self.data = data
        self.cleaned = data  # Mock cleaning - just return data as-is

    def get_cleaned_data(self):
        return self.cleaned

# Create test split data
split_dir = cleaner.splitfilespath
os.makedirs(os.path.dirname(split_dir), exist_ok=True)

test_split_df = create_test_data(500)
dask_split_df = dd.from_pandas(test_split_df, npartitions=3)
dask_split_df.to_parquet(split_dir, engine='pyarrow', compression='snappy', write_index=False)

# Update cleaner configuration
cleaner.cleanerClass = MockCleaner
cleaner.cleanFunc = lambda c: c
cleaner.filterFunc = None

# Remove existing combined data to force clean_data() to run
if os.path.exists(cleaner.combinedcleandatapath):
    shutil.rmtree(cleaner.combinedcleandatapath)

# Run clean_data()
result = cleaner.clean_data()
assert result is cleaner  # Method chaining
assert os.path.exists(cleaner.combinedcleandatapath)
assert cleaner._is_valid_parquet_directory(cleaner.combinedcleandatapath)
print("✓ clean_data() executed successfully")
print(f"  - Created cleaned data at: {cleaner.combinedcleandatapath}")
print(f"  - Method chaining works (returned self)")

# Verify cleaned data
df_cleaned = dd.read_parquet(cleaner.combinedcleandatapath)
cleaned_row_count = len(df_cleaned)
print(f"  - Cleaned data has {cleaned_row_count} rows (expected 500)")
assert cleaned_row_count == 500

# Test 10: clean_data() skip if exists
print("\n" + "="*70)
print("TEST 10: clean_data() Skip If Exists")
print("="*70)

# Run clean_data() again - should skip
result = cleaner.clean_data()
assert result is cleaner
print("✓ clean_data() skipped regeneration (data already exists)")
print("  - Check logs for 'Found cleaned data! Skipping regeneration.'")

# Summary
print("\n" + "="*70)
print("VALIDATION SUMMARY")
print("="*70)
print("✓ All 10 tests passed!")
print("\nTests completed:")
print("  1. ✓ Initialization")
print("  2. ✓ Path validation (_is_valid_parquet_directory)")
print("  3. ✓ getNRows() method")
print("  4. ✓ getNumOfRows() method")
print("  5. ✓ getAllRows() method")
print("  6. ✓ combine_data() method (no-op)")
print("  7. ✓ Error handling (FileNotFoundError)")
print("  8. ✓ Method chaining")
print("  9. ✓ clean_data() with mock cleaner")
print(" 10. ✓ clean_data() skip if exists")

print("\n" + "="*70)
print("DaskConnectedDrivingLargeDataCleaner validation PASSED ✓")
print("="*70)

# Cleanup
print(f"\nCleaning up test directory: {TEST_DIR}")
shutil.rmtree(TEST_DIR)
print("Done!")

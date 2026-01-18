# Pytest Migration Guide

## Overview

This guide explains how to migrate tests from the custom `ITest` framework to pytest.

## Migration Strategy

### Before (ITest Custom Framework)

```python
from Test.ITest import ITest

class TestFileCache(ITest):

    def run(self):
        # Test code here
        assert(some_condition)

    def cleanup(self):
        # Cleanup code here
        os.remove("temp_file.txt")
```

### After (Pytest)

```python
import pytest

class TestFileCache:

    def test_cache_functionality(self):
        # Test code here
        assert some_condition

    @pytest.fixture(autouse=True)
    def cleanup(self):
        # Setup code (before yield)
        yield
        # Cleanup code (after yield)
        os.remove("temp_file.txt")
```

## Key Differences

### 1. No Base Class Required
- **ITest**: Requires inheriting from `ITest`
- **Pytest**: No inheritance needed, just name the class `Test*`

### 2. Test Method Naming
- **ITest**: Single `run()` method contains all tests
- **Pytest**: Multiple `test_*()` methods, one per test case

### 3. Setup and Teardown
- **ITest**: `cleanup()` method for teardown
- **Pytest**: Use fixtures with `yield` for setup/teardown

### 4. Assertions
- **ITest**: Uses `assert(condition)` (function call style)
- **Pytest**: Uses `assert condition` (statement style, with rich introspection)

## Step-by-Step Migration

### Step 1: Update Test File Structure

**Before:**
```python
from Test.ITest import ITest

class TestMyComponent(ITest):
    def run(self):
        # All tests in one method
        assert(self.test_case_1())
        assert(self.test_case_2())

    def cleanup(self):
        # Cleanup logic
```

**After:**
```python
import pytest

class TestMyComponent:
    def test_case_1(self):
        # Test case 1
        assert result == expected

    def test_case_2(self):
        # Test case 2
        assert result == expected

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        # Setup code here (optional)
        yield
        # Cleanup code here
```

### Step 2: Break Up Single `run()` Method

Split the monolithic `run()` method into individual test methods:

**Before:**
```python
def run(self):
    # Test 1
    assert(self.some_function(1, 2) == 3)

    # Test 2
    assert(self.some_function(1, 3) == 4)

    # Test 3
    assert(os.path.exists("cache/file.txt"))
```

**After:**
```python
def test_addition_simple(self):
    assert self.some_function(1, 2) == 3

def test_addition_different_values(self):
    assert self.some_function(1, 3) == 4

def test_cache_file_created(self):
    assert os.path.exists("cache/file.txt")
```

### Step 3: Convert Cleanup to Fixtures

**Before:**
```python
def cleanup(self):
    try:
        os.remove("cache/file1.txt")
        os.remove("cache/file2.txt")
    except FileNotFoundError:
        pass
```

**After:**
```python
@pytest.fixture(autouse=True)
def cleanup_cache_files(self):
    """Fixture to clean up cache files after each test."""
    yield  # Test runs here

    # Cleanup after test
    for filepath in ["cache/file1.txt", "cache/file2.txt"]:
        try:
            os.remove(filepath)
        except FileNotFoundError:
            pass
```

### Step 4: Use Pytest Fixtures from conftest.py

Pytest provides shared fixtures in `conftest.py`:

```python
def test_with_temp_directory(temp_dir):
    """Use the temp_dir fixture from conftest.py."""
    test_file = os.path.join(temp_dir, "test.txt")
    with open(test_file, "w") as f:
        f.write("test data")
    assert os.path.exists(test_file)
    # No cleanup needed - temp_dir is auto-cleaned
```

## Example: Full Migration

### Before (ITest)

```python
# Test/Tests/TestFileCache.py
from Test.ITest import ITest
import os
import hashlib

class TestFileCache(ITest):

    @FileCache
    def some_function(self, a, b) -> int:
        return a + b

    def run(self):
        # Test basic caching
        assert(self.some_function(1, 2) == 3)
        assert(self.some_function(1, 2) == 3)

        # Test different inputs
        assert(self.some_function(1, 3) == 4)

        # Test cache file exists
        cache_file = f"cache/model/{hashlib.md5('some_function_1_2'.encode()).hexdigest()}.txt"
        assert(os.path.exists(cache_file))

    def cleanup(self):
        try:
            os.remove(f"cache/model/{hashlib.md5('some_function_1_2'.encode()).hexdigest()}.txt")
            os.remove(f"cache/model/{hashlib.md5('some_function_1_3'.encode()).hexdigest()}.txt")
        except FileNotFoundError:
            pass
```

### After (Pytest)

```python
# Test/test_file_cache.py
import pytest
import os
import hashlib
from Decorators.FileCache import FileCache

class TestFileCache:
    """Tests for FileCache decorator."""

    @FileCache
    def some_function(self, a, b) -> int:
        return a + b

    @pytest.fixture(autouse=True)
    def cleanup_cache(self):
        """Clean up cache files after each test."""
        yield

        # Cleanup
        cache_files = [
            f"cache/model/{hashlib.md5('some_function_1_2'.encode()).hexdigest()}.txt",
            f"cache/model/{hashlib.md5('some_function_1_3'.encode()).hexdigest()}.txt"
        ]
        for filepath in cache_files:
            try:
                os.remove(filepath)
            except FileNotFoundError:
                pass

    def test_basic_caching(self):
        """Test that function results are cached correctly."""
        result1 = self.some_function(1, 2)
        result2 = self.some_function(1, 2)

        assert result1 == 3
        assert result2 == 3

    def test_different_inputs_different_cache(self):
        """Test that different inputs create different cache entries."""
        result1 = self.some_function(1, 2)
        result2 = self.some_function(1, 3)

        assert result1 == 3
        assert result2 == 4

    def test_cache_file_created(self):
        """Test that cache file is created on disk."""
        self.some_function(1, 2)

        cache_file = f"cache/model/{hashlib.md5('some_function_1_2'.encode()).hexdigest()}.txt"
        assert os.path.exists(cache_file)
```

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Specific Test File
```bash
pytest Test/test_file_cache.py
```

### Run Specific Test
```bash
pytest Test/test_file_cache.py::TestFileCache::test_basic_caching
```

### Run Tests with Markers
```bash
# Run only unit tests
pytest -m unit

# Run only Spark tests
pytest -m spark

# Run everything except slow tests
pytest -m "not slow"
```

### Run Tests with Coverage
```bash
pytest --cov=. --cov-report=html
```

## Pytest Features to Use

### 1. Parametrize for Multiple Test Cases
```python
@pytest.mark.parametrize("a,b,expected", [
    (1, 2, 3),
    (5, 5, 10),
    (0, 0, 0),
    (-1, 1, 0),
])
def test_addition_parametrized(a, b, expected):
    assert some_function(a, b) == expected
```

### 2. Fixtures for Shared Setup
```python
@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    import pandas as pd
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

def test_dataframe_processing(sample_dataframe):
    result = process_data(sample_dataframe)
    assert len(result) == 3
```

### 3. Markers for Test Organization
```python
@pytest.mark.slow
def test_large_dataset_processing():
    # This test takes a long time
    pass

@pytest.mark.integration
def test_end_to_end_pipeline():
    # Integration test
    pass
```

### 4. Exception Testing
```python
def test_invalid_input_raises_error():
    with pytest.raises(ValueError, match="Invalid input"):
        process_invalid_data()
```

### 5. Temporary Files and Directories
```python
def test_file_creation(tmp_path):
    """pytest provides tmp_path fixture for temporary directories."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello, World!")
    assert test_file.read_text() == "Hello, World!"
```

## Migration Checklist

For each test file:

- [ ] Remove `from Test.ITest import ITest`
- [ ] Remove `: ITest` inheritance
- [ ] Rename `run()` method to multiple `test_*()` methods
- [ ] Convert `cleanup()` to a pytest fixture with `yield`
- [ ] Update assertions from `assert(condition)` to `assert condition`
- [ ] Add appropriate pytest markers (`@pytest.mark.unit`, etc.)
- [ ] Add docstrings to test methods
- [ ] Move shared fixtures to `conftest.py` if used in multiple files
- [ ] Update test file name to `test_*.py` convention (optional but recommended)
- [ ] Run tests with pytest to verify migration
- [ ] Update test coverage to 70%+ if needed

## Benefits of Pytest

1. **Better Test Discovery**: Automatically finds all `test_*.py` files
2. **Rich Assertions**: Detailed output when assertions fail
3. **Fixtures**: Powerful dependency injection for test setup/teardown
4. **Parametrization**: Run same test with multiple inputs easily
5. **Markers**: Organize and filter tests by category
6. **Plugins**: Extensive ecosystem (pytest-cov, pytest-spark, etc.)
7. **Parallel Execution**: Run tests faster with pytest-xdist
8. **Industry Standard**: Most Python projects use pytest

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Pytest Fixtures](https://docs.pytest.org/en/stable/fixture.html)
- [Pytest Parametrize](https://docs.pytest.org/en/stable/parametrize.html)
- [Pytest Markers](https://docs.pytest.org/en/stable/mark.html)
- [pytest-spark Plugin](https://github.com/malexer/pytest-spark)

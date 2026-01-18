# Test Directory

## Overview

This directory contains all tests for the ConnectedDrivingPipelineV4 project. The project is currently migrating from a custom `ITest` framework to pytest.

## Test Structure

```
Test/
├── README.md                    # This file
├── PYTEST_MIGRATION_GUIDE.md   # Guide for migrating from ITest to pytest
├── ITest.py                     # Legacy custom test framework (deprecated)
├── TestSchemaValidator.py       # Modern unittest-based test (example)
├── Tests/                       # Legacy ITest-based tests (to be migrated)
│   ├── TestFileCache.py
│   ├── TestCSVCache.py
│   ├── TestChildContextProviders.py
│   ├── TestPathProviders.py
│   ├── TestLocationDiff.py
│   ├── TestPassByRef.py
│   ├── TestStandardDependencyInjection.py
│   ├── TestDictProvider.py
│   ├── TestKeyProvider.py
│   └── TestPathProvider.py
└── Fixtures/                    # Pytest fixtures (to be created)
    └── SparkFixtures.py         # PySpark test fixtures (planned)
```

## Running Tests

### With Pytest (Recommended)

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=. --cov-report=html

# Run specific test file
pytest Test/TestSchemaValidator.py

# Run specific test class
pytest Test/TestSchemaValidator.py::TestSchemaValidator

# Run specific test method
pytest Test/TestSchemaValidator.py::TestSchemaValidator::test_exact_schema_match

# Run tests by marker
pytest -m unit          # Run only unit tests
pytest -m spark         # Run only Spark tests
pytest -m integration   # Run only integration tests
pytest -m "not slow"    # Skip slow tests
```

### With Legacy ITest Framework (Deprecated)

The old test runner is still available but deprecated:

```python
from Test.Tests.TestFileCache import TestFileCache

test = TestFileCache()
test.run()
test.cleanup()
```

## Test Categories

Tests are organized by markers (see `pytest.ini`):

- **unit**: Unit tests for individual components
- **integration**: Integration tests for end-to-end workflows
- **spark**: Tests that require SparkSession
- **pandas**: Tests for pandas-based legacy code
- **pyspark**: Tests for PySpark migration code
- **cache**: Tests for caching decorators
- **schema**: Tests for schema validation
- **udf**: Tests for PySpark UDFs
- **attack**: Tests for attack simulation
- **ml**: Tests for machine learning pipeline
- **filter**: Tests for data filtering operations
- **cleaner**: Tests for data cleaning operations
- **slow**: Tests that take significant time to run

## Test Coverage Goals

- **Current**: ~15-20% code coverage
- **Target**: 70%+ code coverage
- **Priority Areas**:
  - Core cleaners (ConnectedDrivingCleaner, etc.)
  - Attack simulation (StandardPositionalOffsetAttacker, etc.)
  - ML pipeline (MDataClassifier, MClassifierPipeline)
  - PySpark UDFs (when implemented)

## Writing New Tests

### Use Pytest (Not ITest)

All new tests should use pytest. See `PYTEST_MIGRATION_GUIDE.md` for details.

**Example test file structure:**

```python
# Test/test_my_component.py
import pytest

class TestMyComponent:
    """Tests for MyComponent."""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Setup before each test, cleanup after."""
        # Setup code here (optional)
        yield
        # Cleanup code here (optional)

    def test_basic_functionality(self):
        """Test basic functionality."""
        result = my_function(input_data)
        assert result == expected_output

    @pytest.mark.parametrize("input,expected", [
        (1, 2),
        (5, 10),
        (0, 0),
    ])
    def test_multiple_inputs(self, input, expected):
        """Test with multiple input values."""
        assert my_function(input) == expected
```

## Shared Fixtures

Common fixtures are available in `conftest.py` at the project root:

- `project_root_path`: Path to project root directory
- `temp_dir`: Temporary directory (auto-cleaned after test)
- `cache_dir`: Temporary cache directory with CACHE_DIR env var set
- `sample_bsm_data_dict`: Sample BSM data dictionary for testing
- `cleanup_cache_files`: Helper to register files for cleanup

## Migration Status

### Completed
- ✅ Pytest configuration (`pytest.ini`)
- ✅ Coverage configuration (`.coveragerc`)
- ✅ Shared fixtures (`conftest.py`)
- ✅ Migration guide (`PYTEST_MIGRATION_GUIDE.md`)
- ✅ One modern test file (`TestSchemaValidator.py` - using unittest)

### In Progress
- 🔄 Migrating 10 legacy ITest tests to pytest

### Planned
- ⏳ PySpark test fixtures (`Test/Fixtures/SparkFixtures.py`)
- ⏳ Sample test datasets (`Test/Data/`)
- ⏳ DataFrame comparison utilities (`Test/Utils/DataFrameComparator.py`)
- ⏳ Integration tests for PySpark pipeline
- ⏳ UDF unit tests
- ⏳ Attack simulation tests
- ⏳ ML pipeline integration tests

## Contributing

When adding new tests:

1. Use pytest framework (not ITest)
2. Follow naming convention: `test_*.py` for files, `test_*()` for methods
3. Add appropriate markers (`@pytest.mark.unit`, `@pytest.mark.spark`, etc.)
4. Include docstrings for test classes and methods
5. Use fixtures for setup/teardown instead of `setUp()`/`tearDown()`
6. Aim for 70%+ code coverage
7. Run tests locally before committing: `pytest -v`

## Troubleshooting

### Tests Not Found
- Ensure test files match naming pattern: `test_*.py` or `*_test.py` or `Test*.py`
- Ensure test methods start with `test_`
- Check that test files are in the `Test/` directory

### Import Errors
- Ensure project root is in Python path (conftest.py handles this)
- Check that all dependencies are installed: `pip install -r requirements.txt`

### Spark Tests Failing
- Ensure Java 8 or 11 is installed (required for PySpark)
- Check `JAVA_HOME` environment variable is set
- Use `pytest -m "not spark"` to skip Spark tests if needed

### Coverage Report Not Generated
- Ensure pytest-cov is installed: `pip install pytest-cov`
- Check `.coveragerc` configuration
- Run with: `pytest --cov=. --cov-report=html`

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [pytest-spark Plugin](https://github.com/malexer/pytest-spark)
- [pytest-cov Plugin](https://pytest-cov.readthedocs.io/)
- [Migration Guide](PYTEST_MIGRATION_GUIDE.md)

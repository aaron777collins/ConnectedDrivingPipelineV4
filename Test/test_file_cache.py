import os
import hashlib
import pytest
from Decorators.FileCache import FileCache
from ServiceProviders.DictProvider import DictProvider
from ServiceProviders.PathProvider import PathProvider


class TestFileCache:
    """Test suite for FileCache decorator functionality.

    Migrated from legacy ITest framework to pytest.
    Tests basic caching, cache key generation, and file persistence.
    """

    @FileCache
    def cached_function(self, a, b) -> int:
        """Simple cached function for testing."""
        return a + b

    def cached_function_with_variables(self, a, b) -> int:
        """Cached function using explicit cache_variables parameter."""
        return self._cached_function_impl(a, b, cache_variables=[a, b])

    @FileCache
    def _cached_function_impl(self, a, b, cache_variables=["REPLACE ME"]) -> int:
        """Implementation of cached function with explicit cache variables."""
        return a + b

    @pytest.fixture(autouse=True)
    def cleanup_cache_files(self):
        """Fixture to clean up cache files after each test."""
        yield
        # Cleanup after test
        cache_files = [
            f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('cached_function_1_2'.encode()).hexdigest()}.txt",
            f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('cached_function_1_3'.encode()).hexdigest()}.txt",
            f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_cached_function_impl_1_2'.encode()).hexdigest()}.txt",
            f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_cached_function_impl_1_3'.encode()).hexdigest()}.txt",
        ]

        for cache_file in cache_files:
            try:
                if os.path.exists(cache_file):
                    os.remove(cache_file)
            except FileNotFoundError:
                pass

        PathProvider.clear()

    def test_basic_caching_returns_correct_value(self):
        """Test that FileCache correctly returns the computed value."""
        result = self.cached_function(1, 2)
        assert result == 3

    def test_cached_value_is_reused(self):
        """Test that calling the same cached function twice returns the same result."""
        first_call = self.cached_function(1, 2)
        second_call = self.cached_function(1, 2)
        assert first_call == second_call == 3

    def test_different_inputs_produce_different_results(self):
        """Test that cache correctly handles different input parameters."""
        result1 = self.cached_function(1, 3)
        result2 = self.cached_function(1, 2)
        assert result1 == 4
        assert result2 == 3

    def test_cache_file_created_on_disk(self):
        """Test that cache files are actually created at expected locations."""
        self.cached_function(1, 2)
        cache_path = f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('cached_function_1_2'.encode()).hexdigest()}.txt"
        assert os.path.exists(cache_path), f"Cache file should exist at {cache_path}"

    def test_multiple_cache_files_created(self):
        """Test that multiple calls with different parameters create multiple cache files."""
        self.cached_function(1, 2)
        self.cached_function(1, 3)

        cache_path1 = f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('cached_function_1_2'.encode()).hexdigest()}.txt"
        cache_path2 = f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('cached_function_1_3'.encode()).hexdigest()}.txt"

        assert os.path.exists(cache_path1), "First cache file should exist"
        assert os.path.exists(cache_path2), "Second cache file should exist"

    def test_cache_with_explicit_variables_returns_correct_value(self):
        """Test caching with explicit cache_variables parameter."""
        result = self.cached_function_with_variables(1, 2)
        assert result == 3

    def test_cache_with_explicit_variables_is_reused(self):
        """Test that explicit cache_variables correctly enables caching."""
        first_call = self.cached_function_with_variables(1, 2)
        second_call = self.cached_function_with_variables(1, 2)
        assert first_call == second_call == 3

    def test_cache_with_explicit_variables_handles_different_inputs(self):
        """Test that explicit cache_variables works with different parameters."""
        result1 = self.cached_function_with_variables(1, 3)
        result2 = self.cached_function_with_variables(1, 2)
        assert result1 == 4
        assert result2 == 3

    def test_cache_with_explicit_variables_creates_file(self):
        """Test that cache files are created when using explicit cache_variables."""
        self.cached_function_with_variables(1, 2)
        cache_path = f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_cached_function_impl_1_2'.encode()).hexdigest()}.txt"
        assert os.path.exists(cache_path), f"Cache file should exist at {cache_path}"

    def test_cache_with_explicit_variables_multiple_files(self):
        """Test that multiple cache files are created with explicit cache_variables."""
        self.cached_function_with_variables(1, 2)
        self.cached_function_with_variables(1, 3)

        cache_path1 = f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_cached_function_impl_1_2'.encode()).hexdigest()}.txt"
        cache_path2 = f"cache/{PathProvider.DEFAULT_MODEL_NAME}/{hashlib.md5('_cached_function_impl_1_3'.encode()).hexdigest()}.txt"

        assert os.path.exists(cache_path1), "First cache file with explicit variables should exist"
        assert os.path.exists(cache_path2), "Second cache file with explicit variables should exist"

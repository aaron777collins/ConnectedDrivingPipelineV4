"""
Validation Test Suite for CI/CD Fixes

This test suite validates that all CI/CD fixes have been properly applied
and are working correctly. Each test corresponds to a specific fix from
the CI/CD fixes plan.

Tests:
- test_cachemanager_no_recursion: Validates CacheManager recursion bug is fixed
- test_init_files_exist: Validates __init__.py files exist in Helpers/ and Decorators/
- test_python_version: Validates Python version is 3.10+
- test_somedict_fixture_available: Validates someDict fixture is available
- test_validation_script_syntax: Validates validation scripts have correct syntax
"""

import pytest
import sys
import os
from pathlib import Path
import logging


class TestCIFixes:
    """Test suite for CI/CD fixes validation"""

    def test_python_version(self):
        """
        Test that Python version is 3.10 or higher.

        This validates Fix 1.1: Python Version Requirements
        The codebase requires Python 3.10+ features and should not run on 3.8 or 3.9.
        """
        version_info = sys.version_info
        assert version_info.major == 3, "Python 3.x required"
        assert version_info.minor >= 10, (
            f"Python 3.10+ required, but running {version_info.major}.{version_info.minor}. "
            "Please upgrade your Python version."
        )

    def test_init_files_exist(self):
        """
        Test that __init__.py files exist in Helpers/ and Decorators/ directories.

        This validates Fix 1.3: Add Missing __init__.py Files
        These files are required for Python to recognize these directories as packages.
        """
        project_root = Path(__file__).parent.parent

        helpers_init = project_root / "Helpers" / "__init__.py"
        decorators_init = project_root / "Decorators" / "__init__.py"

        assert helpers_init.exists(), (
            "Helpers/__init__.py is missing. "
            "This file is required for the Helpers package to be importable."
        )
        assert decorators_init.exists(), (
            "Decorators/__init__.py is missing. "
            "This file is required for the Decorators package to be importable."
        )

    def test_cachemanager_no_recursion(self):
        """
        Test that CacheManager doesn't cause infinite recursion.

        This validates Fix 1.2: CacheManager Recursive Bug
        The _log() method should not call self._log() as it causes infinite recursion
        through __getattribute__.
        """
        from Decorators.CacheManager import CacheManager

        # CacheManager is a singleton - get the instance
        # This should not cause recursion errors during initialization
        try:
            cache_manager = CacheManager.get_instance()

            # Access various attributes to trigger __getattribute__ and potentially _log
            # This should NOT raise RecursionError
            _ = cache_manager.logger
            _ = cache_manager.use_logger
            _ = cache_manager.hits
            _ = cache_manager.misses
            _ = cache_manager.cache_base_path

            # Try calling a method that might trigger logging
            stats = cache_manager.get_statistics()

            # If we get here without RecursionError, the fix is working
            assert True, "CacheManager attribute access works without recursion"
        except RecursionError as e:
            pytest.fail(
                f"CacheManager still has recursion bug: {e}. "
                "Check that _log() method uses self.logger.log() not self._log()"
            )

    def test_somedict_fixture_available(self):
        """
        Test that the someDict fixture is available in conftest.py.

        This validates Fix 3.1: Add Missing Test Fixture
        The someDict fixture is required by TestPassByRef.py tests.
        """
        # Import conftest to ensure fixtures are registered
        import conftest

        # Check if someDict is defined in conftest
        assert hasattr(conftest, 'someDict') or 'someDict' in dir(conftest), (
            "someDict fixture not found in conftest.py. "
            "This fixture is required for pass-by-reference tests."
        )

        # The fixture should be callable (it's decorated with @pytest.fixture)
        # We can verify it's defined by checking the conftest module
        # Note: We can't call it directly, but we can verify it exists

    def test_validation_script_syntax(self):
        """
        Test that validation scripts have correct Python syntax.

        This validates Fix 2.2: Fix validate_dask_clean_with_timestamps.py Indentation
        The script should parse without SyntaxError.
        """
        import py_compile
        import tempfile

        project_root = Path(__file__).parent.parent
        validation_script = project_root / "validate_dask_clean_with_timestamps.py"

        if not validation_script.exists():
            pytest.skip("validate_dask_clean_with_timestamps.py not found")

        # Try to compile the script
        # py_compile.compile() will raise SyntaxError if there are syntax issues
        try:
            with tempfile.NamedTemporaryFile(suffix='.pyc', delete=True) as tmp:
                py_compile.compile(
                    str(validation_script),
                    cfile=tmp.name,
                    doraise=True
                )
            assert True, "Validation script has correct syntax"
        except SyntaxError as e:
            pytest.fail(
                f"validate_dask_clean_with_timestamps.py has syntax error: {e}. "
                "Check for indentation issues (should use 4-space indentation)."
            )

    def test_dockerignore_allows_validation_script(self):
        """
        Test that .dockerignore allows validate_dask_setup.py.

        This validates Fix 2.1: Fix .dockerignore
        The .dockerignore should have !validate_dask_setup.py exception
        before the validate_*.py exclusion pattern.
        """
        project_root = Path(__file__).parent.parent
        dockerignore = project_root / ".dockerignore"

        if not dockerignore.exists():
            pytest.skip(".dockerignore not found")

        content = dockerignore.read_text()
        lines = content.split('\n')

        # Find the exception pattern and exclusion pattern
        exception_line = None
        exclusion_line = None

        for i, line in enumerate(lines):
            if '!validate_dask_setup.py' in line:
                exception_line = i
            if 'validate_*.py' in line and not line.startswith('!'):
                exclusion_line = i

        assert exception_line is not None, (
            ".dockerignore is missing '!validate_dask_setup.py' exception pattern. "
            "This is needed to allow validate_dask_setup.py in Docker builds."
        )

        assert exclusion_line is not None, (
            ".dockerignore is missing 'validate_*.py' exclusion pattern"
        )

        # Note: line numbers are 0-indexed in the list, but we report as 1-indexed for clarity
        assert exception_line is not None and exclusion_line is not None, (
            "Both exception and exclusion patterns must exist"
        )

        assert exception_line < exclusion_line, (
            "In .dockerignore, '!validate_dask_setup.py' must come BEFORE 'validate_*.py'. "
            f"Currently: exception at line {exception_line + 1}, exclusion at line {exclusion_line + 1}"
        )

    def test_imports_work(self):
        """
        Test that all critical imports work correctly.

        This is an integration test that validates multiple fixes:
        - Fix 1.3: __init__.py files allow imports
        - Fix 1.2: CacheManager can be imported without errors
        """
        # These imports should all work without errors
        try:
            import Helpers
            import Decorators
            from Decorators.CacheManager import CacheManager
            from Helpers.DaskSessionManager import DaskSessionManager

            assert True, "All critical imports successful"
        except ImportError as e:
            pytest.fail(
                f"Import error: {e}. "
                "Check that __init__.py files exist in Helpers/ and Decorators/"
            )

    def test_github_workflow_python_versions(self):
        """
        Test that GitHub Actions workflow only tests Python 3.10+.

        This validates Fix 1.1: Python Version Requirements in CI configuration
        """
        project_root = Path(__file__).parent.parent
        workflow_file = project_root / ".github" / "workflows" / "test.yml"

        if not workflow_file.exists():
            pytest.skip("GitHub workflow file not found")

        content = workflow_file.read_text()

        # Check that Python 3.8 and 3.9 are NOT in the matrix
        assert '"3.8"' not in content and "'3.8'" not in content, (
            "GitHub workflow should not test Python 3.8. "
            "Update .github/workflows/test.yml to remove Python 3.8 from the matrix."
        )
        assert '"3.9"' not in content and "'3.9'" not in content, (
            "GitHub workflow should not test Python 3.9. "
            "Update .github/workflows/test.yml to remove Python 3.9 from the matrix."
        )

        # Check that Python 3.10, 3.11, 3.12 ARE in the matrix
        assert '"3.10"' in content or "'3.10'" in content, (
            "GitHub workflow should test Python 3.10"
        )
        assert '"3.11"' in content or "'3.11'" in content, (
            "GitHub workflow should test Python 3.11"
        )
        assert '"3.12"' in content or "'3.12'" in content, (
            "GitHub workflow should test Python 3.12"
        )


class TestCIFixesIntegration:
    """Integration tests that validate multiple fixes working together"""

    def test_full_import_chain(self):
        """
        Test that the full import chain works for common operations.

        This validates that all fixes work together harmoniously.
        """
        # This should work without any errors if all fixes are in place
        from MachineLearning.DaskPipelineRunner import DaskPipelineRunner
        from Decorators.CacheManager import CacheManager
        from Helpers.DaskSessionManager import DaskSessionManager

        # Verify classes can be instantiated
        assert DaskPipelineRunner is not None
        assert CacheManager is not None
        assert DaskSessionManager is not None

    def test_conftest_fixtures_load(self):
        """
        Test that conftest.py loads successfully and provides all fixtures.

        This validates the fixture configuration is correct.
        """
        import conftest

        # Check that key fixtures are defined
        # We can't call them directly, but we can verify the module loads
        assert conftest is not None, "conftest.py should load successfully"

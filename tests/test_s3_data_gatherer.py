"""
Tests for S3DataGatherer adapter.

Tests cover:
- Adapter initialization with config
- IDataGatherer interface implementation
- Configuration mapping from context providers
- Integration with S3DataFetcher
"""

import sys
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from datetime import date

import pytest

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from Gatherer.S3DataGatherer import S3DataGatherer
from Gatherer.IDataGatherer import IDataGatherer


class TestS3DataGathererInterface:
    """Test that S3DataGatherer properly implements IDataGatherer."""

    def test_implements_idatagatherer(self):
        """Test that S3DataGatherer is a subclass of IDataGatherer."""
        assert issubclass(S3DataGatherer, IDataGatherer)

    def test_has_required_methods(self):
        """Test that S3DataGatherer has all required methods."""
        required_methods = ['gather_data', 'get_gathered_data']
        for method in required_methods:
            assert hasattr(S3DataGatherer, method)
            assert callable(getattr(S3DataGatherer, method))


class TestS3DataGathererConfiguration:
    """Test configuration mapping from context providers."""

    @pytest.fixture
    def mock_context_provider(self):
        """Create a mock context provider."""
        mock = Mock()
        config_values = {
            "S3DataGatherer.source": "wydot",
            "S3DataGatherer.message_type": "BSM",
            "S3DataGatherer.start_date": "2021-04-01",
            "S3DataGatherer.end_date": "2021-04-02",
            "S3DataGatherer.timezone": "UTC",
        }
        mock.get = lambda key: config_values.get(key)
        return mock

    @pytest.fixture
    def mock_path_provider(self):
        """Create a mock path provider."""
        mock = Mock()
        mock.getPathWithModelName = Mock(
            return_value="/tmp/cache/test.parquet"
        )
        return mock

    @pytest.fixture
    def mock_logger(self):
        """Create a mock logger."""
        mock = Mock()
        mock.log = Mock()
        return mock

    def test_build_config_from_providers(self, mock_context_provider, mock_path_provider, mock_logger):
        """Test that config is built correctly from providers."""
        with patch('Gatherer.S3DataGatherer.DaskSessionManager.get_client') as mock_client, \
             patch('Gatherer.S3DataGatherer.Logger', return_value=mock_logger):
            mock_client.return_value = Mock(dashboard_link="http://localhost:8787")

            # Mock the factories that return the providers
            mock_path_factory = Mock(return_value=mock_path_provider)
            mock_context_factory = Mock(return_value=mock_context_provider)

            # Directly call __init__.__wrapped__ to bypass the decorator
            gatherer = object.__new__(S3DataGatherer)
            S3DataGatherer.__init__.__wrapped__(
                gatherer,
                pathprovider=mock_path_factory,
                contextprovider=mock_context_factory
            )

            # Verify config was built correctly
            assert gatherer.config.source == "wydot"
            assert gatherer.config.message_type == "BSM"
            assert gatherer.config.date_range.start_date == date(2021, 4, 1)
            assert gatherer.config.date_range.end_date == date(2021, 4, 2)

    def test_missing_required_config_raises_error(self, mock_path_provider, mock_logger):
        """Test that missing required config raises an error."""
        mock_context = Mock()
        mock_context.get = Mock(return_value=None)  # All config values are None

        with patch('Gatherer.S3DataGatherer.DaskSessionManager.get_client') as mock_client, \
             patch('Gatherer.S3DataGatherer.Logger', return_value=mock_logger):
            mock_client.return_value = Mock(dashboard_link="http://localhost:8787")

            mock_path_factory = Mock(return_value=mock_path_provider)
            mock_context_factory = Mock(return_value=mock_context)

            # Should raise either ValueError or Pydantic ValidationError
            with pytest.raises((ValueError, Exception)):
                gatherer = object.__new__(S3DataGatherer)
                S3DataGatherer.__init__.__wrapped__(
                    gatherer,
                    pathprovider=mock_path_factory,
                    contextprovider=mock_context_factory
                )

    def test_date_string_parsing(self, mock_path_provider, mock_logger):
        """Test that date strings are parsed correctly."""
        mock_context = Mock()
        config_values = {
            "S3DataGatherer.source": "wydot",
            "S3DataGatherer.message_type": "BSM",
            "S3DataGatherer.start_date": "2021-04-01",  # String format
            "S3DataGatherer.end_date": "2021-04-02",    # String format
            "S3DataGatherer.timezone": "UTC",
        }
        mock_context.get = lambda key: config_values.get(key)

        with patch('Gatherer.S3DataGatherer.DaskSessionManager.get_client') as mock_client, \
             patch('Gatherer.S3DataGatherer.Logger', return_value=mock_logger):
            mock_client.return_value = Mock(dashboard_link="http://localhost:8787")

            mock_path_factory = Mock(return_value=mock_path_provider)
            mock_context_factory = Mock(return_value=mock_context)

            gatherer = object.__new__(S3DataGatherer)
            S3DataGatherer.__init__.__wrapped__(
                gatherer,
                pathprovider=mock_path_factory,
                contextprovider=mock_context_factory
            )

            # Verify dates were parsed to date objects
            assert isinstance(gatherer.config.date_range.start_date, date)
            assert isinstance(gatherer.config.date_range.end_date, date)


class TestS3DataGathererMethods:
    """Test S3DataGatherer methods."""

    @pytest.fixture
    def configured_gatherer(self):
        """Create a configured S3DataGatherer instance."""
        mock_context = Mock()
        config_values = {
            "S3DataGatherer.source": "wydot",
            "S3DataGatherer.message_type": "BSM",
            "S3DataGatherer.start_date": "2021-04-01",
            "S3DataGatherer.end_date": "2021-04-02",
            "S3DataGatherer.timezone": "UTC",
        }
        mock_context.get = lambda key: config_values.get(key)

        mock_path = Mock()
        mock_path.getPathWithModelName = Mock(
            return_value="/tmp/cache/test.parquet"
        )

        mock_logger = Mock()
        mock_logger.log = Mock()

        with patch('Gatherer.S3DataGatherer.DaskSessionManager.get_client') as mock_client, \
             patch('Gatherer.S3DataGatherer.Logger', return_value=mock_logger):
            mock_client.return_value = Mock(dashboard_link="http://localhost:8787")

            mock_path_factory = Mock(return_value=mock_path)
            mock_context_factory = Mock(return_value=mock_context)

            gatherer = object.__new__(S3DataGatherer)
            S3DataGatherer.__init__.__wrapped__(
                gatherer,
                pathprovider=mock_path_factory,
                contextprovider=mock_context_factory
            )

            yield gatherer

    def test_get_gathered_data_before_gather_returns_none(self, configured_gatherer):
        """Test that get_gathered_data returns None before gather_data is called."""
        assert configured_gatherer.get_gathered_data() is None

    def test_compute_data_before_gather_raises_error(self, configured_gatherer):
        """Test that compute_data raises error if called before gather_data."""
        with pytest.raises(ValueError, match="No data gathered yet"):
            configured_gatherer.compute_data()

    def test_persist_data_before_gather_raises_error(self, configured_gatherer):
        """Test that persist_data raises error if called before gather_data."""
        with pytest.raises(ValueError, match="No data gathered yet"):
            configured_gatherer.persist_data()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Tests for FormatDetector utility.

This module tests the format detection functionality for CSV and Parquet files,
including path conversion utilities.
"""

import os
import pytest
from Helpers.FormatDetector import (
    FormatDetector,
    DataFormat,
    detect_format,
    is_csv_format,
    is_parquet_format,
    convert_csv_path_to_parquet,
    convert_parquet_path_to_csv,
)


class TestFormatDetection:
    """Test format detection from file paths."""

    @pytest.mark.parametrize("file_path,expected", [
        ("data/input.csv", DataFormat.CSV),
        ("data/output.CSV", DataFormat.CSV),  # Case insensitive
        ("data/file.txt", DataFormat.CSV),
        ("data/data.tsv", DataFormat.CSV),
        ("cache/result.parquet", DataFormat.PARQUET),
        ("output/data.PARQUET", DataFormat.PARQUET),  # Case insensitive
        ("cache/file.pq", DataFormat.PARQUET),
        ("data/unknown.dat", DataFormat.UNKNOWN),
        ("data/noextension", DataFormat.UNKNOWN),
        ("", DataFormat.UNKNOWN),
    ])
    def test_detect_format(self, file_path, expected):
        """Test format detection from various file paths."""
        result = FormatDetector.detect_format(file_path)
        assert result == expected

    def test_detect_parquet_directory(self, temp_dir):
        """Test detection of Parquet directory format."""
        # Create a directory structure that looks like Parquet
        parquet_dir = os.path.join(temp_dir, "data.parquet")
        os.makedirs(parquet_dir)

        # Create _SUCCESS file (Spark writes this)
        success_file = os.path.join(parquet_dir, "_SUCCESS")
        with open(success_file, 'w') as f:
            f.write("")

        # Should detect as Parquet
        result = FormatDetector.detect_format(parquet_dir)
        assert result == DataFormat.PARQUET

    def test_is_csv_format(self):
        """Test CSV format checking."""
        assert FormatDetector.is_csv_format("data.csv") is True
        assert FormatDetector.is_csv_format("data.txt") is True
        assert FormatDetector.is_csv_format("data.parquet") is False
        assert FormatDetector.is_csv_format("data.unknown") is False

    def test_is_parquet_format(self):
        """Test Parquet format checking."""
        assert FormatDetector.is_parquet_format("data.parquet") is True
        assert FormatDetector.is_parquet_format("data.pq") is True
        assert FormatDetector.is_parquet_format("data.csv") is False
        assert FormatDetector.is_parquet_format("data.unknown") is False


class TestPathConversion:
    """Test path conversion between CSV and Parquet."""

    @pytest.mark.parametrize("csv_path,expected_parquet", [
        ("data/input.csv", "data/input.parquet"),
        ("cache/file.txt", "cache/file.parquet"),
        ("output/data.tsv", "output/data.parquet"),
        ("nested/dir/file.csv", "nested/dir/file.parquet"),
        ("file.csv", "file.parquet"),
        ("", ""),
    ])
    def test_convert_csv_to_parquet(self, csv_path, expected_parquet):
        """Test CSV to Parquet path conversion."""
        result = FormatDetector.convert_csv_path_to_parquet(csv_path)
        assert result == expected_parquet

    @pytest.mark.parametrize("parquet_path,expected_csv", [
        ("data/output.parquet", "data/output.csv"),
        ("cache/file.pq", "cache/file.csv"),
        ("nested/dir/data.parquet", "nested/dir/data.csv"),
        ("file.parquet", "file.csv"),
        ("", ""),
    ])
    def test_convert_parquet_to_csv(self, parquet_path, expected_csv):
        """Test Parquet to CSV path conversion."""
        result = FormatDetector.convert_parquet_path_to_csv(parquet_path)
        assert result == expected_csv

    def test_convert_already_parquet(self):
        """Test converting a path that's already Parquet."""
        path = "data/file.parquet"
        result = FormatDetector.convert_csv_path_to_parquet(path)
        assert result == path  # Should return as-is

    def test_convert_already_csv(self):
        """Test converting a path that's already CSV."""
        path = "data/file.csv"
        result = FormatDetector.convert_parquet_path_to_csv(path)
        assert result == path  # Should return as-is

    def test_round_trip_conversion(self):
        """Test round-trip conversion preserves base path."""
        original = "data/myfile.csv"

        # Convert to Parquet and back
        parquet = FormatDetector.convert_csv_path_to_parquet(original)
        back_to_csv = FormatDetector.convert_parquet_path_to_csv(parquet)

        assert back_to_csv == original


class TestFormatValidation:
    """Test format validation functionality."""

    def test_validate_csv_format(self):
        """Test validating CSV format."""
        assert FormatDetector.validate_format("data.csv", DataFormat.CSV) is True
        assert FormatDetector.validate_format("data.parquet", DataFormat.CSV) is False
        assert FormatDetector.validate_format("data.unknown", DataFormat.CSV) is False

    def test_validate_parquet_format(self):
        """Test validating Parquet format."""
        assert FormatDetector.validate_format("data.parquet", DataFormat.PARQUET) is True
        assert FormatDetector.validate_format("data.csv", DataFormat.PARQUET) is False
        assert FormatDetector.validate_format("data.unknown", DataFormat.PARQUET) is False

    def test_validate_unknown_format(self):
        """Test validating unknown format."""
        assert FormatDetector.validate_format("data.xyz", DataFormat.UNKNOWN) is True
        assert FormatDetector.validate_format("data.csv", DataFormat.UNKNOWN) is False


class TestReaderFormatString:
    """Test getting reader format strings for Spark."""

    @pytest.mark.parametrize("file_path,expected", [
        ("data.csv", "csv"),
        ("data.parquet", "parquet"),
        ("data.txt", "csv"),
        ("data.pq", "parquet"),
    ])
    def test_get_reader_format(self, file_path, expected):
        """Test getting reader format string."""
        result = FormatDetector.get_reader_format(file_path)
        assert result == expected

    def test_get_reader_format_unknown_default_csv(self):
        """Test that unknown format defaults to CSV."""
        result = FormatDetector.get_reader_format("data.unknown")
        assert result == "csv"

    def test_get_reader_format_custom_default(self):
        """Test custom default format."""
        result = FormatDetector.get_reader_format("data.unknown", default="parquet")
        assert result == "parquet"


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_detect_format_function(self):
        """Test module-level detect_format function."""
        assert detect_format("data.csv") == DataFormat.CSV
        assert detect_format("data.parquet") == DataFormat.PARQUET

    def test_is_csv_format_function(self):
        """Test module-level is_csv_format function."""
        assert is_csv_format("data.csv") is True
        assert is_csv_format("data.parquet") is False

    def test_is_parquet_format_function(self):
        """Test module-level is_parquet_format function."""
        assert is_parquet_format("data.parquet") is True
        assert is_parquet_format("data.csv") is False

    def test_convert_csv_path_to_parquet_function(self):
        """Test module-level convert_csv_path_to_parquet function."""
        result = convert_csv_path_to_parquet("data.csv")
        assert result == "data.parquet"

    def test_convert_parquet_path_to_csv_function(self):
        """Test module-level convert_parquet_path_to_csv function."""
        result = convert_parquet_path_to_csv("data.parquet")
        assert result == "data.csv"


class TestDataFormatEnum:
    """Test DataFormat enumeration."""

    def test_enum_values(self):
        """Test enum values are correct."""
        assert DataFormat.CSV.value == "csv"
        assert DataFormat.PARQUET.value == "parquet"
        assert DataFormat.UNKNOWN.value == "unknown"

    def test_enum_string_conversion(self):
        """Test enum to string conversion."""
        assert str(DataFormat.CSV) == "csv"
        assert str(DataFormat.PARQUET) == "parquet"
        assert str(DataFormat.UNKNOWN) == "unknown"

    def test_enum_comparison(self):
        """Test enum equality comparison."""
        assert DataFormat.CSV == DataFormat.CSV
        assert DataFormat.CSV != DataFormat.PARQUET
        assert DataFormat.PARQUET != DataFormat.UNKNOWN


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_path(self):
        """Test handling of empty path."""
        assert FormatDetector.detect_format("") == DataFormat.UNKNOWN
        assert FormatDetector.convert_csv_path_to_parquet("") == ""
        assert FormatDetector.convert_parquet_path_to_csv("") == ""

    def test_path_with_multiple_dots(self):
        """Test handling of paths with multiple dots."""
        assert FormatDetector.detect_format("data.backup.csv") == DataFormat.CSV
        assert FormatDetector.detect_format("data.backup.parquet") == DataFormat.PARQUET

        result = FormatDetector.convert_csv_path_to_parquet("data.backup.csv")
        assert result == "data.backup.parquet"

    def test_path_with_no_extension(self):
        """Test handling of paths without extension."""
        assert FormatDetector.detect_format("data") == DataFormat.UNKNOWN

        # Should add extension to path without extension
        result = FormatDetector.convert_csv_path_to_parquet("data")
        assert result == "data.parquet"

    def test_case_insensitive_detection(self):
        """Test that detection is case-insensitive."""
        assert FormatDetector.detect_format("data.CSV") == DataFormat.CSV
        assert FormatDetector.detect_format("data.Csv") == DataFormat.CSV
        assert FormatDetector.detect_format("data.PARQUET") == DataFormat.PARQUET
        assert FormatDetector.detect_format("data.Parquet") == DataFormat.PARQUET

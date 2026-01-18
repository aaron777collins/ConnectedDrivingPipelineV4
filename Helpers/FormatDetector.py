"""
Format detection utility for automatic CSV vs Parquet format detection.

This module provides utilities to detect data file formats and handle automatic
format selection for reading/writing operations in the migration from pandas to PySpark.

Classes:
    FormatDetector: Utility class for detecting and validating file formats

Functions:
    detect_format(file_path): Detect format from file path
    is_csv_format(file_path): Check if path indicates CSV format
    is_parquet_format(file_path): Check if path indicates Parquet format
    convert_csv_path_to_parquet(csv_path): Convert CSV path to Parquet equivalent
    convert_parquet_path_to_csv(parquet_path): Convert Parquet path to CSV equivalent
"""

import os
from enum import Enum
from typing import Union, Optional


class DataFormat(Enum):
    """Enumeration of supported data formats."""
    CSV = "csv"
    PARQUET = "parquet"
    UNKNOWN = "unknown"

    def __str__(self):
        return self.value


class FormatDetector:
    """
    Utility class for detecting data file formats.

    Provides methods to automatically detect whether a file path refers to CSV
    or Parquet format based on file extension and directory structure.

    Attributes:
        CSV_EXTENSIONS: Set of file extensions indicating CSV format
        PARQUET_EXTENSIONS: Set of file extensions indicating Parquet format
    """

    CSV_EXTENSIONS = {'.csv', '.txt', '.tsv'}
    PARQUET_EXTENSIONS = {'.parquet', '.pq'}

    @classmethod
    def detect_format(cls, file_path: str) -> DataFormat:
        """
        Detect the format of a file based on its path.

        Args:
            file_path: Path to the file or directory

        Returns:
            DataFormat enum indicating CSV, PARQUET, or UNKNOWN

        Examples:
            >>> FormatDetector.detect_format("data/input.csv")
            DataFormat.CSV

            >>> FormatDetector.detect_format("data/output.parquet")
            DataFormat.PARQUET

            >>> FormatDetector.detect_format("data/mydata")
            DataFormat.UNKNOWN
        """
        if not file_path:
            return DataFormat.UNKNOWN

        # Check file extension
        _, ext = os.path.splitext(file_path.lower())

        if ext in cls.CSV_EXTENSIONS:
            return DataFormat.CSV
        elif ext in cls.PARQUET_EXTENSIONS:
            return DataFormat.PARQUET

        # Check if directory exists and is a Parquet directory
        if os.path.isdir(file_path):
            # Parquet can be stored as a directory with partition files
            parquet_indicators = ['.parquet', '_SUCCESS', '_metadata']
            if any(os.path.exists(os.path.join(file_path, f)) for f in parquet_indicators):
                return DataFormat.PARQUET

        return DataFormat.UNKNOWN

    @classmethod
    def is_csv_format(cls, file_path: str) -> bool:
        """
        Check if a file path indicates CSV format.

        Args:
            file_path: Path to check

        Returns:
            True if path indicates CSV format, False otherwise

        Examples:
            >>> FormatDetector.is_csv_format("data.csv")
            True

            >>> FormatDetector.is_csv_format("data.parquet")
            False
        """
        return cls.detect_format(file_path) == DataFormat.CSV

    @classmethod
    def is_parquet_format(cls, file_path: str) -> bool:
        """
        Check if a file path indicates Parquet format.

        Args:
            file_path: Path to check

        Returns:
            True if path indicates Parquet format, False otherwise

        Examples:
            >>> FormatDetector.is_parquet_format("data.parquet")
            True

            >>> FormatDetector.is_parquet_format("data.csv")
            False
        """
        return cls.detect_format(file_path) == DataFormat.PARQUET

    @classmethod
    def convert_csv_path_to_parquet(cls, csv_path: str, preserve_structure: bool = True) -> str:
        """
        Convert a CSV file path to its Parquet equivalent.

        Args:
            csv_path: Original CSV file path
            preserve_structure: If True, keeps directory structure; if False, only changes extension

        Returns:
            Converted Parquet path

        Examples:
            >>> FormatDetector.convert_csv_path_to_parquet("data/input.csv")
            'data/input.parquet'

            >>> FormatDetector.convert_csv_path_to_parquet("cache/data.csv")
            'cache/data.parquet'
        """
        if not csv_path:
            return csv_path

        # Get base path and extension
        base_path, ext = os.path.splitext(csv_path)

        # If already parquet, return as-is
        if ext.lower() in cls.PARQUET_EXTENSIONS:
            return csv_path

        # Convert to parquet extension
        return f"{base_path}.parquet"

    @classmethod
    def convert_parquet_path_to_csv(cls, parquet_path: str, preserve_structure: bool = True) -> str:
        """
        Convert a Parquet file path to its CSV equivalent.

        Args:
            parquet_path: Original Parquet file path
            preserve_structure: If True, keeps directory structure; if False, only changes extension

        Returns:
            Converted CSV path

        Examples:
            >>> FormatDetector.convert_parquet_path_to_csv("data/input.parquet")
            'data/input.csv'

            >>> FormatDetector.convert_parquet_path_to_csv("cache/data.parquet")
            'cache/data.csv'
        """
        if not parquet_path:
            return parquet_path

        # Get base path and extension
        base_path, ext = os.path.splitext(parquet_path)

        # If already CSV, return as-is
        if ext.lower() in cls.CSV_EXTENSIONS:
            return parquet_path

        # Convert to CSV extension
        return f"{base_path}.csv"

    @classmethod
    def validate_format(cls, file_path: str, expected_format: DataFormat) -> bool:
        """
        Validate that a file path matches the expected format.

        Args:
            file_path: Path to validate
            expected_format: Expected DataFormat

        Returns:
            True if format matches expected, False otherwise

        Examples:
            >>> FormatDetector.validate_format("data.csv", DataFormat.CSV)
            True

            >>> FormatDetector.validate_format("data.csv", DataFormat.PARQUET)
            False
        """
        detected = cls.detect_format(file_path)
        return detected == expected_format

    @classmethod
    def get_reader_format(cls, file_path: str, default: str = "csv") -> str:
        """
        Get the appropriate reader format string for Spark.

        Args:
            file_path: Path to read from
            default: Default format if detection fails

        Returns:
            Format string suitable for spark.read.format()

        Examples:
            >>> FormatDetector.get_reader_format("data.csv")
            'csv'

            >>> FormatDetector.get_reader_format("data.parquet")
            'parquet'
        """
        detected = cls.detect_format(file_path)
        if detected == DataFormat.UNKNOWN:
            return default
        return str(detected)


# Module-level convenience functions
def detect_format(file_path: str) -> DataFormat:
    """
    Detect the format of a file based on its path.

    Convenience function that delegates to FormatDetector.detect_format().

    Args:
        file_path: Path to the file or directory

    Returns:
        DataFormat enum indicating CSV, PARQUET, or UNKNOWN
    """
    return FormatDetector.detect_format(file_path)


def is_csv_format(file_path: str) -> bool:
    """
    Check if a file path indicates CSV format.

    Convenience function that delegates to FormatDetector.is_csv_format().

    Args:
        file_path: Path to check

    Returns:
        True if path indicates CSV format, False otherwise
    """
    return FormatDetector.is_csv_format(file_path)


def is_parquet_format(file_path: str) -> bool:
    """
    Check if a file path indicates Parquet format.

    Convenience function that delegates to FormatDetector.is_parquet_format().

    Args:
        file_path: Path to check

    Returns:
        True if path indicates Parquet format, False otherwise
    """
    return FormatDetector.is_parquet_format(file_path)


def convert_csv_path_to_parquet(csv_path: str) -> str:
    """
    Convert a CSV file path to its Parquet equivalent.

    Convenience function that delegates to FormatDetector.convert_csv_path_to_parquet().

    Args:
        csv_path: Original CSV file path

    Returns:
        Converted Parquet path
    """
    return FormatDetector.convert_csv_path_to_parquet(csv_path)


def convert_parquet_path_to_csv(parquet_path: str) -> str:
    """
    Convert a Parquet file path to its CSV equivalent.

    Convenience function that delegates to FormatDetector.convert_parquet_path_to_csv().

    Args:
        parquet_path: Original Parquet file path

    Returns:
        Converted CSV path
    """
    return FormatDetector.convert_parquet_path_to_csv(parquet_path)

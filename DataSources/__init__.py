"""
DataSources - Flexible CV Pilot Data Access

This module provides infrastructure for fetching and caching Connected Vehicle
Pilot data from the USDOT ITS Data Sandbox (AWS S3).

Features:
- Multi-source support (WYDOT, THEA, NYCDOT)
- Flexible date-range queries
- Hierarchical caching with integrity verification
- Memory-efficient processing
- Robust error handling and resume support

Usage:
    from DataSources import S3DataFetcher, CacheManager, DataSourceConfig
    
    config = DataSourceConfig(
        source="wydot",
        message_type="BSM",
        date_range={"start_date": "2021-04-01", "end_date": "2021-04-30"}
    )
    
    fetcher = S3DataFetcher(config)
    df = fetcher.get_data()
"""

from .config import (
    DataSourceConfig,
    DateRangeConfig,
    CacheConfig,
    DownloadConfig,
    MemoryConfig,
    ValidationConfig,
    VALID_SOURCES,
    VALID_MESSAGE_TYPES,
    validate_source_and_type,
    generate_cache_key,
    parse_cache_key,
    normalize_date_to_utc,
)

from .CacheManager import CacheManager, CacheEntry, compute_sha256, MANIFEST_VERSION

from .S3DataFetcher import (
    S3DataFetcher,
    S3Object,
    DownloadResult,
    RateLimiter,
)

from .SchemaValidator import (
    BSMSchemaValidator,
    TIMSchemaValidator,
    ValidationReport,
    ValidationError,
    ValidationWarning,
    get_validator,
    get_nested_value,
)

from .Wyoming100MDataSource import (
    Wyoming100MDataSource,
    create_wyoming_100m_cache,
)

__version__ = "1.0.0"
__all__ = [
    # Config
    "DataSourceConfig",
    "DateRangeConfig",
    "CacheConfig",
    "DownloadConfig",
    "MemoryConfig",
    "ValidationConfig",
    "VALID_SOURCES",
    "VALID_MESSAGE_TYPES",
    "validate_source_and_type",
    "generate_cache_key",
    "parse_cache_key",
    "normalize_date_to_utc",
    # Cache
    "CacheManager",
    "CacheEntry",
    "compute_sha256",
    "MANIFEST_VERSION",
    # S3
    "S3DataFetcher",
    "S3Object",
    "DownloadResult",
    "RateLimiter",
    # Validation
    "BSMSchemaValidator",
    "TIMSchemaValidator",
    "ValidationReport",
    "ValidationError",
    "ValidationWarning",
    "get_validator",
    "get_nested_value",
    # Wyoming 100M
    "Wyoming100MDataSource",
    "create_wyoming_100m_cache",
]

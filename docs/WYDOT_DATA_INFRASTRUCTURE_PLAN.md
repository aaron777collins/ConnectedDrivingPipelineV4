# WYDOT Data Infrastructure Plan
## Comprehensive Architecture for Flexible CV Pilot Data Access

**Version:** 1.1  
**Date:** 2026-01-28  
**Status:** Reviewed & Audited  

---

## Executive Summary

This document outlines a comprehensive infrastructure for accessing Wyoming Connected Vehicle (CV) Pilot data from the USDOT ITS Data Sandbox. The architecture supports:

- **Flexible date-range queries** - Fetch any historical data on demand
- **Multiple data sources** - WYDOT, THEA, NYCDOT (extensible)
- **Configurable pipelines** - YAML-based configuration for experiments
- **Memory-efficient processing** - Optimized for 32GB-128GB systems
- **Robust error handling** - Resume-capable downloads with integrity checks

**Related Documents:**
- [Cache Key Specification](./CACHE_KEY_SPECIFICATION.md) - Detailed cache design

---

## Table of Contents

1. [Data Source Overview](#1-data-source-overview)
2. [Architecture Design](#2-architecture-design)
3. [Component Specifications](#3-component-specifications)
4. [Configuration System](#4-configuration-system)
5. [Implementation Plan](#5-implementation-plan)
6. [Dependencies & Prerequisites](#6-dependencies--prerequisites)
7. [Error Handling & Recovery](#7-error-handling--recovery)
8. [Concurrency & Safety](#8-concurrency--safety)
9. [Testing Strategy](#9-testing-strategy)
10. [Appendices](#appendices)

---

## 1. Data Source Overview

### 1.1 Primary Data Source: AWS S3 Bucket

| Property | Value |
|----------|-------|
| **Bucket Name** | `usdot-its-cvpilot-publicdata` |
| **Region** | `us-east-1` |
| **Access** | Public (no auth required for read) |
| **Web Interface** | http://usdot-its-cvpilot-publicdata.s3.amazonaws.com/index.html |

### 1.2 Folder Hierarchy

```
{Source_Name}/{Data_Type}/{Year}/{Month}/{Day}/{Hour}/
```

| Component | Values | Description |
|-----------|--------|-------------|
| Source_Name | `wydot`, `wydot_backup`, `thea`, `nycdot` | Data producer |
| Data_Type | `BSM`, `TIM`, `SPAT`, `EVENT` | Message type |
| Year | `2017`-`2026` | 4-digit year **(UTC)** |
| Month | `01`-`12` | 2-digit month **(UTC)** |
| Day | `01`-`31` | 2-digit day **(UTC)** |
| Hour | `00`-`23` | 2-digit hour **(UTC)** |

**⚠️ CRITICAL: All dates in S3 are in UTC.** See [Time Zone Handling](#84-time-zone-handling).

### 1.3 Data Format

- **Pre-2018-01-18**: One JSON message per file
- **Post-2018-01-18**: Newline-delimited JSON (NDJSON) with multiple messages per file

### 1.4 Available Message Types by Source

| Source | BSM | TIM | SPAT | EVENT |
|--------|-----|-----|------|-------|
| wydot | ✅ | ✅ | ❌ | ❌ |
| wydot_backup | ✅ | ✅ | ❌ | ❌ |
| thea | ✅ | ✅ | ✅ | ❌ |
| nycdot | ❌ | ❌ | ❌ | ✅ |

### 1.5 Known Data Gaps

Some dates may have **no data** due to:
- System maintenance
- Pilot downtime
- Network issues

See: [ITS CV Pilot Known Data Gaps](https://github.com/usdot-its-jpo-data-portal/sandbox/wiki/ITS-CV-Pilot-Data-Sandbox-Known-Data-Gaps-and-Caveats)

---

## 2. Architecture Design

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ data_source  │  │ date_range   │  │ processing_config    │  │
│  │ .yml         │  │ .yml         │  │ .yml                 │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│                    ↓ Pydantic Validation                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Fetching Layer                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   S3DataFetcher                           │  │
│  │  - List objects by date range                             │  │
│  │  - Parallel download with resume                          │  │
│  │  - Rate limiting & backoff                                │  │
│  │  - Integrity verification                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Local Cache                            │  │
│  │  - Hierarchical: {source}/{type}/{year}/{month}/{day}    │  │
│  │  - Manifest with file locking                             │  │
│  │  - LRU eviction when disk full                            │  │
│  │  - Atomic writes (temp file + rename)                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Processing Layer                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────────┐    │
│  │   JSON     │  │  Schema    │  │   DataFrame            │    │
│  │   Parser   │──▶  Validator │──▶  Converter (Dask/Spark)│    │
│  └────────────┘  └────────────┘  └────────────────────────┘    │
│                              │                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Parquet Cache (Processed)                    │  │
│  │  - Consistent schema (merged columns)                     │  │
│  │  - Partitioned by date                                    │  │
│  │  - Lazy loading for large ranges                          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Existing Pipeline Integration                      │
│  ┌────────────────────────────────────────────────────────┐    │
│  │         ConnectedDrivingPipelineV4                      │    │
│  │  - DaskDataGatherer / SparkDataGatherer                 │    │
│  │  - ML Classifier Pipelines                              │    │
│  │  - Attack Simulation                                    │    │
│  └────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

```
User Request (config.yml)
    │
    ▼
┌─────────────────┐
│ Validate Config │ ─── Pydantic validation, fail fast on errors
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ Normalize Dates │ ─── Convert to UTC if local time specified
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ Check Cache     │ ─── Acquire read lock, check manifest
└─────────────────┘
    │
    ├─── Cache Hit ───▶ Verify integrity ───▶ Load Parquet (lazy)
    │
    ├─── Partial Hit ──▶ Fetch missing dates only
    │
    └─── Cache Miss ──▶
                       │
                       ▼
               ┌─────────────────┐
               │ List S3 Objects │ ─── Handle empty results gracefully
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Download Files  │ ─── Parallel, with rate limiting
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Parse & Validate│ ─── Streaming, memory-bounded
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Write Parquet   │ ─── Atomic: temp file → rename
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Update Manifest │ ─── Acquire write lock
               └─────────────────┘
                       │
                       ▼
               Return DataFrame (lazy for Dask)
```

---

## 3. Component Specifications

### 3.1 S3DataFetcher Class

**Location:** `DataSources/S3DataFetcher.py`

```python
class S3DataFetcher:
    """
    Fetches CV Pilot data from USDOT ITS Sandbox S3 bucket.
    
    Features:
    - Date-range based queries
    - Parallel downloads with configurable concurrency
    - Resume support for interrupted downloads
    - Rate limiting with exponential backoff
    - Integrity verification via ETag/MD5
    """
    
    def __init__(self, config: DataSourceConfig):
        self.bucket = config.bucket
        self.source = config.source
        self.message_type = config.message_type
        self.cache_dir = config.cache_dir
        self.max_workers = config.max_workers
        self._rate_limiter = RateLimiter(max_requests_per_second=10)
        
    def list_files(self, start_date: date, end_date: date) -> List[S3Object]:
        """
        List all files in date range.
        
        Returns empty list if no data exists (not an error).
        """
        
    def download_files(
        self, 
        files: List[S3Object], 
        progress_callback: Callable = None
    ) -> List[Path]:
        """
        Download files with parallel execution and resume support.
        
        - Uses atomic writes (temp file + rename)
        - Respects rate limits
        - Retries with exponential backoff
        """
        
    def get_data(
        self, 
        start_date: date, 
        end_date: date,
        force_refresh: bool = False  # Bypass cache
    ) -> dd.DataFrame:  # Returns Dask DataFrame for lazy loading
        """Main entry point: fetch, parse, and return data."""
```

### 3.2 CacheManager Class

**Location:** `DataSources/CacheManager.py`

```python
class CacheManager:
    """
    Manages local cache of downloaded and processed data.
    
    Features:
    - File locking for concurrent access
    - Atomic manifest updates
    - LRU eviction when disk full
    - Tracks "no data" vs "not fetched"
    
    Cache Structure:
    cache/
    ├── manifest.json           # Tracking metadata (with lock)
    ├── wydot/
    │   └── BSM/
    │       └── 2021/04/01.parquet
    └── .locks/                 # Lock files for concurrent access
    """
    
    def __init__(self, cache_dir: Path, max_size_gb: float = 50.0):
        self.cache_dir = cache_dir
        self.max_size_gb = max_size_gb
        self.lock_dir = cache_dir / ".locks"
        self._manifest_lock = FileLock(self.lock_dir / "manifest.lock")
        
    def get_cached_dates(self, source: str, msg_type: str) -> Set[date]:
        """Return set of dates already in cache (including 'no_data' entries)."""
        
    def get_missing_dates(self, source: str, msg_type: str,
                          start: date, end: date) -> List[date]:
        """Return dates not yet cached."""
        
    def save_processed(
        self, 
        df: pd.DataFrame, 
        source: str, 
        msg_type: str, 
        date: date
    ) -> Path:
        """
        Save processed DataFrame to Parquet cache.
        
        Uses atomic write: temp file → rename.
        """
        
    def mark_no_data(self, source: str, msg_type: str, date: date):
        """
        Mark a date as having no data in S3.
        
        Prevents re-fetching empty dates.
        """
        
    def evict_lru(self, bytes_needed: int):
        """Evict least-recently-used entries to free space."""
        
    def clear_cache(self, source: str = None, msg_type: str = None):
        """Clear cache entries (all, or filtered by source/type)."""
```

### 3.3 SchemaValidator Class

**Location:** `DataSources/SchemaValidator.py`

```python
class BSMSchemaValidator:
    """
    Validates BSM records against J2735 schema.
    Handles schema evolution across different time periods.
    """
    
    REQUIRED_FIELDS = [
        'metadata.recordGeneratedAt',
        'payload.data.coreData.position.latitude',
        'payload.data.coreData.position.longitude',
        'payload.data.coreData.elevation',
        'payload.data.coreData.speed',
        'payload.data.coreData.heading',
    ]
    
    # Schema versions by date range
    SCHEMA_VERSIONS = {
        (date(2017, 1, 1), date(2018, 1, 17)): 2,
        (date(2018, 1, 18), date(2020, 6, 30)): 3,
        (date(2020, 7, 1), date(2099, 12, 31)): 6,
    }
    
    def validate(self, record: dict) -> Tuple[bool, List[str]]:
        """Validate single record, return (valid, errors)."""
        
    def batch_validate(self, records: List[dict]) -> ValidationReport:
        """Validate batch with statistics."""
        
    def get_canonical_schema(self) -> pa.Schema:
        """
        Return the canonical PyArrow schema.
        
        Used to ensure all parquet files have consistent columns.
        """
```

### 3.4 DataSourceConfig Dataclass

**Location:** `DataSources/config.py`

```python
from pydantic import BaseModel, validator, Field
from datetime import date
from pathlib import Path
from typing import Optional, Literal

class DateRangeConfig(BaseModel):
    """Date range configuration with validation."""
    
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    days_back: Optional[int] = None  # Relative to today
    timezone: str = "UTC"  # Input timezone
    
    @validator('end_date')
    def end_after_start(cls, v, values):
        if v and values.get('start_date') and v < values['start_date']:
            raise ValueError('end_date must be >= start_date')
        return v
    
    @validator('days_back')
    def days_back_positive(cls, v):
        if v is not None and v <= 0:
            raise ValueError('days_back must be positive')
        return v

class DataSourceConfig(BaseModel):
    """Configuration for data source access with full validation."""
    
    # Source identification
    bucket: str = "usdot-its-cvpilot-publicdata"
    source: Literal["wydot", "wydot_backup", "thea", "nycdot"]
    message_type: str
    
    # Date range
    date_range: DateRangeConfig
    
    # Cache settings
    cache_dir: Path = Path("data/cache")
    cache_ttl_days: int = Field(default=30, ge=1)
    cache_max_size_gb: float = Field(default=50.0, ge=1.0)
    
    # Download settings
    max_workers: int = Field(default=4, ge=1, le=16)
    retry_attempts: int = Field(default=3, ge=1)
    retry_delay: float = Field(default=1.0, ge=0.1)
    timeout_seconds: int = Field(default=300, ge=30)
    
    # Memory settings
    max_memory_gb: float = Field(default=24.0, ge=4.0)
    partition_size_mb: int = Field(default=128, ge=16)
    
    # Processing settings
    validate_schema: bool = True
    drop_invalid: bool = True
    
    @validator('message_type')
    def valid_message_type_for_source(cls, v, values):
        source = values.get('source')
        valid_types = {
            'wydot': {'BSM', 'TIM'},
            'wydot_backup': {'BSM', 'TIM'},
            'thea': {'BSM', 'TIM', 'SPAT'},
            'nycdot': {'EVENT'},
        }
        if source and v not in valid_types.get(source, set()):
            raise ValueError(
                f"Invalid message_type '{v}' for source '{source}'. "
                f"Valid: {valid_types[source]}"
            )
        return v
    
    @classmethod
    def from_yaml(cls, path: Path) -> 'DataSourceConfig':
        """Load and validate configuration from YAML file."""
        import yaml
        
        with open(path) as f:
            raw = yaml.safe_load(f)
        
        # Pydantic validates on construction
        return cls(**raw)
```

---

## 4. Configuration System

### 4.1 Data Source Configuration

**File:** `configs/data_sources/wydot_bsm.yml`

```yaml
# WYDOT BSM Data Source Configuration
# All fields are validated by Pydantic on load

source: "wydot"
message_type: "BSM"
bucket: "usdot-its-cvpilot-publicdata"

date_range:
  # Option 1: Relative dates (for testing)
  days_back: 30
  timezone: "America/Denver"  # Converted to UTC internally
  
  # Option 2: Absolute dates (for production)
  # start_date: "2021-04-01"
  # end_date: "2021-04-30"
  # timezone: "UTC"

cache:
  directory: "data/cache"
  ttl_days: 30
  max_size_gb: 50

download:
  max_workers: 4
  retry_attempts: 3
  retry_delay_seconds: 1.0
  timeout_seconds: 300

memory:
  max_usage_gb: 24
  partition_size_mb: 128

validation:
  enabled: true
  drop_invalid_records: true
```

### 4.2 Experiment Configuration

**File:** `configs/experiments/test_32gb_april_2021.yml`

```yaml
# Experiment: Test pipeline with April 2021 WYDOT data on 32GB system

experiment:
  name: "test_32gb_april_2021"
  description: "Validate 32GB config with one month of WYDOT BSM data"
  
data:
  source: "wydot"
  message_type: "BSM"
  date_range:
    start_date: "2021-04-01"
    end_date: "2021-04-30"
    timezone: "UTC"
  
  # Limit for testing (optional)
  max_rows: 1000000  # 1M rows for quick test
  
processing:
  engine: "dask"
  config: "configs/dask/32gb-production.yml"
  
  cleaning:
    enabled: true
    remove_duplicates: true
    validate_coordinates: true
    coordinate_bounds:
      lat_min: 40.0
      lat_max: 45.0
      lon_min: -112.0
      lon_max: -104.0

output:
  results_dir: "results/test_32gb_april_2021"
  save_model: true
  save_predictions: true
```

---

## 5. Implementation Plan

### 5.1 Phase 1: Core Infrastructure (Week 1)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Create `DataSources/` module structure | P0 | 2h | None |
| Implement Pydantic config models | P0 | 4h | pydantic |
| Implement `S3DataFetcher` with rate limiting | P0 | 8h | boto3 |
| Implement `CacheManager` with file locking | P0 | 8h | filelock |
| Implement atomic file writes | P0 | 2h | None |
| Write unit tests for core classes | P0 | 6h | pytest, moto |

### 5.2 Phase 2: Data Processing (Week 2)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Implement streaming JSON parser | P0 | 6h | Phase 1 |
| Implement `SchemaValidator` with versioning | P0 | 6h | Phase 1 |
| Implement schema merging for Parquet | P0 | 4h | pyarrow |
| Create Dask lazy loading integration | P0 | 8h | dask |
| Create Spark integration (optional) | P1 | 6h | pyspark |

### 5.3 Phase 3: Pipeline Integration (Week 3)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Create adapter for `DataGatherer` interface | P0 | 4h | Phase 2 |
| Update existing gatherers | P0 | 6h | Phase 2 |
| Handle time zone conversion | P0 | 4h | pytz |
| Integration testing | P0 | 8h | All above |

### 5.4 Phase 4: CLI & Polish (Week 4)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Create CLI for data fetching | P1 | 6h | click, rich |
| Create CLI for cache management | P1 | 4h | Phase 3 |
| Add progress bars and logging | P0 | 4h | rich |
| Write user documentation | P0 | 6h | All above |
| Performance benchmarking | P1 | 4h | Phase 3 |

---

## 6. Dependencies & Prerequisites

### 6.1 Python Dependencies

```txt
# AWS Access
boto3>=1.26.0
botocore>=1.29.0

# Data Processing
pandas>=1.5.0
pyarrow>=11.0.0
dask[complete]>=2024.1.0

# Configuration & Validation
pyyaml>=6.0
pydantic>=2.0.0

# Concurrency & Safety
filelock>=3.12.0

# Time Zones
pytz>=2023.3

# CLI
click>=8.0.0
rich>=13.0.0

# Testing
pytest>=7.0.0
pytest-asyncio>=0.21.0
moto>=4.0.0
```

### 6.2 System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 8 GB | 32+ GB |
| Storage | 50 GB SSD | 200+ GB SSD |
| Network | 10 Mbps | 100+ Mbps |
| Python | 3.10+ | 3.11+ |

---

## 7. Error Handling & Recovery

### 7.1 Network Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Connection timeout | Retry with exponential backoff (1s, 2s, 4s, max 3 attempts) |
| Rate limiting (429/503) | Pause based on Retry-After header, reduce concurrency |
| S3 access denied | Log error, fail explicitly (don't silently skip) |
| Incomplete download | Track bytes downloaded, resume from last position |
| DNS failure | Retry after 30s, max 3 attempts |

### 7.2 Data Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Malformed JSON | Log record to invalid.jsonl, skip, continue |
| Missing required field | Apply null/default if safe, else skip record |
| Out-of-range values | Include with `_warning` flag column |
| Unexpected schema | Log warning, use flexible parsing |
| Empty S3 prefix | Mark date as `no_data` in manifest (not an error) |

### 7.3 Disk Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Disk full during write | Evict LRU cache entries, retry |
| Permission denied | Fail explicitly with clear error message |
| Corrupted file | Detect via checksum, remove and re-fetch |

---

## 8. Concurrency & Safety

### 8.1 File Locking Strategy

```python
from filelock import FileLock

class CacheManager:
    def __init__(self, cache_dir: Path):
        self.lock_dir = cache_dir / ".locks"
        self.lock_dir.mkdir(exist_ok=True)
        
    def _get_lock(self, key: str) -> FileLock:
        """Get a lock for a specific cache key."""
        lock_file = self.lock_dir / f"{key.replace('/', '_')}.lock"
        return FileLock(lock_file, timeout=60)
    
    def save_processed(self, df, source, msg_type, date_val):
        key = f"{source}/{msg_type}/{date_val.year}/{date_val.month:02d}/{date_val.day:02d}"
        
        with self._get_lock(key):
            # Atomic write: temp file → rename
            temp_path = self.cache_dir / f".tmp_{uuid.uuid4()}.parquet"
            final_path = self.cache_dir / f"{key}.parquet"
            
            try:
                df.to_parquet(temp_path, compression='snappy')
                temp_path.rename(final_path)  # Atomic on POSIX
            finally:
                temp_path.unlink(missing_ok=True)
            
            # Update manifest atomically
            self._update_manifest(key, final_path)
```

### 8.2 Manifest Updates

```python
def _update_manifest(self, key: str, path: Path):
    """Update manifest with file locking."""
    manifest_lock = FileLock(self.lock_dir / "manifest.lock", timeout=60)
    
    with manifest_lock:
        manifest = self._load_manifest()
        manifest['entries'][key] = {
            'status': 'complete',
            'file_path': str(path.relative_to(self.cache_dir)),
            'checksum_sha256': compute_sha256(path),
            'updated_at': datetime.utcnow().isoformat(),
            # ... other metadata
        }
        self._save_manifest(manifest)
```

### 8.3 Rate Limiting

```python
import time
from threading import Lock

class RateLimiter:
    """Token bucket rate limiter for S3 requests."""
    
    def __init__(self, max_requests_per_second: float = 10.0):
        self.rate = max_requests_per_second
        self.tokens = max_requests_per_second
        self.last_update = time.monotonic()
        self.lock = Lock()
    
    def acquire(self):
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1
```

### 8.4 Time Zone Handling

```python
import pytz
from datetime import date, datetime

def normalize_to_utc(dt: date, timezone: str) -> date:
    """
    Convert a date from local timezone to UTC.
    
    S3 data is stored in UTC, so all queries must use UTC dates.
    """
    if timezone == "UTC":
        return dt
    
    local_tz = pytz.timezone(timezone)
    # Treat date as start of day in local timezone
    local_dt = local_tz.localize(datetime.combine(dt, datetime.min.time()))
    utc_dt = local_dt.astimezone(pytz.UTC)
    return utc_dt.date()

# In config loading:
config.start_date = normalize_to_utc(config.start_date, config.timezone)
config.end_date = normalize_to_utc(config.end_date, config.timezone)
```

---

## 9. Testing Strategy

### 9.1 Unit Tests

```python
class TestConfigValidation:
    def test_invalid_source_rejected(self):
        with pytest.raises(ValueError, match="Invalid source"):
            DataSourceConfig(source="invalid", message_type="BSM", ...)
    
    def test_invalid_message_type_for_source(self):
        with pytest.raises(ValueError, match="Invalid message_type"):
            DataSourceConfig(source="nycdot", message_type="BSM", ...)
    
    def test_end_date_before_start_rejected(self):
        with pytest.raises(ValueError, match="end_date must be >= start_date"):
            DateRangeConfig(start_date=date(2021, 4, 30), end_date=date(2021, 4, 1))

class TestCacheManager:
    def test_concurrent_writes_safe(self):
        """Test that concurrent writes don't corrupt cache."""
        
    def test_atomic_write_on_failure(self):
        """Test that failed writes don't leave partial files."""
        
    def test_no_data_marked_correctly(self):
        """Test that empty S3 prefixes are cached as 'no_data'."""
```

### 9.2 Integration Tests

```python
@pytest.mark.integration
class TestS3Integration:
    def test_fetch_one_hour_real_data(self):
        """Test fetching real WYDOT data."""
        
    def test_rate_limiting_respected(self):
        """Test that rate limits are honored."""
```

---

## 10. CLI Commands

```bash
# Fetch data
python -m datasources fetch \
  --source wydot \
  --type BSM \
  --start 2021-04-01 \
  --end 2021-04-07 \
  --timezone America/Denver

# Show cache status
python -m datasources cache status

# Clear cache
python -m datasources cache clear --source wydot

# Force refresh (bypass cache)
python -m datasources fetch \
  --source wydot \
  --type BSM \
  --start 2021-04-01 \
  --end 2021-04-01 \
  --force-refresh
```

---

## Appendices

### A. S3 Bucket Contents Summary

| Source | Start Date | Data Types | Est. Total Size |
|--------|------------|------------|-----------------|
| wydot | 2017-11 | BSM, TIM | ~500 GB |
| wydot_backup | 2018-01 | BSM, TIM | ~300 GB |
| thea | 2018-06 | BSM, TIM, SPAT | ~200 GB |
| nycdot | 2020-01 | EVENT | ~50 GB |

### B. Related Documents

- [Cache Key Specification](./CACHE_KEY_SPECIFICATION.md)
- [32GB Config](../configs/spark/32gb-single-node.yml)
- [Dask 32GB Config](../configs/dask/32gb-production.yml)

### C. Useful AWS CLI Commands

```bash
# List all sources
aws s3 ls s3://usdot-its-cvpilot-publicdata/ --no-sign-request

# Check if date has data
aws s3 ls s3://usdot-its-cvpilot-publicdata/wydot/BSM/2021/04/01/ --no-sign-request

# Download one day
aws s3 cp s3://usdot-its-cvpilot-publicdata/wydot/BSM/2021/04/01/ \
  ./data/wydot/BSM/2021/04/01/ --recursive --no-sign-request
```

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | Sophie (AI) | Initial draft |
| 1.1 | 2026-01-28 | Sophie (AI) | Audit: Added concurrency, time zones, disk management, validation |

---

**Audit Checklist:**
- [x] Concurrency handling (file locking)
- [x] Disk space management (LRU eviction)
- [x] Time zone handling (UTC normalization)
- [x] Rate limiting (token bucket)
- [x] Atomic file writes (temp + rename)
- [x] Empty data handling (no_data status)
- [x] Config validation (Pydantic)
- [x] Schema consistency (canonical schema)
- [x] Error propagation (fail fast)
- [x] Cache invalidation (force_refresh, clear)

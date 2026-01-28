# WYDOT Data Infrastructure Plan
## Comprehensive Architecture for Flexible CV Pilot Data Access

**Version:** 1.0  
**Date:** 2026-01-28  
**Status:** Draft for Review  

---

## Executive Summary

This document outlines a comprehensive infrastructure for accessing Wyoming Connected Vehicle (CV) Pilot data from the USDOT ITS Data Sandbox. The architecture supports:

- **Flexible date-range queries** - Fetch any historical data on demand
- **Multiple data sources** - WYDOT, THEA, NYCDOT (extensible)
- **Configurable pipelines** - YAML-based configuration for experiments
- **Memory-efficient processing** - Optimized for 32GB-128GB systems
- **Robust error handling** - Resume-capable downloads with integrity checks

---

## Table of Contents

1. [Data Source Overview](#1-data-source-overview)
2. [Architecture Design](#2-architecture-design)
3. [Component Specifications](#3-component-specifications)
4. [Configuration System](#4-configuration-system)
5. [Implementation Plan](#5-implementation-plan)
6. [Dependencies & Prerequisites](#6-dependencies--prerequisites)
7. [Error Handling & Recovery](#7-error-handling--recovery)
8. [Testing Strategy](#8-testing-strategy)
9. [Appendices](#appendices)

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
| Year | `2017`-`2026` | 4-digit year (UTC) |
| Month | `01`-`12` | 2-digit month (UTC) |
| Day | `01`-`31` | 2-digit day (UTC) |
| Hour | `00`-`23` | 2-digit hour (UTC) |

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
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Fetching Layer                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                   S3DataFetcher                           │  │
│  │  - List objects by date range                             │  │
│  │  - Parallel download with resume                          │  │
│  │  - Integrity verification                                 │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Local Cache                            │  │
│  │  - Downloaded JSON files                                  │  │
│  │  - Manifest tracking                                      │  │
│  │  - TTL-based expiration                                   │  │
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
│  │  - Cleaned, validated data                                │  │
│  │  - Partitioned by date                                    │  │
│  │  - Ready for ML pipeline                                  │  │
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
│ Parse Config    │ ─── Validate date ranges, sources, memory limits
└─────────────────┘
    │
    ▼
┌─────────────────┐
│ Check Cache     │ ─── Look for existing processed Parquet files
└─────────────────┘
    │
    ├─── Cache Hit ───▶ Load Parquet ───▶ Return DataFrame
    │
    ├─── Partial Hit ──▶ Fetch missing dates only
    │
    └─── Cache Miss ──▶
                       │
                       ▼
               ┌─────────────────┐
               │ List S3 Objects │ ─── Build file manifest
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Download Files  │ ─── Parallel, resumable
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Parse & Clean   │ ─── Schema validation
               └─────────────────┘
                       │
                       ▼
               ┌─────────────────┐
               │ Save to Parquet │ ─── Partitioned cache
               └─────────────────┘
                       │
                       ▼
               Return DataFrame
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
    - Integrity verification via ETag/MD5
    """
    
    def __init__(self, config: DataSourceConfig):
        self.bucket = config.bucket
        self.source = config.source
        self.message_type = config.message_type
        self.cache_dir = config.cache_dir
        self.max_workers = config.max_workers
        
    def list_files(self, start_date: date, end_date: date) -> List[S3Object]:
        """List all files in date range."""
        
    def download_files(self, files: List[S3Object], 
                       progress_callback: Callable = None) -> List[Path]:
        """Download files with parallel execution and resume support."""
        
    def get_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Main entry point: fetch, parse, and return data."""
```

### 3.2 CacheManager Class

**Location:** `DataSources/CacheManager.py`

```python
class CacheManager:
    """
    Manages local cache of downloaded and processed data.
    
    Cache Structure:
    cache/
    ├── raw/                    # Downloaded JSON files
    │   └── wydot/BSM/2021/04/01/
    ├── processed/              # Cleaned Parquet files
    │   └── wydot_BSM_2021-04-01.parquet
    └── manifest.json           # Tracking metadata
    """
    
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.manifest = self._load_manifest()
        
    def get_cached_dates(self, source: str, msg_type: str) -> Set[date]:
        """Return set of dates already in cache."""
        
    def get_missing_dates(self, source: str, msg_type: str,
                          start: date, end: date) -> List[date]:
        """Return dates not yet cached."""
        
    def save_processed(self, df: pd.DataFrame, source: str, 
                       msg_type: str, date: date) -> Path:
        """Save processed DataFrame to Parquet cache."""
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
    
    def validate(self, record: dict) -> Tuple[bool, List[str]]:
        """Validate single record, return (valid, errors)."""
        
    def batch_validate(self, records: List[dict]) -> ValidationReport:
        """Validate batch with statistics."""
```

### 3.4 DataSourceConfig Dataclass

**Location:** `DataSources/config.py`

```python
@dataclass
class DataSourceConfig:
    """Configuration for data source access."""
    
    # Source identification
    bucket: str = "usdot-its-cvpilot-publicdata"
    source: str = "wydot"  # wydot, thea, nycdot
    message_type: str = "BSM"  # BSM, TIM, SPAT, EVENT
    
    # Date range
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    
    # Cache settings
    cache_dir: Path = Path("data/cache")
    cache_ttl_days: int = 30  # Re-download after N days
    
    # Download settings
    max_workers: int = 4
    chunk_size: int = 8192
    retry_attempts: int = 3
    retry_delay: float = 1.0
    
    # Memory settings (for 32GB config)
    max_memory_gb: float = 24.0
    partition_size_mb: int = 128
    
    # Processing settings
    validate_schema: bool = True
    drop_invalid: bool = True
    
    @classmethod
    def from_yaml(cls, path: Path) -> 'DataSourceConfig':
        """Load configuration from YAML file."""
```

---

## 4. Configuration System

### 4.1 Data Source Configuration

**File:** `configs/data_sources/wydot_bsm.yml`

```yaml
# WYDOT BSM Data Source Configuration
# Use this to fetch BSM data from Wyoming CV Pilot

data_source:
  bucket: "usdot-its-cvpilot-publicdata"
  source: "wydot"
  message_type: "BSM"
  region: "us-east-1"

# Date range for data fetch
# Use relative dates or absolute dates
date_range:
  # Option 1: Relative (for testing/development)
  relative:
    days_back: 30  # Fetch last 30 days
  
  # Option 2: Absolute (for production runs)
  # start_date: "2021-04-01"
  # end_date: "2021-04-30"

# Caching configuration
cache:
  directory: "data/cache"
  ttl_days: 30
  max_size_gb: 50
  
# Download configuration
download:
  max_workers: 4
  chunk_size_bytes: 8192
  retry:
    attempts: 3
    delay_seconds: 1.0
    backoff_multiplier: 2.0
  timeout_seconds: 300
  
# Memory configuration (32GB system)
memory:
  max_usage_gb: 24
  partition_size_mb: 128
  spill_to_disk: true
  spill_directory: "/tmp/wydot-spill"

# Schema validation
validation:
  enabled: true
  drop_invalid_records: true
  log_invalid_records: true
  invalid_records_file: "data/logs/invalid_records.jsonl"

# Output configuration
output:
  format: "parquet"
  compression: "snappy"
  partition_by: ["year", "month", "day"]
```

### 4.2 Experiment Configuration

**File:** `configs/experiments/test_32gb_april_2021.yml`

```yaml
# Experiment: Test pipeline with April 2021 WYDOT data on 32GB system

experiment:
  name: "test_32gb_april_2021"
  description: "Validate 32GB config with one month of WYDOT BSM data"
  
data:
  source_config: "configs/data_sources/wydot_bsm.yml"
  date_range:
    start_date: "2021-04-01"
    end_date: "2021-04-30"
  
  # Limit for testing (optional)
  max_rows: 1000000  # 1M rows for quick test
  
processing:
  engine: "dask"  # or "spark"
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
      
  features:
    include_timestamps: true
    include_path_history: false  # Reduces memory
    
pipeline:
  attack_simulation:
    enabled: true
    attacker_percentage: 30
    attack_type: "random_offset"
    offset_range: [100, 200]  # meters
    
  ml_model:
    type: "random_forest"
    n_estimators: 100
    
output:
  results_dir: "results/test_32gb_april_2021"
  save_model: true
  save_predictions: true
```

### 4.3 Multi-Source Configuration

**File:** `configs/experiments/multi_source_comparison.yml`

```yaml
# Experiment: Compare data from multiple sources

experiment:
  name: "multi_source_comparison"
  description: "Compare BSM patterns across WYDOT, THEA, and backup"

sources:
  - name: "wydot_primary"
    config:
      source: "wydot"
      message_type: "BSM"
    date_range:
      start_date: "2021-04-01"
      end_date: "2021-04-07"
      
  - name: "wydot_backup"
    config:
      source: "wydot_backup"
      message_type: "BSM"
    date_range:
      start_date: "2021-04-01"
      end_date: "2021-04-07"
      
  - name: "thea"
    config:
      source: "thea"
      message_type: "BSM"
    date_range:
      start_date: "2021-04-01"
      end_date: "2021-04-07"

comparison:
  metrics:
    - record_count
    - unique_vehicles
    - geographic_coverage
    - temporal_distribution
```

---

## 5. Implementation Plan

### 5.1 Phase 1: Core Infrastructure (Week 1)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Create `DataSources/` module structure | P0 | 2h | None |
| Implement `S3DataFetcher` base class | P0 | 8h | boto3 |
| Implement `CacheManager` | P0 | 6h | None |
| Create configuration dataclasses | P0 | 4h | pydantic |
| Write unit tests for core classes | P0 | 6h | pytest |

**Deliverables:**
- `DataSources/S3DataFetcher.py`
- `DataSources/CacheManager.py`
- `DataSources/config.py`
- `Test/Tests/test_data_sources.py`

### 5.2 Phase 2: Data Processing (Week 2)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Implement JSON parser for BSM format | P0 | 6h | Phase 1 |
| Implement `SchemaValidator` | P0 | 6h | Phase 1 |
| Create Dask integration for processing | P0 | 8h | Phase 1 |
| Create Spark integration (optional) | P1 | 6h | Phase 1 |
| Implement Parquet caching layer | P0 | 4h | Phase 1 |

**Deliverables:**
- `DataSources/parsers/BSMParser.py`
- `DataSources/SchemaValidator.py`
- `DataSources/processors/DaskProcessor.py`
- `DataSources/processors/SparkProcessor.py`

### 5.3 Phase 3: Pipeline Integration (Week 3)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Create adapter for `DataGatherer` interface | P0 | 4h | Phase 2 |
| Update `DaskDataGatherer` to use new source | P0 | 6h | Phase 2 |
| Update `SparkDataGatherer` to use new source | P1 | 6h | Phase 2 |
| Create configuration loader for experiments | P0 | 4h | Phase 2 |
| Integration testing with existing pipelines | P0 | 8h | All above |

**Deliverables:**
- Updated `Gatherer/DaskDataGatherer.py`
- Updated `Gatherer/SparkDataGatherer.py`
- `configs/experiments/*.yml` templates

### 5.4 Phase 4: CLI & Documentation (Week 4)

| Task | Priority | Effort | Dependencies |
|------|----------|--------|--------------|
| Create CLI for data fetching | P1 | 6h | Phase 3 |
| Create CLI for cache management | P1 | 4h | Phase 3 |
| Write user documentation | P0 | 6h | All above |
| Create example notebooks | P1 | 4h | Phase 3 |
| Performance benchmarking | P1 | 4h | Phase 3 |

**Deliverables:**
- `scripts/fetch_data.py`
- `scripts/manage_cache.py`
- `docs/DATA_FETCHING_GUIDE.md`
- `examples/wydot_data_access.ipynb`

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

# Configuration
pyyaml>=6.0
pydantic>=2.0.0

# CLI
click>=8.0.0
rich>=13.0.0  # For progress bars

# Testing
pytest>=7.0.0
pytest-asyncio>=0.21.0
moto>=4.0.0  # S3 mocking
```

### 6.2 AWS Configuration

**Option A: Anonymous Access (Recommended for Public Data)**
```python
import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
```

**Option B: AWS Credentials (for private data/higher rate limits)**
```bash
# ~/.aws/credentials
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET

# ~/.aws/config
[default]
region = us-east-1
```

### 6.3 System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| RAM | 8 GB | 32+ GB |
| Storage | 50 GB | 200+ GB (for full cache) |
| Network | 10 Mbps | 100+ Mbps |
| Python | 3.10+ | 3.11+ |

---

## 7. Error Handling & Recovery

### 7.1 Network Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Connection timeout | Retry with exponential backoff (max 3 attempts) |
| Rate limiting (429) | Pause 60s, then retry with reduced concurrency |
| S3 access denied | Log error, skip file, continue with others |
| Incomplete download | Track progress, resume from last byte |

### 7.2 Data Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Malformed JSON | Log to invalid records file, skip record |
| Missing required field | Apply default if available, else skip |
| Out-of-range values | Flag for review, include in output with warning |
| Schema version mismatch | Use version-specific parser |

### 7.3 Recovery Mechanisms

```python
class DownloadState:
    """Persistent state for resumable downloads."""
    
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state = self._load_state()
        
    def mark_complete(self, s3_key: str, local_path: Path):
        """Mark file as successfully downloaded."""
        
    def get_incomplete(self) -> List[str]:
        """Get list of files that were started but not completed."""
        
    def save(self):
        """Persist state to disk."""
```

---

## 8. Testing Strategy

### 8.1 Unit Tests

```python
# Test/Tests/test_s3_data_fetcher.py

class TestS3DataFetcher:
    
    @pytest.fixture
    def mock_s3(self):
        """Create mocked S3 bucket with sample data."""
        with moto.mock_s3():
            conn = boto3.resource('s3')
            conn.create_bucket(Bucket='usdot-its-cvpilot-publicdata')
            # Add sample files...
            yield conn
    
    def test_list_files_single_day(self, mock_s3):
        """Test listing files for a single day."""
        
    def test_list_files_date_range(self, mock_s3):
        """Test listing files across multiple days."""
        
    def test_download_with_retry(self, mock_s3):
        """Test download retry on failure."""
        
    def test_resume_interrupted_download(self, mock_s3):
        """Test resuming after interruption."""
```

### 8.2 Integration Tests

```python
# Test/Integration/test_wydot_integration.py

@pytest.mark.integration
class TestWYDOTIntegration:
    
    def test_fetch_one_hour_data(self):
        """Test fetching one hour of real WYDOT data."""
        config = DataSourceConfig(
            source="wydot",
            message_type="BSM",
            start_date=date(2021, 4, 1),
            end_date=date(2021, 4, 1),
        )
        fetcher = S3DataFetcher(config)
        df = fetcher.get_data()
        
        assert len(df) > 0
        assert 'coreData_position_lat' in df.columns
        
    def test_pipeline_integration(self):
        """Test full pipeline with fetched data."""
```

### 8.3 Performance Tests

```python
# Test/Performance/test_download_performance.py

@pytest.mark.performance
class TestDownloadPerformance:
    
    def test_parallel_download_throughput(self):
        """Measure download throughput with varying concurrency."""
        
    def test_memory_usage_during_processing(self):
        """Verify memory stays within 32GB limit."""
```

---

## Appendices

### A. S3 Bucket Contents Summary

Based on sandbox analysis:

| Source | Start Date | Data Types | Est. Total Size |
|--------|------------|------------|-----------------|
| wydot | 2017-11 | BSM, TIM | ~500 GB |
| wydot_backup | 2018-01 | BSM, TIM | ~300 GB |
| thea | 2018-06 | BSM, TIM, SPAT | ~200 GB |
| nycdot | 2020-01 | EVENT | ~50 GB |

### B. Sample BSM Record Structure

```json
{
  "metadata": {
    "logFileName": "bsmTx_1511786692_fe80::226:adff:fe05:14c1.csv",
    "recordType": "bsmTx",
    "payloadType": "us.dot.its.jpo.ode.model.OdeBsmPayload",
    "serialId": {
      "streamId": "5df4d384-dae1-4dcc-b662-95cd7e7bde5f",
      "bundleSize": 1,
      "bundleId": 4096,
      "recordId": 2,
      "serialNumber": 0
    },
    "odeReceivedAt": "2017-11-27T12:50:16.377Z[UTC]",
    "schemaVersion": 3,
    "recordGeneratedAt": "2017-11-20T12:13:36.398Z[UTC]",
    "recordGeneratedBy": "OBU",
    "validSignature": true,
    "sanitized": true
  },
  "payload": {
    "dataType": "us.dot.its.jpo.ode.plugin.j2735.J2735Bsm",
    "data": {
      "coreData": {
        "msgCnt": 66,
        "id": "3DEF0000",
        "secMark": 36400,
        "position": {
          "latitude": 41.1135612,
          "longitude": -104.8557380,
          "elevation": 1856.8
        },
        "accelSet": {"accelYaw": 0.00},
        "accuracy": {"semiMajor": 1.90, "semiMinor": 2.90},
        "transmission": "NEUTRAL",
        "speed": 19.44,
        "heading": 259.7250,
        "brakes": {...},
        "size": {"width": 490, "length": 1190}
      },
      "partII": [...]
    }
  }
}
```

### C. Useful AWS CLI Commands

```bash
# List all sources
aws s3 ls s3://usdot-its-cvpilot-publicdata/ --no-sign-request

# List data for specific date
aws s3 ls s3://usdot-its-cvpilot-publicdata/wydot/BSM/2021/04/01/ --no-sign-request

# Download one day of data
aws s3 cp s3://usdot-its-cvpilot-publicdata/wydot/BSM/2021/04/01/ ./data/wydot/BSM/2021/04/01/ --recursive --no-sign-request

# Get size of one month
aws s3 ls s3://usdot-its-cvpilot-publicdata/wydot/BSM/2021/04/ --recursive --no-sign-request --summarize | tail -2
```

### D. Related Resources

- [ITS DataHub](https://www.its.dot.gov/data/)
- [CV Pilot Sandbox README](https://github.com/usdot-its-jpo-data-portal/sandbox)
- [Sandbox Exporter Tool](https://github.com/usdot-its-jpo-data-portal/sandbox_exporter)
- [J2735 Standard Documentation](https://www.sae.org/standards/content/j2735_200911/)
- [Known Data Gaps](https://github.com/usdot-its-jpo-data-portal/sandbox/wiki/ITS-CV-Pilot-Data-Sandbox-Known-Data-Gaps-and-Caveats)

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-28 | Sophie (AI) | Initial draft |

---

**Next Steps:**
1. Review and approve this plan
2. Set up AWS credentials (optional, for higher rate limits)
3. Begin Phase 1 implementation
4. Create initial test configuration for April 2021 data

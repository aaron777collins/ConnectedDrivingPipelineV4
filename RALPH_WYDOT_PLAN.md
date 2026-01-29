# WYDOT Data Infrastructure Implementation Plan

## Goal

Implement the WYDOT data fetching infrastructure as specified in `docs/WYDOT_DATA_INFRASTRUCTURE_PLAN.md` and `docs/CACHE_KEY_SPECIFICATION.md`.

This will enable flexible, on-demand access to Connected Vehicle Pilot data from the USDOT ITS Data Sandbox (AWS S3), with proper caching, validation, and pipeline integration.

## Context

- **Repository:** `/home/ubuntu/ConnectedDrivingPipelineV4`
- **Target:** Create `DataSources/` module for fetching WYDOT data from S3
- **Memory:** Optimized for 32GB systems (config at `configs/spark/32gb-single-node.yml`)
- **Architecture docs:** 
  - `docs/WYDOT_DATA_INFRASTRUCTURE_PLAN.md` - Full architecture
  - `docs/CACHE_KEY_SPECIFICATION.md` - Cache key design

## Requirements

### Core Components (Phase 1)

1. **Create DataSources module structure**
   - `DataSources/__init__.py`
   - `DataSources/config.py` - Pydantic config models with validation
   - `DataSources/S3DataFetcher.py` - S3 access with rate limiting
   - `DataSources/CacheManager.py` - Hierarchical cache with file locking

2. **Configuration validation**
   - Use Pydantic for config validation
   - Validate source/message_type combinations (e.g., nycdot only has EVENT)
   - Fail fast on invalid configs

3. **Cache isolation**
   - Cache key: `{source}/{message_type}/{year}/{month:02d}/{day:02d}`
   - Source is FIRST path component (prevents mixing)
   - Manifest.json tracks status, checksums, schema version
   - File locking for concurrent access
   - Atomic writes (temp file → rename)

4. **S3 access**
   - Anonymous access to `usdot-its-cvpilot-publicdata` bucket
   - Rate limiting (token bucket, ~10 req/s)
   - Resume support for interrupted downloads
   - Handle empty S3 prefixes gracefully (mark as `no_data`)

### Data Processing (Phase 2)

5. **JSON parsing**
   - Stream NDJSON files (memory efficient)
   - Handle pre-2018 single-message format
   - Schema validation with versioning

6. **Parquet caching**
   - Consistent schema across days
   - Snappy compression
   - Lazy loading via Dask

### Pipeline Integration (Phase 3)

7. **DataGatherer integration**
   - Create adapter for existing `DataGatherer` interface
   - Update `DaskDataGatherer` to use new source

8. **CLI**
   - `fetch_data.py` - Download data by date range
   - `manage_cache.py` - Cache status, clear, evict

### Testing

9. **Unit tests**
   - Test config validation (invalid source rejected)
   - Test cache isolation (source confusion impossible)
   - Test concurrent writes (file locking works)

10. **Integration tests**
    - Fetch real WYDOT data (one hour)
    - Verify pipeline integration

## Implementation Notes

- **Use sub-agents** for parallel work where appropriate (e.g., tests while implementing)
- **Think of contingencies** - what if S3 is slow? What if disk fills up?
- **Reference docs** at `docs/WYDOT_DATA_INFRASTRUCTURE_PLAN.md` for detailed specifications
- **All dates are UTC** - normalize user-provided local times
- **Never infer source** - always require explicit parameters

## Acceptance Criteria

- [ ] `DataSources/` module created with all core components
- [ ] Config validation rejects invalid source/type combinations
- [ ] Cache isolation prevents source confusion (test proves it)
- [ ] S3 fetching works with rate limiting and resume
- [ ] Tests pass for core functionality
- [ ] CLI commands work for fetch and cache management
- [ ] Integrated with existing pipeline (can load data via DaskDataGatherer)

## Files to Read First

1. `docs/WYDOT_DATA_INFRASTRUCTURE_PLAN.md` - Architecture
2. `docs/CACHE_KEY_SPECIFICATION.md` - Cache design
3. `configs/spark/32gb-single-node.yml` - Memory constraints
4. `Gatherer/DaskDataGatherer.py` - Existing interface

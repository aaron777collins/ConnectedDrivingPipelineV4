# Dask Pipeline Troubleshooting Guide

This comprehensive guide covers common issues, error messages, and solutions for the Dask-based Connected Driving data pipeline.

## Table of Contents

1. [Common Error Messages](#1-common-error-messages)
2. [Configuration Issues](#2-configuration-issues)
3. [Memory Management](#3-memory-management)
4. [Cache Problems](#4-cache-problems)
5. [Performance Issues](#5-performance-issues)
6. [Integration Issues](#6-integration-issues)
7. [Validation Scripts](#7-validation-scripts)
8. [Debugging Tools](#8-debugging-tools)
9. [Recovery Procedures](#9-recovery-procedures)
10. [Quick Reference](#10-quick-reference)

---

## 1. Common Error Messages

### Type Validation Errors

#### `TypeError: Expected Dask DataFrame, got <type>`

**Location**: DaskConnectedDrivingAttacker, DaskConnectedDrivingCleaner initialization

**Cause**: Passing non-Dask DataFrame to attacker or cleaner classes

**Solution**:
```python
# Incorrect
import pandas as pd
df = pd.read_csv("data.csv")
attacker = DaskConnectedDrivingAttacker(df)  # Error!

# Correct
from Gatherer.DaskDataGatherer import DaskDataGatherer
gatherer = DaskDataGatherer(...)
df = gatherer.gather_data()  # Returns Dask DataFrame
attacker = DaskConnectedDrivingAttacker(df)  # Works!
```

**Prevention**: Always use `isinstance(data, dd.DataFrame)` checks before passing data

---

### Configuration Errors

#### `ValueError: No data specified`

**Location**: DaskConnectedDrivingCleaner initialization

**Cause**: Missing data parameter when `shouldGatherAutomatically=False`

**Solution**:
```python
# Option 1: Pass data explicitly
cleaner = DaskConnectedDrivingCleaner(data=dask_df, ...)

# Option 2: Enable automatic gathering
ConnectedDrivingCleaner.shouldGatherAutomatically = True
cleaner = DaskConnectedDrivingCleaner(...)  # Will gather automatically
```

---

#### `KeyError: Function not registered`

**Location**: DaskUDFRegistry.get_function()

**Cause**: Trying to access UDF not in registry

**Solution**:
```python
from Helpers.DaskUDFRegistry import DaskUDFRegistry

# Register UDF before use
@DaskUDFRegistry.register("my_function")
def my_function(x):
    return x * 2

# Now it's available
func = DaskUDFRegistry.get_function("my_function")
```

---

### Data Gathering Errors

#### `ValueError: No data gathered yet`

**Location**: DaskDataGatherer.compute_data() and persist_data()

**Cause**: Calling compute/persist before gather_data()

**Solution**:
```python
# Incorrect
gatherer = DaskDataGatherer(...)
df = gatherer.compute_data()  # Error!

# Correct
gatherer = DaskDataGatherer(...)
df = gatherer.gather_data()  # Must call this first
computed_df = gatherer.compute_data()  # Now works
```

---

#### `FileNotFoundError: No such file or directory`

**Location**: DaskDataGatherer when loading CSV

**Cause**: Invalid or missing source_file path in config

**Solution**:
```bash
# Verify file exists
ls -lh /path/to/data.csv

# Check config has correct path
cat config.json | grep source_file

# Use absolute paths in config
{
  "data": {
    "source_file": "/absolute/path/to/data.csv"
  }
}
```

---

## 2. Configuration Issues

### Required Configuration Fields

All pipeline configs **MUST** include these sections:

```json
{
  "pipeline_name": "unique_identifier",
  "data": {
    "source_file": "path/to/data.csv",
    "filtering": { "type": "xy_offset_position" }
  },
  "features": {
    "column_set": "minimal_xy_elev"
  },
  "attacks": {
    "type": "none",
    "attack_ratio": 0.0
  },
  "ml": {
    "split_method": "ratio",
    "train_ratio": 0.8
  },
  "cache": {
    "enabled": true
  }
}
```

### Common Config Issues

| Issue | Root Cause | Solution |
|-------|-----------|----------|
| **Invalid JSON** | Syntax errors | Run `python -m json.tool config.json` to validate |
| **Missing filtering.type** | Defaults applied | Explicitly set to: `none`, `xy_offset_position`, or `bounding_box` |
| **Invalid date format** | Wrong format | Use "YYYY-MM-DD" format in date_range |
| **Missing source_file** | CSV path not provided | Add `"source_file": "path/to/data.csv"` |
| **Invalid attack.type** | Unsupported type | Use: `none`, `rand_offset`, `const_offset`, `const_offset_per_id`, `swap_rand`, `override_const`, `override_rand` |
| **attack_ratio out of range** | Value not in [0.0, 1.0) | Set between 0.0 and 0.99 |

### Validate Configuration

```bash
# Run validation script
python scripts/validate_pipeline_configs.py --verbose

# Validate specific config
python -m json.tool config.json

# Check config in Python
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner
runner = DaskPipelineRunner.from_config("config.json")  # Will raise if invalid
```

---

## 3. Memory Management

### Peak Memory Usage Profile

- **Baseline**: 48GB (Dask cluster: 6 workers × 8GB each)
- **Data gathering**: 15M rows at 64MB blocksize = ~67 partitions
- **Attack operations**: ~3x data size (compute + copy + result)
- **Target**: Keep peak <52GB to avoid worker crashes

### Memory Configuration

**DaskDataGatherer blocksize tuning**:
```python
# Default: 64MB (optimized in Task 49)
gatherer = DaskDataGatherer(
    source_file="data.csv",
    blocksize="64MB"  # Produces ~225K rows/partition
)

# For limited memory systems
gatherer = DaskDataGatherer(
    source_file="data.csv",
    blocksize="32MB"  # Smaller partitions, more memory-friendly
)
```

**Worker memory limits**:
```python
from Helpers.DaskSessionManager import DaskSessionManager

# Default: 8GB per worker (for 64GB system)
client = DaskSessionManager.get_client()

# For smaller systems
client = DaskSessionManager.get_cluster(memory_limit='4GB')
```

### Memory Monitoring

```python
from Gatherer.DaskDataGatherer import DaskDataGatherer

gatherer = DaskDataGatherer(...)
gatherer.gather_data()

# Log memory usage
gatherer.log_memory_usage()
# Output: "Cluster memory: Worker 0: 6.2GB / 8GB, Worker 1: 5.8GB / 8GB, ..."
```

### Common OOM Errors

#### 1. Compute on 15M rows without caution

**Symptom**: Workers repeatedly spill to disk, performance degrades

**Solution**:
```python
# Bad: Computes entire 15M row dataset
df_computed = df.compute()

# Good: Use .head() for testing
df_sample = df.head(1000)

# Good: Keep in Dask format and use lazy evaluation
result = df.groupby("column").mean()  # Not computed yet
```

---

#### 2. Large position swap operations

**Symptom**: Memory usage spikes to >52GB, worker crashes

**Solution**:
```python
# Memory peaks at ~3x data size during swap
# Ensure 52GB+ available before running position_swap attack

# Monitor during operation
gatherer.log_memory_usage()
```

---

#### 3. Per-partition memory exceeded

**Symptom**: "Worker exceeded memory limit" error

**Solution**:
```python
# Reduce blocksize to create smaller partitions
gatherer = DaskDataGatherer(
    source_file="data.csv",
    blocksize="32MB"  # Down from default 64MB
)
```

---

## 4. Cache Problems

### Target: ≥85% Cache Hit Rate

**Check cache health**:
```bash
python scripts/monitor_cache_health.py

# Output:
# Cache Statistics:
#   Total entries: 47
#   Hit rate: 87.3%  ✓ (target: ≥85%)
#   Total size: 23.4 GB
#   Status: EXCELLENT
```

### Low Hit Rate (<70%)

**Symptom**: Cache misses exceed 30%

**Causes**:
1. Non-deterministic cache key generation
2. Cache variables don't include all relevant parameters
3. Configuration changes invalidate old entries

**Solution**:
```bash
# 1. Run health check
python scripts/monitor_cache_health.py

# 2. Check hit rate
# - ≥85%: Excellent (goal achieved)
# - 70-85%: Acceptable but optimize
# - <70%: Investigate cache key generation

# 3. Run cleanup to remove stale entries
python scripts/monitor_cache_health.py --cleanup

# 4. Exit codes for automation
# 0 = success (≥85%)
# 1 = error (<70%)
# 2 = warning (70-85%)
```

### Cache Key Issues

**Non-deterministic keys**: Cache keys must be deterministic for reproducibility

```python
# Bad: Uses timestamp (non-deterministic)
@FileCache(cache_variables=["timestamp"])
def process_data(timestamp):
    return data

# Good: Uses all relevant config parameters
@FileCache(cache_variables=["source_file", "random_seed", "attack_type"])
def process_data(source_file, random_seed, attack_type):
    return data
```

### Parquet Cache Errors

#### `pyarrow.lib.ArrowException` when reading cache

**Cause**: Corrupted or incomplete parquet write

**Solution**:
```bash
# Delete corrupted cache file
rm -rf cache/path/to/entry.parquet

# Re-run pipeline to regenerate
python scripts/run_pipeline.py config.json
```

---

#### `FileNotFoundError` for cache directory

**Cause**: Cache directory doesn't exist or was deleted

**Solution**:
```python
# Ensure cache directories exist
import os
os.makedirs("cache", exist_ok=True)

# Or set in config
{
  "cache": {
    "enabled": true,
    "cache_dir": "cache"  # Will be created if missing
  }
}
```

### Cache Manager Operations

```python
from Decorators.CacheManager import CacheManager

# Reset singleton (for testing)
CacheManager.reset_instance()

# Get statistics
manager = CacheManager.get_instance()
stats = manager.get_statistics()
print(f"Hit rate: {stats['hit_rate_percent']:.1f}%")

# Cleanup old entries (LRU policy)
manager.cleanup_cache(max_size_gb=100)
```

---

## 5. Performance Issues

### Slow Operations - Common Bottlenecks

| Operation | Typical Time (15M rows) | Troubleshooting |
|-----------|------------------------|-----------------|
| **CSV→Dask read** | 30-60s | Use blocksize=64MB; ensure adequate memory |
| **Position attacks** | 2-5min | Use compute-then-daskify strategy (automatic) |
| **ML training** | 5-15min | Use sample data for testing; increase to full for final |
| **Parquet write** | 1-3min | Use snappy compression; enable overwrite |

### Performance Targets

- **Baseline**: Original pandas/Spark implementation
- **Target**: Dask implementation ≥2x faster (achieved in Task 48)
- **Key**: Distributed processing + lazy evaluation

### Optimization Checklist

✓ **Partition count**: Match worker count (6 workers = 6+ partitions)
```python
# Check partition count
df = gatherer.gather_data()
print(f"Partitions: {df.npartitions}")  # Should be 6+
```

✓ **Blocksize**: Use 64MB default; adjust based on row size
```python
gatherer = DaskDataGatherer(blocksize="64MB")  # Default
```

✓ **Caching**: Ensure >85% cache hit rate
```bash
python scripts/monitor_cache_health.py
```

✓ **Persistence**: Use `.persist()` for data accessed multiple times
```python
# If accessing data multiple times
df = gatherer.gather_data()
df = df.persist()  # Keep in worker memory
```

✓ **Compute**: Only call `.compute()` when absolutely needed
```python
# Bad: Computes unnecessarily
df_computed = df.compute()
result = df_computed.mean()

# Good: Lazy evaluation
result = df.mean().compute()  # Only computes final result
```

### Slow Pipeline Diagnosis

**Symptom**: Pipeline takes >10 minutes for 15M rows

**Diagnosis steps**:
```bash
# 1. Check Dask dashboard for bottlenecks
# Open dashboard URL from pipeline logs

# 2. Enable profiling
python scripts/profile_pipeline.py config.json

# 3. Common causes:
# - Too many small partitions → increase blocksize
# - Disk I/O bottleneck → use faster SSD
# - Insufficient workers → increase n_workers (if RAM allows)
# - Cache misses → check cache hit rate
```

---

## 6. Integration Issues

### sklearn Integration

**Issue**: Dask DataFrame not compatible with sklearn

**Solution**: Call `.compute()` BEFORE passing to sklearn

```python
# Incorrect
from sklearn.ensemble import RandomForestClassifier
classifier = RandomForestClassifier()
classifier.fit(train_X, train_Y)  # Error if Dask DataFrames!

# Correct
classifier = RandomForestClassifier()
X_train = train_X.compute()  # Convert to pandas
Y_train = train_Y.compute()
classifier.fit(X_train, Y_train)  # Works!
```

### pandas/Dask Interoperability

**Converting between formats**:
```python
import pandas as pd
import dask.dataframe as dd

# pandas → Dask
pandas_df = pd.read_csv("data.csv")
dask_df = dd.from_pandas(pandas_df, npartitions=6)

# Dask → pandas
dask_df = dd.read_csv("data.csv", blocksize="64MB")
pandas_df = dask_df.compute()
```

**Common mistake**: Using pandas methods on Dask DataFrame
```python
# Bad: .map() doesn't exist in Dask
df['new_col'] = df['old_col'].map(lambda x: x * 2)

# Good: Use .map_partitions()
df['new_col'] = df['old_col'].map_partitions(lambda x: x * 2)
```

### Supported sklearn Classifiers

The pipeline supports these classifiers:
- `RandomForestClassifier`
- `DecisionTreeClassifier`
- `KNeighborsClassifier`

**Metrics available**: accuracy, precision, recall, f1, specificity

**Note**: All classifiers require computed (pandas) data for training

---

## 7. Validation Scripts

### Available Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| **validate_pipeline_configs.py** | Validates JSON configs | `python scripts/validate_pipeline_configs.py --verbose` |
| **monitor_cache_health.py** | Cache hit rate & size | `python scripts/monitor_cache_health.py --report` |
| **validate_dask_data_loading.py** | Tests data loading | `python scripts/validate_dask_data_loading.py` |
| **validate_profiling_script.py** | Validates profiling | `python scripts/validate_profiling_script.py` |

### Health Check Routine

```bash
# 1. Validate pipeline configuration
python scripts/validate_pipeline_configs.py --fail-fast

# 2. Monitor cache health
python scripts/monitor_cache_health.py
# Exit codes: 0=success (≥85%), 1=error (<70%), 2=warning (70-85%)

# 3. Run data loading validation
python scripts/validate_dask_data_loading.py

# 4. Run full test suite
pytest Test/test_dask*.py -v
```

### Programmatic Health Check

```python
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner

# Check pipeline initialization
runner = DaskPipelineRunner.from_config("config.json")
runner.logger.log("Pipeline initialized successfully")

# Run pipeline with validation
result = runner.run()
if result is not None:
    print("Pipeline completed successfully")
```

---

## 8. Debugging Tools

### Logging

**Enable logging**:
```python
from Logger.Logger import Logger

logger = Logger("MyComponent")
logger.log("Info message")
logger.log(f"Debug: variable={variable}")
```

**Important log messages to watch for**:
- `DaskPipelineRunner.run()` - Pipeline start
- `Step X: ...` - Each pipeline stage
- `Dask client created` - Client initialization
- `Cache HIT/MISS` - Cache operations
- `Cluster memory` - Memory monitoring

### Dask Dashboard

The Dask dashboard provides real-time visualization of:
- Task graph and execution
- Worker status and resource usage
- Memory usage per worker
- Task progress and bottlenecks

**Access dashboard**:
```python
from Helpers.DaskSessionManager import DaskSessionManager

client = DaskSessionManager.get_client()
print(f"Dashboard: {client.dashboard_link}")
# Open URL in browser: http://localhost:8787
```

### Memory Monitoring

```python
from Helpers.DaskSessionManager import DaskSessionManager

# Get memory usage per worker
memory_usage = DaskSessionManager.get_memory_usage()
print(memory_usage)
# Output: {'worker-0': '6.2GB / 8GB', 'worker-1': '5.8GB / 8GB', ...}
```

---

## 9. Recovery Procedures

### Pipeline Fails During Data Gathering

```bash
# 1. Check error message for file path issues
cat logs/pipeline.log | grep "FileNotFoundError"

# 2. Verify source file exists and is readable
ls -lh /path/to/data.csv

# 3. Run validation
python scripts/validate_dask_data_loading.py

# 4. Check memory
python -c "from Gatherer.DaskDataGatherer import DaskDataGatherer; \
           g = DaskDataGatherer('data.csv'); \
           g.log_memory_usage()"

# 5. If OOM: Reduce blocksize or increase workers
# Edit config: blocksize="32MB"
```

### Pipeline Fails During ML Training

```bash
# 1. Verify data has correct columns
python -c "import dask.dataframe as dd; \
           df = dd.read_csv('data.csv'); \
           print(df.columns.tolist())"

# 2. Check for NaN values
python -c "import dask.dataframe as dd; \
           df = dd.read_csv('data.csv'); \
           print(df.isna().sum().compute())"

# 3. Try with smaller dataset first
# Edit config: "numrows": 10000

# 4. Verify feature columns match config
cat config.json | grep -A 3 "features"
```

### Cache Corruption Recovery

```bash
# 1. Delete cache directory
rm -rf cache/

# 2. Clear metadata
rm cache_metadata.json

# 3. Re-run pipeline (will regenerate cache)
python scripts/run_pipeline.py config.json

# 4. Verify cache health
python scripts/monitor_cache_health.py
```

### Memory Pressure Recovery

```python
from Gatherer.DaskDataGatherer import DaskDataGatherer
from Helpers.DaskSessionManager import DaskSessionManager

# 1. Monitor memory
gatherer = DaskDataGatherer(...)
gatherer.log_memory_usage()

# 2. If >80% used: Reduce blocksize or numrows
gatherer = DaskDataGatherer(source_file="data.csv", blocksize="32MB")

# 3. If >90% used: Restart Dask cluster
DaskSessionManager.restart()

# 4. Verify: Check dashboard for task distribution
client = DaskSessionManager.get_client()
print(client.dashboard_link)
```

---

## 10. Quick Reference

### File Locations

```
/tmp/original-repo/
├── MachineLearning/
│   ├── DaskPipelineRunner.py          # Main pipeline
│   └── DaskMClassifierPipeline.py     # ML classifier wrapper
├── Gatherer/
│   └── DaskDataGatherer.py            # Data loading
├── Generator/Attackers/
│   └── DaskConnectedDrivingAttacker.py # Attack simulations
├── Cleaners/
│   ├── DaskCleanerWithPassthroughFilter.py
│   ├── DaskCleanerWithFilterWithinRange.py
│   └── DaskCleanerWithFilterWithinRangeXY.py
├── Decorators/
│   ├── CacheManager.py                # Cache statistics
│   ├── FileCache.py                   # File caching
│   └── DaskParquetCache.py            # Parquet caching
├── Helpers/
│   ├── DaskSessionManager.py          # Dask cluster management
│   └── DaskUDFRegistry.py             # UDF registration
├── scripts/
│   ├── validate_pipeline_configs.py   # Config validation
│   ├── monitor_cache_health.py        # Cache monitoring
│   └── validate_dask_data_loading.py  # Data loading tests
├── Test/
│   ├── test_dask_cleaners.py
│   ├── test_dask_attackers.py
│   └── test_dask_pipeline_runner.py
└── MClassifierPipelines/configs/      # 55+ example configs
```

### Key Metrics and Thresholds

| Metric | Target | Warning | Critical |
|--------|--------|---------|----------|
| **Cache hit rate** | ≥85% | 70-84% | <70% |
| **Memory usage** | <48GB | 48-52GB | >52GB |
| **Pipeline runtime** | 5-20min | 20-40min | >40min |
| **Partition count** | 6+ | 4-5 | <4 |
| **Cache size** | <50GB | 50-100GB | >100GB |

### Common Commands

```bash
# Validate configuration
python scripts/validate_pipeline_configs.py --verbose

# Check cache health
python scripts/monitor_cache_health.py

# Run pipeline
python scripts/run_pipeline.py config.json

# Run tests
pytest Test/test_dask*.py -v

# Monitor memory
python -c "from Helpers.DaskSessionManager import DaskSessionManager; \
           print(DaskSessionManager.get_memory_usage())"

# Clean cache
python scripts/monitor_cache_health.py --cleanup
```

### Emergency Contacts

- **GitHub Issues**: Report bugs and issues at the repository
- **Documentation**: See `README.md` and `docs/DaskPipelineRunner_Configuration_Guide.md`
- **Examples**: Check `MClassifierPipelines/configs/` for 55+ working examples

---

## Summary

This troubleshooting guide covers the most common issues encountered when using the Dask-based Connected Driving pipeline. For additional help:

1. Check the [README.md](../README.md) for quick start and basic usage
2. Review the [Configuration Guide](DaskPipelineRunner_Configuration_Guide.md) for detailed config docs
3. Examine example configs in `MClassifierPipelines/configs/`
4. Run validation scripts in `scripts/`
5. Check test files in `Test/` for usage examples

If you encounter an issue not covered here, please check the logs and Dask dashboard first, then consult the documentation or file an issue.

# Caching Guide: CSVCache vs ParquetCache

## Overview

This project uses two caching decorators for performance optimization:
- **CSVCache**: For pandas-based DataFrames (CSV format)
- **ParquetCache**: For PySpark-based DataFrames (Parquet format)

Both decorators use MD5-based hashing for cache key generation and share the same underlying FileCache mechanism.

## When to Use Each Decorator

### Use CSVCache for:
- Functions that return **pandas DataFrames**
- Legacy code not yet migrated to PySpark
- Small to medium datasets that fit in memory
- Quick prototyping and testing

**Example classes using CSVCache:**
- `DataGatherer` (pandas-based)
- `ConnectedDrivingCleaner` (pandas-based)
- `ConnectedDrivingLargeDataCleaner` (pandas-based)
- `MConnectedDrivingDataCleaner` (pandas-based)
- All filter cleaners in `CleanersWithFilters/`

### Use ParquetCache for:
- Functions that return **PySpark DataFrames**
- Spark-based implementations
- Large datasets requiring distributed processing
- Production pipelines migrated to PySpark

**Example classes using ParquetCache:**
- `SparkDataGatherer`
- `SparkConnectedDrivingCleaner`
- `SparkConnectedDrivingLargeDataCleaner`

## Migration Path

When migrating a class from pandas to PySpark:

1. Create a new Spark-based version (prefix with "Spark")
2. Change return types from `pd.DataFrame` to `pyspark.sql.DataFrame`
3. Replace `@CSVCache` with `@ParquetCache`
4. Update all DataFrame operations to use PySpark API

**Before (pandas):**
```python
from Decorators.CSVCache import CSVCache
import pandas as pd

class DataGatherer:
    @CSVCache
    def _gather_data(self, full_file_cache_path="REPLACE_ME") -> pd.DataFrame:
        df = pd.read_csv(self.filepath, nrows=self.numrows)
        return df
```

**After (PySpark):**
```python
from Decorators.ParquetCache import ParquetCache
from pyspark.sql import DataFrame

class SparkDataGatherer:
    @ParquetCache
    def _gather_data(self, full_file_cache_path="REPLACE_ME") -> DataFrame:
        spark = SparkSessionManager.get_session()
        df = spark.read.csv(self.filepath, header=True)
        df = df.limit(self.numrows)
        return df
```

## Decorator API

Both decorators support the same parameters:

### Parameters

**cache_variables** (list, optional)
- Variables to include in cache key generation
- Default: all function arguments (excluding kwargs)
- Use to control which parameters affect caching

**full_file_cache_path** (str, optional)
- Override the default cache path
- Default: auto-generated based on function name and parameters
- For CSVCache: include `.csv` extension
- For ParquetCache: do NOT include `.parquet` extension (it's a directory)

### Examples

```python
# Basic usage
@ParquetCache
def process_data(self, file_path) -> DataFrame:
    # ... processing ...
    return df

# With explicit cache variables
@ParquetCache
def process_data(self, file_path, debug_param) -> DataFrame:
    # Only file_path affects cache, debug_param does not
    result = self._process_internal(
        file_path,
        debug_param,
        cache_variables=[file_path]  # Only cache based on file_path
    )
    return result

# With explicit cache path
@ParquetCache
def process_data(self, file_path) -> DataFrame:
    result = self._process_internal(
        file_path,
        full_file_cache_path="/path/to/custom/cache"
    )
    return result
```

## Cache Storage

### CSVCache
- Format: CSV files
- Storage: Single CSV file per cache entry
- Location: `cache/{MODEL_NAME}/{md5_hash}.csv`
- Size: Larger due to text format
- Speed: Slower read/write, no compression

### ParquetCache
- Format: Parquet directories
- Storage: Columnar Parquet format (directory-based)
- Location: `cache/{MODEL_NAME}/{md5_hash}.parquet/`
- Size: 40-60% smaller due to compression
- Speed: Faster read/write, built-in compression

## Performance Comparison

Based on benchmarks (Test/test_csv_vs_parquet_performance.py):

| Operation | CSV | Parquet | Winner |
|-----------|-----|---------|--------|
| File Size (100k rows) | 100% | 38.8% | Parquet |
| Write Speed | Baseline | ~15% slower | CSV |
| Read Speed | Baseline | ~50% faster | Parquet |
| Filtered Queries | Baseline | ~70% faster | Parquet |

**Recommendation**: Use Parquet for all new PySpark code. The space savings and read performance far outweigh the slightly slower write times.

## Testing

Both decorators have comprehensive test coverage:

- `Test/Tests/TestCSVCache.py` - Legacy tests for CSVCache
- `Test/test_parquet_cache.py` - pytest tests for ParquetCache (100% coverage)

Run tests:
```bash
# CSVCache tests
python3 -m pytest Test/test_parquet_cache.py -v

# ParquetCache tests
python3 -m pytest Test/test_parquet_cache.py -v

# Performance benchmarks
python3 -m pytest Test/test_csv_vs_parquet_performance.py -v
```

## Common Issues

### Issue: Cache not invalidating when expected

**Cause**: Cache key is based on function arguments. If you modify the function logic but keep the same arguments, the old cache will be returned.

**Solution**:
- Delete the cache directory manually
- Add a version parameter to cache_variables
- Include relevant state in cache_variables

### Issue: Parquet cache is a directory, not a file

**Cause**: Parquet format in Spark uses a directory structure with multiple part files.

**Solution**: This is expected behavior. When specifying `full_file_cache_path`, do NOT include the `.parquet` extension.

### Issue: Memory errors when reading large cached CSV

**Cause**: CSVCache loads entire DataFrame into memory using pandas.

**Solution**: Migrate to ParquetCache which supports distributed loading.

## Future Work

1. **Automatic Format Detection**: Create a unified cache decorator that automatically chooses CSV or Parquet based on DataFrame type
2. **Delta Lake Support**: Add DeltaCache for ACID transactions and time travel
3. **Cache Cleanup Utilities**: Tools for managing cache size and expiration
4. **Cache Monitoring**: Metrics on cache hit/miss rates and storage usage

## Related Files

- `Decorators/FileCache.py` - Base caching mechanism
- `Decorators/CSVCache.py` - CSV cache implementation
- `Decorators/ParquetCache.py` - Parquet cache implementation
- `Helpers/SparkSessionManager.py` - Spark session management
- `Test/test_parquet_cache.py` - ParquetCache tests
- `Test/test_csv_vs_parquet_performance.py` - Performance benchmarks

## References

- [PySpark Migration Plan](/tmp/pyspark-migration-plan.md) - Section 6.1
- [Parquet Format Documentation](https://parquet.apache.org/docs/)
- [PySpark DataFrames](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)

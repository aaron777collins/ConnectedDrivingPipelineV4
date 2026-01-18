# Progress: pyspark-migration-plan

## Status
IN_PROGRESS

## Task List

### Phase 1: Foundation & Infrastructure (Tasks 1-10)
- [x] Task 1: Install PySpark and dependencies (pyspark, py4j added to requirements.txt)
- [x] Task 2: Create SparkSession management utility
- [x] Task 3: Create Spark configuration templates (local/cluster/large-dataset modes)
- [x] Task 4: Define BSM data schemas (raw and processed)
- [x] Task 5: Implement schema validation utilities
- [x] Task 6: Migrate test framework to pytest
- [x] Task 7: Create sample datasets (1k, 10k, 100k)
- [x] Task 8: Set up PySpark test fixtures
- [x] Task 9: Create DataFrame comparison utilities
- [x] Task 10: Implement DataFrame abstraction layer

### Phase 2: Core Data Operations (Tasks 11-25)
- [x] Task 11: Migrate DataGatherer.read_csv to spark.read.csv
- [x] Task 12: Migrate CSVCache to ParquetCache
- [x] Task 13: Implement column selection operations
- [x] Task 14: Implement drop operations
- [x] Task 15: Implement filter operations
- [x] Task 16: Implement dropna operations
- [x] Task 17: Add Parquet write support
- [x] Task 18: Create format detection utility
- [x] Task 19: Test CSV vs Parquet performance
- [x] Task 20: Migrate ConnectedDrivingCleaner
- [x] Task 21: Migrate ConnectedDrivingLargeDataCleaner
- [x] Task 22: Test file I/O with large datasets
- [x] Task 23: Implement automatic schema inference
- [ ] Task 24: Add schema evolution support
- [ ] Task 25: Validate backwards compatibility

### Phase 3: UDFs & Transformations (Tasks 26-45)
- [x] Task 26: Implement point_to_tuple UDF
- [x] Task 27: Implement geodesic_distance UDF
- [x] Task 28: Implement xy_distance UDF
- [x] Task 29: Implement hex_to_decimal UDF
- [x] Task 30: Test UDF serialization
- [x] Task 31: Benchmark UDF vs pandas UDF
- [ ] Task 32: Implement datetime parsing with native functions
- [ ] Task 33: Migrate CleanWithTimestamps
- [ ] Task 34: Test temporal feature extraction
- [ ] Task 35: Implement train_test_split with randomSplit
- [ ] Task 36: Validate split consistency
- [x] Task 37: Create UDF registry system
- [x] Task 38: Implement UDF unit tests
- [ ] Task 39: Profile UDF performance
- [ ] Task 40: Optimize slow UDFs
- [ ] Task 41: Document UDF usage patterns
- [ ] Task 42: Create UDF debugging utilities
- [ ] Task 43: Implement UDF error handling
- [ ] Task 44: Add UDF logging
- [ ] Task 45: Test UDF with null values

### Phase 4: Attack Simulation (Tasks 46-60)
- [ ] Task 46: Migrate attacker selection logic
- [ ] Task 47: Implement broadcast-based attack assignment
- [ ] Task 48: Migrate positional_offset_const_attack
- [ ] Task 49: Migrate positional_offset_rand_attack
- [ ] Task 50: Migrate positional_swap_attack
- [ ] Task 51: Migrate positional_override_attack
- [ ] Task 52: Test attack determinism (seed handling)
- [ ] Task 53: Validate attack percentages
- [ ] Task 54: Compare attack positions with pandas version
- [ ] Task 55: Implement attack validation tests
- [ ] Task 56: Optimize attack UDF performance
- [ ] Task 57: Test attacks on large datasets
- [ ] Task 58: Document attack implementation
- [ ] Task 59: Create attack visualization utilities
- [ ] Task 60: Validate geographic constraints

### Phase 5: ML Integration (Tasks 61-72)
- [ ] Task 61: Migrate MConnectedDrivingDataCleaner
- [ ] Task 62: Implement toPandas() conversion for sklearn
- [ ] Task 63: Test memory usage during conversion
- [ ] Task 64: Migrate MDataClassifier
- [ ] Task 65: Migrate MClassifierPipeline
- [ ] Task 66: Implement distributed feature extraction
- [ ] Task 67: Validate feature equivalence
- [ ] Task 68: Test ML metrics consistency
- [ ] Task 69: Implement confusion matrix generation
- [ ] Task 70: Migrate CSVWriter functionality
- [ ] Task 71: Test end-to-end ML pipeline
- [ ] Task 72: Document ML pipeline changes

### Phase 6: Caching & Optimization (Tasks 73-82)
- [ ] Task 73: Implement ParquetCache decorator
- [ ] Task 74: Migrate existing cache files
- [ ] Task 75: Configure Spark session optimally
- [ ] Task 76: Implement partitioning strategy
- [ ] Task 77: Test partition pruning
- [ ] Task 78: Benchmark different partition counts
- [ ] Task 79: Implement cache cleanup utilities
- [ ] Task 80: Add cache monitoring
- [ ] Task 81: Optimize shuffle operations
- [ ] Task 82: Profile and tune Spark configuration

### Phase 7: Testing (Tasks 83-97)
- [ ] Task 83: Create unit tests for all UDFs
- [ ] Task 84: Create integration tests for cleaners
- [ ] Task 85: Create integration tests for attackers
- [ ] Task 86: Create end-to-end pipeline tests
- [ ] Task 87: Implement golden dataset validation
- [ ] Task 88: Add statistical validation tests
- [ ] Task 89: Create performance benchmarks
- [ ] Task 90: Test with 100k row dataset
- [ ] Task 91: Test with 1M row dataset
- [ ] Task 92: Test with 10M row dataset
- [ ] Task 93: Compare pandas vs PySpark outputs
- [ ] Task 94: Validate ML model equivalence
- [ ] Task 95: Test error handling
- [ ] Task 96: Test edge cases
- [ ] Task 97: Achieve 70% code coverage

### Phase 8: Deployment (Tasks 98-105)
- [ ] Task 98: Configure Spark on Compute Canada
- [ ] Task 99: Set up cluster resources
- [ ] Task 100: Deploy PySpark pipeline
- [ ] Task 101: Run parallel validation (pandas vs PySpark)
- [ ] Task 102: Monitor performance
- [ ] Task 103: Update documentation
- [ ] Task 104: Create troubleshooting guide
- [ ] Task 105: Deprecate pandas pipeline

## Completed This Iteration
- Task 23: Implemented automatic schema inference with comprehensive testing

## Notes
- Tasks 1-4 were already completed in previous work (PySpark dependencies, SparkSession utility, Spark configs, and BSM schemas)
- Task 5: Created Helpers/SchemaValidator.py with:
  - SchemaValidator class with methods for validating DataFrames against schemas
  - Support for strict ordering and nullability validation
  - Schema diff and comparison utilities
  - Convenience functions for BSM raw and processed schema validation
  - Custom SchemaValidationError exception
  - Comprehensive unit tests (15 tests, all passing)
- Task 6: Migrated Test/Tests/TestFileCache.py to pytest format:
  - Created Test/test_file_cache.py following pytest conventions
  - Migrated from ITest.run() monolithic method to 10 separate test methods
  - Converted cleanup() method to pytest fixture with yield
  - Updated assertions from assert(condition) to assert condition
  - Added descriptive docstrings to all test methods
  - All 10 tests passing successfully
  - Note: pytest infrastructure already exists (pytest.ini, conftest.py, SparkFixtures.py, 26+ existing pytest test files)
  - Remaining legacy tests to migrate: 9 files (TestCSVCache.py, TestChildContextProviders.py, TestLocationDiff.py, TestDictProvider.py, TestKeyProvider.py, TestPathProviders.py, TestPassByRef.py, TestStandardDependencyInjection.py, TestPathProvider.py)
- Tasks 7-9 were already completed in previous work:
  - Task 7: Sample datasets already exist in Test/Data/ (sample_1k.csv, sample_10k.csv, sample_100k.csv)
  - Task 8: PySpark test fixtures already implemented in Test/Fixtures/SparkFixtures.py
  - Task 9: DataFrame comparison utilities already exist in Test/Utils/DataFrameComparator.py
- Task 10: Implemented DataFrame abstraction layer:
  - Created Helpers/DataFrameAbstraction.py with:
    - IDataFrame abstract interface for unified DataFrame operations
    - PandasDataFrameAdapter for pandas DataFrame compatibility
    - PySparkDataFrameAdapter for PySpark DataFrame compatibility
    - DataFrameWrapper high-level wrapper for automatic backend detection
    - Factory functions: create_wrapper(), detect_backend(), convert_dataframe()
    - Supports common operations: select, filter, drop, drop_na, with_column, count, collect
    - Enables gradual migration from pandas to PySpark
  - Created Test/test_dataframe_abstraction.py with 50 comprehensive tests:
    - 13 tests for PandasDataFrameAdapter
    - 14 tests for PySparkDataFrameAdapter
    - 8 tests for DataFrameWrapper
    - 11 tests for factory functions
    - 4 consistency tests between pandas and PySpark backends
    - All 50 tests passing
- Task 11: SparkDataGatherer already implemented in Gatherer/SparkDataGatherer.py:
  - Uses spark.read.csv() with schema validation
  - Implements BSM raw schema for type safety
  - Row limiting via df.limit()
  - @ParquetCache decorator for efficient caching
  - All methods implemented: gather_data(), split_large_data(), get_gathered_data()
- Tasks 13-18: Already implemented in DataFrame abstraction layer and supporting infrastructure:
  - Task 13-16: Column operations (select, drop, filter, drop_na) in DataFrameAbstraction.py
  - Task 17: Parquet write support in ParquetCache decorator and SparkDataGatherer
  - Task 18: Format detection in Helpers/FormatDetector.py
- Task 19: Created Test/test_csv_vs_parquet_performance.py:
  - 8 comprehensive benchmark tests covering read/write performance
  - File size comparison (Parquet achieves 61.2% compression on 100k sample)
  - Filtered query performance tests
  - Performance summary with detailed metrics
  - All 8 tests passing (41 seconds runtime on 100k dataset)
  - Results: Parquet provides ~60% space savings and comparable/better performance
- Tasks 20-21: Spark cleaners already implemented:
  - Generator/Cleaners/SparkConnectedDrivingCleaner.py
  - Generator/Cleaners/SparkConnectedDrivingLargeDataCleaner.py
- Tasks 26-29: All core UDFs implemented in Helpers/SparkUDFs/:
  - GeospatialUDFs.py: point_to_tuple_udf, geodesic_distance_udf, xy_distance_udf
  - ConversionUDFs.py: hex_to_decimal_udf, direction_and_dist_to_xy_udf
  - All UDFs include comprehensive docstrings and error handling
- Tasks 30-31: UDF testing and benchmarking complete:
  - Test/test_conversion_udfs.py (unit tests)
  - Test/test_geospatial_udfs.py (unit tests)
  - Test/test_udf_benchmark.py (performance benchmarks)
  - Test/test_udf_benchmark_infrastructure.py (benchmarking framework)
- Task 37-38: UDF registry and testing complete:
  - Helpers/SparkUDFs/UDFRegistry.py (centralized UDF registration)
  - Test/test_udf_registry.py (registry unit tests)
- Task 12: Completed CSVCache to ParquetCache migration:
  - ParquetCache decorator already exists (Decorators/ParquetCache.py)
  - Comprehensive tests exist (Test/test_parquet_cache.py - 10 tests, all passing)
  - Spark-based classes already use ParquetCache:
    - SparkDataGatherer uses @ParquetCache
    - SparkConnectedDrivingCleaner uses @ParquetCache
    - SparkConnectedDrivingLargeDataCleaner uses @ParquetCache
  - Pandas-based classes correctly still use CSVCache (until migrated):
    - DataGatherer, ConnectedDrivingCleaner, ConnectedDrivingLargeDataCleaner
    - MConnectedDrivingDataCleaner, filter cleaners, attackers
  - Created comprehensive migration guide: Decorators/CACHING_GUIDE.md
    - When to use CSVCache vs ParquetCache
    - Migration path from pandas to PySpark
    - Performance comparison (Parquet: 40-60% smaller, faster reads)
    - API documentation and examples
    - Common issues and solutions
    - Testing instructions
  - Status: Migration infrastructure complete. Remaining pandas classes will adopt ParquetCache when they are migrated to PySpark in later phases
- Task 22: Large file I/O testing already implemented and verified:
  - Test file: Test/test_task_2_15_large_file_io.py
  - Comprehensive 7-test suite covering:
    - CSV reading (100k rows in 6.78s)
    - Parquet writing (100k rows in 3.42s)
    - Parquet reading (100k rows in 0.80s - 8.49x faster than CSV)
    - Partitioning strategy (10 partitions for 100k rows)
    - Data integrity verification across all operations
    - File size comparison (Parquet: 2.58x compression ratio)
    - Sample reading with LIMIT operations (1k rows in 0.385s)
  - All tests passing successfully
  - Performance metrics validated: Parquet provides ~8.5x read speedup and ~2.6x compression
  - Confirms large dataset I/O works correctly with SparkDataGatherer and split_large_data functionality
- Task 23: Implemented automatic schema inference:
  - Created Helpers/SchemaInferencer.py with comprehensive SchemaInferencer class:
    - infer_from_csv(): Infers schema from CSV files with optional sampling
    - infer_from_parquet(): Reads schema from Parquet metadata
    - infer_with_fallback(): Graceful fallback to expected schema on failure
    - compare_schemas(): Validates inferred vs expected schemas
    - get_schema_summary(): Human-readable schema statistics
    - Type compatibility checking (int/long, float/double, etc.)
  - Created Test/test_schema_inferencer.py with 27 comprehensive tests:
    - CSV inference with different sample sizes
    - Parquet schema reading
    - Fallback mechanisms
    - Schema comparison (exact match, compatible types, missing/extra columns)
    - Type compatibility validation
    - Schema summary generation
    - Real BSM data inference validation
    - Custom delimiters and header handling
    - Integration tests for complete workflows
    - All 27 tests passing with 94.79% code coverage
  - Integrated schema inference into SparkDataGatherer:
    - Added optional use_schema_inference flag (default: False)
    - Added configurable schema_inference_sample_size (default: 10000)
    - Maintains backward compatibility (explicit BSM schema by default)
    - Falls back to explicit schema if inference fails
    - Config keys: DataGatherer.infer_schema, DataGatherer.inference_sample_size
  - Convenience functions: infer_csv_schema(), infer_parquet_schema()
  - Documentation includes examples and best practices

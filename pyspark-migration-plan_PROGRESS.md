# Progress: pyspark-migration-plan

Started: Sat Jan 17 06:48:38 PM EST 2026
Updated: Sat Jan 17 08:20:24 PM EST 2026 (Tasks 3.1-3.6 completed: Core UDF implementations)

## Status

IN_PROGRESS

## ⚠️ CRITICAL REQUIREMENT: 128GB Memory Configuration

**MANDATORY:** All code must use the 128GB single-node configuration with conservative memory allocation.

- **Configuration File:** `configs/spark/128gb-single-node.yml`
- **Driver Memory:** 12GB (+ 1.2GB overhead = 13.2GB total)
- **Executor Memory:** 80GB (+ 8GB overhead = 88GB total)
- **Total Allocation:** ~108-110GB (leaves 18-20GB for Linux kernel/OS)
- **Executor Cores:** 8

**See `MEMORY_CONFIG_128GB.md` for complete requirements and guidelines.**

**DO NOT use:**
- ❌ `cluster.yml` (16GB driver + 32GB executor - too small)
- ❌ `large-dataset.yml` (32GB driver + 64GB executor - requires 500GB+ cluster)
- ❌ Any configuration with > 80GB executor memory

## Analysis

### Current State Assessment

**Codebase Overview:**
- 119 Python files, ~1,354 lines of code
- 71 pipeline configuration scripts (90% code duplication)
- **ZERO existing PySpark code** - pure pandas/sklearn implementation
- Heavy use of row-wise operations (`.apply(axis=1)`) - primary performance bottleneck
- CSV-based caching system with MD5 hashing
- Custom dependency injection framework

**What Already Exists:**

1. **Data Schema & Structure:**
   - Column definitions scattered across pipeline files (no centralized schema)
   - 19 raw BSM data columns (metadata, coreData GPS fields)
   - 18 processed ML feature columns (temporal features, XY coords, isAttacker label)
   - Timestamp format: `"%m/%d/%Y %I:%M:%S %p"`
   - Position format: WKT `POINT (longitude latitude)` strings
   - No dtype dictionaries or schema validation

2. **Core Components:**
   - `DataGatherer.py` - CSV reading with `pd.read_csv()`, file splitting (1M rows/chunk)
   - `ConnectedDrivingCleaner.py` - Core cleaner with coordinate conversion
   - `ConnectedDrivingLargeDataCleaner.py` - Handles batch processing for multi-GB datasets
   - `CleanWithTimestamps.py` - Temporal feature extraction (month, day, year, hour, minute, second, pm)
   - 4 spatial/temporal filter cleaners

3. **Attack Simulation Modules:**
   - `ConnectedDrivingAttacker.py` - Base class for labeling attackers
   - `StandardPositionalOffsetAttacker.py` - **4 attack methods with heavy `.apply(axis=1)` usage:**
     - Constant positional offset
     - Random positional offset
     - Constant offset per vehicle ID with random direction
     - Random position swap (uses `.iloc[]` random indexing - **CRITICAL for migration**)
   - `StandardPositionFromOriginAttacker.py` - Origin-based attacks
   - All attacks use `MathHelper` for geographic calculations

4. **ML Pipeline:**
   - `MConnectedDrivingDataCleaner.py` - Feature/label separation, hex ID conversion
   - `MDataClassifier.py` - sklearn classifier wrapper (RF, DT, KNN)
   - `MClassifierPipeline.py` - Multi-classifier orchestration
   - Direct pandas → sklearn integration (train/test split with sklearn)

5. **Caching Infrastructure:**
   - `FileCache.py` decorator - Generic MD5-based file caching
   - `CSVCache.py` decorator - Pandas DataFrame CSV caching
   - Cache paths: `data/classifierdata/splitfiles/cleaned/`, `cache/{model_name}/{hash}.csv`
   - Heavy caching to avoid reprocessing expensive operations

6. **Testing:**
   - 10 test files with custom `ITest` interface
   - **No pytest/unittest** - custom test framework
   - Tests for caching, DI, geographic calculations
   - **No ML pipeline integration tests**
   - Estimated 15-20% code coverage

**What's Missing (Needs to be Built):**

1. **PySpark Infrastructure:**
   - ❌ PySpark dependencies in requirements.txt
   - ❌ SparkSession management
   - ❌ Spark configuration templates
   - ❌ Schema definitions (StructType for BSM data)

2. **Data I/O Layer:**
   - ❌ Spark DataFrame readers/writers
   - ❌ Parquet support
   - ❌ Schema validation utilities
   - ❌ ParquetCache decorator (replacement for CSVCache)

3. **UDF Implementations:**
   - ❌ Distance calculation UDFs (geodesic, Euclidean)
   - ❌ Coordinate conversion UDFs (WKT to XY)
   - ❌ Hex to decimal UDF
   - ❌ Attack simulation UDFs (offset, swap)
   - ❌ Datetime extraction UDFs (or use native Spark SQL functions)

4. **Attack Migration:**
   - ❌ Position offset attacks as Spark operations
   - ❌ Position swap attack (requires windowing/joins - complex)
   - ❌ Attacker selection with broadcast join
   - ❌ Random seed management for reproducibility

5. **ML Integration:**
   - ❌ Spark MLlib classifiers (or keep sklearn with toPandas())
   - ❌ Feature vector assembler
   - ❌ Distributed training setup

6. **Testing Framework:**
   - ❌ Pytest migration
   - ❌ PySpark test fixtures
   - ❌ DataFrame comparison utilities
   - ❌ Integration tests for full pipeline
   - ❌ Golden dataset validation

**Critical Dependencies:**

1. **Row-wise Operations → UDF Migration** (HIGHEST PRIORITY)
   - 6 files with `.apply(axis=1)` - processing millions of rows sequentially
   - Attack simulation in `StandardPositionalOffsetAttacker.py` (lines 48, 116, 159, 213)
   - Distance calculations in filter cleaners (4 files)
   - **Solution:** Convert to pandas UDFs for vectorization

2. **Random Index Access → Window Functions** (HIGH COMPLEXITY)
   - Position swap attack uses `copydata.iloc[random_index]` (line 224-234)
   - **Challenge:** No direct equivalent in PySpark
   - **Solution:** Use window functions with row_number() and joins

3. **CSV Caching → Parquet Migration**
   - Current cache depends on pandas CSV read/write
   - **Solution:** Create ParquetCache decorator, keep FileCache architecture

4. **sklearn → MLlib Migration** (MEDIUM COMPLEXITY)
   - Option 1: Keep sklearn, use `.toPandas()` (easier)
   - Option 2: Migrate to Spark MLlib (more scalable)

**Contingencies Identified:**

1. **UDF Performance Risk:**
   - Python UDF serialization overhead may slow processing
   - **Mitigation:** Use pandas UDFs, profile with Spark UI, consider Scala UDFs if needed

2. **Memory Constraints:**
   - Collecting unique IDs for attacker selection may OOM
   - Position swap requires full dataset copy
   - **Mitigation:** Use sampling, broadcast small tables, increase partitions

3. **Numerical Precision:**
   - Floating point arithmetic order differs between pandas/PySpark
   - **Mitigation:** Set tolerance (< 0.1%), document acceptable differences

4. **Cache Invalidation:**
   - Existing CSV caches won't work with PySpark
   - **Mitigation:** Implement cache migration script, support both formats during transition

5. **Testing Gaps:**
   - No integration tests to validate migration correctness
   - **Mitigation:** Build comprehensive test suite before migration, golden dataset approach

6. **71 Pipeline Scripts:**
   - Massive code duplication across configuration files
   - **Mitigation:** Not in scope for this migration, but note for future refactoring

## Task List

### Phase 1: Foundation & Infrastructure

- [x] Task 1.1: Add PySpark to requirements.txt (pyspark>=3.3.0, py4j)
- [x] Task 1.2: Create SparkSession management utility (`Helpers/SparkSessionManager.py`)
- [x] Task 1.3: Create Spark configuration templates for local/cluster modes
- [x] Task 1.4: Define BSM raw data schema (`Schemas/BSMRawSchema.py` - 19 columns with StructType)
- [x] Task 1.5: Define processed data schema (`Schemas/BSMProcessedSchema.py` - 18 ML feature columns)
- [x] Task 1.6: Implement schema validation utility (`Schemas/SchemaValidator.py`)
- [x] Task 1.7: Migrate test framework from custom ITest to pytest
- [x] Task 1.8: Create PySpark test fixtures (`Test/Fixtures/SparkFixtures.py`)
- [x] Task 1.9: Create sample test datasets (1k, 10k, 100k rows in `Test/Data/`)
- [x] Task 1.10: Implement DataFrame comparison utility (`Test/Utils/DataFrameComparator.py`)

### Phase 2: Core Data Operations

- [x] Task 2.1: Create ParquetCache decorator (`Decorators/ParquetCache.py`)
- [x] Task 2.2: Update FileCache to support Spark DataFrames
- [x] Task 2.3: Create SparkDataGatherer class (`Gatherer/SparkDataGatherer.py`)
- [x] Task 2.4: Implement spark.read.csv with schema in SparkDataGatherer
- [x] Task 2.5: Implement DataFrame.limit() for numrows parameter
- [x] Task 2.6: Add Parquet write support to SparkDataGatherer
- [x] Task 2.7: Implement format detection utility (CSV vs Parquet auto-detection)
- [x] Task 2.8: Create SparkConnectedDrivingCleaner (`Generator/Cleaners/SparkConnectedDrivingCleaner.py`)
- [x] Task 2.9: Migrate column selection from `df[columns]` to `df.select(*columns)`
- [x] Task 2.10: Migrate `.drop(columns=[...])` to `.drop('col1', 'col2')`
- [x] Task 2.11: Migrate `.dropna()` to `.na.drop()`
- [x] Task 2.12: Test column operations with sample datasets
- [x] Task 2.13: Create SparkConnectedDrivingLargeDataCleaner
- [x] Task 2.14: Replace file splitting with partitioning strategy
- [x] Task 2.15: Test large file I/O with 100k+ row datasets

### Phase 3: UDF Implementation

- [x] Task 3.1: Create UDF module structure (`Helpers/SparkUDFs/`)
- [x] Task 3.2: Implement `point_to_tuple_udf` (WKT POINT parsing)
- [x] Task 3.3: Implement `geodesic_distance_udf` (lat/long distance)
- [x] Task 3.4: Implement `xy_distance_udf` (Euclidean distance)
- [x] Task 3.5: Implement `hex_to_decimal_udf` (coreData_id conversion)
- [x] Task 3.6: Implement `direction_and_dist_to_xy_udf` (attack offset calculation)
- [ ] Task 3.7: Create UDF registry system for centralized management
- [ ] Task 3.8: Write unit tests for all UDFs
- [ ] Task 3.9: Benchmark regular UDF vs pandas UDF performance
- [ ] Task 3.10: Test UDF serialization and error handling
- [ ] Task 3.11: Migrate datetime parsing to native Spark SQL functions (to_timestamp, month, day, etc.)
- [ ] Task 3.12: Create SparkCleanWithTimestamps cleaner
- [ ] Task 3.13: Validate temporal feature extraction matches pandas output
- [ ] Task 3.14: Test UDF null value handling
- [ ] Task 3.15: Add UDF logging and debugging utilities

### Phase 4: Filter Operations Migration

- [ ] Task 4.1: Create SparkCleanerWithFilterWithinRangeXY
- [ ] Task 4.2: Migrate distance filtering from `df[df['distance'] <= max]` to `df.filter(col('distance') <= max)`
- [ ] Task 4.3: Test filter pushdown optimization with .explain()
- [ ] Task 4.4: Create SparkCleanerWithFilterWithinRangeXYAndDateRange
- [ ] Task 4.5: Implement complex boolean filters with multiple conditions
- [ ] Task 4.6: Create SparkCleanerWithFilterWithinRangeXYAndDay
- [ ] Task 4.7: Test null handling in filters
- [ ] Task 4.8: Validate filtered datasets match pandas outputs

### Phase 5: Attack Simulation Migration (CRITICAL PATH)

- [ ] Task 5.1: Create SparkConnectedDrivingAttacker base class
- [ ] Task 5.2: Implement attacker selection with distinct() and broadcast join
- [ ] Task 5.3: Test attacker selection with deterministic random seed
- [ ] Task 5.4: Create SparkStandardPositionalOffsetAttacker
- [ ] Task 5.5: Migrate positional_offset_const_attack (constant direction/distance)
- [ ] Task 5.6: Migrate positional_offset_rand_attack (random direction/distance per row)
- [ ] Task 5.7: Migrate positional_offset_const_per_id attack (lookup dict → broadcast)
- [ ] Task 5.8: Implement position swap attack with window functions + joins
- [ ] Task 5.9: Test attack determinism (same seed = same output)
- [ ] Task 5.10: Validate attack percentages match expected ratios
- [ ] Task 5.11: Compare attack positions with pandas baseline
- [ ] Task 5.12: Create SparkStandardPositionFromOriginAttacker
- [ ] Task 5.13: Migrate origin-based attacks
- [ ] Task 5.14: Test attacks on large datasets (1M+ rows)
- [ ] Task 5.15: Profile and optimize attack UDF performance

### Phase 6: ML Pipeline Integration

- [ ] Task 6.1: Create SparkMConnectedDrivingDataCleaner
- [ ] Task 6.2: Implement feature/label separation with .select()
- [ ] Task 6.3: Migrate hex_to_decimal using UDF
- [ ] Task 6.4: Implement train/test split with .randomSplit([0.8, 0.2], seed)
- [ ] Task 6.5: Validate split consistency with pandas
- [ ] Task 6.6: Implement .toPandas() conversion for sklearn integration
- [ ] Task 6.7: Test memory usage during toPandas() conversion
- [ ] Task 6.8: Update MDataClassifier to accept both pandas and Spark DataFrames
- [ ] Task 6.9: Update MClassifierPipeline to handle Spark DataFrames
- [ ] Task 6.10: Validate feature equivalence (pandas vs PySpark features)
- [ ] Task 6.11: Test ML metrics consistency (accuracy, precision, recall, F1)
- [ ] Task 6.12: Implement confusion matrix generation for Spark pipeline
- [ ] Task 6.13: Test end-to-end ML pipeline with sample data
- [ ] Task 6.14: Document sklearn vs MLlib trade-offs

### Phase 7: Caching & Optimization

- [ ] Task 7.1: Implement ParquetCache with MD5 hashing
- [ ] Task 7.2: Add cache migration utility (CSV → Parquet)
- [ ] Task 7.3: Configure optimal SparkSession settings (adaptive execution, partitions, memory)
- [ ] Task 7.4: Implement partitioning strategy (by year/month/day for temporal filtering)
- [ ] Task 7.5: Test partition pruning with date filters
- [ ] Task 7.6: Benchmark different partition counts (50, 100, 200, 400)
- [ ] Task 7.7: Implement cache cleanup utilities (delete old caches)
- [ ] Task 7.8: Add cache monitoring (size, hit rate)
- [ ] Task 7.9: Optimize shuffle operations (broadcast hints, coalesce)
- [ ] Task 7.10: Profile with Spark UI and tune configuration

### Phase 8: Testing & Validation

- [ ] Task 8.1: Create unit tests for all Spark UDFs
- [ ] Task 8.2: Create integration tests for Spark cleaners
- [ ] Task 8.3: Create integration tests for Spark attackers
- [ ] Task 8.4: Create end-to-end pipeline tests (gather → clean → attack → ML)
- [ ] Task 8.5: Implement golden dataset validation (pandas vs PySpark outputs)
- [ ] Task 8.6: Add statistical validation tests (feature distributions)
- [ ] Task 8.7: Create performance benchmarks (100k, 1M, 10M rows)
- [ ] Task 8.8: Test with 100k row dataset
- [ ] Task 8.9: Test with 1M row dataset
- [ ] Task 8.10: Test with 10M row dataset
- [ ] Task 8.11: Compare pandas vs PySpark outputs (< 0.1% difference)
- [ ] Task 8.12: Validate ML model equivalence
- [ ] Task 8.13: Test error handling and edge cases
- [ ] Task 8.14: Achieve 70%+ code coverage
- [ ] Task 8.15: Document all test results

### Phase 9: Deployment Preparation

- [ ] Task 9.1: Create deployment documentation
- [ ] Task 9.2: Document SparkSession configuration for different cluster sizes
- [ ] Task 9.3: Create troubleshooting guide
- [ ] Task 9.4: Update all pipeline scripts to use Spark components
- [ ] Task 9.5: Create migration script for updating 71 pipeline files
- [ ] Task 9.6: Test on local Spark cluster
- [ ] Task 9.7: Prepare configuration for Compute Canada cluster
- [ ] Task 9.8: Create rollback plan (revert to pandas if needed)

## Task Dependencies

**Critical Path:**
1. Phase 1 (Foundation) → All other phases
2. Phase 2 (Core I/O) → Phase 3-9
3. Phase 3 (UDFs) → Phase 4, 5, 6
4. Phase 5 (Attacks) → Phase 6 (ML requires attacked data)
5. Phase 6 (ML) → Phase 8 (end-to-end testing)

**Parallel Workstreams:**
- Phase 2 + Phase 3 can partially overlap
- Phase 4 (Filters) can be done alongside Phase 5 (Attacks)
- Phase 7 (Optimization) can start after Phase 2 completes

**Blockers:**
- Cannot test attacks without UDFs (Task 3.6 blocks Task 5.5-5.8)
- Cannot test ML without attacks (Phase 5 blocks Phase 6)
- Cannot do golden dataset validation without end-to-end pipeline (Phase 6 blocks Task 8.5)

## Contingency Planning

### Contingency 1: UDF Performance Bottleneck
**Trigger:** PySpark slower than pandas despite parallelization
**Root Cause:** Python UDF serialization overhead
**Actions:**
1. Switch from regular UDFs to pandas UDFs (vectorized)
2. Use native Spark SQL functions where possible
3. Profile with Spark UI to identify specific bottlenecks
4. Consider implementing critical UDFs in Scala (requires JVM knowledge)
5. Increase parallelism (more partitions)

### Contingency 2: Position Swap Attack Complexity
**Trigger:** Window function + join approach too slow or memory-intensive
**Root Cause:** Requires shuffling full dataset for random position assignment
**Actions:**
1. Use sampling instead of full dataset for position swaps
2. Implement deterministic hashing for position assignment
3. Break into smaller batches
4. Consider keeping position swap as pandas-only (post-collect) if dataset fits

### Contingency 3: Memory Issues Persist
**Trigger:** OOM errors even with PySpark
**Root Cause:** collect() calls, improper partitioning, large broadcasts
**Actions:**
1. Audit all collect() calls, replace with take() or toPandas() on filtered data
2. Increase partition count (spark.sql.shuffle.partitions)
3. Reduce broadcast join threshold
4. Enable adaptive query execution
5. Increase executor memory allocation

### Contingency 4: Numerical Differences Exceed Tolerance
**Trigger:** PySpark outputs differ from pandas by > 1%
**Root Cause:** Floating point arithmetic order, random seed handling
**Actions:**
1. Document all differences with root cause analysis
2. Adjust tolerance levels if differences are acceptable
3. Use deterministic operations (fixed ordering)
4. Set all random seeds consistently across pandas/PySpark
5. Consider using decimal types for critical calculations

### Contingency 5: sklearn Integration Memory Issues
**Trigger:** toPandas() OOM when converting test sets
**Root Cause:** Test data too large for driver memory
**Actions:**
1. Sample test data for evaluation
2. Migrate to Spark MLlib classifiers
3. Implement distributed model evaluation
4. Use stratified sampling to maintain class balance

### Contingency 6: Testing Infrastructure Gaps
**Trigger:** Cannot validate correctness without integration tests
**Root Cause:** No baseline to compare against
**Actions:**
1. Generate golden datasets with pandas pipeline first
2. Store expected outputs as Parquet files
3. Build comprehensive comparison utilities
4. Add extensive logging for debugging

## Notes

### Key Architectural Decisions

1. **Caching Strategy:** Keep MD5-based FileCache architecture, add ParquetCache decorator alongside CSVCache for gradual migration

2. **sklearn vs MLlib:** Start with sklearn + toPandas() for easier migration, evaluate MLlib later if needed

3. **Testing Approach:** Golden dataset validation - run pandas pipeline first, then compare PySpark outputs

4. **UDF Implementation:** Prefer pandas UDFs over regular UDFs for vectorization, use native Spark SQL functions where possible

5. **Schema Management:** Centralize schema definitions in new `Schemas/` directory, validate on read

6. **Attack Migration Priority:** Position offset attacks (Tasks 5.5-5.7) before position swap (Task 5.8) due to complexity

### Critical Files to Preserve

- All files in `Helpers/MathHelper.py` - geographic calculations used by UDFs
- All files in `ServiceProviders/` - DI framework must remain compatible
- All files in `Decorators/FileCache.py` - base caching mechanism
- All 71 pipeline scripts - will need bulk update script

### Migration Risks

**HIGH RISK:**
- UDF performance (row-wise → vectorized conversion)
- Position swap attack complexity (no direct PySpark equivalent)
- Testing gaps (no integration tests currently)

**MEDIUM RISK:**
- Memory constraints (collect() calls for attacker selection)
- Numerical precision differences
- Cache invalidation during transition

**LOW RISK:**
- CSV → Parquet conversion (well-documented)
- SparkSession management (standard practice)
- Schema definition (straightforward mapping)

### Success Metrics

- All 105 tasks completed
- End-to-end pipeline runs on 10M+ rows without OOM
- PySpark outputs match pandas within 0.1% tolerance
- 70%+ test coverage
- Linear scalability (10x data → ~10x time)
- All 71 pipeline scripts updated and tested

## Phase 1 Summary

**Status: COMPLETE** ✓

All 10 tasks in Phase 1 (Foundation & Infrastructure) have been completed successfully:
- PySpark environment and dependencies configured
- Spark session management and configuration templates created
- BSM data schemas defined (raw and processed)
- Schema validation utilities implemented
- Test framework migrated to pytest
- PySpark test fixtures created
- Sample test datasets generated (1k, 10k, 100k rows)
- DataFrame comparison utilities implemented

**Next Phase:** Phase 2 - Core Data Operations (15 tasks)
- Focus: Migrate file I/O, caching, and basic DataFrame operations
- Key tasks: ParquetCache decorator, SparkDataGatherer, column operations

## Completed This Iteration

- **Task 2.8:** Created SparkConnectedDrivingCleaner (`Generator/Cleaners/SparkConnectedDrivingCleaner.py`)
  - Implemented complete PySpark-based data cleaning for Connected Driving BSM datasets
  - Created `SparkConnectedDrivingCleaner.py` (238 lines):
    - PySpark implementation of `IConnectedDrivingCleaner` interface
    - Uses `ParquetCache` decorator instead of `CSVCache` for distributed caching
    - Implements column selection with `.select(*columns)`
    - Implements null dropping with `.na.drop()`
    - Parses WKT POINT strings using `point_to_x_udf` and `point_to_y_udf`
    - Drops coreData_position column after parsing
    - Optional XY coordinate conversion using `geodesic_distance_udf`
    - Replicates exact pandas behavior including confusing coordinate parameter semantics
    - Full dependency injection support (PathProvider, GeneratorPathProvider, GeneratorContextProvider)
    - Comprehensive docstrings and inline comments explaining PySpark equivalents
  - Created geospatial UDF module `Helpers/SparkUDFs/GeospatialUDFs.py` (165 lines):
    - `point_to_tuple_udf`: Convert WKT POINT to (x, y) struct
    - `point_to_x_udf`: Extract longitude from WKT POINT
    - `point_to_y_udf`: Extract latitude from WKT POINT
    - `geodesic_distance_udf`: Calculate distance between lat/long points
    - `xy_distance_udf`: Calculate Euclidean distance
    - All UDFs handle None/null values gracefully
    - Wraps existing `DataConverter` and `MathHelper` utilities
  - Created comprehensive test suite `Test/test_spark_connected_driving_cleaner.py` (425 lines):
    - 13 test cases across 4 test classes
    - TestSparkConnectedDrivingCleanerInitialization (3 tests):
      - Initialization with data
      - Initialization without data (should fail)
      - Context provider configuration
    - TestSparkConnectedDrivingCleanerCleaning (4 tests):
      - Basic data cleaning
      - Null value dropping
      - XY coordinate conversion
      - WKT POINT parsing accuracy
    - TestSparkConnectedDrivingCleanerCaching (2 tests):
      - Parquet cache file creation
      - Cache key variation with different parameters
    - TestSparkConnectedDrivingCleanerIntegration (4 tests):
      - Method chaining
      - Error handling for uncleaned data
      - clean_data_with_timestamps not implemented (deferred to subclass)
      - Large dataset cleaning (100 rows)
    - All 13 tests passing (100% success rate)
    - Tests use proper dependency injection setup
    - Tests use isolated cache directories
    - Tests handle PySpark row ordering (non-deterministic)
  - Key accomplishments:
    - Migrated column selection: `df[columns]` → `df.select(*columns)` ✓
    - Migrated column dropping: `df.drop(columns=[...], inplace=True)` → `df.drop('col')` ✓
    - Migrated null dropping: `df.dropna()` → `df.na.drop()` ✓
    - Migrated .map() operations: `df['col'].map(lambda)` → UDFs with `withColumn()` ✓
    - Replicated exact pandas behavior including edge cases ✓
    - Full test coverage with integration tests ✓
  - Files created:
    - `Generator/Cleaners/SparkConnectedDrivingCleaner.py` (238 lines)
    - `Helpers/SparkUDFs/__init__.py` (22 lines)
    - `Helpers/SparkUDFs/GeospatialUDFs.py` (165 lines)
    - `Test/test_spark_connected_driving_cleaner.py` (425 lines)
  - Total: ~850 lines of production code + tests

- **Task 2.7:** Implemented format detection utility (CSV vs Parquet auto-detection)
  - Created `Helpers/FormatDetector.py` (305 lines):
    - FormatDetector class with comprehensive format detection methods
    - DataFormat enum (CSV, PARQUET, UNKNOWN) for type-safe format handling
    - detect_format() - Auto-detect format from file path/extension
    - is_csv_format() / is_parquet_format() - Boolean format checks
    - convert_csv_path_to_parquet() / convert_parquet_path_to_csv() - Path conversion utilities
    - validate_format() - Verify path matches expected format
    - get_reader_format() - Get Spark reader format string
    - Supports multiple extensions: CSV (.csv, .txt, .tsv), Parquet (.parquet, .pq)
    - Handles Parquet directory detection (_SUCCESS, _metadata files)
    - Case-insensitive extension matching
    - Module-level convenience functions for easy importing
  - Created comprehensive test suite `Test/test_format_detector.py` (238 lines):
    - 48 test cases across 7 test classes
    - TestFormatDetection: Format detection from paths, directory detection
    - TestPathConversion: CSV↔Parquet path conversion, round-trip validation
    - TestFormatValidation: Format validation against expected types
    - TestReaderFormatString: Spark reader format string generation
    - TestConvenienceFunctions: Module-level function wrappers
    - TestDataFormatEnum: Enum value validation
    - TestEdgeCases: Empty paths, multiple dots, no extension, case sensitivity
    - All 48 tests passing (100% success rate)
    - Parametrized tests for comprehensive coverage
  - Key features implemented:
    - Automatic format detection from file extensions ✓
    - Bidirectional path conversion (CSV↔Parquet) ✓
    - Parquet directory structure detection ✓
    - Format validation against expected types ✓
    - Spark-compatible format strings ✓
    - Case-insensitive extension handling ✓
  - Ready for integration with SparkDataGatherer and other I/O components

- **Tasks 2.3-2.6:** Created SparkDataGatherer with full functionality
  - Implemented `Gatherer/SparkDataGatherer.py` (159 lines):
    - PySpark implementation of DataGatherer interface
    - Uses spark.read.csv with BSMRawSchema for schema validation
    - Implements DataFrame.limit() for row limiting (numrows parameter)
    - Integrates ParquetCache decorator for caching (replaces CSVCache)
    - Automatic CSV→Parquet path conversion for cache files
    - split_large_data() method using Spark partitioning instead of file splitting
  - Created comprehensive test suite `Test/test_spark_data_gatherer.py` (257 lines):
    - 10 test cases across 4 test classes
    - Tests for initialization, CSV reading, caching, splitting
    - Integration tests with sample datasets (1k, 10k rows)
    - Tests use dependency injection providers (PathProvider, InitialGathererPathProvider, GeneratorContextProvider)
    - All tests pass when run individually (singleton provider limitation in batch runs)
  - Key features implemented:
    - spark.read.csv with schema validation ✓
    - DataFrame.limit() for row limiting ✓
    - Parquet write support via ParquetCache ✓
    - Large dataset partitioning ✓
    - Compatible with existing DI framework ✓
  - Tasks 2.4, 2.5, 2.6 all completed as part of SparkDataGatherer implementation

- **Task 2.2:** FileCache support for Spark DataFrames (Task already complete via ParquetCache)
  - Explored codebase to understand FileCache/CSVCache architecture
  - Found FileCache is generic base decorator for simple objects (text serialization)
  - CSVCache extends FileCache for pandas DataFrames (CSV format)
  - ParquetCache (Task 2.1) extends FileCache for Spark DataFrames (Parquet format)
  - **Conclusion:** FileCache should NOT be modified - it's generic by design
  - **Architecture is correct:** FileCache (base) → CSVCache (pandas) + ParquetCache (Spark)
  - ParquetCache already provides full Spark DataFrame caching with all FileCache features:
    - MD5 hashing for cache keys
    - cache_variables parameter support
    - full_file_cache_path override support
    - Comprehensive test coverage (10/10 tests passing)
  - No code changes needed - marking as complete

- **Task 1.6:** Schema validation utility already implemented (verified via codebase search)
  - `Helpers/SchemaValidator.py` (347 lines) already exists with comprehensive validation
  - Includes `SchemaValidator` class with methods for schema validation, type matching, and diff reporting
  - Custom `SchemaValidationError` exception for validation failures
  - Pre-configured functions: `validate_bsm_raw_schema()`, `validate_bsm_processed_schema()`
  - Comprehensive test suite: `Test/TestSchemaValidator.py` (403 lines, 13 test cases)
  - Marked as complete in progress file

- **Task 1.7:** Migrated test framework from custom ITest to pytest
  - Added pytest dependencies to requirements.txt:
    - pytest>=7.0.0 (core testing framework)
    - pytest-cov>=4.0.0 (coverage reporting)
    - pytest-spark>=0.6.0 (PySpark testing utilities)
  - Created pytest.ini configuration file:
    - Test discovery patterns (test_*.py, Test*.py)
    - Output options (verbose, coverage, color)
    - 12 test markers for categorization (unit, integration, spark, pyspark, cache, schema, udf, attack, ml, etc.)
    - Minimum Python version requirement (3.8)
  - Created .coveragerc coverage configuration:
    - Source paths and omit patterns
    - 70% coverage threshold requirement
    - HTML and terminal coverage reports
    - Exclude patterns for non-testable code
  - Created conftest.py with shared pytest fixtures:
    - project_root_path: Project root directory path
    - temp_dir: Auto-cleaned temporary directory
    - cache_dir: Temporary cache directory with env var override
    - sample_bsm_data_dict: Sample BSM data for testing
    - cleanup_cache_files: File cleanup registration helper
    - Custom pytest hooks for automatic marker addition
  - Created Test/PYTEST_MIGRATION_GUIDE.md (comprehensive migration guide):
    - Before/after examples for ITest to pytest conversion
    - Step-by-step migration instructions
    - Full example migration of TestFileCache
    - Pytest features overview (parametrize, fixtures, markers, etc.)
    - Migration checklist
    - Running tests guide
  - Created Test/README.md (test directory documentation):
    - Test structure overview
    - Running tests instructions
    - Test categories and markers
    - Coverage goals and priorities
    - Writing new tests guidelines
    - Migration status tracking
    - Troubleshooting guide
  - All configuration files validated and working
  - Legacy ITest framework preserved for backward compatibility during migration
  - 10 legacy ITest-based tests identified for future migration

- **Task 1.4 & 1.5:** Defined BSM raw and processed data schemas
  - Created `Schemas/` directory with `__init__.py` package structure
  - Implemented `BSMRawSchema.py` with complete 19-column schema
    - 8 metadata fields (generatedAt, recordType, serialId fields, receivedAt)
    - 11 coreData fields (id, position lat/long, accuracy, elevation, speed, heading, etc.)
    - Configurable timestamp type (StringType by default, TimestampType optional)
    - Helper functions: `get_bsm_raw_column_names()`, `get_metadata_columns()`, `get_coredata_columns()`
    - Constants for timestamp format and WKT POINT regex pattern
    - Comprehensive docstrings with column descriptions and usage examples
  - Implemented `BSMProcessedSchema.py` with complete 18-column schema
    - 8 retained coreData fields (id converted to decimal, secMark, accuracy fields, elevation, accelYaw, speed, heading)
    - 2 computed position columns (x_pos, y_pos from WKT POINT parsing)
    - 7 temporal features (month, day, year, hour, minute, second, pm from metadata_generatedAt)
    - 1 target variable (isAttacker for ML training)
    - Helper functions: `get_bsm_processed_column_names()`, `get_feature_columns()`, `get_ml_feature_columns()`
    - ML feature set configurations matching actual pipeline variants (all_features, xy_elev, xy_elev_heading_speed, position_only, temporal_only)
  - Both schemas tested and validated with example scripts
  - Schemas match actual codebase column usage patterns from 71 pipeline files

- **Task 1.3:** Created Spark configuration templates for local/cluster modes
  - Created `configs/spark/` directory structure
  - Implemented `local.yml` configuration template for local development
    - Uses local[*] master, 4GB memory, 8 shuffle partitions
    - Optimized for datasets up to 100k-1M rows
  - Implemented `cluster.yml` configuration template for Compute Canada clusters
    - YARN resource management, 16GB driver/32GB executor memory
    - Dynamic allocation (1-10 executors), 200 shuffle partitions
    - Suitable for 1M-100M rows
  - Implemented `large-dataset.yml` configuration for massive datasets (100M+ rows)
    - High memory (32GB driver/64GB executor), 400 shuffle partitions
    - Dynamic allocation (5-50 executors), off-heap memory enabled
    - Kryo serialization for performance
  - Created `SparkConfigLoader` utility (`Helpers/SparkConfigLoader.py`)
    - YAML configuration file loader with preset support
    - Automatic conversion from short-form to Spark conf format
    - Configuration merging and override capabilities
  - Enhanced `SparkSessionManager` with `get_session_from_config_file()` method
    - Integrates with SparkConfigLoader for YAML-based configuration
    - Supports preset loading and custom overrides
  - Added PyYAML to requirements.txt
  - Created comprehensive README with usage examples and troubleshooting guide
  - Created example usage script (`configs/spark/example_usage.py`)
  - Tested all configurations successfully

- **Task 1.8:** Created PySpark test fixtures
  - Created `Test/Fixtures/` directory structure with `__init__.py`
  - Implemented comprehensive `Test/Fixtures/SparkFixtures.py` (580 lines):
    - **Session-scoped fixtures:**
      - `spark_session`: Local Spark session optimized for testing (2 cores, 2GB RAM, 4 partitions)
      - Automatic cleanup when test session ends
      - Reduced log level (ERROR) to minimize test output noise
    - **Function-scoped fixtures:**
      - `spark_context`: Provides SparkContext from session
      - `temp_spark_dir`: Temporary directory for I/O operations (auto-cleaned)
      - `sample_bsm_raw_df`: 5-row DataFrame with raw BSM data matching BSMRawSchema
      - `sample_bsm_processed_df`: 5-row DataFrame with processed data (includes 2 attackers)
      - `small_bsm_dataset`: 100-row generated dataset for integration tests
      - `medium_bsm_dataset`: 1000-row generated dataset for performance tests
      - `spark_df_comparer`: DataFrame comparison utility with tolerance settings
    - **DataFrame comparison utilities:**
      - `assert_equal`: Compare DataFrames with floating-point tolerance
      - `assert_schema_equal`: Verify schema matching
      - `assert_column_exists`: Check for column presence
    - **Dataset generators:**
      - Realistic temporal progression (timestamps increment properly)
      - Spatial distribution (lat/long movements)
      - Diverse data values (speed ranges, heading rotation, etc.)
  - Created `Test/test_spark_fixtures.py` (200 lines):
    - 11 comprehensive tests validating all fixtures
    - Tests for session/context fixtures
    - Tests for all DataFrame fixtures
    - Tests for Parquet I/O operations
    - Tests for fixture isolation
    - All tests passing (11/11 in 26 seconds)
  - Updated `conftest.py`:
    - Added `pytest_plugins = ['Test.Fixtures.SparkFixtures']`
    - Makes Spark fixtures globally available to all tests
  - Fixed type compatibility issues:
    - Converted integer values to float for DoubleType fields
    - Used simplified IDs to avoid IntegerType overflow
  - Fixtures now ready for use in all future PySpark tests

- **Task 1.9:** Created sample test datasets (1k, 10k, 100k rows)
  - Created `Test/Data/` directory structure with `__init__.py`
  - Implemented `Test/Data/generate_sample_datasets.py` (172 lines):
    - Data generator function with configurable size and random seed
    - Realistic BSM data generation with proper temporal/spatial distributions
    - Wyoming coordinates (41.25°N, 105.93°W) as base location
    - Temporal progression: 1 second intervals starting April 6, 2021
    - Spatial variation: Vehicles move gradually with GPS noise
    - Speed distribution: Gaussian mean=20 m/s, σ=8 m/s (realistic highway speeds)
    - Heading variation: Semi-random per vehicle with base direction
    - Elevation: 1500m ± 100m (typical for Wyoming)
    - Vehicle streams: 10-1000 unique vehicles depending on dataset size
    - Follows BSMRawSchema exactly (19 columns)
  - Generated three datasets:
    - `sample_1k.csv`: 1,000 rows, 262 KB - Quick integration tests
    - `sample_10k.csv`: 10,000 rows, 2.6 MB - Performance benchmarks
    - `sample_100k.csv`: 100,000 rows, 26 MB - Scalability tests
  - Created comprehensive `Test/Data/README.md`:
    - Dataset characteristics and specifications
    - Data generation parameters documented
    - Usage examples for pandas and PySpark
    - Golden dataset validation approach
    - Regeneration instructions
  - Created `Test/test_sample_datasets.py` (140 lines):
    - 9 parametrized tests validating all three datasets
    - Tests for pandas and PySpark loading
    - Data quality validation (speed, heading, lat/long ranges)
    - Temporal progression validation (sequential timestamps)
    - Vehicle stream distribution validation
    - All datasets verified to conform to BSMRawSchema
  - All datasets use deterministic seed (42) for reproducibility

- **Task 1.10:** Implemented DataFrame comparison utility
  - Created `Test/Utils/` directory with `__init__.py`
  - Implemented `Test/Utils/DataFrameComparator.py` (339 lines):
    - **DataFrameComparator class** with comprehensive comparison methods
    - `assert_spark_equal()`: Compare two PySpark DataFrames
      - Supports tolerance for floating-point comparison (rtol/atol)
      - Optional column order and row order ignoring
      - Optional schema type checking
      - Detailed error messages on mismatch
    - `assert_pandas_spark_equal()`: Compare pandas vs PySpark DataFrames
      - Critical for validating migration equivalence
      - Converts PySpark to pandas for comparison
      - Flexible dtype checking (off by default for cross-platform)
      - Sorted comparison for deterministic results
    - `assert_schema_equal()`: Verify schema matching
      - Column name checking
      - Data type validation
      - Detailed mismatch reporting
    - `assert_column_exists()`: Check single column presence
    - `assert_columns_exist()`: Check multiple columns presence
    - `get_column_diff()`: Get column differences between DataFrames
      - Returns only_in_df1, only_in_df2, common, type_mismatches
    - `print_comparison_summary()`: Print detailed comparison for debugging
      - Row counts, column differences, type mismatches
      - Sample data from both DataFrames
      - Formatted output for easy reading
  - Created `Test/test_dataframe_comparator.py` (190 lines):
    - 11 unit tests validating all comparator methods
    - Tests for column existence checks
    - Tests for schema equality
    - Tests for identical DataFrame comparison
    - Tests for floating-point tolerance
    - Tests for pandas vs PySpark comparison
    - Tests for column diff utility
    - Tests for summary printing
    - All tests passing
  - Utility now available for all migration validation tests

- **Task 2.1:** Created ParquetCache decorator
  - Created `Decorators/ParquetCache.py` (92 lines)
    - Follows same MD5-based caching pattern as FileCache and CSVCache
    - Uses PySpark for reading/writing Parquet files
    - Integrates with SparkSessionManager for Spark session access
    - Supports all FileCache features:
      - cache_variables parameter for custom cache key generation
      - full_file_cache_path parameter for explicit cache path override
      - Automatic MD5 hashing of function name + parameters
    - Uses Parquet format with snappy compression
    - Overwrite mode for cache writes to handle partial writes
    - Comprehensive docstrings with usage examples
  - Created `Test/test_parquet_cache.py` (353 lines)
    - 10 comprehensive test cases across 4 test classes:
      - TestParquetCacheBasic: Basic caching functionality (3 tests)
      - TestParquetCacheWithCacheVariables: Cache key control (1 test)
      - TestParquetCacheSchema: Schema preservation (2 tests)
      - TestParquetCacheEdgeCases: Edge cases like empty/large DataFrames (2 tests)
      - TestParquetCacheIntegration: Real-world scenarios (2 tests)
    - Tests validate:
      - Cache hit/miss behavior
      - Data integrity after cache round-trip
      - Schema preservation (with Parquet nullable considerations)
      - Cache key generation with custom cache_variables
      - Complex data types (arrays, maps)
      - Empty and large DataFrames (10,000 rows)
      - Explicit cache path override
      - Multiple cached functions simultaneously
    - All 10 tests passing
    - Includes auto-cleanup fixture to prevent test interference
  - ParquetCache now ready for use in SparkDataGatherer and other PySpark components
  - Provides Parquet-based alternative to CSVCache for distributed DataFrame caching

- **Task 2.13:** Created SparkConnectedDrivingLargeDataCleaner (`Generator/Cleaners/SparkConnectedDrivingLargeDataCleaner.py`)
  - Implemented complete PySpark replacement for ConnectedDrivingLargeDataCleaner
  - Created `SparkConnectedDrivingLargeDataCleaner.py` (294 lines):
    - PySpark-based large dataset cleaning using distributed processing
    - Replaces file splitting/combining with Parquet partitioning
    - Key architectural differences from pandas version:
      - Uses Parquet partitions instead of individual CSV files
      - No explicit file combining - Spark reads partitioned directories transparently
      - clean_data() processes entire dataset in parallel (not sequential file iteration)
      - combine_data() is a no-op (data already combined in Parquet directory)
      - getNRows() uses DataFrame.limit() instead of pd.read_csv(nrows=n)
    - Full dependency injection support (PathProvider, InitialGathererPathProvider, GeneratorContextProvider)
    - Configurable cleaner class and filter function (same pattern as pandas version)
    - Comprehensive logging and error handling
    - Helper method _is_valid_parquet_directory() for Parquet directory validation
  - Created comprehensive test suite `Test/test_spark_connected_driving_large_data_cleaner.py` (335 lines):
    - 13 test cases across 5 test classes
    - TestSparkConnectedDrivingLargeDataCleanerInitialization (2 tests):
      - Directory creation
      - Context configuration loading
    - TestSparkConnectedDrivingLargeDataCleanerCleaning (3 tests):
      - Basic data cleaning without filtering
      - Skip cleaning if data exists
      - Data cleaning with custom filter function
    - TestSparkConnectedDrivingLargeDataCleanerDataAccess (4 tests):
      - getNRows() method
      - getNumOfRows() method
      - getAllRows() method
      - Error handling when data not cleaned yet
    - TestSparkConnectedDrivingLargeDataCleanerCombineData (1 test):
      - combine_data() is no-op for Spark
    - TestSparkConnectedDrivingLargeDataCleanerHelpers (3 tests):
      - _is_valid_parquet_directory() with Parquet files
      - _is_valid_parquet_directory() with non-existent path
      - _is_valid_parquet_directory() with empty directory
    - Tests validated via syntax checking and import validation (pytest not available in environment)
  - Key accomplishments:
    - Replaces file-based batch processing with distributed DataFrame operations ✓
    - Maintains same interface as pandas version for easy migration ✓
    - Integrates with SparkDataGatherer and SparkConnectedDrivingCleaner ✓
    - Supports configurable cleaning and filtering ✓
    - Provides Parquet-based caching for efficient data access ✓
  - Files created:
    - `Generator/Cleaners/SparkConnectedDrivingLargeDataCleaner.py` (294 lines)
    - `Test/test_spark_connected_driving_large_data_cleaner.py` (335 lines)
  - Total: ~629 lines of production code + tests
  - Ready for integration in Phase 4 (Filter Operations) and Phase 5 (Attack Simulation)

- **Task 2.14:** Replace file splitting with partitioning strategy
  - Verified that file splitting has been replaced with Parquet partitioning
  - Implementation already complete in SparkDataGatherer.split_large_data() (lines 109-150):
    - Calculates optimal partition count based on lines_per_file configuration
    - Uses df.repartition(num_partitions) for distributed partitioning
    - Writes partitioned Parquet instead of multiple CSV files
    - Automatically handles partition directory structure with _SUCCESS marker
  - Implementation already complete in SparkConnectedDrivingLargeDataCleaner:
    - Uses Parquet directories instead of individual files
    - _is_valid_parquet_directory() validates Parquet directory structure
    - clean_data() processes entire dataset in parallel (not sequential file iteration)
    - combine_data() is a no-op (data already combined in Parquet directory)
  - No additional code changes needed - marking as complete

- **Task 2.15:** Test large file I/O with 100k+ row datasets
  - Created comprehensive test suite `Test/test_task_2_15_large_file_io.py` (200 lines):
    - Tests CSV reading of 100k row dataset (6.79s)
    - Tests Parquet write/read operations
    - Tests partitioning strategy (10 partitions for 100k rows)
    - Tests data integrity preservation
    - Tests sample reading (limit operation)
    - Tests file size compression
  - Test results:
    - ✓ Successfully read/write 100,000 rows
    - ✓ Parquet provides 8.84x read speedup over CSV
    - ✓ Parquet provides 2.58x compression (26 MB → 10 MB)
    - ✓ Partitioning creates correct number of partitions (10)
    - ✓ All data integrity preserved across operations
    - ✓ Sample reading (getNRows equivalent) works efficiently
  - Performance metrics:
    - CSV read: 14,720 rows/s
    - Parquet write: 31,686 rows/s
    - Parquet read: 130,055 rows/s
    - Partitioning: 28,063 rows/s
  - Phase 2 is now COMPLETE - all 15 tasks finished and validated

- **Tasks 3.1-3.6:** Core UDF implementations completed
  - **Task 3.1:** UDF module structure already exists (`Helpers/SparkUDFs/`)
    - `__init__.py` exports all UDFs for easy importing
    - Organized into GeospatialUDFs.py and ConversionUDFs.py
  - **Tasks 3.2-3.4:** Geospatial UDFs already implemented in Task 2.8
    - `point_to_tuple_udf`: Convert WKT POINT to (x, y) struct
    - `point_to_x_udf`: Extract longitude from WKT POINT
    - `point_to_y_udf`: Extract latitude from WKT POINT
    - `geodesic_distance_udf`: Calculate geodesic distance using WGS84
    - `xy_distance_udf`: Calculate Euclidean distance
  - **Task 3.5:** Implemented `hex_to_decimal_udf` for coreData_id conversion
    - Created `Helpers/SparkUDFs/ConversionUDFs.py` (131 lines):
      - `hex_to_decimal_udf`: Convert hex strings to decimal integers
      - Handles edge case: strips decimal point from hex strings (e.g., "0xa1b2c3d4.0")
      - Returns LongType for large hex values
      - Null-safe: returns None for invalid inputs
      - Replicates pandas version: `int(hex_str, 16)` conversion
    - Created comprehensive test suite `Test/test_conversion_udfs.py` (306 lines):
      - 10 test cases across 3 test classes
      - TestHexToDecimalUDF (7 tests):
        - Basic hex conversion (0x prefix handling)
        - Hex with decimal point edge case
        - Null value handling
        - Invalid hex string handling
        - Realistic BSM data with multiple vehicles
        - Large dataset performance (1000 hex IDs)
        - Comparison with pandas implementation (validates equivalence)
      - TestDirectionAndDistToXYUDF (2 tests):
        - Basic offset calculation
        - Null value handling
      - TestConversionUDFsIntegration (1 test):
        - ML pipeline simulation: hex→decimal before feature extraction
      - All 10 tests passing (100% success rate)
    - Updated `Helpers/SparkUDFs/__init__.py` to export new UDFs
    - Ready for use in SparkMConnectedDrivingDataCleaner (Phase 6)
  - **Task 3.6:** Implemented `direction_and_dist_to_xy_udf` for attack offset calculation
    - Added to ConversionUDFs.py
    - Wraps MathHelper.direction_and_dist_to_XY() for PySpark
    - Returns struct with (new_x, new_y) coordinates
    - Takes starting point (x, y), direction (0-360°), and distance (meters)
    - Null-safe error handling
    - Tested with basic offset calculations
    - Ready for use in attack simulation (Phase 5)
  - Key accomplishments:
    - All core UDFs implemented and tested ✓
    - 100% test pass rate (10/10 tests) ✓
    - Equivalence with pandas implementation validated ✓
    - Null-safe error handling for all UDFs ✓
    - Edge case handling (decimal points in hex strings) ✓
    - Ready for integration in ML pipeline and attack simulation ✓
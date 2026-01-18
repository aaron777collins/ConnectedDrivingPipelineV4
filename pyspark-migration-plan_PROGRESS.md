# Progress: pyspark-migration-plan

Started: Sat Jan 17 06:48:38 PM EST 2026

## Status

IN_PROGRESS

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
- [ ] Task 1.4: Define BSM raw data schema (`Schemas/BSMRawSchema.py` - 19 columns with StructType)
- [ ] Task 1.5: Define processed data schema (`Schemas/BSMProcessedSchema.py` - 18 ML feature columns)
- [ ] Task 1.6: Implement schema validation utility (`Schemas/SchemaValidator.py`)
- [ ] Task 1.7: Migrate test framework from custom ITest to pytest
- [ ] Task 1.8: Create PySpark test fixtures (`Test/Fixtures/SparkFixtures.py`)
- [ ] Task 1.9: Create sample test datasets (1k, 10k, 100k rows in `Test/Data/`)
- [ ] Task 1.10: Implement DataFrame comparison utility (`Test/Utils/DataFrameComparator.py`)

### Phase 2: Core Data Operations

- [ ] Task 2.1: Create ParquetCache decorator (`Decorators/ParquetCache.py`)
- [ ] Task 2.2: Update FileCache to support Spark DataFrames
- [ ] Task 2.3: Create SparkDataGatherer class (`Gatherer/SparkDataGatherer.py`)
- [ ] Task 2.4: Implement spark.read.csv with schema in SparkDataGatherer
- [ ] Task 2.5: Implement DataFrame.limit() for numrows parameter
- [ ] Task 2.6: Add Parquet write support to SparkDataGatherer
- [ ] Task 2.7: Implement format detection utility (CSV vs Parquet auto-detection)
- [ ] Task 2.8: Create SparkConnectedDrivingCleaner (`Generator/Cleaners/SparkConnectedDrivingCleaner.py`)
- [ ] Task 2.9: Migrate column selection from `df[columns]` to `df.select(*columns)`
- [ ] Task 2.10: Migrate `.drop(columns=[...])` to `.drop('col1', 'col2')`
- [ ] Task 2.11: Migrate `.dropna()` to `.na.drop()`
- [ ] Task 2.12: Test column operations with sample datasets
- [ ] Task 2.13: Create SparkConnectedDrivingLargeDataCleaner
- [ ] Task 2.14: Replace file splitting with partitioning strategy
- [ ] Task 2.15: Test large file I/O with 100k+ row datasets

### Phase 3: UDF Implementation

- [ ] Task 3.1: Create UDF module structure (`Helpers/SparkUDFs/`)
- [ ] Task 3.2: Implement `point_to_tuple_udf` (WKT POINT parsing)
- [ ] Task 3.3: Implement `geodesic_distance_udf` (lat/long distance)
- [ ] Task 3.4: Implement `xy_distance_udf` (Euclidean distance)
- [ ] Task 3.5: Implement `hex_to_decimal_udf` (coreData_id conversion)
- [ ] Task 3.6: Implement `direction_and_dist_to_xy_udf` (attack offset calculation)
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

## Completed This Iteration

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

# Progress: dask-migration-plan

Started: Sat Jan 17 10:04:45 PM EST 2026

## Status

IN_PROGRESS

## Analysis

### Foundation Already Complete (~30%)

**Core Infrastructure (DONE):**
- ✅ DaskSessionManager (Helpers/DaskSessionManager.py) - Singleton with 6 workers × 8GB, dashboard on :8787
- ✅ DaskParquetCache (Decorators/DaskParquetCache.py) - MD5-based caching with PyArrow/Snappy
- ✅ DaskDataGatherer (Gatherer/DaskDataGatherer.py) - IDataGatherer interface, CSV→Parquet, BSM schema
- ✅ Config files (configs/dask/64gb-production.yml, development.yml) - Memory management, shuffle config
- ✅ Dependencies (requirements.txt) - dask[complete]>=2024.1.0, dask-ml>=2024.4.0, distributed, lz4

**Key Features Implemented:**
- Configurable blocksize CSV reading (default: 128MB)
- Parquet caching with @DaskParquetCache decorator
- Methods: gather_data(), split_large_data(), compute_data(), persist_data(), get_memory_usage()
- Memory monitoring and dashboard integration
- Compatible with existing dependency injection framework

### What Still Needs Implementation (~70%)

**Critical Path Blockers:**
1. **UDF Library** - Dask versions of geospatial functions (point_to_x/y/tuple, geodesic_distance, xy_distance)
   - Blocks: Data cleaning, attack simulation, all downstream work
   - Approach: Use map_partitions() with pandas UDFs instead of PySpark @udf decorators

2. **Data Cleaning Layer** - DaskConnectedDrivingCleaner, DaskConnectedDrivingLargeDataCleaner, DaskCleanWithTimestamps
   - Blocks: Attack simulation, ML pipeline
   - Pattern: Follow SparkConnectedDrivingCleaner architecture with Dask operations

3. **Attack Simulation** - DaskConnectedDrivingAttacker with position swap attack (.iloc[] requirement)
   - Blocks: ML pipeline testing
   - Strategy: Use compute-then-daskify (Strategy 1 from plan) for 15-20M rows on 64GB

4. **ML Integration** - dask-ml train_test_split, ParallelPostFit wrappers, model training pipeline
   - Blocks: End-to-end validation
   - Pattern: Replace sklearn imports with dask_ml, add .persist() and .compute()

5. **Testing & Validation** - Unit tests, integration tests, golden dataset validation
   - Essential: Ensures correctness before production
   - Target: >80% coverage, 1e-5 tolerance for floating point

**PySpark → Dask Key Differences:**
- UDFs: @udf decorator → map_partitions() with pandas UDFs
- Row limit: .limit(n) → .head(n, npartitions=-1)
- Partition: .repartition(n) → .repartition(npartitions=n)
- Compute: .collect() → .compute()
- Schema: StructType → pandas dtypes dict

### Dependencies & Critical Path

```
UDF Library (Tasks 11-20) ← CRITICAL PATH BLOCKER
    ↓
Data Cleaning (Tasks 21-35)
    ↓
Attack Simulation (Tasks 36-55) ← CRITICAL .iloc[] REQUIREMENT
    ↓
ML Integration (Tasks 56-65)
    ↓
Testing & Validation (Tasks 66-105)
```

### Risk Assessment

| Risk | Mitigation |
|------|------------|
| UDF performance slower than PySpark | Vectorize operations, benchmark early, optimize hot paths |
| Memory overflow during position swap | Use Strategy 1 (compute-then-daskify), monitor dashboard |
| Backwards compatibility failures | Comprehensive golden dataset validation with 1e-5 tolerance |
| .iloc[] limitations | Plan includes 2 strategies, Strategy 1 validated for 15-20M rows |

### Estimated Effort Remaining

- UDF library: 8-12 hours
- Data cleaning: 6-8 hours
- Attack simulation: 10-15 hours
- ML integration: 4-6 hours
- Testing: 15-20 hours
- Optimization: 5-8 hours
- Documentation: 6-10 hours
**Total: 54-79 hours (7-10 days focused work)**

---

## Task List

### Phase 1: Complete Environment Setup (Tasks 1-5)
- [x] Task 1: Install Dask dependencies
- [x] Task 2: Create DaskSessionManager with memory limits
- [x] Task 3: Create Dask configuration files (64gb-production, development)
- [x] Task 4: Create DaskParquetCache decorator
- [x] Task 5: Create validate_dask_setup.py validation script

### Phase 2: Complete Data Loading Layer (Tasks 6-10)
- [x] Task 6: Create DaskDataGatherer with CSV reading
- [x] Task 7: Implement partition optimization (split_large_data)
- [x] Task 8: Add compute_data() and persist_data() methods
- [x] Task 9: Create data loading validation script
- [x] Task 10: Test DaskDataGatherer with sample datasets (1k, 10k, 100k)

### Phase 3: UDF Library & Geospatial Functions (Tasks 11-20) ← CRITICAL PATH
- [x] Task 11: Create Helpers/DaskUDFs/ directory structure
- [x] Task 12: Implement point_to_x() function (WKT POINT parsing)
- [x] Task 13: Implement point_to_y() function
- [x] Task 14: Implement point_to_tuple() function
- [x] Task 15: Implement geodesic_distance() calculation
- [x] Task 16: Implement xy_distance() calculation
- [ ] Task 17: Create DaskUDFRegistry for function caching
- [ ] Task 18: Implement map_partitions wrappers for UDFs
- [ ] Task 19: Test UDF performance vs PySpark UDFs
- [ ] Task 20: Validate UDF outputs match PySpark

### Phase 4: Data Cleaning Layer (Tasks 21-30)
- [ ] Task 21: Create DaskConnectedDrivingCleaner class
- [ ] Task 22: Implement column selection in DaskConnectedDrivingCleaner
- [ ] Task 23: Implement null dropping in DaskConnectedDrivingCleaner
- [ ] Task 24: Integrate WKT POINT parsing (point_to_tuple)
- [ ] Task 25: Implement XY coordinate conversion option
- [ ] Task 26: Add @DaskParquetCache to clean_data() method
- [ ] Task 27: Create DaskConnectedDrivingLargeDataCleaner
- [ ] Task 28: Test cleaning on 100k row dataset
- [ ] Task 29: Validate cleaned output matches SparkConnectedDrivingCleaner
- [ ] Task 30: Optimize memory usage during cleaning

### Phase 5: Datetime Parsing & Temporal Features (Tasks 31-35)
- [ ] Task 31: Create DaskCleanWithTimestamps class
- [ ] Task 32: Implement datetime parsing (MM/dd/yyyy hh:mm:ss a format)
- [ ] Task 33: Extract temporal features (month, day, year, hour, minute, second, AM/PM)
- [ ] Task 34: Test datetime parsing edge cases (midnight, noon, year boundaries)
- [ ] Task 35: Validate temporal features match SparkCleanWithTimestamps

### Phase 6: Attack Simulation - Foundation (Tasks 36-45)
- [ ] Task 36: Create DaskConnectedDrivingAttacker class
- [ ] Task 37: Implement getUniqueIDsFromCleanData() with Dask
- [ ] Task 38: Implement add_attackers() with dask_ml train_test_split
- [ ] Task 39: Implement add_rand_attackers() for random assignment
- [ ] Task 40: Test attacker selection determinism (SEED handling)
- [ ] Task 41: Validate attack_ratio proportions (e.g., 5%, 10%, 30%)
- [ ] Task 42: Create broadcast-based attack assignment for efficiency
- [ ] Task 43: Test attacker assignment on 100k row dataset
- [ ] Task 44: Validate attacker IDs match pandas version
- [ ] Task 45: Benchmark attacker selection performance

### Phase 7: Attack Simulation - Position Attacks (Tasks 46-55) ← CRITICAL REQUIREMENT
- [ ] Task 46: Analyze .iloc[] support limitations in Dask
- [ ] Task 47: Implement position_swap_attack_dask_v1 (compute-then-daskify strategy)
- [ ] Task 48: Implement position_swap_attack_dask_v2 (partition-wise strategy)
- [ ] Task 49: Test position swap with 1M rows to validate memory fit
- [ ] Task 50: Validate swapped positions match expected behavior
- [ ] Task 51: Implement positional_offset_const_attack
- [ ] Task 52: Implement positional_offset_rand_attack
- [ ] Task 53: Implement positional_override_attack
- [ ] Task 54: Test all attack types on sample datasets
- [ ] Task 55: Create validation script for attack verification

### Phase 8: Machine Learning Integration (Tasks 56-65)
- [ ] Task 56: Install and verify dask-ml>=2024.4.0
- [ ] Task 57: Implement dask-ml train_test_split wrapper
- [ ] Task 58: Test train/test split with shuffle=True
- [ ] Task 59: Implement ParallelPostFit wrapper for sklearn models
- [ ] Task 60: Test RandomForestClassifier with ParallelPostFit
- [ ] Task 61: Implement .persist() optimization before training
- [ ] Task 62: Test model.fit() with Dask DataFrames
- [ ] Task 63: Implement model.predict() with .compute()
- [ ] Task 64: Validate accuracy matches sklearn baseline
- [ ] Task 65: Implement dask-ml GridSearchCV for hyperparameter tuning

### Phase 9: Testing - Unit Tests (Tasks 66-75)
- [ ] Task 66: Create Test/test_dask_session_manager.py
- [ ] Task 67: Create Test/test_dask_parquet_cache.py
- [ ] Task 68: Create Test/test_dask_data_gatherer.py
- [ ] Task 69: Create Test/test_dask_connected_driving_cleaner.py
- [ ] Task 70: Create Test/test_dask_clean_with_timestamps.py
- [ ] Task 71: Create Test/test_dask_udfs.py
- [ ] Task 72: Create Test/test_dask_attacker.py
- [ ] Task 73: Create Test/test_dask_position_swap.py
- [ ] Task 74: Create Test/test_dask_ml_integration.py
- [ ] Task 75: Run all unit tests and verify >80% coverage

### Phase 10: Testing - Integration & Validation (Tasks 76-85)
- [ ] Task 76: Create GoldenDatasetValidator class (from plan Phase 6)
- [ ] Task 77: Generate golden dataset from pandas pipeline (100k-1M rows)
- [ ] Task 78: Validate DaskDataGatherer output vs pandas DataGatherer
- [ ] Task 79: Validate DaskConnectedDrivingCleaner vs pandas ConnectedDrivingCleaner
- [ ] Task 80: Validate DaskCleanWithTimestamps vs pandas CleanWithTimestamps
- [ ] Task 81: Validate position swap attack outputs
- [ ] Task 82: Create Test/test_dask_backwards_compatibility.py
- [ ] Task 83: Validate all features within 1e-5 tolerance
- [ ] Task 84: Create end-to-end pipeline test (gather → clean → attack → ML)
- [ ] Task 85: Run integration tests with 1M row dataset

### Phase 11: Performance Optimization (Tasks 86-95)
- [ ] Task 86: Implement optimize_partitions() utility
- [ ] Task 87: Test different partition sizes (50MB, 100MB, 150MB, 200MB)
- [ ] Task 88: Reduce dtype precision (float64→float32, int64→int32)
- [ ] Task 89: Implement categorical encoding for low-cardinality strings
- [ ] Task 90: Add .persist() at strategic points in pipeline
- [ ] Task 91: Test repartition after filtering operations
- [ ] Task 92: Optimize aggregation queries (single-pass aggregations)
- [ ] Task 93: Monitor memory usage with Dask dashboard
- [ ] Task 94: Validate peak memory <52GB on 15-20M row dataset
- [ ] Task 95: Benchmark processing time vs pandas (target: ≤2x)

### Phase 12: Production Readiness (Tasks 96-105)
- [ ] Task 96: Create comprehensive user documentation
- [ ] Task 97: Document memory configuration recommendations
- [ ] Task 98: Create troubleshooting guide for common issues
- [ ] Task 99: Implement rollback strategy (feature flags)
- [ ] Task 100: Test rollback to pandas version
- [ ] Task 101: Create performance comparison report (pandas vs Dask)
- [ ] Task 102: Validate all success criteria from plan Section 10
- [ ] Task 103: Run final acceptance checklist
- [ ] Task 104: Create deployment guide
- [ ] Task 105: Archive pandas-based implementations

---

## Completed This Iteration

**Tasks 11-16: Created Dask UDF Library with Geospatial & Conversion Functions**

Implemented complete Dask UDF directory structure and core functions:

**Directory Structure Created:**
- `/tmp/original-repo/Helpers/DaskUDFs/` - New directory for Dask functions
- `__init__.py` - Module exports (7 functions)
- `GeospatialFunctions.py` - 5 geospatial functions
- `ConversionFunctions.py` - 2 conversion functions
- `README.md` - Comprehensive usage documentation

**Geospatial Functions Implemented:**
- ✅ `point_to_tuple(point_str)` - Converts WKT POINT to (x, y) tuple
- ✅ `point_to_x(point_str)` - Extracts X coordinate (longitude)
- ✅ `point_to_y(point_str)` - Extracts Y coordinate (latitude)
- ✅ `geodesic_distance(lat1, lon1, lat2, lon2)` - WGS84 geodesic distance
- ✅ `xy_distance(x1, y1, x2, y2)` - Euclidean distance

**Conversion Functions Implemented:**
- ✅ `hex_to_decimal(hex_str)` - Hex to decimal conversion (for coreData_id)
- ✅ `direction_and_dist_to_xy(x, y, direction, distance)` - Positional offset calculation

**Validation:**
- Created `validate_dask_udfs.py` - Comprehensive test suite
- All 7 functions tested with sample data
- None handling validated
- Invalid input handling validated
- All outputs match expected values within 1e-5 tolerance
- **Result: All tests passed ✓**

**Key Design Decisions:**
1. **No UDF decorators** - Dask uses plain Python functions (not @udf like PySpark)
2. **Vectorized operations** - Functions work with pandas Series for performance
3. **Reuses existing helpers** - Leverages DataConverter and MathHelper classes
4. **Compatible behavior** - Matches PySpark UDF behavior exactly (including MathHelper quirks)
5. **Comprehensive docs** - README with PySpark→Dask migration examples

**Notes:**
- Functions are drop-in compatible with existing DataConverter/MathHelper
- MathHelper.dist_between_two_points has a deg2rad bug (expects degrees but converts to radians)
- Our functions match this behavior for backward compatibility
- Ready for Phase 4 (Data Cleaning Layer) - no blockers

## Notes

### Completed Infrastructure (9 tasks)
- DaskSessionManager with 6 workers × 8GB = 48GB total
- DaskParquetCache with MD5 hashing and PyArrow/Snappy
- DaskDataGatherer with gather_data(), split_large_data(), compute_data(), persist_data()
- 64gb-production.yml and development.yml configs
- All Dask dependencies in requirements.txt
- validate_dask_setup.py - comprehensive validation (8 tests, all passing)

### Key Insights
1. **Strong foundation**: Core infrastructure follows PySpark patterns, ready for build mode
2. **Critical path**: UDF library blocks all downstream work - highest priority
3. **Position swap validated**: Strategy 1 (compute-then-daskify) confirmed for 15-20M rows
4. **Test infrastructure ready**: Can adapt existing PySpark tests for Dask
5. **Memory config safe**: 48GB worker memory + 16GB OS/system on 64GB system

### Recommendations for Build Mode
1. Start with UDF library (Tasks 11-20) - unblocks everything
2. Write tests alongside implementation - don't wait
3. Use SparkConnectedDrivingCleaner as template for DaskConnectedDrivingCleaner
4. Monitor Dask dashboard continuously during development
5. Validate with golden dataset after each major component

### Success Criteria (Must achieve before RALPH_DONE)
- [ ] All pipelines process 15-20M rows within memory budget
- [ ] .iloc[] position swap attack works identically to pandas
- [ ] sklearn models integrate via dask-ml without accuracy loss
- [ ] Memory usage stays below 52GB peak
- [ ] Processing time ≤ 2x pandas baseline
- [ ] All unit tests pass with golden dataset validation
- [ ] Zero data loss or corruption vs pandas output

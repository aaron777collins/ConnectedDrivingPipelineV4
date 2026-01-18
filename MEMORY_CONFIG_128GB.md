# ⚠️ CRITICAL: 128GB Memory Configuration

**Date:** 2026-01-17
**Status:** ACTIVE REQUIREMENT

## Overview

This PySpark migration targets a **128GB RAM single-node system**. All code, tests, and configurations MUST use conservative memory allocation to leave 18-20GB for the Linux kernel and system processes.

## Mandatory Configuration

### Target Memory Allocation
```
Driver JVM:           13.2 GB  (12GB heap + 1.2GB overhead)
Executor JVM:         88.0 GB  (80GB heap + 8GB overhead)
Python UDF workers:    4-6 GB  (8 cores × 0.5-0.75GB each)
Linux kernel + OS:    18-20 GB (CRITICAL - must remain free)
───────────────────────────────────────────────────────
TOTAL:               ~123-127 GB
```

### Configuration File
**USE THIS:** `configs/spark/128gb-single-node.yml`

```python
from Helpers.SparkSessionManager import SparkSessionManager

# CORRECT: Load 128GB configuration
spark = SparkSessionManager.get_session_from_config_file(
    'configs/spark/128gb-single-node.yml'
)
```

### Key Parameters
```yaml
driver_memory: "12g"
executor_memory: "80g"
executor_memoryOverhead: "8g"
executor_cores: 8
memory_fraction: 0.8
memory_storage_fraction: 0.4
sql_shuffle_partitions: 200
```

## DO NOT Use These Configurations

❌ **WRONG:** `cluster.yml` (designed for multi-node, 16GB driver + 32GB executor)
❌ **WRONG:** `large-dataset.yml` (designed for 500GB+ cluster, 32GB driver + 64GB executor)
❌ **WRONG:** Custom configs with > 80GB executor memory

## Implementation Guidelines

### 1. All Tests MUST Use Conservative Memory
```python
# Test fixtures in Test/Fixtures/SparkFixtures.py
@pytest.fixture(scope="session")
def spark_session():
    """Session-scoped Spark session for testing"""
    spark = SparkSession.builder \
        .master("local[2]") \
        .config("spark.driver.memory", "2g") \  # SMALL for tests
        .config("spark.executor.memory", "2g") \  # SMALL for tests
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    return spark
```

### 2. Production Code MUST Reference 128GB Config
```python
# Example in pipeline scripts
from Helpers.SparkSessionManager import SparkSessionManager

# Initialize with 128GB configuration
spark = SparkSessionManager.get_session_from_config_file(
    'configs/spark/128gb-single-node.yml'
)

# Process data
df = spark.read.parquet("large_bsm_data.parquet")
# ... rest of pipeline
```

### 3. Documentation MUST Reflect 128GB Requirement
- All README files should specify 128GB RAM as minimum for production
- Examples should use `128gb-single-node.yml`
- Performance estimates should assume single-node, 8-core execution

## Validation Checklist

Before completing any task, verify:
- [ ] Tests use ≤ 2-4GB total memory allocation
- [ ] Production code references `128gb-single-node.yml`
- [ ] No hardcoded memory values > 80GB executor / 12GB driver
- [ ] Documentation mentions 128GB requirement
- [ ] Examples show correct configuration loading

## Contingency Plans

### If Memory Pressure Detected
1. **First:** Reduce `executor_memory` from 80g to 72g
2. **Second:** Reduce `driver_memory` from 12g to 10g
3. **Third:** Increase `memory_fraction` from 0.8 to 0.85
4. **Last Resort:** Reduce dataset size or add chunking

### If Shuffle Errors
1. Increase `executor_memoryOverhead` from 8g to 10g
2. Reduce `sql_shuffle_partitions` from 200 to 150
3. Add disk-based shuffle spilling configuration

### If UDF Performance Issues
1. Increase `python_worker_memory` from 2g to 3g
2. Reduce `executor_cores` from 8 to 6 (fewer but more memory per worker)
3. Convert to pandas UDFs for vectorization

## Monitoring Commands

### During Execution
```bash
# Monitor system memory in real-time
watch -n 1 free -h

# Check Spark memory usage
curl -s http://localhost:4040/api/v1/applications | jq '.[0].attempts[0].sparkUser'

# Check for OOM kills
dmesg | grep -i "out of memory"

# Monitor Spark UI
firefox http://localhost:4040
```

### Post-Execution
```bash
# Check Spark event logs
ls -lh /tmp/spark-events/

# Analyze GC overhead
grep "GC" /tmp/spark-events/*.log
```

## References

- **Main Migration Plan:** `/tmp/pyspark-migration-plan.md` (see Phase 6.2)
- **Configuration File:** `configs/spark/128gb-single-node.yml`
- **Configuration README:** `configs/spark/README.md`
- **Progress Tracker:** `pyspark-migration-plan_PROGRESS.md`

---

## For Ralph (Build Agent)

**CRITICAL INSTRUCTIONS:**

1. **Every task you implement** must respect the 128GB memory limit
2. **Every test you write** must use ≤ 2-4GB (test fixtures)
3. **Every example you create** should reference `128gb-single-node.yml`
4. **Phase 7** (Optimization) - Use 128GB config values, NOT the old 16g/32g values
5. **Phase 9** (Deployment) - Document 128GB as production requirement

If you create any code that exceeds these limits, STOP and revise immediately.

# Spark Configuration Templates

This directory contains Spark configuration templates for different deployment scenarios of the ConnectedDrivingPipelineV4 PySpark migration.

## Configuration Files

### `128gb-single-node.yml` ⭐ **RECOMMENDED FOR PRODUCTION**
**Use for:** Processing large BSM datasets on a 128GB RAM single-node workstation/server

**Characteristics:**
- **Target System:** 128GB RAM (conservatively allocates 108-110GB, leaves 18-20GB for OS/kernel)
- Single-node execution with 8 cores (`local[8]`)
- **Driver memory:** 12GB (+ 1.2GB overhead = 13.2GB total)
- **Executor memory:** 80GB (+ 8GB overhead = 88GB total)
- Optimized for pandas UDF performance (memory_fraction=0.8)
- Shuffle partitions: 200 (optimal for 100M+ rows on single node)
- Suitable for datasets: 10M-100M+ rows
- Can cache 2-3 large DataFrames simultaneously
- All shuffle operations in-memory (no disk spill)

**Memory Breakdown:**
```
Driver JVM:           13.2 GB
Executor JVM:         88.0 GB
Python UDF workers:    4-6 GB
Linux kernel + OS:    18-20 GB
─────────────────────────────
TOTAL:               ~123-127 GB
```

**Example usage:**
```python
from Helpers.SparkSessionManager import SparkSessionManager

# Load 128GB configuration from YAML
spark = SparkSessionManager.get_session_from_config_file(
    'configs/spark/128gb-single-node.yml'
)

# Or use direct configuration
spark = SparkSessionManager.get_local_session(
    cores=8,
    driver_memory="12g",
    executor_memory="80g"
)
```

**⚠️ CRITICAL WARNINGS:**
- **DO NOT** increase memory beyond these values - system stability requires 15-20GB free for OS/kernel
- Monitor system memory during execution: `watch -n 1 free -h`
- If OOM detected: reduce `executor_memory` to 72g
- Check Spark UI at http://localhost:4040 during execution

---

### `local.yml`
**Use for:** Local development and testing on laptops/workstations

**Characteristics:**
- Uses all available CPU cores (`local[*]`)
- Conservative memory settings (4GB driver/executor)
- Reduced shuffle partitions (8) for faster startup
- Suitable for datasets up to 100k-1M rows
- No dynamic allocation

**Example usage:**
```python
from Helpers.SparkSessionManager import SparkSessionManager

# Option 1: Use built-in local preset
spark = SparkSessionManager.get_local_session(cores="*")

# Option 2: Load custom configuration from YAML
import yaml
with open('configs/spark/local.yml') as f:
    config = yaml.safe_load(f)['spark_config']
spark = SparkSessionManager.get_session(
    app_name=config['app_name'],
    config=config
)
```

### `cluster.yml`
**Use for:** Production deployment on Compute Canada HPC clusters

**Characteristics:**
- YARN resource management
- Production-grade memory (16GB driver, 32GB executor)
- Dynamic allocation (1-10 executors)
- Shuffle partitions: 200 (standard for large datasets)
- Suitable for datasets of 1M-100M rows
- Event logging enabled for Spark History Server

**Example usage:**
```python
from Helpers.SparkSessionManager import SparkSessionManager

# Option 1: Use built-in cluster preset
spark = SparkSessionManager.get_cluster_session(
    driver_memory="16g",
    executor_memory="32g",
    executor_cores=4,
    num_executors=10
)

# Option 2: Load from YAML
import yaml
with open('configs/spark/cluster.yml') as f:
    config = yaml.safe_load(f)['spark_config']
spark = SparkSessionManager.get_session(
    app_name=config['app_name'],
    config=config
)
```

### `large-dataset.yml`
**Use for:** Processing massive datasets (100M+ rows, multi-GB Parquet files)

**Characteristics:**
- High memory allocation (32GB driver, 64GB executor)
- Aggressive dynamic allocation (5-50 executors)
- Increased shuffle partitions (400)
- Off-heap memory enabled (16GB)
- Kryo serialization for performance
- Suitable for datasets > 100M rows
- Requires significant cluster resources (500GB+ total memory)

**Example usage:**
```python
import yaml
from Helpers.SparkSessionManager import SparkSessionManager

with open('configs/spark/large-dataset.yml') as f:
    config = yaml.safe_load(f)['spark_config']

spark = SparkSessionManager.get_session(
    app_name=config['app_name'],
    config=config
)
```

## Configuration Parameters Explained

### Memory Settings
- **`driver_memory`**: Memory allocated to the driver process (runs your main program)
- **`executor_memory`**: Memory per executor (workers that process data)
- **`memory_fraction`**: Fraction of heap space for execution and storage (default: 0.6)
- **`memory_storage_fraction`**: Fraction of memory_fraction for cached data (default: 0.5)

### Parallelism Settings
- **`executor_cores`**: Number of CPU cores per executor
- **`sql_shuffle_partitions`**: Number of partitions for shuffle operations (joins, aggregations)
- **`default_parallelism`**: Default parallelism for RDD operations

### Dynamic Allocation
- **`dynamicAllocation_enabled`**: Enable automatic scaling of executors
- **`dynamicAllocation_minExecutors`**: Minimum executors to keep alive
- **`dynamicAllocation_maxExecutors`**: Maximum executors to scale up to
- **`dynamicAllocation_initialExecutors`**: Starting number of executors

### Performance Tuning
- **`sql_adaptive_enabled`**: Enable adaptive query execution (AQE)
- **`sql_adaptive_coalesce_partitions_enabled`**: Reduce partitions after shuffle
- **`sql_adaptive_skewJoin_enabled`**: Handle skewed data in joins
- **`sql_autoBroadcastJoinThreshold`**: Max size for broadcast joins

## Choosing the Right Configuration

### Dataset Size Guide

| Dataset Size | Rows | Configuration | Expected Memory | Executors |
|--------------|------|---------------|-----------------|-----------|
| Small | < 100k | `local.yml` | 4GB | 1 (local) |
| Medium | 100k - 1M | `local.yml` or `cluster.yml` | 8-16GB | 2-4 |
| Large | 1M - 100M | `cluster.yml` | 64-256GB | 4-10 |
| Very Large | 100M+ | `large-dataset.yml` | 500GB+ | 10-50 |

### Deployment Environment Guide

| Environment | Configuration | Notes |
|-------------|---------------|-------|
| Laptop/Workstation | `local.yml` | For development and testing |
| Compute Canada Cedar/Graham | `cluster.yml` | Standard production workload |
| Compute Canada Niagara | `large-dataset.yml` | High-memory nodes available |
| AWS EMR / Databricks | `cluster.yml` (modified) | Adjust for cloud-specific settings |

## Customizing Configurations

### Method 1: Edit YAML Files Directly
Simply modify the YAML files to adjust parameters for your specific needs.

### Method 2: Override in Code
```python
import yaml
from Helpers.SparkSessionManager import SparkSessionManager

# Load base config
with open('configs/spark/cluster.yml') as f:
    config = yaml.safe_load(f)['spark_config']

# Override specific settings
config['executor_memory'] = '64g'
config['sql_shuffle_partitions'] = 300

spark = SparkSessionManager.get_session(
    app_name=config['app_name'],
    config=config
)
```

### Method 3: Environment-Specific Configs
Create custom YAML files for specific scenarios:
```bash
cp configs/spark/cluster.yml configs/spark/cedar-production.yml
# Edit cedar-production.yml with cluster-specific settings
```

## Configuration for Specific Pipeline Stages

### Data Gathering (Reading Large CSVs)
- Use higher `executor_memory` (32GB+)
- Increase `sql_files_maxPartitionBytes` to control file splits
- Enable `sql_adaptive_coalesce_partitions_enabled`

### Attack Simulation (UDF-Heavy Operations)
- Increase `python_worker_memory` (4GB+)
- Use `python_worker_reuse: true`
- Consider `executor_cores: 4-5` for better parallelism

### ML Training (sklearn with toPandas())
- High `driver_memory` (32GB+) - driver collects data
- Moderate `executor_memory` (16-32GB)
- Lower `dynamicAllocation_maxExecutors` to preserve memory

### Caching & Checkpointing
- Increase `memory_storage_fraction` (0.5-0.6)
- Set `spark.local.dir` to fast SSD storage
- Enable `storage_memoryMapThreshold`

## Monitoring and Debugging

### Spark UI
Access the Spark UI at `http://<driver-host>:4040` to monitor:
- Job stages and tasks
- Memory usage
- Shuffle read/write
- Executor metrics

### Event Logs
If `eventLog_enabled: true`, view logs with Spark History Server:
```bash
# Start History Server
spark-class org.apache.spark.deploy.history.HistoryServer
# Access at http://localhost:18080
```

### Common Issues and Solutions

**Issue: Out of Memory Errors**
- Increase `executor_memory` and `driver_memory`
- Increase `sql_shuffle_partitions` to distribute data
- Enable `memory_offHeap_enabled`

**Issue: Slow Shuffles**
- Increase `sql_shuffle_partitions`
- Enable `sql_adaptive_enabled`
- Check for data skew with `sql_adaptive_skewJoin_enabled`

**Issue: Python Worker Crashes**
- Increase `python_worker_memory`
- Simplify UDF logic (avoid heavy Python libraries)
- Consider pandas UDFs instead of regular UDFs

**Issue: Slow Startup**
- Reduce `dynamicAllocation_initialExecutors`
- Use `local.yml` for development (faster)
- Reuse SparkSession across pipeline stages

## Integration with Existing Shell Scripts

The existing shell scripts (`defaultrunnerconfig.sh`, `jobpipelinedefaultrunnerconfig.sh`) can be updated to pass configuration file paths:

```bash
# Updated defaultrunnerconfig.sh usage
SPARK_CONFIG="configs/spark/cluster.yml" python MClassifierLargePipeline.py
```

## Version History

- **v1.0** (2026-01-17): Initial configuration templates created
  - local.yml: Local development configuration
  - cluster.yml: Compute Canada cluster configuration
  - large-dataset.yml: Large dataset optimization configuration

## References

- [PySpark Configuration Documentation](https://spark.apache.org/docs/latest/configuration.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)
- [Compute Canada Spark Guide](https://docs.alliancecan.ca/wiki/Apache_Spark)
- ConnectedDrivingPipelineV4 Migration Plan: `/tmp/pyspark-migration-plan.md`

# API Reference - Dask Components

**Version:** 1.0.0
**Last Updated:** 2026-01-18
**Framework:** ConnectedDrivingPipelineV4 (Dask Migration)

---

## Table of Contents

1. [Overview](#overview)
2. [Core Infrastructure](#core-infrastructure)
   - [DaskSessionManager](#dasksessionmanager)
   - [DaskParquetCache](#daskparquetcache)
3. [Pipeline Components](#pipeline-components)
   - [DaskPipelineRunner](#daskpipelinerunner)
   - [DaskMClassifierPipeline](#daskmclassifierpipeline)
4. [Data Layer](#data-layer)
   - [DaskDataGatherer](#daskdatagatherer)
   - [DaskConnectedDrivingCleaner](#daskconnecteddrivingcleaner)
   - [DaskCleanWithTimestamps](#daskcleanwithtimestamps)
   - [DaskConnectedDrivingLargeDataCleaner](#daskconnecteddrivinglargedatacleaner)
5. [Filtering Components](#filtering-components)
   - [DaskCleanerWithPassthroughFilter](#daskcleanerwithpassthroughfilter)
   - [DaskCleanerWithFilterWithinRange](#daskcleanerwithfilterwithinrange)
   - [DaskCleanerWithFilterWithinRangeXY](#daskcleanerwithfilterwithinrangexy)
   - [DaskCleanerWithFilterWithinRangeXYAndDay](#daskcleanerwithfilterwithinrangexyandday)
   - [DaskCleanerWithFilterWithinRangeXYAndDateRange](#daskcleanerwithfilterwithinrangexyanddaterange)
6. [ML Components](#ml-components)
   - [DaskMConnectedDrivingDataCleaner](#daskmconnecteddrivingdatacleaner)
   - [DaskConnectedDrivingAttacker](#daskconnecteddrivingattacker)
7. [Utilities](#utilities)
   - [DaskUDFRegistry](#daskudfregistry)
8. [Testing](#testing)
   - [DaskFixtures](#daskfixtures)

---

<a name="overview"></a>
## Overview

The Dask migration provides distributed, memory-efficient alternatives to the original pandas/PySpark components. All Dask components follow consistent patterns:

- **Lazy evaluation**: Operations build computation graphs, execute with `.compute()`
- **Parquet caching**: Using `@DaskParquetCache` decorator for efficient persistence
- **Memory safety**: Optimized for 64GB systems with 15M+ row datasets
- **Interface compatibility**: Same method signatures as pandas versions where possible

**System Requirements:**
- 64GB RAM (recommended)
- 6-12 CPU cores
- 500GB+ SSD storage

**Dependencies:**
```python
dask[complete]>=2024.1.0
dask-ml>=2024.4.0
distributed>=2024.1.0
pyarrow>=14.0.0
```

For installation instructions, see the main [README.md](../README.md).

---

<a name="core-infrastructure"></a>
## Core Infrastructure

<a name="dasksessionmanager"></a>
### DaskSessionManager

**Module:** `Helpers/DaskSessionManager.py`

Centralized singleton for managing Dask distributed cluster and client lifecycle.

#### Features

- **Singleton pattern** - One cluster per application
- **Auto-configuration** - Loads settings from YAML config files
- **Memory-safe defaults** - Optimized for 64GB systems (6 workers × 8GB)
- **Dashboard integration** - Built-in monitoring interface
- **Graceful shutdown** - Proper resource cleanup

#### Usage Example

```python
from Helpers.DaskSessionManager import DaskSessionManager

# Get or create Dask client (singleton)
client = DaskSessionManager.get_client()

# Access dashboard for monitoring
print(f"Dashboard: {client.dashboard_link}")

# Check worker info
workers = DaskSessionManager.get_worker_info()
print(f"Active workers: {len(workers)}")

# Check memory usage
memory = DaskSessionManager.get_memory_usage()
print(f"Total memory: {memory['total_memory_gb']:.2f} GB")
print(f"Used memory: {memory['used_memory_gb']:.2f} GB")

# Restart cluster if needed
DaskSessionManager.restart()

# Shutdown at end of application
DaskSessionManager.shutdown()
```

#### API Reference

##### `get_cluster() -> LocalCluster`

Creates or retrieves the singleton Dask LocalCluster.

**Returns:**
- `LocalCluster`: Configured Dask cluster instance

**Configuration:**
- Default: 6 workers, 8GB per worker (48GB total)
- Configurable via `config/dask_config.yaml`

**Example:**
```python
cluster = DaskSessionManager.get_cluster()
print(f"Scheduler address: {cluster.scheduler_address}")
```

---

##### `get_client() -> Client`

Gets or creates the Dask Client connected to the cluster.

**Returns:**
- `Client`: Dask distributed client

**Notes:**
- Automatically creates cluster if not exists
- Reuses existing client (singleton)
- Safe to call multiple times

**Example:**
```python
client = DaskSessionManager.get_client()
futures = client.map(lambda x: x**2, range(10))
results = client.gather(futures)
```

---

##### `shutdown() -> None`

Shuts down the Dask cluster and client, releasing all resources.

**Side Effects:**
- Closes all active workers
- Terminates scheduler
- Clears singleton state

**Example:**
```python
# At end of application
DaskSessionManager.shutdown()
```

---

##### `restart() -> None`

Restarts the Dask cluster (shutdown + get_client).

**Use Cases:**
- Recovering from worker failures
- Clearing memory leaks
- Applying new configuration

**Example:**
```python
# After heavy computation
DaskSessionManager.restart()
```

---

##### `get_dashboard_link() -> str`

Returns the URL for the Dask diagnostic dashboard.

**Returns:**
- `str`: Dashboard URL (e.g., `http://127.0.0.1:8787/status`)

**Example:**
```python
url = DaskSessionManager.get_dashboard_link()
print(f"Open dashboard: {url}")
```

---

##### `get_worker_info() -> dict`

Returns information about active workers.

**Returns:**
- `dict`: Worker information with keys:
  - `num_workers` (int): Number of active workers
  - `total_cores` (int): Total CPU cores
  - `total_memory_gb` (float): Total worker memory in GB
  - `workers` (list): List of worker details

**Example:**
```python
info = DaskSessionManager.get_worker_info()
print(f"Workers: {info['num_workers']}")
print(f"Cores: {info['total_cores']}")
print(f"Memory: {info['total_memory_gb']} GB")
```

---

##### `get_memory_usage() -> dict`

Returns current memory usage statistics.

**Returns:**
- `dict`: Memory stats with keys:
  - `total_memory_gb` (float): Total available memory
  - `used_memory_gb` (float): Currently used memory
  - `available_memory_gb` (float): Available memory
  - `percent_used` (float): Percentage used (0-100)

**Example:**
```python
mem = DaskSessionManager.get_memory_usage()
if mem['percent_used'] > 80:
    print("Warning: High memory usage!")
    DaskSessionManager.restart()
```

---

<a name="daskparquetcache"></a>
### DaskParquetCache

**Module:** `Decorators/DaskParquetCache.py`

Decorator for caching Dask DataFrame function results as Parquet files.

#### Features

- **MD5 hash-based cache keys** - Automatic cache invalidation
- **Parquet columnar storage** - Efficient compression (~70% size reduction)
- **Lazy evaluation compatible** - Handles Dask DataFrames correctly
- **Selective caching** - Choose which arguments affect cache key
- **Custom cache paths** - Override default location

#### Usage Example

```python
from Decorators.DaskParquetCache import DaskParquetCache
import dask.dataframe as dd

@DaskParquetCache
def process_large_dataset(self, file_path: str, year: int) -> dd.DataFrame:
    """Process BSM data for specific year."""
    df = dd.read_csv(file_path, blocksize='128MB')
    df = df[df['year'] == year]
    # ... expensive processing ...
    return df

# First call: computes and caches
result = obj.process_large_dataset("data.csv", 2023)

# Second call: loads from cache (fast!)
result = obj.process_large_dataset("data.csv", 2023)
```

#### Advanced Usage

```python
# Cache only on specific variables
@DaskParquetCache(cache_variables=['year', 'region'])
def filter_data(self, file_path, year, region, debug=False) -> dd.DataFrame:
    # 'debug' flag won't affect cache key
    df = dd.read_csv(file_path)
    return df[(df['year'] == year) & (df['region'] == region)]

# Override cache location
@DaskParquetCache(full_file_cache_path='/custom/path/cache')
def custom_location(self, data_file) -> dd.DataFrame:
    return dd.read_csv(data_file)
```

#### API Reference

##### Decorator Parameters

**`cache_variables`** (optional)
- Type: `list[str]`
- Default: All function arguments (excluding `self`, `**kwargs`)
- Description: Specify which arguments affect the cache key

**`full_file_cache_path`** (optional)
- Type: `str`
- Default: Auto-generated from function name + args hash
- Description: Override cache file path (DO NOT include `.parquet` extension)

##### Requirements

1. **Type annotation required:**
   ```python
   # ✅ Correct
   @DaskParquetCache
   def process(self, data) -> dd.DataFrame:
       return dd.read_csv(data)

   # ❌ Wrong (missing return type)
   @DaskParquetCache
   def process(self, data):
       return dd.read_csv(data)
   ```

2. **Return type must be `dask.dataframe.DataFrame`**

3. **Cache directory automatically created**

##### Cache Location

Default cache directory: `.cache_files/parquet/`

Cache filename format: `{function_name}_{md5_hash}.parquet/`

**Example cache path:**
```
.cache_files/parquet/process_large_dataset_a3f2e1b9d4c5.parquet/
```

##### Cache Invalidation

Cache is invalidated when:
- Function arguments change (based on `cache_variables`)
- Function source code changes
- Cached file is manually deleted

---

<a name="pipeline-components"></a>
## Pipeline Components

<a name="daskpipelinerunner"></a>
### DaskPipelineRunner

**Module:** `MachineLearning/DaskPipelineRunner.py`

Unified, config-driven ML pipeline executor that replaces 55+ individual pipeline scripts.

#### Features

- **JSON configuration** - Complete pipeline defined in config file
- **7 attack types** - Rand offset, const offset, position swap, etc.
- **4 feature sets** - From minimal (6 cols) to full (30+ cols)
- **3 filtering modes** - No filter, XY offset, bounding box
- **Reproducible** - Random seed support
- **Parquet caching** - Intermediate results cached

#### Usage Example

```python
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner

# From JSON config file
runner = DaskPipelineRunner.from_config("configs/pipeline_2000m_rand_offset.json")
results = runner.run()

print(f"Train size: {results['train_size']}")
print(f"Test size: {results['test_size']}")
print(f"Accuracy: {results['accuracy']:.4f}")
print(f"Precision: {results['precision']:.4f}")
print(f"Recall: {results['recall']:.4f}")
```

#### Configuration Example

```json
{
  "pipeline_name": "2000m_rand_offset_30pct",
  "data": {
    "source_file": "data/BSM_2023.csv",
    "filtering_type": "xy_offset_position",
    "filtering_xy_offset_position_distance": 2000,
    "partitions": 60,
    "date_ranges": [["2023-01-01", "2023-12-31"]]
  },
  "features": {
    "column_set": "minimal_xy_elev"
  },
  "attacks": {
    "attack_type": "rand_offset",
    "attacker_percentage": 0.30,
    "attack_min_distance": 10,
    "attack_max_distance": 20,
    "random_seed": 42
  },
  "ml": {
    "split_type": "fixed_size",
    "test_size": 100000,
    "random_seed": 42
  },
  "cache": {
    "enabled": true
  }
}
```

For complete configuration reference, see [DaskPipelineRunner_Configuration_Guide.md](DaskPipelineRunner_Configuration_Guide.md).

#### API Reference

##### `__init__(config: dict)`

Initializes pipeline from configuration dictionary.

**Parameters:**
- `config` (dict): Pipeline configuration

**Raises:**
- `ValueError`: If config is invalid or missing required fields

**Example:**
```python
config = {
    "pipeline_name": "baseline",
    "data": {"source_file": "data.csv", ...},
    ...
}
runner = DaskPipelineRunner(config)
```

---

##### `from_config(config_path: str) -> DaskPipelineRunner`

Class method to create pipeline from JSON file.

**Parameters:**
- `config_path` (str): Path to JSON configuration file

**Returns:**
- `DaskPipelineRunner`: Configured pipeline instance

**Raises:**
- `FileNotFoundError`: If config file doesn't exist
- `json.JSONDecodeError`: If config file is invalid JSON
- `ValueError`: If config validation fails

**Example:**
```python
runner = DaskPipelineRunner.from_config("configs/experiment_001.json")
```

---

##### `run() -> dict`

Executes the complete ML pipeline.

**Returns:**
- `dict`: Results dictionary with keys:
  - `pipeline_name` (str): Pipeline identifier
  - `train_size` (int): Training set size
  - `test_size` (int): Test set size
  - `attacker_count` (int): Number of attackers in dataset
  - `accuracy` (float): Model accuracy (0-1)
  - `precision` (float): Model precision (0-1)
  - `recall` (float): Model recall (0-1)
  - `f1_score` (float): F1 score (0-1)
  - `confusion_matrix` (array): 2x2 confusion matrix
  - `execution_time_seconds` (float): Total runtime
  - `cache_hit_rate` (float): Parquet cache hit rate (0-1)

**Pipeline Steps:**
1. Data gathering (CSV → Dask DataFrame)
2. Large data cleaning (spatial/temporal filtering)
3. Train/test split
4. Attack simulation (add attackers + position attacks)
5. ML feature preparation (hex conversion, column selection)
6. Classifier training (RandomForest)
7. Results collection

**Example:**
```python
results = runner.run()

print(f"Pipeline: {results['pipeline_name']}")
print(f"Accuracy: {results['accuracy']:.4f}")
print(f"Runtime: {results['execution_time_seconds']:.1f}s")
print(f"Cache hits: {results['cache_hit_rate']:.1%}")
```

---

##### Attack Types

The pipeline supports 7 attack simulation types:

| Attack Type | Description | Parameters |
|------------|-------------|------------|
| `rand_offset` | Random position offset per message | `min_distance`, `max_distance` |
| `const_offset_per_id` | Fixed offset per vehicle ID | `min_distance`, `max_distance` |
| `rand_position` | Completely random positions | `min_distance`, `max_distance` |
| `position_swap` | Swap positions between vehicle pairs | `swap_distance` (optional) |
| `const_offset` | Fixed offset for all attackers | `offset_x`, `offset_y` |
| `override_const` | All attackers report same position | `target_x`, `target_y` |
| `override_rand` | Each message reports random position | `min_x`, `max_x`, `min_y`, `max_y` |

See [Configuration Guide](DaskPipelineRunner_Configuration_Guide.md#attack-types-reference) for detailed parameter descriptions.

---

##### Feature Column Sets

Four predefined column sets for different analysis needs:

| Column Set | Columns | Description |
|-----------|---------|-------------|
| `minimal_xy_elev` | 6 | Core positional: `latitude`, `longitude`, `elevation`, `x`, `y`, `TxRxTime` |
| `standard` | 15 | Adds speed, heading, accuracy metrics |
| `full_standard` | 25+ | Standard + acceleration, brake status, vehicle metadata |
| `all_available` | 30+ | All BSM fields |

---

<a name="daskmclassifierpipeline"></a>
### DaskMClassifierPipeline

**Module:** `MachineLearning/DaskMClassifierPipeline.py`

ML classifier training pipeline using Dask DataFrames with sklearn integration.

#### Features

- **Lazy evaluation** - Builds computation graph, executes on `.compute()`
- **sklearn compatibility** - Auto-converts to pandas before training
- **Multiple classifiers** - RandomForest, DecisionTree, KNeighbors
- **Attack labeling** - Binary classification (normal vs attacker)

#### Usage Example

```python
from MachineLearning.DaskMClassifierPipeline import DaskMClassifierPipeline
import dask.dataframe as dd

# Load cleaned, attacked data
data = dd.read_parquet("cleaned_data.parquet")

# Train classifier
pipeline = DaskMClassifierPipeline()
results = pipeline.run_classifier(
    data=data,
    classifier_type='RandomForest',
    test_size=0.2,
    random_state=42
)

print(f"Accuracy: {results['accuracy']:.4f}")
print(f"F1 Score: {results['f1_score']:.4f}")
```

#### API Reference

##### `run_classifier(data, classifier_type, test_size, random_state) -> dict`

Trains ML classifier and returns performance metrics.

**Parameters:**
- `data` (dd.DataFrame): Dask DataFrame with features and `isAttacker` label
- `classifier_type` (str): One of `'RandomForest'`, `'DecisionTree'`, `'KNeighbors'`
- `test_size` (float or int): Test set size (0-1 for fraction, >1 for count)
- `random_state` (int): Random seed for reproducibility

**Returns:**
- `dict`: Results with keys:
  - `accuracy` (float)
  - `precision` (float)
  - `recall` (float)
  - `f1_score` (float)
  - `confusion_matrix` (array)
  - `train_size` (int)
  - `test_size` (int)

**Notes:**
- Automatically calls `.compute()` before sklearn training
- Drops non-numeric columns automatically
- Requires `isAttacker` column for labels

---

<a name="data-layer"></a>
## Data Layer

<a name="daskdatagatherer"></a>
### DaskDataGatherer

**Module:** `Gatherer/DaskDataGatherer.py`

Dask-based data loading with Parquet caching.

#### Usage Example

```python
from Gatherer.DaskDataGatherer import DaskDataGatherer

gatherer = DaskDataGatherer()
data = gatherer.gather_data()  # Returns dd.DataFrame

# Lazy evaluation - no computation yet
filtered = data[data['speed'] > 0]

# Trigger computation
result = filtered.compute()  # Returns pandas DataFrame
```

#### API Reference

##### `gather_data() -> dd.DataFrame`

Gathers BSM data from source file with automatic Parquet caching.

**Returns:**
- `dd.DataFrame`: Lazy Dask DataFrame

**Caching:**
- First call: Reads CSV, caches as Parquet
- Subsequent calls: Loads from Parquet (10x faster)

**Configuration:**
- Source file: Set via dependency injection
- Blocksize: 128MB default
- Partitions: Auto-calculated

---

<a name="daskconnecteddrivingcleaner"></a>
### DaskConnectedDrivingCleaner

**Module:** `Cleaners/DaskConnectedDrivingCleaner.py`

Core data cleaner for BSM datasets.

#### Features

- Drops invalid records (speed < 0, missing coordinates)
- Converts hex IDs to integers
- Standardizes column types
- Handles timestamp parsing

#### Usage Example

```python
from Cleaners.DaskConnectedDrivingCleaner import DaskConnectedDrivingCleaner
import dask.dataframe as dd

data = dd.read_csv("raw_data.csv")
cleaner = DaskConnectedDrivingCleaner()
cleaned = cleaner.clean(data)

# Count removed records
original_count = data.shape[0].compute()
cleaned_count = cleaned.shape[0].compute()
print(f"Removed {original_count - cleaned_count} invalid records")
```

#### API Reference

##### `clean(data: dd.DataFrame) -> dd.DataFrame`

Cleans raw BSM data.

**Parameters:**
- `data` (dd.DataFrame): Raw Dask DataFrame

**Returns:**
- `dd.DataFrame`: Cleaned DataFrame

**Cleaning Operations:**
1. Drop records with `speed < 0`
2. Drop records with missing `latitude`/`longitude`
3. Convert hex `deviceId` to integer
4. Parse timestamp fields
5. Standardize column types

---

<a name="daskcleanwithtimestamps"></a>
### DaskCleanWithTimestamps

**Module:** `Cleaners/DaskCleanWithTimestamps.py`

Cleaner with timestamp-based filtering.

#### Usage Example

```python
from Cleaners.DaskCleanWithTimestamps import DaskCleanWithTimestamps

cleaner = DaskCleanWithTimestamps(
    start_time="2023-01-01 00:00:00",
    end_time="2023-12-31 23:59:59"
)
cleaned = cleaner.clean(data)
```

#### API Reference

##### `__init__(start_time: str, end_time: str)`

**Parameters:**
- `start_time` (str): ISO format timestamp
- `end_time` (str): ISO format timestamp

##### `clean(data: dd.DataFrame) -> dd.DataFrame`

Filters data to timestamp range.

---

<a name="daskconnecteddrivinglargedatacleaner"></a>
### DaskConnectedDrivingLargeDataCleaner

**Module:** `Cleaners/DaskConnectedDrivingLargeDataCleaner.py`

Wrapper for applying spatial/temporal filtering to large datasets.

#### Usage Example

```python
from Cleaners.DaskConnectedDrivingLargeDataCleaner import DaskConnectedDrivingLargeDataCleaner

cleaner = DaskConnectedDrivingLargeDataCleaner(
    filter_type="xy_offset_position",
    distance=2000,
    center_x=500000,
    center_y=4500000
)
cleaned = cleaner.clean(data)
```

#### API Reference

##### `__init__(filter_type: str, **filter_params)`

**Parameters:**
- `filter_type` (str): One of `'passthrough'`, `'xy_offset_position'`, `'bounding_box'`
- `**filter_params`: Filter-specific parameters

##### `clean(data: dd.DataFrame) -> dd.DataFrame`

Applies configured filtering.

---

<a name="filtering-components"></a>
## Filtering Components

<a name="daskcleanerwithpassthroughfilter"></a>
### DaskCleanerWithPassthroughFilter

**Module:** `Cleaners/DaskCleanerWithPassthroughFilter.py`

No-op filter (returns data unchanged).

#### Usage

```python
from Cleaners.DaskCleanerWithPassthroughFilter import DaskCleanerWithPassthroughFilter

cleaner = DaskCleanerWithPassthroughFilter()
result = cleaner.clean(data)  # Returns data as-is
```

---

<a name="daskcleanerwithfilterwithinrange"></a>
### DaskCleanerWithFilterWithinRange

**Module:** `Cleaners/DaskCleanerWithFilterWithinRange.py`

Filters records within distance from center point.

#### Usage

```python
from Cleaners.DaskCleanerWithFilterWithinRange import DaskCleanerWithFilterWithinRange

cleaner = DaskCleanerWithFilterWithinRange(
    distance=2000,  # meters
    center_x=500000,
    center_y=4500000
)
filtered = cleaner.clean(data)
```

---

<a name="daskcleanerwithfilterwithinrangexy"></a>
### DaskCleanerWithFilterWithinRangeXY

**Module:** `Cleaners/DaskCleanerWithFilterWithinRangeXY.py`

Filters using XY offset (rectangular bounding box).

#### Usage

```python
from Cleaners.DaskCleanerWithFilterWithinRangeXY import DaskCleanerWithFilterWithinRangeXY

cleaner = DaskCleanerWithFilterWithinRangeXY(
    x_offset=2000,
    y_offset=2000,
    center_x=500000,
    center_y=4500000
)
filtered = cleaner.clean(data)
```

---

<a name="daskcleanerwithfilterwithinrangexyandday"></a>
### DaskCleanerWithFilterWithinRangeXYAndDay

**Module:** `Cleaners/DaskCleanerWithFilterWithinRangeXYAndDay.py`

Filters by XY offset and specific day.

#### Usage

```python
from Cleaners.DaskCleanerWithFilterWithinRangeXYAndDay import DaskCleanerWithFilterWithinRangeXYAndDay

cleaner = DaskCleanerWithFilterWithinRangeXYAndDay(
    x_offset=2000,
    y_offset=2000,
    center_x=500000,
    center_y=4500000,
    day=15  # Day of month
)
filtered = cleaner.clean(data)
```

---

<a name="daskcleanerwithfilterwithinrangexyanddaterange"></a>
### DaskCleanerWithFilterWithinRangeXYAndDateRange

**Module:** `Cleaners/DaskCleanerWithFilterWithinRangeXYAndDateRange.py`

Filters by XY offset and date range.

#### Usage

```python
from Cleaners.DaskCleanerWithFilterWithinRangeXYAndDateRange import DaskCleanerWithFilterWithinRangeXYAndDateRange

cleaner = DaskCleanerWithFilterWithinRangeXYAndDateRange(
    x_offset=2000,
    y_offset=2000,
    center_x=500000,
    center_y=4500000,
    start_date="2023-01-01",
    end_date="2023-12-31"
)
filtered = cleaner.clean(data)
```

---

<a name="ml-components"></a>
## ML Components

<a name="daskmconnecteddrivingdatacleaner"></a>
### DaskMConnectedDrivingDataCleaner

**Module:** `MachineLearning/DaskMConnectedDrivingDataCleaner.py`

Prepares data for ML training (feature engineering, column selection).

#### Features

- Hex ID conversion
- Column selection (4 feature sets)
- Missing value handling
- Type standardization

#### Usage Example

```python
from MachineLearning.DaskMConnectedDrivingDataCleaner import DaskMConnectedDrivingDataCleaner

cleaner = DaskMConnectedDrivingDataCleaner(
    column_set='minimal_xy_elev'
)
ml_ready = cleaner.clean(attacked_data)
```

#### API Reference

##### `__init__(column_set: str = 'minimal_xy_elev')`

**Parameters:**
- `column_set` (str): One of `'minimal_xy_elev'`, `'standard'`, `'full_standard'`, `'all_available'`

##### `clean(data: dd.DataFrame) -> dd.DataFrame`

Prepares data for ML training.

**Returns:**
- `dd.DataFrame`: ML-ready features + `isAttacker` label

---

<a name="daskconnecteddrivingattacker"></a>
### DaskConnectedDrivingAttacker

**Module:** `Attacks/DaskConnectedDrivingAttacker.py`

Simulates position falsification attacks on BSM data.

#### Features

- 7 attack types (rand_offset, const_offset, position_swap, etc.)
- Configurable attacker percentage
- Random seed support
- Adds `isAttacker` label column

#### Usage Example

```python
from Attacks.DaskConnectedDrivingAttacker import DaskConnectedDrivingAttacker

attacker = DaskConnectedDrivingAttacker(
    attack_type='rand_offset',
    attacker_percentage=0.30,
    min_distance=10,
    max_distance=20,
    random_seed=42
)
attacked_data = attacker.add_attackers(clean_data)

# Check attack statistics
total = attacked_data.shape[0].compute()
attackers = attacked_data[attacked_data['isAttacker'] == 1].shape[0].compute()
print(f"Attackers: {attackers}/{total} ({attackers/total:.1%})")
```

#### API Reference

##### `__init__(attack_type: str, attacker_percentage: float, **attack_params)`

**Parameters:**
- `attack_type` (str): Attack type (see table below)
- `attacker_percentage` (float): Fraction of attackers (0.0-1.0)
- `random_seed` (int, optional): For reproducibility
- `**attack_params`: Attack-specific parameters

##### `add_attackers(data: dd.DataFrame) -> dd.DataFrame`

Applies attack simulation.

**Returns:**
- `dd.DataFrame`: Data with `isAttacker` column (0=normal, 1=attacker)

##### Attack Parameters by Type

| Attack Type | Required Parameters |
|------------|---------------------|
| `rand_offset` | `min_distance`, `max_distance` |
| `const_offset_per_id` | `min_distance`, `max_distance` |
| `rand_position` | `min_distance`, `max_distance` |
| `position_swap` | None (optional: `swap_distance`) |
| `const_offset` | `offset_x`, `offset_y` |
| `override_const` | `target_x`, `target_y` |
| `override_rand` | `min_x`, `max_x`, `min_y`, `max_y` |

---

<a name="utilities"></a>
## Utilities

<a name="daskudfregistry"></a>
### DaskUDFRegistry

**Module:** `Helpers/DaskUDFs/DaskUDFRegistry.py`

Registry of Dask user-defined functions for common operations.

#### Available Functions

- `haversine_distance(lat1, lon1, lat2, lon2)` - Great circle distance
- `euclidean_distance_2d(x1, y1, x2, y2)` - Planar distance
- `bearing(lat1, lon1, lat2, lon2)` - Heading between points
- `hex_to_int(hex_str)` - Hex string to integer
- `timestamp_to_unix(timestamp)` - Timestamp to epoch

#### Usage Example

```python
from Helpers.DaskUDFs.DaskUDFRegistry import DaskUDFRegistry
import dask.dataframe as dd

# Register functions
registry = DaskUDFRegistry()
registry.register_all()

# Use in Dask operations
data['distance'] = registry.haversine_distance(
    data['lat1'], data['lon1'],
    data['lat2'], data['lon2']
)
```

For complete UDF documentation, see [Helpers/DaskUDFs/README.md](../Helpers/DaskUDFs/README.md).

---

<a name="testing"></a>
## Testing

<a name="daskfixtures"></a>
### DaskFixtures

**Module:** `Test/DaskFixtures.py`

Pytest fixtures for Dask testing.

#### Available Fixtures

- `dask_client` - Configured Dask client
- `sample_dask_dataframe` - Small test DataFrame
- `large_dask_dataframe` - 100k row test DataFrame

#### Usage Example

```python
import pytest
from Test.DaskFixtures import *

def test_pipeline(dask_client, sample_dask_dataframe):
    # Test with pre-configured client and data
    result = my_pipeline.run(sample_dask_dataframe)
    assert result.shape[0].compute() > 0
```

---

## Performance Guidelines

### Memory Management

**Rule of thumb:** Each worker needs ~8GB for 15M row datasets

```python
# Recommended configuration
n_workers = 6
memory_per_worker = '8GB'
total_memory_required = 64  # GB
```

**Monitor memory:**
```python
mem = DaskSessionManager.get_memory_usage()
if mem['percent_used'] > 80:
    # Reduce partitions or restart cluster
    DaskSessionManager.restart()
```

### Caching Best Practices

**When to use `@DaskParquetCache`:**
- ✅ Expensive operations (file reads, complex joins)
- ✅ Repeated operations with same inputs
- ✅ Intermediate pipeline stages

**When NOT to cache:**
- ❌ Cheap operations (simple filters, column selection)
- ❌ One-time operations
- ❌ Operations on small DataFrames (<1000 rows)

**Cache hit rate target:** ≥85% for iterative workflows

### Partitioning Guidelines

**Partition size recommendations:**

| Dataset Size | Recommended Partitions | Blocksize |
|-------------|----------------------|-----------|
| 1M rows | 20-30 | 64MB |
| 5M rows | 40-50 | 128MB |
| 15M rows | 60-80 | 128MB |
| 50M+ rows | 100-120 | 256MB |

**Configure partitions:**
```python
# In DaskPipelineRunner config
{
  "data": {
    "partitions": 60,  # For 15M rows
    ...
  }
}
```

### Computation Best Practices

**Avoid eager evaluation:**
```python
# ❌ Bad: Forces computation multiple times
count = data.shape[0].compute()
mean = data['speed'].mean().compute()
max_val = data['speed'].max().compute()

# ✅ Good: Build graph, compute once
result = data.agg({
    'count': 'size',
    'speed_mean': ('speed', 'mean'),
    'speed_max': ('speed', 'max')
}).compute()
```

**Use persist() for reused DataFrames:**
```python
# If data will be used multiple times
data = data.persist()

# Now multiple operations are fast
filtered1 = data[data['speed'] > 0]
filtered2 = data[data['heading'] < 180]
```

---

## Migration from Pandas

### Common Patterns

**Pandas:**
```python
df = pd.read_csv("data.csv")
result = df[df['speed'] > 0].groupby('deviceId').mean()
```

**Dask equivalent:**
```python
df = dd.read_csv("data.csv", blocksize='128MB')
result = df[df['speed'] > 0].groupby('deviceId').mean().compute()
#                                                        ^^^^^^^^^ Add this
```

### Key Differences

| Operation | Pandas | Dask |
|-----------|--------|------|
| Read CSV | `pd.read_csv()` | `dd.read_csv(..., blocksize='128MB')` |
| Get result | Direct | Append `.compute()` |
| Length | `len(df)` | `df.shape[0].compute()` |
| Unique values | `df['col'].unique()` | `df['col'].unique().compute()` |
| Iteration | `for row in df.iterrows()` | Not recommended (use map_partitions) |

### sklearn Integration

Always compute before sklearn:
```python
# ❌ Wrong
model.fit(dask_df[features], dask_df['label'])

# ✅ Correct
X = dask_df[features].compute()
y = dask_df['label'].compute()
model.fit(X, y)
```

---

## Error Handling

### Common Errors

**1. `TypeError: expected DataFrame, got DataFrame`**
- **Cause:** Passing Dask DataFrame to pandas/sklearn function
- **Solution:** Call `.compute()` first

**2. `MemoryError` or `KilledWorker`**
- **Cause:** Partition too large, exceeded worker memory
- **Solution:** Increase partitions or reduce blocksize

**3. `KeyError: 'column_name'`**
- **Cause:** Column doesn't exist (check case sensitivity)
- **Solution:** Use `data.columns.tolist()` to verify

**4. Low cache hit rate (<50%)**
- **Cause:** Cache keys changing (arguments or code modified)
- **Solution:** Check `cache_variables` parameter

For comprehensive troubleshooting, see [Troubleshooting_Guide.md](Troubleshooting_Guide.md).

---

## Additional Resources

- **Configuration Guide:** [DaskPipelineRunner_Configuration_Guide.md](DaskPipelineRunner_Configuration_Guide.md)
- **Troubleshooting:** [Troubleshooting_Guide.md](Troubleshooting_Guide.md)
- **UDF Documentation:** [Helpers/DaskUDFs/README.md](../Helpers/DaskUDFs/README.md)
- **Main README:** [README.md](../README.md)
- **Example Configs:** [MClassifierPipelines/configs/](../MClassifierPipelines/configs/)

---

**Document Version:** 1.0.0
**Last Updated:** 2026-01-18
**Maintained by:** ConnectedDrivingPipelineV4 Development Team

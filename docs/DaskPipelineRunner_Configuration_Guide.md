# DaskPipelineRunner Configuration Guide

This guide provides comprehensive documentation for configuring `DaskPipelineRunner` pipelines using JSON configuration files.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration Schema](#configuration-schema)
3. [Section Reference](#section-reference)
   - [pipeline_name](#pipeline_name)
   - [data](#data)
   - [features](#features)
   - [attacks](#attacks)
   - [ml](#ml)
   - [cache](#cache)
4. [Attack Types Reference](#attack-types-reference)
5. [Feature Column Sets](#feature-column-sets)
6. [Filtering Types](#filtering-types)
7. [Complete Examples](#complete-examples)
8. [Configuration Validation](#configuration-validation)
9. [Best Practices](#best-practices)

---

## Quick Start

### Minimal Working Configuration

The simplest configuration requires only the essential fields:

```json
{
  "pipeline_name": "my_first_pipeline",
  "data": {
    "source_file": "data/bsm_data.csv",
    "filtering": {
      "type": "none"
    }
  },
  "features": {
    "columns": "minimal_xy_elev"
  },
  "attacks": {
    "enabled": false
  },
  "ml": {
    "train_test_split": {
      "type": "random",
      "train_ratio": 0.8,
      "test_ratio": 0.2,
      "random_seed": 42
    }
  }
}
```

### Usage

```python
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner

# Load from JSON file
pipeline = DaskPipelineRunner.from_config("config.json")

# Or create from dict
pipeline = DaskPipelineRunner({
    "pipeline_name": "my_pipeline",
    # ... config dict ...
})

# Run the pipeline
results = pipeline.run()
```

---

## Configuration Schema

A complete DaskPipelineRunner configuration is a JSON object with six top-level sections:

```json
{
  "pipeline_name": "string (required)",
  "data": { /* object (required) */ },
  "features": { /* object (required) */ },
  "attacks": { /* object (required) */ },
  "ml": { /* object (required) */ },
  "cache": { /* object (optional) */ }
}
```

---

## Section Reference

### `pipeline_name`

**Type:** `string` (required)

**Description:** Unique identifier for this pipeline run. Used for:
- Log file naming
- Cache directory naming
- Output file prefixes
- Tracking in Dask dashboard

**Example:**
```json
"pipeline_name": "xy_offset_2000m_rand_offset_30attackers"
```

**Best Practices:**
- Use descriptive names that indicate key parameters
- Include attack type, ratio, and filtering in the name
- Avoid special characters (use underscores instead of spaces)
- Keep under 100 characters for filesystem compatibility

---

### `data`

**Type:** `object` (required)

**Description:** Configures data loading, spatial filtering, and temporal filtering.

#### Required Fields

##### `source_file`
**Type:** `string` (required)

Path to the source CSV file containing BSM data.

```json
"source_file": "data/bsm_data.csv"
```

##### `filtering`
**Type:** `object` (required)

Spatial filtering configuration. See [Filtering Types](#filtering-types) for details.

```json
"filtering": {
  "type": "xy_offset_position",
  "distance_meters": 2000,
  "center_x": -106.0831353,
  "center_y": 41.5430216
}
```

#### Optional Fields

##### `num_subsection_rows`
**Type:** `integer` (optional, default: 100000)

Number of rows per Dask partition. Controls parallelism and memory usage.

```json
"num_subsection_rows": 100000
```

**Tuning Guide:**
- **Small datasets (<1M rows):** 50000-100000
- **Medium datasets (1-10M rows):** 100000-500000
- **Large datasets (>10M rows):** 500000-1000000
- **64GB RAM systems:** Max 1000000 (6 workers × 8GB each)

##### `date_range`
**Type:** `object` (optional)

Temporal filtering to restrict data to specific date ranges.

```json
"date_range": {
  "start_day": 1,
  "end_day": 30,
  "start_month": 4,
  "end_month": 4,
  "start_year": 2021,
  "end_year": 2021
}
```

**All fields required if `date_range` is specified:**
- `start_day`, `end_day`: Day of month (1-31)
- `start_month`, `end_month`: Month (1-12)
- `start_year`, `end_year`: Year (2000-2099)

**Example Use Cases:**
- Single month: `start_month = end_month = 4`
- Multi-month: `start_month = 4, end_month = 6` (April to June)
- Single day: All start/end values identical

---

### `features`

**Type:** `object` (required)

**Description:** Specifies which BSM columns to include in the feature set.

##### `columns`
**Type:** `string` (required)

Predefined column set name. See [Feature Column Sets](#feature-column-sets) for available options.

```json
"features": {
  "columns": "minimal_xy_elev"
}
```

**Available Values:**
- `"minimal_xy_elev"` - 5 columns (recommended for quick experiments)
- `"extended_timestamps"` or `"extended_with_timestamps"` - 13 columns
- `"minimal_xy_elev_heading_speed"` - 7 columns
- `"all"` - ~50 columns (all BSM fields)

---

### `attacks`

**Type:** `object` (required)

**Description:** Configures attack simulation parameters.

#### Required Fields

##### `enabled`
**Type:** `boolean` (required)

Enable or disable attack simulation.

```json
"enabled": true
```

**When `enabled: false`:**
- No attack simulation is performed
- All vehicles labeled as legitimate (`is_attacker = 0`)
- Other attack fields are ignored
- Use for baseline/control experiments

**When `enabled: true`:**
- Attack simulation is performed according to other attack parameters
- Must specify `attack_ratio`, `type`, and type-specific parameters

#### Optional Fields (required if `enabled: true`)

##### `attack_ratio`
**Type:** `number` (required if enabled, range: 0.0-1.0)

Fraction of vehicles to compromise.

```json
"attack_ratio": 0.3
```

**Examples:**
- `0.1` = 10% of vehicles are attackers
- `0.3` = 30% of vehicles are attackers (typical)
- `0.5` = 50% of vehicles are attackers

**Best Practices:**
- Use 0.1-0.3 for realistic scenarios
- Use 0.5+ for stress testing classifiers
- Always include `random_seed` for reproducibility

##### `type`
**Type:** `string` (required if enabled)

Attack type identifier. See [Attack Types Reference](#attack-types-reference) for all types.

```json
"type": "rand_offset"
```

##### `random_seed`
**Type:** `integer` (optional, recommended)

Random seed for reproducible attack selection.

```json
"random_seed": 42
```

##### Attack Type-Specific Parameters

Each attack type requires additional parameters. See [Attack Types Reference](#attack-types-reference).

**Example: `rand_offset` Attack**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "rand_offset",
  "min_distance": 10,
  "max_distance": 20,
  "random_seed": 42
}
```

---

### `ml`

**Type:** `object` (required)

**Description:** Machine learning configuration including train/test split strategy.

##### `train_test_split`
**Type:** `object` (required)

Configures how to split data into training and test sets.

#### Random Split (Default)

```json
"ml": {
  "train_test_split": {
    "type": "random",
    "train_ratio": 0.8,
    "test_ratio": 0.2,
    "random_seed": 42
  }
}
```

**Fields:**
- `type`: Must be `"random"`
- `train_ratio`: Fraction for training (0.0-1.0)
- `test_ratio`: Fraction for testing (0.0-1.0)
- `random_seed`: Seed for reproducibility

**Constraints:**
- `train_ratio + test_ratio` must equal 1.0
- Common splits: 80/20, 70/30, 90/10

#### Fixed-Size Split

```json
"ml": {
  "train_test_split": {
    "type": "fixed_size",
    "train_size": 80000,
    "test_size": 20000,
    "random_seed": 42
  }
}
```

**Fields:**
- `type`: Must be `"fixed_size"`
- `train_size`: Exact number of rows for training
- `test_size`: Exact number of rows for testing
- `random_seed`: Seed for reproducibility

**Use Cases:**
- Consistent dataset sizes across experiments
- Ensuring minimum test set size
- Comparing models with identical data sizes

---

### `cache`

**Type:** `object` (optional)

**Description:** Controls Parquet caching for intermediate results.

```json
"cache": {
  "enabled": true
}
```

**Default Behavior (if omitted):** Caching is **disabled**

**When Enabled:**
- Intermediate results cached to `cache/` directory
- Uses Parquet format with LZ4 compression
- Automatic cache key generation based on config
- Typical cache hit rates: ≥85% after first run
- Reduces re-computation time by 60-80%

**When to Enable:**
- Iterating on ML models with same data
- Running multiple experiments with same base config
- Working with large datasets (>5M rows)

**When to Disable:**
- One-off experiments
- Testing caching infrastructure itself
- Disk space constraints
- Ensuring fresh computation

**Cache Directory Structure:**
```
cache/
├── data_gatherer/
│   └── {config_hash}_raw.parquet
├── large_cleaner/
│   └── {config_hash}_cleaned.parquet
├── train_test_split/
│   ├── {config_hash}_train.parquet
│   └── {config_hash}_test.parquet
└── attacks/
    ├── {config_hash}_train_attacked.parquet
    └── {config_hash}_test_attacked.parquet
```

---

## Attack Types Reference

### 1. `rand_offset` - Random Position Offset

**Description:** Each message from attacker vehicles has a random position offset applied. Different offset for each message.

**Use Case:** Simulates GPS jamming or sensor noise attacks.

**Required Parameters:**
- `min_distance` (number): Minimum offset in meters
- `max_distance` (number): Maximum offset in meters

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "rand_offset",
  "min_distance": 10,
  "max_distance": 20,
  "random_seed": 42
}
```

**Typical Values:**
- Light attack: `min: 10, max: 20` (10-20m offset)
- Moderate attack: `min: 50, max: 100` (50-100m offset)
- Severe attack: `min: 100, max: 200` (100-200m offset)

---

### 2. `const_offset_per_id` - Constant Offset Per Vehicle

**Description:** Each attacker vehicle has a fixed offset applied to all messages. Different vehicles get different offsets.

**Use Case:** Simulates miscalibrated sensors or systematic position bias.

**Required Parameters:**
- `min_distance` (number): Minimum offset in meters
- `max_distance` (number): Maximum offset in meters

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "const_offset_per_id",
  "min_distance": 100,
  "max_distance": 200,
  "random_seed": 42
}
```

**Typical Values:**
- Moderate attack: `min: 50, max: 100` (50-100m offset)
- Severe attack: `min: 100, max: 200` (100-200m offset)
- Extreme attack: `min: 200, max: 500` (200-500m offset)

**Offset Selection:** Each vehicle's offset is randomly chosen once from `[min_distance, max_distance]` range and applied consistently to all messages from that vehicle.

---

### 3. `rand_position` - Random Position Within Area

**Description:** Attacker positions are completely randomized within the filtered area, ignoring actual vehicle positions.

**Use Case:** Simulates Sybil attacks or complete position spoofing.

**Required Parameters:**
- `min_distance` (number): Minimum radius from center (typically 0)
- `max_distance` (number): Maximum radius from center (matches filtering distance)

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "rand_position",
  "min_distance": 0,
  "max_distance": 2000,
  "random_seed": 42
}
```

**Typical Configuration:**
- Set `max_distance` to match `data.filtering.distance_meters`
- Results in uniformly distributed fake positions
- Most aggressive attack type

---

### 4. `position_swap` - Swap Positions Between Vehicles

**Description:** Attacker vehicles are paired up and swap positions with each other.

**Use Case:** Simulates coordinated position exchange attacks.

**Required Parameters:** None (only `enabled`, `attack_ratio`, `type`)

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.4,
  "type": "position_swap",
  "random_seed": 42
}
```

**Behavior:**
- Requires even number of attackers (pairs)
- If odd number, one attacker remains unswapped
- Positions swapped for all messages
- Maintains realistic position values (from other real vehicles)

---

### 5. `const_offset` - Fixed Offset (All Vehicles)

**Description:** All attacker vehicles receive the same offset in the same direction.

**Use Case:** Simulates coordinated attacks or regional GPS interference.

**Required Parameters:**
- `direction_angle` (number): Offset direction in degrees (0-360)
- `distance_meters` (number): Offset distance in meters

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "const_offset",
  "direction_angle": 90,
  "distance_meters": 100,
  "random_seed": 42
}
```

**Direction Convention:**
- `0°` = North
- `90°` = East
- `180°` = South
- `270°` = West

---

### 6. `override_const` - Override with Fixed Position

**Description:** All attacker messages report the same fixed position (calculated from center + offset).

**Use Case:** Simulates stationary fake vehicle broadcasts.

**Required Parameters:**
- `direction_angle` (number): Direction from center (0-360)
- `distance_meters` (number): Distance from center

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "override_const",
  "direction_angle": 45,
  "distance_meters": 500,
  "random_seed": 42
}
```

**Behavior:**
- All attacker messages report identical position
- Position calculated as: `center + (distance * direction)`
- Easily detectable by most classifiers

---

### 7. `override_rand` - Override with Random Position

**Description:** Each attacker message reports a random position within specified range from center.

**Use Case:** Simulates random fake position broadcasts.

**Required Parameters:**
- `min_distance` (number): Minimum distance from center
- `max_distance` (number): Maximum distance from center

**Example:**
```json
"attacks": {
  "enabled": true,
  "attack_ratio": 0.3,
  "type": "override_rand",
  "min_distance": 100,
  "max_distance": 500,
  "random_seed": 42
}
```

**Behavior:**
- New random position for each message
- Positions uniformly distributed in annulus
- Ignores actual vehicle trajectory

---

## Feature Column Sets

### `minimal_xy_elev`
**Column Count:** 5

**Columns:**
- `latitude`
- `longitude`
- `elevation`
- `speed`
- `heading`

**Use Case:**
- Quick experiments and prototyping
- Minimal memory footprint
- Baseline classifier performance
- Development and debugging

**Example:**
```json
"features": {
  "columns": "minimal_xy_elev"
}
```

---

### `extended_timestamps` / `extended_with_timestamps`
**Column Count:** 13

**Columns:**
- All columns from `minimal_xy_elev` (5)
- `secMark` - Milliseconds within current minute
- `elevation_change` - Rate of altitude change
- Plus additional timestamp-derived features

**Use Case:**
- Temporal pattern analysis
- Time-series classification
- Attack detection based on timing
- Production classifiers

**Example:**
```json
"features": {
  "columns": "extended_timestamps"
}
```

---

### `minimal_xy_elev_heading_speed`
**Column Count:** 7

**Columns:**
- All columns from `minimal_xy_elev` (5)
- Additional heading-derived features
- Additional speed-derived features

**Use Case:**
- Movement pattern analysis
- Velocity-based attack detection

**Example:**
```json
"features": {
  "columns": "minimal_xy_elev_heading_speed"
}
```

---

### `all`
**Column Count:** ~50

**Columns:** All available BSM data fields including:
- Position (lat, lon, elev)
- Motion (speed, heading, acceleration)
- Timestamps (secMark, time-of-day, day-of-week)
- Vehicle metadata
- Message metadata

**Use Case:**
- Feature engineering exploration
- Maximum classifier information
- Research experiments

**Warning:**
- High memory usage (2-3x more than minimal)
- Longer computation time
- May cause overfitting with small datasets

**Example:**
```json
"features": {
  "columns": "all"
}
```

---

## Filtering Types

### 1. `none` - No Filtering (Passthrough)

**Description:** Load all data from source file without spatial filtering.

**Example:**
```json
"filtering": {
  "type": "none"
}
```

**Use Case:**
- Small datasets that fit in memory
- No spatial constraints
- Testing data loading pipeline

---

### 2. `xy_offset_position` - Radial Distance Filter

**Description:** Keep only records within specified distance from a center point.

**Required Parameters:**
- `distance_meters` (number): Radius in meters
- `center_x` (number): Center longitude
- `center_y` (number): Center latitude

**Example:**
```json
"filtering": {
  "type": "xy_offset_position",
  "distance_meters": 2000,
  "center_x": -106.0831353,
  "center_y": 41.5430216
}
```

**Use Case:**
- Focus on specific intersection or area
- Reduce dataset size
- Geographic region analysis

**Distance Recommendations:**
- Small intersection: 500-1000m
- Large intersection: 1000-2000m
- Regional area: 2000-5000m

---

### 3. `bounding_box` - Rectangular Area Filter

**Description:** Keep only records within specified lat/lon bounding box.

**Required Parameters:**
- `min_latitude` (number)
- `max_latitude` (number)
- `min_longitude` (number)
- `max_longitude` (number)

**Example:**
```json
"filtering": {
  "type": "bounding_box",
  "min_latitude": 41.0,
  "max_latitude": 42.0,
  "min_longitude": -107.0,
  "max_longitude": -106.0
}
```

**Use Case:**
- Rectangular study areas
- City/county boundaries
- Grid-based analysis

---

## Complete Examples

### Example 1: Basic Experiment (No Attacks)

```json
{
  "pipeline_name": "baseline_no_attacks",
  "data": {
    "source_file": "data/bsm_data.csv",
    "filtering": {
      "type": "xy_offset_position",
      "distance_meters": 1000,
      "center_x": -106.0831353,
      "center_y": 41.5430216
    }
  },
  "features": {
    "columns": "minimal_xy_elev"
  },
  "attacks": {
    "enabled": false
  },
  "ml": {
    "train_test_split": {
      "type": "random",
      "train_ratio": 0.8,
      "test_ratio": 0.2,
      "random_seed": 42
    }
  }
}
```

---

### Example 2: Random Offset Attack (30% Attackers)

```json
{
  "pipeline_name": "rand_offset_10_20m_30pct_attackers",
  "data": {
    "source_file": "data/bsm_data.csv",
    "num_subsection_rows": 100000,
    "filtering": {
      "type": "xy_offset_position",
      "distance_meters": 2000,
      "center_x": -106.0831353,
      "center_y": 41.5430216
    },
    "date_range": {
      "start_day": 1,
      "end_day": 30,
      "start_month": 4,
      "end_month": 4,
      "start_year": 2021,
      "end_year": 2021
    }
  },
  "features": {
    "columns": "extended_timestamps"
  },
  "attacks": {
    "enabled": true,
    "attack_ratio": 0.3,
    "type": "rand_offset",
    "min_distance": 10,
    "max_distance": 20,
    "random_seed": 42
  },
  "ml": {
    "train_test_split": {
      "type": "random",
      "train_ratio": 0.8,
      "test_ratio": 0.2,
      "random_seed": 42
    }
  },
  "cache": {
    "enabled": true
  }
}
```

---

### Example 3: Constant Offset Per Vehicle (Severe Attack)

```json
{
  "pipeline_name": "const_offset_per_id_100_200m_40pct",
  "data": {
    "source_file": "data/bsm_data.csv",
    "num_subsection_rows": 500000,
    "filtering": {
      "type": "xy_offset_position",
      "distance_meters": 2000,
      "center_x": -106.0831353,
      "center_y": 41.5430216
    }
  },
  "features": {
    "columns": "all"
  },
  "attacks": {
    "enabled": true,
    "attack_ratio": 0.4,
    "type": "const_offset_per_id",
    "min_distance": 100,
    "max_distance": 200,
    "random_seed": 42
  },
  "ml": {
    "train_test_split": {
      "type": "random",
      "train_ratio": 0.8,
      "test_ratio": 0.2,
      "random_seed": 42
    }
  },
  "cache": {
    "enabled": true
  }
}
```

---

### Example 4: Fixed-Size Split with Position Swap

```json
{
  "pipeline_name": "position_swap_fixed_size_80k_20k",
  "data": {
    "source_file": "data/bsm_data.csv",
    "filtering": {
      "type": "none"
    }
  },
  "features": {
    "columns": "minimal_xy_elev_heading_speed"
  },
  "attacks": {
    "enabled": true,
    "attack_ratio": 0.2,
    "type": "position_swap",
    "random_seed": 42
  },
  "ml": {
    "train_test_split": {
      "type": "fixed_size",
      "train_size": 80000,
      "test_size": 20000,
      "random_seed": 42
    }
  },
  "cache": {
    "enabled": true
  }
}
```

---

### Example 5: Multiple Date Ranges (April 2021)

```json
{
  "pipeline_name": "april_2021_rand_position_attack",
  "data": {
    "source_file": "data/bsm_data.csv",
    "filtering": {
      "type": "xy_offset_position",
      "distance_meters": 2000,
      "center_x": -106.0831353,
      "center_y": 41.5430216
    },
    "date_range": {
      "start_day": 1,
      "end_day": 30,
      "start_month": 4,
      "end_month": 4,
      "start_year": 2021,
      "end_year": 2021
    }
  },
  "features": {
    "columns": "extended_timestamps"
  },
  "attacks": {
    "enabled": true,
    "attack_ratio": 0.3,
    "type": "rand_position",
    "min_distance": 0,
    "max_distance": 2000,
    "random_seed": 42
  },
  "ml": {
    "train_test_split": {
      "type": "random",
      "train_ratio": 0.8,
      "test_ratio": 0.2,
      "random_seed": 42
    }
  },
  "cache": {
    "enabled": true
  }
}
```

---

## Configuration Validation

### Using the Validation Script

The codebase includes a validation script to check configuration files before running pipelines:

```bash
python scripts/validate_pipeline_configs.py MClassifierPipelines/configs/my_config.json
```

**What It Checks:**
- JSON syntax validity
- Required fields present
- Field types correct
- Value ranges valid
- Attack parameters match attack type
- File paths exist

### Common Validation Errors

#### Missing Required Field
```
Error: Missing required field 'pipeline_name'
```
**Fix:** Add the missing field to your config.

#### Invalid Attack Ratio
```
Error: attack_ratio must be between 0.0 and 1.0, got 1.5
```
**Fix:** Set `attack_ratio` to a value in range [0.0, 1.0].

#### Train/Test Ratios Don't Sum to 1.0
```
Error: train_ratio (0.7) + test_ratio (0.2) must equal 1.0
```
**Fix:** Adjust ratios to sum to 1.0: `"train_ratio": 0.8, "test_ratio": 0.2`

#### Missing Attack Parameters
```
Error: rand_offset attack requires min_distance and max_distance
```
**Fix:** Add required parameters for the attack type.

---

## Best Practices

### 1. Always Use `random_seed`

**Why:** Ensures reproducible results across runs.

```json
"attacks": {
  "random_seed": 42
},
"ml": {
  "train_test_split": {
    "random_seed": 42
  }
}
```

### 2. Enable Caching for Iterative Work

**Why:** Reduces re-computation time by 60-80% when running multiple experiments with the same base data.

```json
"cache": {
  "enabled": true
}
```

**When to Disable:**
- One-off experiments
- Testing caching system itself
- Debugging data loading issues

### 3. Use Descriptive `pipeline_name`

**Good:**
```json
"pipeline_name": "xy2000m_rand_offset_10_20m_30pct_minimal_cols"
```

**Bad:**
```json
"pipeline_name": "test1"
```

### 4. Match Attack Distance to Filtering Distance

For `rand_position` attacks, set `max_distance` to match the filtering radius:

```json
"data": {
  "filtering": {
    "distance_meters": 2000
  }
},
"attacks": {
  "type": "rand_position",
  "max_distance": 2000  // Match filtering distance
}
```

### 5. Start with `minimal_xy_elev` Columns

**Why:** Faster computation, easier debugging, lower memory usage.

Once the pipeline works, scale up to `extended_timestamps` or `all`.

### 6. Tune `num_subsection_rows` for Your System

**64GB RAM System:**
- Safe: 100000-500000 rows/partition
- Aggressive: 500000-1000000 rows/partition

**32GB RAM System:**
- Safe: 50000-100000 rows/partition

**Monitor:** Check Dask dashboard at `http://localhost:8787` for memory usage.

### 7. Use Fixed-Size Splits for Comparisons

When comparing different models or attacks:

```json
"ml": {
  "train_test_split": {
    "type": "fixed_size",
    "train_size": 80000,
    "test_size": 20000,
    "random_seed": 42
  }
}
```

Ensures identical dataset sizes across all experiments.

### 8. Validate Before Running

```bash
python scripts/validate_pipeline_configs.py config.json
```

Catches errors before starting expensive computations.

---

## Additional Resources

- **Main README:** [/README.md](../README.md) - Installation, quick start, architecture
- **Example Configs:** [/MClassifierPipelines/configs/](../MClassifierPipelines/configs/) - 55+ real-world examples
- **API Documentation:** [/docs/API/](../docs/API/) - Class and method references
- **Performance Guide:** [/docs/performance/](../docs/performance/) - Benchmarks and optimization
- **Testing Guide:** [/Test/README.md](../Test/README.md) - Running tests

---

## Support

If you encounter issues:

1. Check the [Troubleshooting section in README](../README.md#troubleshooting)
2. Validate your config: `python scripts/validate_pipeline_configs.py config.json`
3. Check Dask dashboard: `http://localhost:8787`
4. Review logs in `logs/` directory
5. Open an issue on GitHub with:
   - Your config file
   - Error message
   - System specs (RAM, CPU, OS)

---

**Last Updated:** 2026-01-18
**Version:** 1.0
**Dask Migration:** Complete (51/58 tasks, Phase 8 in progress)

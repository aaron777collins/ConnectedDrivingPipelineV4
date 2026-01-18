# MClassifierPipelines Directory

This directory organizes the ConnectedDrivingPipelineV4 machine learning classifier pipeline configurations and scripts.

## Directory Structure

```
MClassifierPipelines/
├── README.md                 # This file
├── configs/                  # JSON configuration files for DaskPipelineRunner
├── original_scripts/         # Original 55+ MClassifierLargePipeline*.py scripts (moved from root)
├── legacy/                   # Deprecated or legacy scripts (for reference only)
└── deprecated/               # Scripts marked for removal (kept for historical reference)
```

## Purpose

The MClassifierPipelines directory serves as the central location for all ML classifier pipeline configurations and scripts. The migration from pandas to Dask aims to consolidate the 55+ individual pipeline scripts into a single parameterized `DaskPipelineRunner` that reads JSON configs.

## Directory Usage

### `configs/`
Contains JSON configuration files that define pipeline parameters:
- Data filtering (distance, date ranges, coordinates)
- Attack simulation parameters (type, distance, ratio)
- ML classifier settings (train/test split, feature columns)
- Cache and output settings

Each config file corresponds to one of the original pipeline scripts but in a declarative format.

### `original_scripts/`
Contains the original 55+ MClassifierLargePipeline*.py scripts moved from the repository root. These scripts are preserved for:
- Reference during migration
- Backwards compatibility validation
- Parameter extraction for config generation

These scripts will eventually be replaced by DaskPipelineRunner + configs.

### `legacy/`
Contains older or deprecated scripts that are no longer actively used but kept for historical reference.

### `deprecated/`
Contains scripts marked for removal after migration is complete.

## Migration Strategy

1. **Phase 1**: Move all original scripts to `original_scripts/` (preserves root directory)
2. **Phase 2**: Create config generator script to extract parameters from filenames
3. **Phase 3**: Generate JSON configs for all 55+ pipelines in `configs/`
4. **Phase 4**: Implement DaskPipelineRunner to execute pipelines from configs
5. **Phase 5**: Validate DaskPipelineRunner outputs match original script outputs
6. **Phase 6**: Deprecate original scripts, move to `deprecated/`

## Pipeline Naming Conventions

Original pipeline scripts follow this naming pattern:
```
MClassifierLargePipelineUser[Variant][Distance][DataSelection][Features][Attackers][Location][Date].py
```

Examples:
- `WithXYOffsetPos2000mDist` = 2000m distance filtering from XY offset position
- `RandSplit80PercentTrain20PercentTest` = 80/20 train/test split
- `EXTTimestampsCols` = Extended timestamp feature columns
- `30attackersRandOffset100To200` = 30% attackers with 100-200m random offset
- `xN106y41d01to30m04y2021` = Location x=-106, y=41, dates 01-30 April 2021

## Config File Structure

JSON configs will follow this structure (example):

```json
{
  "pipeline_name": "2000m_rand_offset_100_200_ext_timestamps_30_attackers",
  "data": {
    "filtering": {
      "type": "xy_offset_position",
      "distance_meters": 2000,
      "center_x": -106,
      "center_y": 41
    },
    "date_range": {
      "start": "2021-04-01",
      "end": "2021-04-30"
    }
  },
  "features": {
    "column_set": "ext_timestamps",
    "columns": ["x_pos", "y_pos", "elevation", "month", "day", "year", "hour", "minute", "second", "am_pm"]
  },
  "attacks": {
    "enabled": true,
    "ratio": 0.30,
    "type": "rand_offset",
    "min_distance": 100,
    "max_distance": 200
  },
  "ml": {
    "train_test_split": {
      "type": "random",
      "train_ratio": 0.80,
      "test_ratio": 0.20,
      "seed": 42
    },
    "classifiers": ["RandomForest", "DecisionTree", "KNeighbors"]
  },
  "cache": {
    "enabled": true,
    "format": "parquet"
  }
}
```

## Related Files

- `/tmp/original-repo/MachineLearning/DaskPipelineRunner.py` - Parameterized pipeline executor (to be implemented)
- `/tmp/original-repo/scripts/generate_pipeline_configs.py` - Config generator (to be implemented)
- `/tmp/original-repo/scripts/validate_pipeline_migration.py` - Migration validator (to be implemented)

## Current Status

- [x] Directory structure created (Task 26)
- [ ] Original scripts moved to `original_scripts/` (Task 27)
- [ ] Config generator script implemented (Task 28)
- [ ] JSON configs generated (Task 29)
- [ ] Configs validated (Task 30)
- [ ] DaskPipelineRunner implemented (Task 27)
- [ ] Migration validated (Task 33)

## Notes

This directory is part of the COMPREHENSIVE_DASK_MIGRATION_PLAN. See `/tmp/COMPREHENSIVE_DASK_MIGRATION_PLAN.md` and `COMPREHENSIVE_DASK_MIGRATION_PLAN_PROGRESS.md` for full migration details.

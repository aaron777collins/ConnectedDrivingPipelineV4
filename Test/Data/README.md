# Test Data Directory

This directory contains sample BSM (Basic Safety Message) datasets for testing the ConnectedDrivingPipelineV4 PySpark migration.

## Available Datasets

| File | Rows | Size | Purpose |
|------|------|------|---------|
| `sample_1k.csv` | 1,000 | ~260 KB | Quick integration tests |
| `sample_10k.csv` | 10,000 | ~2.6 MB | Performance benchmarks |
| `sample_100k.csv` | 100,000 | ~26 MB | Scalability tests |

## Dataset Characteristics

All datasets follow the **BSMRawSchema** structure (19 columns):

### Metadata Fields (8 columns)
- `metadata_generatedAt`: Timestamp when message was generated
- `metadata_recordType`: Record type (always "bsmTx")
- `metadata_serialId_streamId`: Stream identifier (hex string)
- `metadata_serialId_bundleSize`: Bundle size (integer)
- `metadata_serialId_bundleId`: Bundle identifier (integer)
- `metadata_serialId_recordId`: Record ID within bundle (integer)
- `metadata_serialId_serialNumber`: Serial number (integer)
- `metadata_receivedAt`: Timestamp when message was received

### Core Data Fields (11 columns)
- `coreData_id`: Vehicle temporary ID (hex string)
- `coreData_secMark`: Second mark/timestamp marker (0-59999 ms)
- `coreData_position_lat`: GPS latitude (decimal degrees)
- `coreData_position_long`: GPS longitude (decimal degrees)
- `coreData_accuracy_semiMajor`: Positional accuracy semi-major axis (meters)
- `coreData_accuracy_semiMinor`: Positional accuracy semi-minor axis (meters)
- `coreData_elevation`: Vehicle elevation (meters)
- `coreData_accelset_accelYaw`: Acceleration yaw component
- `coreData_speed`: Vehicle speed (m/s)
- `coreData_heading`: Vehicle heading (degrees, 0-359)
- `coreData_position`: Combined position as WKT POINT string

## Data Generation Parameters

### Temporal Distribution
- **Base timestamp**: April 6, 2021, 10:00:00 AM
- **Interval**: 1 second between consecutive records
- **Format**: `MM/dd/yyyy hh:mm:ss a` (e.g., "04/06/2021 10:00:00 AM")

### Spatial Distribution
- **Base location**: Wyoming coordinates (41.25°N, 105.93°W)
- **Movement pattern**: Vehicles move gradually northward and eastward
- **GPS accuracy**: 1-10 meters (realistic GPS variation)

### Vehicle Streams
- **1k dataset**: 10 unique vehicle streams (~100 messages per vehicle)
- **10k dataset**: 100 unique vehicle streams (~100 messages per vehicle)
- **100k dataset**: 1,000 unique vehicle streams (~100 messages per vehicle)

### Speed Distribution
- **Range**: 0-45 m/s (0-100 mph)
- **Mean**: ~20 m/s (~45 mph)
- **Distribution**: Gaussian with σ=8 m/s

### Other Parameters
- **Heading**: Semi-random (each vehicle maintains general direction)
- **Elevation**: 1,500m ± 100m (typical for Wyoming)
- **Acceleration**: Small random values (Gaussian, mean=0, σ=5)

## Regenerating Datasets

To regenerate the sample datasets (e.g., with different parameters):

```bash
python3 -m Test.Data.generate_sample_datasets
```

The generator script uses `seed=42` for reproducibility, so regeneration will produce identical datasets.

## Usage Examples

### Pandas

```python
import pandas as pd

# Read sample dataset
df = pd.read_csv('Test/Data/sample_1k.csv')

# Verify schema
print(f"Rows: {len(df)}")
print(f"Columns: {len(df.columns)}")
print(f"Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
```

### PySpark

```python
from pyspark.sql import SparkSession
from Schemas.BSMRawSchema import get_bsm_raw_schema

spark = SparkSession.builder.getOrCreate()
schema = get_bsm_raw_schema()

# Read with schema
df = spark.read.schema(schema).csv(
    'Test/Data/sample_1k.csv',
    header=True
)

# Verify
print(f"Rows: {df.count()}")
print(f"Columns: {len(df.columns)}")
df.printSchema()
```

### Integration Testing

```python
import pytest
from pyspark.sql import SparkSession

def test_pipeline_with_sample_data(spark_session):
    """Test full pipeline with 1k sample data."""
    from Schemas.BSMRawSchema import get_bsm_raw_schema

    schema = get_bsm_raw_schema()
    df = spark_session.read.schema(schema).csv(
        'Test/Data/sample_1k.csv',
        header=True
    )

    # Run pipeline
    result = run_pipeline(df)

    # Validate
    assert result.count() == 1000
    assert 'isAttacker' in result.columns
```

## Golden Dataset Validation

These datasets can be used for golden dataset validation when comparing pandas vs PySpark outputs:

1. **Generate baseline**: Run pandas pipeline on `sample_1k.csv`, save output
2. **Test migration**: Run PySpark pipeline on same `sample_1k.csv`
3. **Compare**: Assert outputs match within tolerance (< 0.1% difference)

```python
# Generate golden dataset
pandas_output = pandas_pipeline.run('Test/Data/sample_1k.csv')
pandas_output.to_csv('Test/Data/golden_1k_pandas.csv', index=False)

# Test PySpark migration
pyspark_output = pyspark_pipeline.run('Test/Data/sample_1k.csv')
pyspark_df = pyspark_output.toPandas()

# Compare
import pandas.testing as pdt
pdt.assert_frame_equal(
    pandas_output.sort_index(),
    pyspark_df.sort_index(),
    check_dtype=False,
    rtol=1e-3  # 0.1% tolerance
)
```

## Notes

- **Deterministic generation**: All datasets use `seed=42` for reproducibility
- **Realistic data**: Temporal/spatial patterns mimic real Wyoming CV Pilot data
- **Schema compliance**: All datasets conform to `BSMRawSchema` structure
- **Size considerations**:
  - `sample_1k.csv`: Small enough to load entirely in memory
  - `sample_10k.csv`: Good for initial performance testing
  - `sample_100k.csv`: Tests PySpark partitioning without being too large

## Git Considerations

These CSV files are relatively large. Consider:

1. **Add to .gitignore** if repository size is a concern
2. **Use Git LFS** for version control of large files
3. **Regenerate on demand** using the generator script

Current recommendation: **Commit to repository** for easy access by all developers, as they're under 30 MB total.

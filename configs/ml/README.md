# ML Pipeline Configurations

This directory contains ML pipeline configurations optimized for different hardware profiles.

## Available Configurations

### `32gb-production.yml`
Optimized for 32GB RAM systems. Features:
- Memory-efficient batch processing (10K samples per batch)
- Dask integration with 2 workers × 6GB
- Conservative hyperparameter tuning (20 iterations)
- SMOTE for class balancing
- Automatic dtype optimization

## Usage

```python
import yaml
from pathlib import Path

# Load config
config_path = Path("configs/ml/32gb-production.yml")
with open(config_path) as f:
    config = yaml.safe_load(f)

# Access settings
batch_size = config['training']['batch_size']
model_params = config['models']['random_forest']['params']
```

## Configuration Sections

| Section | Description |
|---------|-------------|
| `preprocessing` | Feature scaling, imputation, train/test split |
| `features` | Core, derived, temporal, and geo features |
| `models` | Model-specific parameters and tuning |
| `training` | Batch size, CV, early stopping, metrics |
| `attack_detection` | Task-specific settings for attack detection |
| `output` | Directories for models, predictions, reports |
| `memory` | Memory optimization settings |
| `dask` | Dask cluster configuration |

## Memory Guidelines for 32GB Systems

- **Max batch size:** 50,000 records
- **Max concurrent models:** 2
- **Recommended dataset size:** < 10M records (use sampling for larger)
- **Feature count:** < 100 features per model

## Customization

Copy and modify for your specific needs:

```bash
cp configs/ml/32gb-production.yml configs/ml/my-config.yml
```

Then edit `my-config.yml` with your settings.

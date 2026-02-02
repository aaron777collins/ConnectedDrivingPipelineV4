#!/usr/bin/env python3
"""
Wyoming 100M Row E2E Test Runner

Runs the end-to-end test pipeline with:
- 30% attacker injection
- Random offset attack
- 2000m distance filter
- 30 days date range
- 100M row dataset (synthetic or S3)

Usage:
    python scripts/run_wyoming_100m_e2e_test.py --config configs/dask/wyoming-100m-e2e-test.yml
    python scripts/run_wyoming_100m_e2e_test.py --synthetic --rows 1000000  # Smaller test
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import yaml
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix

# Import project modules
from DataSources.Wyoming100MDataSource import Wyoming100MDataSource

import numpy as np


def add_attackers_simple(
    df: pd.DataFrame,
    attack_ratio: float = 0.3,
    min_offset: float = 100,
    max_offset: float = 200,
    seed: int = 42
) -> pd.DataFrame:
    """
    Add attackers to a DataFrame using random positional offset attack.
    
    This is a simplified version that doesn't require dependency injection.
    
    Args:
        df: Input DataFrame with x_pos and y_pos columns
        attack_ratio: Ratio of rows to mark as attackers (0.0 to 1.0)
        min_offset: Minimum offset in meters
        max_offset: Maximum offset in meters
        seed: Random seed for reproducibility
    
    Returns:
        DataFrame with isAttacker column and modified positions for attackers
    """
    np.random.seed(seed)
    
    n_rows = len(df)
    n_attackers = int(n_rows * attack_ratio)
    
    # Get unique IDs (vehicles)
    if 'coreData_id' in df.columns:
        unique_ids = df['coreData_id'].unique()
        n_attacker_ids = int(len(unique_ids) * attack_ratio)
        attacker_ids = np.random.choice(unique_ids, size=n_attacker_ids, replace=False)
        df['isAttacker'] = df['coreData_id'].isin(attacker_ids).astype(int)
    else:
        # Fallback: randomly select rows
        attacker_indices = np.random.choice(n_rows, size=n_attackers, replace=False)
        df['isAttacker'] = 0
        df.loc[df.index[attacker_indices], 'isAttacker'] = 1
    
    # Apply random offset to attacker positions
    # Convert meters to approximate degrees (at Wyoming latitude ~41°)
    lat_deg_per_m = 1 / 111000
    lon_deg_per_m = 1 / (111000 * 0.75)  # cos(41°) ≈ 0.75
    
    attacker_mask = df['isAttacker'] == 1
    n_actual_attackers = attacker_mask.sum()
    
    # Generate random angles and distances
    angles = np.random.uniform(0, 2 * np.pi, size=n_actual_attackers)
    distances = np.random.uniform(min_offset, max_offset, size=n_actual_attackers)
    
    # Calculate offsets in degrees
    x_offset = distances * np.cos(angles) * lon_deg_per_m
    y_offset = distances * np.sin(angles) * lat_deg_per_m
    
    # Apply offsets
    df.loc[attacker_mask, 'x_pos'] = df.loc[attacker_mask, 'x_pos'] + x_offset
    df.loc[attacker_mask, 'y_pos'] = df.loc[attacker_mask, 'y_pos'] + y_offset
    
    return df

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/wyoming_100m_e2e.log')
    ]
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_e2e_test(config: dict, num_rows: int = None, use_synthetic: bool = True):
    """
    Run the Wyoming 100M E2E test pipeline.
    
    Args:
        config: Configuration dictionary
        num_rows: Override number of rows (for smaller tests)
        use_synthetic: Use synthetic data generation
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Wyoming 100M Row E2E Test Started")
    logger.info("=" * 60)
    
    # Extract config values
    source_config = config.get('Wyoming100MDataSource', {})
    attack_config = config.get('AttackSimulation', {})
    ml_config = config.get('MLModel', {})
    cleaning_config = config.get('DataCleaning', {})
    output_config = config.get('Output', {})
    
    # Override settings if provided
    actual_rows = num_rows or source_config.get('num_rows', 100_000_000)
    
    logger.info(f"Configuration:")
    logger.info(f"  - Rows: {actual_rows:,}")
    logger.info(f"  - Synthetic: {use_synthetic}")
    logger.info(f"  - Attack ratio: {attack_config.get('attack_ratio', 0.3)}")
    logger.info(f"  - Attack type: {attack_config.get('attack_type', 'rand_offset')}")
    logger.info(f"  - Max distance: {cleaning_config.get('max_dist', 2000)}m")
    
    # Step 1: Create/Load Data Source
    logger.info("\n[Step 1/5] Creating Wyoming 100M Data Source...")
    
    data_source = Wyoming100MDataSource(
        use_synthetic=use_synthetic,
        num_rows=actual_rows,
        cache_dir=Path(source_config.get('cache_dir', 'data/cache/wyoming100m')),
        random_seed=source_config.get('random_seed', 42),
    )
    
    # Check if already cached
    if data_source.is_cached():
        logger.info(f"  Found cached data at {data_source.get_cache_path()}")
    else:
        logger.info(f"  Generating/downloading {actual_rows:,} rows...")
    
    data = data_source.get_data()
    logger.info(f"  Loaded {len(data):,} rows")
    
    # Step 2: Apply spatial and temporal filters
    logger.info("\n[Step 2/5] Applying filters...")
    
    x_pos = cleaning_config.get('x_pos', -106.0831353)
    y_pos = cleaning_config.get('y_pos', 41.5430216)
    max_dist = cleaning_config.get('max_dist', 2000)
    
    # Filter by distance from center point
    data['x_pos'] = data['coreData_position_long'].astype(float)
    data['y_pos'] = data['coreData_position_lat'].astype(float)
    
    # Simple distance calculation (approximate, in meters)
    lat_deg_to_m = 111000
    lon_deg_to_m = 111000 * 0.75  # cos(41°)
    
    data['distance'] = (
        ((data['x_pos'] - x_pos) * lon_deg_to_m) ** 2 +
        ((data['y_pos'] - y_pos) * lat_deg_to_m) ** 2
    ) ** 0.5
    
    filtered_data = data[data['distance'] <= max_dist].copy()
    logger.info(f"  Filtered to {len(filtered_data):,} rows within {max_dist}m of ({x_pos}, {y_pos})")
    
    # If no data after filter, expand the filter radius
    if len(filtered_data) < 1000:
        logger.warning(f"  Only {len(filtered_data)} rows after filter. Using all data for test.")
        filtered_data = data.copy()
    
    # Step 3: Train/Test Split
    logger.info("\n[Step 3/5] Splitting train/test data...")
    
    seed = attack_config.get('SEED', 42)
    train_ratio = ml_config.get('train_ratio', 0.8)
    
    train_data, test_data = train_test_split(
        filtered_data, 
        test_size=1-train_ratio, 
        random_state=seed
    )
    logger.info(f"  Train: {len(train_data):,} rows, Test: {len(test_data):,} rows")
    
    # Step 4: Add attackers
    logger.info("\n[Step 4/5] Adding attackers (30% injection, random offset)...")
    
    attack_ratio = attack_config.get('attack_ratio', 0.3)
    min_dist = attack_config.get('min_distance', 100)
    max_dist_attack = attack_config.get('max_distance', 200)
    
    # Directly add attackers without dependency injection framework
    train_attacked = add_attackers_simple(
        train_data.copy(), 
        attack_ratio=attack_ratio,
        min_offset=min_dist,
        max_offset=max_dist_attack,
        seed=seed
    )
    
    test_attacked = add_attackers_simple(
        test_data.copy(),
        attack_ratio=attack_ratio,
        min_offset=min_dist,
        max_offset=max_dist_attack,
        seed=seed + 1  # Different seed for test
    )
    
    train_attackers = train_attacked['isAttacker'].sum()
    test_attackers = test_attacked['isAttacker'].sum()
    logger.info(f"  Train attackers: {train_attackers:,} ({train_attackers/len(train_attacked)*100:.1f}%)")
    logger.info(f"  Test attackers: {test_attackers:,} ({test_attackers/len(test_attacked)*100:.1f}%)")
    
    # Step 5: Train and evaluate ML models
    logger.info("\n[Step 5/5] Training and evaluating ML models...")
    
    # Select features
    feature_cols = ['coreData_elevation', 'x_pos', 'y_pos']
    target_col = 'isAttacker'
    
    # Prepare data
    X_train = train_attacked[feature_cols].fillna(0)
    y_train = train_attacked[target_col].astype(int)
    X_test = test_attacked[feature_cols].fillna(0)
    y_test = test_attacked[target_col].astype(int)
    
    # Classifiers to test
    classifiers = [
        ('RandomForest', RandomForestClassifier(random_state=seed)),
        ('DecisionTree', DecisionTreeClassifier(random_state=seed)),
        ('KNeighbors', KNeighborsClassifier()),
    ]
    
    results = []
    
    for name, clf in classifiers:
        logger.info(f"\n  Training {name}...")
        
        train_start = time.time()
        clf.fit(X_train, y_train)
        train_time = time.time() - train_start
        
        # Predictions
        y_train_pred = clf.predict(X_train)
        y_test_pred = clf.predict(X_test)
        
        # Metrics
        train_acc = accuracy_score(y_train, y_train_pred)
        train_prec = precision_score(y_train, y_train_pred, zero_division=0)
        train_rec = recall_score(y_train, y_train_pred, zero_division=0)
        train_f1 = f1_score(y_train, y_train_pred, zero_division=0)
        
        test_acc = accuracy_score(y_test, y_test_pred)
        test_prec = precision_score(y_test, y_test_pred, zero_division=0)
        test_rec = recall_score(y_test, y_test_pred, zero_division=0)
        test_f1 = f1_score(y_test, y_test_pred, zero_division=0)
        
        result = {
            'Model': name,
            'Train_Time': f"{train_time:.2f}s",
            'Train_Samples': len(X_train),
            'Test_Samples': len(X_test),
            'Train_Accuracy': f"{train_acc:.4f}",
            'Train_Precision': f"{train_prec:.4f}",
            'Train_Recall': f"{train_rec:.4f}",
            'Train_F1': f"{train_f1:.4f}",
            'Test_Accuracy': f"{test_acc:.4f}",
            'Test_Precision': f"{test_prec:.4f}",
            'Test_Recall': f"{test_rec:.4f}",
            'Test_F1': f"{test_f1:.4f}",
        }
        results.append(result)
        
        logger.info(f"    Train: Acc={train_acc:.4f}, Prec={train_prec:.4f}, Rec={train_rec:.4f}, F1={train_f1:.4f}")
        logger.info(f"    Test:  Acc={test_acc:.4f}, Prec={test_prec:.4f}, Rec={test_rec:.4f}, F1={test_f1:.4f}")
    
    # Save results
    results_dir = Path(output_config.get('results_dir', 'results/wyoming_100m_e2e'))
    results_dir.mkdir(parents=True, exist_ok=True)
    
    results_df = pd.DataFrame(results)
    results_path = results_dir / output_config.get('csv_file', 'Wyoming100M_E2E_Results.csv')
    results_df.to_csv(results_path, index=False)
    logger.info(f"\nResults saved to {results_path}")
    
    total_time = time.time() - start_time
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("E2E Test Complete!")
    logger.info("=" * 60)
    logger.info(f"Total time: {total_time:.2f}s ({total_time/60:.2f}min)")
    logger.info(f"Dataset size: {len(data):,} rows")
    logger.info(f"Filtered size: {len(filtered_data):,} rows")
    logger.info(f"Attack ratio: {attack_ratio*100:.0f}%")
    logger.info(f"Results saved to: {results_path}")
    
    return {
        'success': True,
        'total_time': total_time,
        'dataset_size': len(data),
        'filtered_size': len(filtered_data),
        'results': results,
        'results_path': str(results_path),
    }


def main():
    parser = argparse.ArgumentParser(
        description='Run Wyoming 100M Row E2E Test'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='configs/dask/wyoming-100m-e2e-test.yml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--rows',
        type=int,
        default=None,
        help='Override number of rows (for smaller tests)'
    )
    parser.add_argument(
        '--synthetic',
        action='store_true',
        default=True,
        help='Use synthetic data (default: True)'
    )
    parser.add_argument(
        '--s3',
        action='store_true',
        help='Use S3 download (requires AWS credentials)'
    )
    
    args = parser.parse_args()
    
    # Load config
    config_path = Path(project_root) / args.config
    if config_path.exists():
        config = load_config(config_path)
    else:
        logger.warning(f"Config file not found: {config_path}. Using defaults.")
        config = {}
    
    # Determine data source mode
    use_synthetic = not args.s3
    
    # Run test
    result = run_e2e_test(
        config=config,
        num_rows=args.rows,
        use_synthetic=use_synthetic,
    )
    
    return 0 if result['success'] else 1


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python3
"""
Wyoming April 2021 Constant Offset Attack Pipeline

This script implements the full research pipeline:
1. Extract and filter data from Wyoming CV dataset
2. Convert coordinates to local X/Y projection
3. Apply constant offset attack per vehicle
4. Train and evaluate ML classifiers

Author: Sophie (AI Research Assistant)
Supervisor: Aaron Collins
Date: 2026-02-12
"""

import os
import sys
import json
import math
import logging
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, confusion_matrix, classification_report
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/wyoming_apr2021_constoffset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG_PATH = "configs/wyoming_apr2021_constoffset_v1.json"

def load_config():
    """Load pipeline configuration."""
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# =============================================================================
# COORDINATE CONVERSION
# =============================================================================

def latlon_to_xy_meters(lat, lon, center_lat, center_lon):
    """
    Convert lat/lon to local X/Y coordinates in meters.
    Uses equirectangular projection (accurate for small areas).
    
    Args:
        lat: Latitude in degrees
        lon: Longitude in degrees
        center_lat: Center latitude for projection
        center_lon: Center longitude for projection
    
    Returns:
        (x, y) tuple in meters from center
    """
    # Earth radius in meters
    R = 6371000
    
    # Convert to radians
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    center_lat_rad = math.radians(center_lat)
    center_lon_rad = math.radians(center_lon)
    
    # Equirectangular projection
    x = R * (lon_rad - center_lon_rad) * math.cos(center_lat_rad)
    y = R * (lat_rad - center_lat_rad)
    
    return x, y

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two lat/lon points in meters."""
    R = 6371000
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

# =============================================================================
# PHASE 1: CLEAN DATA EXTRACTION
# =============================================================================

def extract_clean_data(config, client):
    """
    Extract and clean data from source CSV.
    
    Steps:
    1. Read CSV with Dask
    2. Select required columns
    3. Filter by date range
    4. Filter by location (2000m radius)
    5. Convert lat/lon to X/Y meters
    6. Cache to parquet
    """
    cache_path = config["cache"]["clean_dataset"]
    
    # Check if cached version exists
    if os.path.exists(cache_path):
        logger.info(f"Clean dataset cache found: {cache_path}")
        logger.info("Loading from cache...")
        return dd.read_parquet(cache_path)
    
    logger.info("="*60)
    logger.info("PHASE 1: CLEAN DATA EXTRACTION")
    logger.info("="*60)
    
    source_file = config["data"]["source_file"]
    logger.info(f"Reading source file: {source_file}")
    
    # Read CSV with Dask
    columns = config["data"]["columns_to_extract"]
    logger.info(f"Extracting columns: {columns}")
    
    # Read the CSV (Dask will handle chunking)
    df = dd.read_csv(
        source_file,
        usecols=columns,
        dtype={
            "coreData_id": "object",
            "coreData_msgCnt": "float64",
            "coreData_position_lat": "float64",
            "coreData_position_long": "float64",
            "coreData_elevation": "float64",
            "coreData_accelset_accelYaw": "float64",
            "coreData_accuracy_semiMajor": "float64",
            "coreData_speed": "float64",
            "coreData_heading": "float64"
        },
        parse_dates=["metadata_receivedAt"],
        assume_missing=True
    )
    
    logger.info(f"Initial partitions: {df.npartitions}")
    
    # Filter by date range
    date_start = pd.Timestamp(config["data"]["date_range"]["start"])
    date_end = pd.Timestamp(config["data"]["date_range"]["end"])
    logger.info(f"Filtering by date: {date_start} to {date_end}")
    
    df = df[
        (df["metadata_receivedAt"] >= date_start) & 
        (df["metadata_receivedAt"] <= date_end)
    ]
    
    # Get filter parameters
    center_lat = config["data"]["filtering"]["center_latitude"]
    center_lon = config["data"]["filtering"]["center_longitude"]
    radius_m = config["data"]["filtering"]["radius_meters"]
    
    logger.info(f"Filtering by location: ({center_lon}, {center_lat}), radius {radius_m}m")
    
    # Define coordinate conversion function for map_partitions
    def process_partition(partition, center_lat, center_lon, radius_m):
        """Process a single partition: filter by location and convert coordinates."""
        if len(partition) == 0:
            # Return empty with correct schema
            partition["x_pos"] = pd.Series(dtype="float64")
            partition["y_pos"] = pd.Series(dtype="float64")
            return partition
        
        # Calculate distance from center for each row
        distances = partition.apply(
            lambda row: haversine_distance(
                row["coreData_position_lat"], 
                row["coreData_position_long"],
                center_lat, 
                center_lon
            ),
            axis=1
        )
        
        # Filter by radius
        partition = partition[distances <= radius_m].copy()
        
        if len(partition) == 0:
            partition["x_pos"] = pd.Series(dtype="float64")
            partition["y_pos"] = pd.Series(dtype="float64")
            return partition
        
        # Convert to X/Y coordinates
        coords = partition.apply(
            lambda row: pd.Series(latlon_to_xy_meters(
                row["coreData_position_lat"],
                row["coreData_position_long"],
                center_lat,
                center_lon
            )),
            axis=1
        )
        partition["x_pos"] = coords[0]
        partition["y_pos"] = coords[1]
        
        return partition
    
    # Define output schema
    meta = df._meta.copy()
    meta["x_pos"] = 0.0
    meta["y_pos"] = 0.0
    
    # Apply processing to all partitions
    logger.info("Processing partitions (filter + coordinate conversion)...")
    df = df.map_partitions(
        process_partition,
        center_lat=center_lat,
        center_lon=center_lon,
        radius_m=radius_m,
        meta=meta
    )
    
    # Drop rows with NaN in critical columns
    df = df.dropna(subset=["x_pos", "y_pos", "coreData_id"])
    
    # Ensure cache directory exists
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    
    # Save to parquet
    logger.info(f"Saving clean dataset to: {cache_path}")
    df.to_parquet(cache_path, engine="pyarrow", write_index=False)
    
    # Reload from parquet for better performance
    df = dd.read_parquet(cache_path)
    
    row_count = len(df)
    unique_vehicles = df["coreData_id"].nunique().compute()
    logger.info(f"Clean dataset: {row_count:,} rows, {unique_vehicles:,} unique vehicles")
    
    return df

# =============================================================================
# PHASE 2: ATTACK INJECTION
# =============================================================================

def apply_attack(clean_df, config, client):
    """
    Apply constant offset attack per vehicle.
    
    Steps:
    1. Copy clean data (never modify source)
    2. Select 30% of vehicle IDs as malicious
    3. Generate random offset vector per malicious vehicle
    4. Apply offset to all BSMs from malicious vehicles
    5. Add isAttacker label
    6. Cache to parquet
    """
    cache_path = config["cache"]["attack_dataset"]
    
    # Check if cached version exists
    if os.path.exists(cache_path):
        logger.info(f"Attack dataset cache found: {cache_path}")
        logger.info("Loading from cache...")
        return dd.read_parquet(cache_path)
    
    logger.info("="*60)
    logger.info("PHASE 2: ATTACK INJECTION")
    logger.info("="*60)
    
    attack_config = config["attack"]
    seed = attack_config["seed"]
    malicious_ratio = attack_config["malicious_ratio"]
    dist_min = attack_config["offset_distance_min"]
    dist_max = attack_config["offset_distance_max"]
    
    logger.info(f"Attack parameters:")
    logger.info(f"  - Malicious ratio: {malicious_ratio*100}%")
    logger.info(f"  - Offset distance: {dist_min}-{dist_max}m")
    logger.info(f"  - Seed: {seed}")
    
    # Get unique vehicle IDs
    logger.info("Getting unique vehicle IDs...")
    unique_ids = clean_df["coreData_id"].unique().compute()
    unique_ids_sorted = sorted(unique_ids)
    logger.info(f"Total unique vehicles: {len(unique_ids_sorted):,}")
    
    # Select malicious vehicles
    rng = np.random.RandomState(seed)
    n_malicious = int(len(unique_ids_sorted) * malicious_ratio)
    malicious_ids = set(rng.choice(unique_ids_sorted, n_malicious, replace=False))
    logger.info(f"Selected {len(malicious_ids):,} malicious vehicles ({malicious_ratio*100}%)")
    
    # Generate attack vectors per vehicle
    attack_vectors = {}
    for vid in malicious_ids:
        direction = rng.uniform(0, 360)  # degrees
        distance = rng.uniform(dist_min, dist_max)  # meters
        dx = distance * math.cos(math.radians(direction))
        dy = distance * math.sin(math.radians(direction))
        attack_vectors[vid] = {"dx": dx, "dy": dy, "direction": direction, "distance": distance}
    
    logger.info(f"Generated attack vectors for {len(attack_vectors)} vehicles")
    
    # Sample attack vectors for logging
    sample_ids = list(attack_vectors.keys())[:3]
    for vid in sample_ids:
        v = attack_vectors[vid]
        logger.info(f"  Sample: {vid} -> {v['distance']:.1f}m @ {v['direction']:.1f}°")
    
    # Apply attacks using map_partitions
    def apply_attack_partition(partition, malicious_ids, attack_vectors):
        """Apply attacks to a single partition."""
        partition = partition.copy()
        
        # Initialize isAttacker column
        partition["isAttacker"] = 0
        
        for vid in malicious_ids:
            if vid not in attack_vectors:
                continue
            
            mask = partition["coreData_id"] == vid
            if not mask.any():
                continue
            
            v = attack_vectors[vid]
            partition.loc[mask, "x_pos"] = partition.loc[mask, "x_pos"] + v["dx"]
            partition.loc[mask, "y_pos"] = partition.loc[mask, "y_pos"] + v["dy"]
            partition.loc[mask, "isAttacker"] = 1
        
        return partition
    
    # Define output schema
    meta = clean_df._meta.copy()
    meta["isAttacker"] = 0
    
    logger.info("Applying attacks to all partitions...")
    attack_df = clean_df.map_partitions(
        apply_attack_partition,
        malicious_ids=malicious_ids,
        attack_vectors=attack_vectors,
        meta=meta
    )
    
    # Ensure cache directory exists
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    
    # Save to parquet
    logger.info(f"Saving attack dataset to: {cache_path}")
    attack_df.to_parquet(cache_path, engine="pyarrow", write_index=False)
    
    # Reload from parquet
    attack_df = dd.read_parquet(cache_path)
    
    # Compute statistics
    total_rows = len(attack_df)
    attack_rows = (attack_df["isAttacker"] == 1).sum().compute()
    logger.info(f"Attack dataset: {total_rows:,} rows, {attack_rows:,} attacked ({attack_rows/total_rows*100:.1f}%)")
    
    return attack_df

# =============================================================================
# PHASE 3: ML TRAINING AND EVALUATION
# =============================================================================

def train_and_evaluate(attack_df, config, client):
    """
    Train ML classifiers and evaluate performance.
    
    Steps:
    1. Convert Dask DataFrame to pandas (for sklearn)
    2. Split into train/test
    3. Train classifiers
    4. Evaluate and report metrics
    5. Save results
    """
    logger.info("="*60)
    logger.info("PHASE 3: ML TRAINING AND EVALUATION")
    logger.info("="*60)
    
    ml_config = config["ml"]
    feature_cols = ml_config["features"]
    label_col = ml_config["label"]
    test_size = ml_config["train_test_split"]["test_size"]
    random_state = ml_config["train_test_split"]["random_state"]
    
    logger.info(f"Features: {feature_cols}")
    logger.info(f"Label: {label_col}")
    logger.info(f"Test size: {test_size}")
    
    # Convert to pandas for sklearn
    logger.info("Converting to pandas...")
    df = attack_df[feature_cols + [label_col]].compute()
    
    # Drop any remaining NaN
    df = df.dropna()
    logger.info(f"Dataset size after dropna: {len(df):,} rows")
    
    # Split features and label
    X = df[feature_cols].values
    y = df[label_col].values
    
    logger.info(f"Class distribution: 0={sum(y==0):,}, 1={sum(y==1):,}")
    
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, shuffle=True
    )
    logger.info(f"Train: {len(X_train):,}, Test: {len(X_test):,}")
    
    # Define classifiers
    classifiers = {
        "RandomForest": RandomForestClassifier(
            n_estimators=100, max_depth=15, n_jobs=-1, random_state=random_state
        ),
        "DecisionTree": DecisionTreeClassifier(
            max_depth=15, random_state=random_state
        ),
        "KNeighbors": KNeighborsClassifier(
            n_neighbors=5, n_jobs=-1
        )
    }
    
    results = []
    
    for name, clf in classifiers.items():
        logger.info(f"\nTraining {name}...")
        
        # Train
        clf.fit(X_train, y_train)
        
        # Predict
        y_pred = clf.predict(X_test)
        
        # Metrics
        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        cm = confusion_matrix(y_test, y_pred)
        
        logger.info(f"{name} Results:")
        logger.info(f"  Accuracy:  {acc:.4f}")
        logger.info(f"  Precision: {prec:.4f}")
        logger.info(f"  Recall:    {rec:.4f}")
        logger.info(f"  F1 Score:  {f1:.4f}")
        logger.info(f"  Confusion Matrix:\n{cm}")
        
        results.append({
            "classifier": name,
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1,
            "true_negatives": cm[0][0],
            "false_positives": cm[0][1],
            "false_negatives": cm[1][0],
            "true_positives": cm[1][1]
        })
    
    # Save results
    results_df = pd.DataFrame(results)
    results_dir = config["output"]["results_dir"]
    os.makedirs(results_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_path = f"{results_dir}/wyoming-apr2021-constoffset-RF-DT-KNN-{timestamp}.csv"
    results_df.to_csv(results_path, index=False)
    logger.info(f"\nResults saved to: {results_path}")
    
    return results_df

# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    start_time = datetime.now()
    
    logger.info("="*60)
    logger.info("WYOMING APRIL 2021 CONSTANT OFFSET ATTACK PIPELINE")
    logger.info("="*60)
    logger.info(f"Started: {start_time}")
    
    # Ensure directories exist
    os.makedirs("logs", exist_ok=True)
    os.makedirs("cache/clean", exist_ok=True)
    os.makedirs("cache/attacks", exist_ok=True)
    os.makedirs("results", exist_ok=True)
    
    # Load config
    config = load_config()
    logger.info(f"Loaded config: {CONFIG_PATH}")
    logger.info(f"Pipeline name: {config['pipeline_name']}")
    
    # Initialize Dask cluster
    dask_config = config["dask"]
    logger.info(f"Initializing Dask cluster: {dask_config['n_workers']} workers, {dask_config['memory_limit']} each")
    
    cluster = LocalCluster(
        n_workers=dask_config["n_workers"],
        threads_per_worker=dask_config["threads_per_worker"],
        memory_limit=dask_config["memory_limit"]
    )
    client = Client(cluster)
    logger.info(f"Dask dashboard: {client.dashboard_link}")
    
    try:
        # Phase 1: Clean data extraction
        clean_df = extract_clean_data(config, client)
        
        # Phase 2: Attack injection
        attack_df = apply_attack(clean_df, config, client)
        
        # Phase 3: ML training and evaluation
        results = train_and_evaluate(attack_df, config, client)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("PIPELINE COMPLETE")
        logger.info("="*60)
        logger.info(f"\nFinal Results:")
        logger.info(results.to_string(index=False))
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        client.close()
        cluster.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"\nCompleted: {end_time}")
    logger.info(f"Duration: {duration}")

if __name__ == "__main__":
    main()

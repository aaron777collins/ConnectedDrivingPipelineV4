#!/usr/bin/env python3
"""
Wyoming April 2021 Constant Offset Attack Pipeline

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
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"logs/wyoming_apr2021_constoffset_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = "configs/wyoming_apr2021_constoffset_v1.json"

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# =============================================================================
# PHASE 1: CLEAN DATA EXTRACTION
# =============================================================================

def extract_clean_data(config, client):
    cache_path = config["cache"]["clean_dataset"]
    
    if os.path.exists(cache_path):
        logger.info(f"Clean dataset cache found: {cache_path}")
        return dd.read_parquet(cache_path)
    
    logger.info("="*60)
    logger.info("PHASE 1: CLEAN DATA EXTRACTION")
    logger.info("="*60)
    
    source_file = config["data"]["source_file"]
    columns = config["data"]["columns_to_extract"]
    
    logger.info(f"Reading source: {source_file}")
    df = dd.read_parquet(source_file, columns=columns)
    logger.info(f"Initial partitions: {df.npartitions}")
    
    # Filter parameters
    center_lat = config["data"]["filtering"]["center_latitude"]
    center_lon = config["data"]["filtering"]["center_longitude"]
    radius_m = config["data"]["filtering"]["radius_meters"]
    date_start_str = config["data"]["date_range"]["start"]  # "2021-04-01"
    date_end_str = config["data"]["date_range"]["end"]      # "2021-04-30"
    
    # Convert to MM/DD/YYYY format for string matching
    # date_start_str = "2021-04-01" -> month=04, year=2021
    ds = datetime.strptime(date_start_str, "%Y-%m-%d")
    de = datetime.strptime(date_end_str, "%Y-%m-%d")
    target_month = f"{ds.month:02d}"
    target_year = str(ds.year)
    
    logger.info(f"Filtering for month {target_month}/{target_year}, location ({center_lon}, {center_lat}), radius {radius_m}m")
    
    def process_partition(partition, center_lat, center_lon, radius_m, target_month, target_year):
        if len(partition) == 0:
            partition["x_pos"] = pd.Series(dtype="float64")
            partition["y_pos"] = pd.Series(dtype="float64")
            return partition
        
        # Convert date string (MM/DD/YYYY ...) to datetime for filtering
        partition = partition.copy()
        
        # Filter by date using string operations (format: MM/DD/YYYY HH:MM:SS AM/PM)
        date_col = partition["metadata_receivedAt"].astype(str)
        month_match = date_col.str[:2] == target_month
        year_match = date_col.str[6:10] == target_year
        date_mask = month_match & year_match
        
        partition = partition[date_mask]
        
        if len(partition) == 0:
            partition["x_pos"] = pd.Series(dtype="float64")
            partition["y_pos"] = pd.Series(dtype="float64")
            return partition
        
        # Drop NaN positions
        partition = partition.dropna(subset=["coreData_position_lat", "coreData_position_long"])
        
        if len(partition) == 0:
            partition["x_pos"] = pd.Series(dtype="float64")
            partition["y_pos"] = pd.Series(dtype="float64")
            return partition
        
        # Calculate distance (vectorized haversine)
        lat = partition["coreData_position_lat"].values
        lon = partition["coreData_position_long"].values
        
        R = 6371000
        lat_rad = np.radians(lat)
        center_lat_rad = np.radians(center_lat)
        delta_lat = np.radians(lat - center_lat)
        delta_lon = np.radians(lon - center_lon)
        
        a = np.sin(delta_lat/2)**2 + np.cos(lat_rad) * np.cos(center_lat_rad) * np.sin(delta_lon/2)**2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
        distances = R * c
        
        # Filter by radius
        partition = partition[distances <= radius_m].copy()
        
        if len(partition) == 0:
            partition["x_pos"] = pd.Series(dtype="float64")
            partition["y_pos"] = pd.Series(dtype="float64")
            return partition
        
        # Convert to X/Y meters
        lat = partition["coreData_position_lat"].values
        lon = partition["coreData_position_long"].values
        
        lon_rad = np.radians(lon)
        lat_rad = np.radians(lat)
        center_lon_rad = np.radians(center_lon)
        center_lat_rad = np.radians(center_lat)
        
        partition["x_pos"] = R * (lon_rad - center_lon_rad) * np.cos(center_lat_rad)
        partition["y_pos"] = R * (lat_rad - center_lat_rad)
        
        return partition
    
    meta = df._meta.copy()
    meta["x_pos"] = 0.0
    meta["y_pos"] = 0.0
    
    logger.info("Processing partitions...")
    df = df.map_partitions(
        process_partition,
        center_lat=center_lat,
        center_lon=center_lon,
        radius_m=radius_m,
        target_month=target_month,
        target_year=target_year,
        meta=meta
    )
    
    df = df.dropna(subset=["x_pos", "y_pos", "coreData_id"])
    
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    logger.info(f"Saving to: {cache_path}")
    df.to_parquet(cache_path, engine="pyarrow", write_index=False)
    
    df = dd.read_parquet(cache_path)
    
    row_count = len(df)
    unique_vehicles = df["coreData_id"].nunique().compute()
    logger.info(f"Clean dataset: {row_count:,} rows, {unique_vehicles:,} unique vehicles")
    
    return df

# =============================================================================
# PHASE 2: ATTACK INJECTION
# =============================================================================

def apply_attack(clean_df, config, client):
    cache_path = config["cache"]["attack_dataset"]
    
    if os.path.exists(cache_path):
        logger.info(f"Attack dataset cache found: {cache_path}")
        return dd.read_parquet(cache_path)
    
    logger.info("="*60)
    logger.info("PHASE 2: ATTACK INJECTION")
    logger.info("="*60)
    
    attack_config = config["attack"]
    seed = attack_config["seed"]
    malicious_ratio = attack_config["malicious_ratio"]
    dist_min = attack_config["offset_distance_min"]
    dist_max = attack_config["offset_distance_max"]
    
    logger.info(f"Attack: {malicious_ratio*100}% malicious, {dist_min}-{dist_max}m offset")
    
    unique_ids = clean_df["coreData_id"].unique().compute()
    unique_ids_sorted = sorted(unique_ids)
    logger.info(f"Total vehicles: {len(unique_ids_sorted):,}")
    
    rng = np.random.RandomState(seed)
    n_malicious = max(1, int(len(unique_ids_sorted) * malicious_ratio))
    malicious_ids = set(rng.choice(unique_ids_sorted, n_malicious, replace=False))
    logger.info(f"Malicious vehicles: {len(malicious_ids):,}")
    
    # Generate attack vectors
    attack_vectors = {}
    for vid in malicious_ids:
        direction = rng.uniform(0, 360)
        distance = rng.uniform(dist_min, dist_max)
        dx = distance * math.cos(math.radians(direction))
        dy = distance * math.sin(math.radians(direction))
        attack_vectors[vid] = {"dx": dx, "dy": dy, "direction": direction, "distance": distance}
        logger.info(f"  {vid}: {distance:.1f}m @ {direction:.1f}deg")
    
    def apply_attack_partition(partition, malicious_ids, attack_vectors):
        partition = partition.copy()
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
    
    meta = clean_df._meta.copy()
    meta["isAttacker"] = 0
    
    logger.info("Applying attacks...")
    attack_df = clean_df.map_partitions(
        apply_attack_partition,
        malicious_ids=malicious_ids,
        attack_vectors=attack_vectors,
        meta=meta
    )
    
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    logger.info(f"Saving to: {cache_path}")
    attack_df.to_parquet(cache_path, engine="pyarrow", write_index=False)
    
    attack_df = dd.read_parquet(cache_path)
    
    total_rows = len(attack_df)
    attack_rows = (attack_df["isAttacker"] == 1).sum().compute()
    logger.info(f"Attack dataset: {total_rows:,} rows, {attack_rows:,} attacked ({attack_rows/total_rows*100:.1f}%)")
    
    return attack_df

# =============================================================================
# PHASE 3: ML TRAINING
# =============================================================================

def train_and_evaluate(attack_df, config, client):
    logger.info("="*60)
    logger.info("PHASE 3: ML TRAINING AND EVALUATION")
    logger.info("="*60)
    
    ml_config = config["ml"]
    feature_cols = ml_config["features"]
    label_col = ml_config["label"]
    test_size = ml_config["train_test_split"]["test_size"]
    random_state = ml_config["train_test_split"]["random_state"]
    
    logger.info(f"Features: {feature_cols}")
    logger.info("Converting to pandas...")
    
    df = attack_df[feature_cols + [label_col]].compute()
    df = df.dropna()
    logger.info(f"Dataset: {len(df):,} rows")
    
    X = df[feature_cols].values
    y = df[label_col].values
    
    n_benign = sum(y == 0)
    n_attack = sum(y == 1)
    logger.info(f"Class distribution: benign={n_benign:,}, attack={n_attack:,}")
    
    if n_attack == 0:
        logger.error("No attack samples! Check attack injection.")
        return None
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, shuffle=True, stratify=y
    )
    logger.info(f"Train: {len(X_train):,}, Test: {len(X_test):,}")
    
    classifiers = {
        "RandomForest": RandomForestClassifier(n_estimators=100, max_depth=15, n_jobs=-1, random_state=random_state),
        "DecisionTree": DecisionTreeClassifier(max_depth=15, random_state=random_state),
        "KNeighbors": KNeighborsClassifier(n_neighbors=5, n_jobs=-1)
    }
    
    results = []
    
    for name, clf in classifiers.items():
        logger.info(f"\nTraining {name}...")
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        
        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        cm = confusion_matrix(y_test, y_pred, labels=[0, 1])
        
        logger.info(f"{name}: Acc={acc:.4f}, Prec={prec:.4f}, Rec={rec:.4f}, F1={f1:.4f}")
        logger.info(f"Confusion Matrix:\n{cm}")
        
        results.append({
            "classifier": name,
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "f1_score": f1,
            "true_negatives": int(cm[0][0]),
            "false_positives": int(cm[0][1]),
            "false_negatives": int(cm[1][0]),
            "true_positives": int(cm[1][1])
        })
    
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
    start_time = datetime.now()
    
    logger.info("="*60)
    logger.info("WYOMING APRIL 2021 CONSTANT OFFSET ATTACK PIPELINE")
    logger.info("="*60)
    logger.info(f"Started: {start_time}")
    
    os.makedirs("logs", exist_ok=True)
    os.makedirs("cache/clean", exist_ok=True)
    os.makedirs("cache/attacks", exist_ok=True)
    os.makedirs("results", exist_ok=True)
    
    config = load_config()
    logger.info(f"Config: {CONFIG_PATH}")
    
    dask_config = config["dask"]
    logger.info(f"Dask: {dask_config['n_workers']} workers, {dask_config['memory_limit']}")
    
    cluster = LocalCluster(
        n_workers=dask_config["n_workers"],
        threads_per_worker=dask_config["threads_per_worker"],
        memory_limit=dask_config["memory_limit"]
    )
    client = Client(cluster)
    logger.info(f"Dashboard: {client.dashboard_link}")
    
    try:
        # Clear old cache to regenerate with fixed filters
        import shutil
        for path in [config["cache"]["clean_dataset"], config["cache"]["attack_dataset"]]:
            if os.path.exists(path):
                logger.info(f"Clearing old cache: {path}")
                shutil.rmtree(path)
        
        clean_df = extract_clean_data(config, client)
        attack_df = apply_attack(clean_df, config, client)
        results = train_and_evaluate(attack_df, config, client)
        
        if results is not None:
            logger.info("\n" + "="*60)
            logger.info("PIPELINE COMPLETE")
            logger.info("="*60)
            logger.info(f"\n{results.to_string(index=False)}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        client.close()
        cluster.close()
    
    logger.info(f"\nDuration: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()

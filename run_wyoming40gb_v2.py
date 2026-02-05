#!/usr/bin/env python3
"""
Wyoming 40GB Pipeline Runner (v2 - with Dask cluster config)

Configures Dask LocalCluster with memory limits to prevent worker deaths.

Usage:
    python run_wyoming40gb_v2.py
    # Or with nohup:
    nohup python run_wyoming40gb_v2.py > wyoming40gb_run.log 2>&1 &

Author: Sophie (Aaron's AI Partner)
Date: 2026-02-04
"""

import os
import sys
import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure Dask BEFORE any dask imports
import dask
dask.config.set({
    'distributed.worker.memory.target': 0.7,    # Start spilling at 70%
    'distributed.worker.memory.spill': 0.8,     # Spill to disk at 80%
    'distributed.worker.memory.pause': 0.9,     # Pause workers at 90%
    'distributed.worker.memory.terminate': 0.95 # Kill only at 95%
})

from dask.distributed import Client, LocalCluster

def main():
    # Create cluster with explicit memory limits
    # 12 CPUs, 62GB RAM - use 4 workers with 12GB each (leaves room for system)
    print("Setting up Dask cluster...")
    cluster = LocalCluster(
        n_workers=4,
        threads_per_worker=3,
        memory_limit='12GB',  # Per worker
        silence_logs=False
    )
    client = Client(cluster)
    print(f"Dask dashboard: {client.dashboard_link}")
    print(f"Workers: {len(client.scheduler_info()['workers'])}")
    
    # Now import pipeline (after Dask is configured)
    print("Loading config...")
    from MachineLearning.DaskPipelineRunner import DaskPipelineRunner
    
    config_path = "configs/wyoming40gb_rand_offset_100_200.json"
    
    print(f"Pipeline config: {config_path}")
    runner = DaskPipelineRunner.from_config(config_path)
    
    print(f"Pipeline: {runner.pipeline_name}")
    print(f"Start time: {datetime.datetime.now()}")
    print("Starting run...")
    
    try:
        results = runner.run()
        print(f"\n=== Pipeline Complete ===")
        print(f"End time: {datetime.datetime.now()}")
        print(f"Results: {len(results)} classifiers trained")
        
        for classifier, train_results, test_results in results:
            print(f"\n{classifier.__class__.__name__}:")
            print(f"  Train accuracy: {train_results[0]:.4f}")
            print(f"  Test accuracy: {test_results[0]:.4f}")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        client.close()
        cluster.close()
        sys.exit(1)
    
    print("\n=== Output Files ===")
    print(f"CSV results: {runner.pipeline_name}.csv")
    print(f"Logs: logs/{runner.pipeline_name}/")
    print(f"Cleaned data: data/mclassifierdata/cleaned/{runner.pipeline_name}/")
    print(f"Confusion matrices: data/mclassifierdata/results/{runner.pipeline_name}/")
    
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()

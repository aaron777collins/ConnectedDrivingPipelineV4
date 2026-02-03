#!/usr/bin/env python3
"""
Wyoming 40GB Pipeline Runner

This script runs the ML pipeline on the full 40GB Wyoming CSV dataset
using DaskPipelineRunner with proper Dask configuration.

Usage:
    python run_wyoming40gb.py
    # Or with nohup:
    nohup python run_wyoming40gb.py > wyoming40gb_run.log 2>&1 &

Author: Sophie (Aaron's AI Partner)
Date: 2026-02-03
"""

import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("Loading config...")

from MachineLearning.DaskPipelineRunner import DaskPipelineRunner

def main():
    config_path = "configs/wyoming40gb_rand_offset_100_200.json"
    
    print(f"Pipeline config: {config_path}")
    runner = DaskPipelineRunner.from_config(config_path)
    
    print(f"Pipeline: {runner.pipeline_name}")
    print("Starting run...")
    
    try:
        results = runner.run()
        print(f"\n=== Pipeline Complete ===")
        print(f"Results: {len(results)} classifiers trained")
        
        for classifier, train_results, test_results in results:
            print(f"\n{classifier.__class__.__name__}:")
            print(f"  Train accuracy: {train_results[0]:.4f}")
            print(f"  Test accuracy: {test_results[0]:.4f}")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n=== Output Files ===")
    print(f"CSV results: {runner.pipeline_name}.csv")
    print(f"Logs: logs/{runner.pipeline_name}/")
    print(f"Cleaned data: data/mclassifierdata/cleaned/{runner.pipeline_name}/")
    print(f"Confusion matrices: data/mclassifierdata/results/{runner.pipeline_name}/")

if __name__ == "__main__":
    main()

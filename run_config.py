#!/usr/bin/env python3
"""
Generic config runner - runs any JSON config via DaskPipelineRunner
Usage: python run_config.py production_configs/basic_100km_pipeline_config.json
"""
import sys
import os
import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure Dask
import dask
dask.config.set({
    'distributed.worker.memory.target': 0.7,
    'distributed.worker.memory.spill': 0.8,
    'distributed.worker.memory.pause': 0.9,
    'distributed.worker.memory.terminate': 0.95
})

from dask.distributed import Client, LocalCluster

def main():
    if len(sys.argv) < 2:
        print('Usage: python run_config.py <config.json>')
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    print(f'Setting up Dask cluster...')
    cluster = LocalCluster(n_workers=4, threads_per_worker=3, memory_limit='12GB')
    client = Client(cluster)
    print(f'Dask dashboard: {client.dashboard_link}')
    
    from MachineLearning.DaskPipelineRunner import DaskPipelineRunner
    
    print(f'Loading config: {config_path}')
    runner = DaskPipelineRunner.from_config(config_path)
    print(f'Pipeline: {runner.pipeline_name}')
    print(f'Start: {datetime.datetime.now()}')
    
    try:
        results = runner.run()
        print(f'\n=== Complete ===')
        print(f'End: {datetime.datetime.now()}')
        for classifier, train_res, test_res in results:
            print(f'{classifier.__class__.__name__}: Train={train_res[0]:.4f}, Test={test_res[0]:.4f}')
    except Exception as e:
        print(f'ERROR: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()
        cluster.close()

if __name__ == '__main__':
    main()

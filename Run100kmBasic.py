#!/usr/bin/env python3
"""100km Basic Feature Set Pipeline - Random Offset 100-200m Attack"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dask
dask.config.set({'distributed.worker.memory.target': 0.7, 'distributed.worker.memory.spill': 0.8, 'distributed.worker.memory.pause': 0.9, 'distributed.worker.memory.terminate': 0.95})
from dask.distributed import Client, LocalCluster
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner
import datetime

print('Setting up Dask cluster...')
cluster = LocalCluster(n_workers=4, threads_per_worker=3, memory_limit='12GB')
client = Client(cluster)
print(f'Dask dashboard: {client.dashboard_link}')

config_path = 'production_configs/basic_100km_pipeline_config.json'
print(f'Config: {config_path}')
runner = DaskPipelineRunner.from_config(config_path)
print(f'Pipeline: {runner.pipeline_name}')
print(f'Start: {datetime.datetime.now()}')

try:
    results = runner.run()
    print(f'\n=== Complete @ {datetime.datetime.now()} ===')
    for classifier, train_res, test_res in results:
        print(f'{classifier.__class__.__name__}: Train={train_res[0]:.4f}, Test={test_res[0]:.4f}')
except Exception as e:
    print(f'ERROR: {e}')
    import traceback
    traceback.print_exc()
finally:
    client.close()
    cluster.close()

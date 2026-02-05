# Running on Jaekel Server

**Server:** 65.108.237.46 (Hetzner)  
**Specs:** AMD Ryzen 5 3600 (6-core/12-thread), 64GB RAM, 2x 512GB NVMe RAID0

## Quick Start

```bash
# SSH to server
ssh ubuntu@65.108.237.46

# Navigate to repo
cd ~/repos/ConnectedDrivingPipelineV4

# Run with default pipeline
./run-pipeline-64gb.sh

# Or specify a different pipeline
./run-pipeline-64gb.sh MyPipeline.py
```

## What the Script Does

1. **Activates venv** — Uses the pre-configured Python 3.12 environment
2. **Sets Dask config** — Uses `configs/dask/64gb-production.yml` for 64GB RAM optimization
3. **Runs pipeline** — Executes the specified (or default) pipeline file
4. **Auto-copies results** — Copies outputs to `/var/www/static/pipeline-results/<timestamp>/`
5. **Generates URL** — Results viewable at `http://65.108.237.46/pipeline-results/<timestamp>/`

## Results Auto-Copy

After the pipeline finishes, these files are automatically copied to the static server:

- `*.csv` files under 100MB (from `data/` and root)
- `*.json` files (from `data/`)
- `*.png` files (from `data/` and root)
- `pipeline.log` — Full stdout/stderr capture
- `README.txt` — Run metadata

Large files (>100MB) are excluded to keep the results folder manageable.

## Viewing Results

Results are served via nginx with a file browser UI:

- **All results:** http://65.108.237.46/pipeline-results/
- **Specific run:** http://65.108.237.46/pipeline-results/<timestamp>/

## Running in Background

For long-running pipelines, use `nohup` or `tmux`:

```bash
# With nohup
nohup ./run-pipeline-64gb.sh > /dev/null 2>&1 &

# With tmux (recommended)
tmux new -s pipeline
./run-pipeline-64gb.sh
# Ctrl+B, D to detach
# tmux attach -t pipeline to reattach
```

## Environment Details

- **Python:** 3.12 (venv at `~/repos/ConnectedDrivingPipelineV4/venv`)
- **Dask config:** `configs/dask/64gb-production.yml`
- **Data location:** `~/repos/ConnectedDrivingPipelineV4/` (includes Wyoming parquet data)
- **Static server:** nginx serving `/var/www/static/`

## Troubleshooting

**Pipeline fails immediately:**
- Check venv activation: `source venv/bin/activate && python -c "import dask; print(dask.__version__)"`

**Out of memory:**
- Check Dask config workers/memory limits in `configs/dask/64gb-production.yml`
- Consider using `conservative.yml` for lower memory usage

**Results not appearing:**
- Check nginx: `sudo systemctl status nginx`
- Check permissions: `ls -la /var/www/static/pipeline-results/`

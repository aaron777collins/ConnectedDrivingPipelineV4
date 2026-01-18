# Docker Deployment Guide

This guide covers Docker deployment for ConnectedDrivingPipelineV4, optimized for Dask distributed computing on 64GB systems.

## Quick Start

### Prerequisites

**Host System Requirements:**
- Docker Engine 20.10+ and Docker Compose 2.0+
- 64GB RAM (minimum)
- 500GB+ SSD storage
- Linux/macOS (Docker Desktop on Windows may work but not tested)

**Install Docker:**
```bash
# Ubuntu/Debian
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Verify installation
docker --version
docker compose version
```

### Build and Run

**1. Build the Docker image:**
```bash
docker compose build
```

**2. Run validation test:**
```bash
docker compose run --rm pipeline python validate_dask_setup.py
```

Expected output:
```
✅ All Dask dependencies installed correctly
✅ 64GB RAM detected (sufficient for 15M rows)
✅ Dask LocalCluster initialized successfully
✅ System ready for production workloads
```

**3. Start the pipeline service:**
```bash
docker compose up -d pipeline
```

**4. Access Dask dashboard:**
Open http://localhost:8787 in your browser to monitor pipeline execution.

## Usage Examples

### Running a Pipeline from Config

```bash
# Run a specific pipeline configuration
docker compose run --rm pipeline python -c "
from MachineLearning.DaskPipelineRunner import DaskPipelineRunner

runner = DaskPipelineRunner.from_config('MClassifierPipelines/configs/example_pipeline.json')
results = runner.run()
print(f'Pipeline complete. Results: {results}')
"
```

### Interactive Shell

```bash
# Start an interactive bash shell in the container
docker compose run --rm pipeline /bin/bash

# Inside the container, run Python
python
>>> from Helpers.DaskSessionManager import DaskSessionManager
>>> manager = DaskSessionManager(n_workers=4, threads_per_worker=3, memory_limit='12GB')
>>> client = manager.get_client()
>>> print(client)
```

### Running Tests

```bash
# Run the full test suite
docker compose run --rm pipeline pytest -v

# Run specific tests
docker compose run --rm pipeline pytest Test/Cleaners/ -v

# Run with coverage
docker compose run --rm pipeline pytest --cov=. --cov-report=html
```

## Docker Compose Services

### Main Pipeline Service

The `pipeline` service runs the ConnectedDrivingPipeline with the following configuration:

**Resource Allocation:**
- CPUs: 6-12 cores (adjust based on host)
- Memory: 32-56GB (leaves 8GB for host OS)
- Dask Workers: 4 workers with 12GB each
- Threads per Worker: 3

**Ports:**
- `8787`: Dask dashboard (http://localhost:8787)
- `8786`: Dask scheduler

**Volumes:**
- `./data`: Input/output data files
- `./cache`: Parquet cache directory
- `./logs`: Pipeline execution logs
- `./configs`: Pipeline configuration files

### Jupyter Notebook Service (Optional)

For interactive analysis and experimentation:

```bash
# Start Jupyter Lab
docker compose --profile jupyter up -d jupyter

# Access Jupyter at http://localhost:8888
```

**Note:** Jupyter service is in the `jupyter` profile and won't start by default.

## Configuration

### Environment Variables

Customize Dask cluster settings via environment variables in `docker-compose.yml`:

```yaml
environment:
  - DASK_WORKERS=4                  # Number of worker processes
  - DASK_THREADS_PER_WORKER=3       # Threads per worker
  - DASK_MEMORY_LIMIT=12GB          # Memory per worker
  - DASK_DASHBOARD_PORT=8787        # Dashboard port
```

### Resource Limits

Adjust resource limits based on your host system:

```yaml
deploy:
  resources:
    limits:
      cpus: '12'        # Maximum CPUs
      memory: 56G       # Maximum memory
    reservations:
      cpus: '6'         # Reserved CPUs
      memory: 32G       # Reserved memory
```

**Recommended Configurations:**

| Host RAM | Container Memory | Workers | Memory/Worker | Total Dask Memory |
|----------|------------------|---------|---------------|-------------------|
| 64GB     | 56GB             | 4       | 12GB          | 48GB              |
| 128GB    | 120GB            | 8       | 14GB          | 112GB             |
| 256GB    | 240GB            | 16      | 14GB          | 224GB             |

## Volume Management

### Data Directory Structure

```
/data
  ├── raw/           # Raw BSM CSV files
  ├── processed/     # Cleaned and processed data
  └── results/       # Pipeline output files

/cache
  ├── cleaned/       # Cached cleaned data (Parquet)
  ├── attacks/       # Cached attack simulations
  └── features/      # Cached feature engineering

/logs
  ├── pipeline/      # Pipeline execution logs
  └── dask/          # Dask worker logs
```

### Creating Data Directories

```bash
# Create host directories before first run
mkdir -p data/{raw,processed,results}
mkdir -p cache/{cleaned,attacks,features}
mkdir -p logs/{pipeline,dask}

# Set permissions (if needed)
chmod -R 755 data cache logs
```

### Mounting External Volumes

To use external storage for data:

```yaml
volumes:
  - /mnt/external-ssd/data:/data:rw
  - /mnt/external-ssd/cache:/cache:rw
```

## Advanced Usage

### Multi-Container Deployment

For distributed Dask clusters across multiple machines, use separate containers for scheduler and workers:

**scheduler.yml:**
```yaml
version: '3.8'
services:
  scheduler:
    image: connected-driving-pipeline:latest
    command: dask-scheduler
    ports:
      - "8786:8786"
      - "8787:8787"
```

**worker.yml:**
```yaml
version: '3.8'
services:
  worker:
    image: connected-driving-pipeline:latest
    command: dask-worker scheduler:8786 --nworkers 4 --nthreads 3 --memory-limit 12GB
    depends_on:
      - scheduler
```

### Building for Production

**Optimize image size:**
```dockerfile
# Use multi-stage build
FROM python:3.11-slim as builder
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

FROM python:3.11-slim
COPY --from=builder /root/.local /root/.local
COPY . /app
```

**Tag and version:**
```bash
docker build -t connected-driving-pipeline:4.0.0 .
docker tag connected-driving-pipeline:4.0.0 connected-driving-pipeline:latest
```

## Troubleshooting

### Container Won't Start

**Check logs:**
```bash
docker compose logs pipeline
```

**Verify resource limits:**
```bash
docker stats cdp-pipeline
```

**Memory errors:**
If you see OOM (Out of Memory) errors:
1. Reduce `DASK_WORKERS` or `DASK_MEMORY_LIMIT`
2. Increase host Docker memory limit (Docker Desktop settings)
3. Enable swap in Docker daemon

### Dashboard Not Accessible

**Check port mappings:**
```bash
docker compose ps
netstat -tuln | grep 8787
```

**Firewall issues:**
```bash
# Allow port 8787
sudo ufw allow 8787/tcp
```

### Slow Performance

**Optimize Dask configuration:**
- Reduce number of workers if CPU-bound
- Increase memory per worker if memory-bound
- Enable disk spilling for large datasets

**Monitor resource usage:**
```bash
# Inside container
docker compose exec pipeline python -c "
from Helpers.DaskSessionManager import DaskSessionManager
manager = DaskSessionManager()
print(manager.get_memory_info())
"
```

## Maintenance

### Cleaning Up

**Remove stopped containers:**
```bash
docker compose down
```

**Remove all data (CAUTION: deletes volumes):**
```bash
docker compose down -v
```

**Prune unused images:**
```bash
docker image prune -a
```

### Updating the Image

```bash
# Pull latest code
git pull origin main

# Rebuild image
docker compose build --no-cache

# Restart services
docker compose up -d
```

## Security Considerations

### Running as Non-Root

The Dockerfile creates a non-root `pipeline` user (UID 1000) for security.

**If you need to run as a different user:**
```yaml
services:
  pipeline:
    user: "1001:1001"  # Match your host UID:GID
```

### Secrets Management

For production deployments with sensitive data:

```bash
# Use Docker secrets
echo "my_api_key" | docker secret create api_key -

# Reference in docker-compose.yml
secrets:
  - api_key
```

### Network Isolation

The containers run on an isolated `dask-network` bridge network. Only exposed ports are accessible from the host.

## Performance Benchmarks

### Expected Performance (64GB host, 4 workers, 12GB each)

| Operation | Dataset Size | Time | Memory Peak |
|-----------|-------------|------|-------------|
| Data Cleaning | 15M rows | ~45s | 18GB |
| Attack Simulation | 15M rows | ~2min | 24GB |
| Feature Engineering | 15M rows | ~1min | 20GB |
| ML Training | 12M train rows | ~15min | 32GB |

### Container Overhead

Docker adds minimal overhead (~2-3%) compared to native Python execution.

## Support

For issues specific to Docker deployment:
1. Check logs: `docker compose logs pipeline`
2. Verify Dask setup: `docker compose run --rm pipeline python validate_dask_setup.py`
3. Open an issue on GitHub with logs and configuration

For general pipeline issues, see the main [README.md](README.md).

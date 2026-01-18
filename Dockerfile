# ConnectedDrivingPipelineV4 Dockerfile
# Optimized for Dask distributed computing on 64GB systems

# Use Python 3.11 slim image as base (compatible with Dask 2024.1.0+)
FROM python:3.11-slim

# Set metadata
LABEL maintainer="ConnectedDrivingPipelineV4"
LABEL description="Dask-powered pipeline for connected driving dataset processing and ML"
LABEL version="4.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies required for scientific computing
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    gfortran \
    libopenblas-dev \
    liblapack-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 -s /bin/bash pipeline && \
    mkdir -p /app /data /cache && \
    chown -R pipeline:pipeline /app /data /cache

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY --chown=pipeline:pipeline requirements.txt .

# Install Python dependencies as non-root user
USER pipeline
RUN pip install --user --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=pipeline:pipeline . .

# Expose Dask dashboard port (8787) and scheduler port (8786)
EXPOSE 8787 8786

# Set up volume mount points
VOLUME ["/data", "/cache", "/app/logs"]

# Health check for Dask scheduler
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import dask; print('Dask OK')" || exit 1

# Default command: run validation
CMD ["python", "validate_dask_setup.py"]

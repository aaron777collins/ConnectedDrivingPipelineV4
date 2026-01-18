.PHONY: help build up down restart logs clean test validate shell

# Default target
help:
	@echo "ConnectedDrivingPipelineV4 Docker Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make build       - Build Docker image"
	@echo "  make up          - Start pipeline service"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart pipeline service"
	@echo "  make logs        - Show pipeline logs"
	@echo "  make clean       - Remove containers and images"
	@echo "  make test        - Run test suite"
	@echo "  make validate    - Run Dask validation"
	@echo "  make shell       - Open interactive shell"
	@echo "  make jupyter     - Start Jupyter Lab"
	@echo "  make prod-up     - Start production cluster"
	@echo "  make prod-down   - Stop production cluster"

# Build Docker image
build:
	docker compose build

# Start services
up:
	docker compose up -d pipeline

# Stop services
down:
	docker compose down

# Restart services
restart:
	docker compose restart pipeline

# View logs
logs:
	docker compose logs -f pipeline

# Run tests
test:
	docker compose run --rm pipeline pytest -v

# Run validation
validate:
	docker compose run --rm pipeline python validate_dask_setup.py

# Open shell
shell:
	docker compose run --rm pipeline /bin/bash

# Clean up
clean:
	docker compose down -v
	docker image prune -f

# Start Jupyter Lab
jupyter:
	docker compose --profile jupyter up -d jupyter
	@echo "Jupyter Lab available at http://localhost:8888"

# Production: Start cluster
prod-up:
	docker compose -f docker-compose.prod.yml up -d

# Production: Stop cluster
prod-down:
	docker compose -f docker-compose.prod.yml down

# Production: Scale workers
prod-scale:
	docker compose -f docker-compose.prod.yml up -d --scale worker=8

# Production: View dashboard
prod-dashboard:
	@echo "Dask Dashboard: http://localhost:8787"
	@echo "Prometheus: http://localhost:9090 (if monitoring profile enabled)"
	@echo "Grafana: http://localhost:3000 (if monitoring profile enabled)"

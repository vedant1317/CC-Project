# PitLane

PitLane is a local benchmark project that compares PostgreSQL, MongoDB, and DynamoDB Local on realistic e-commerce workloads.

This repository includes:
- Database containers (Docker Compose)
- Setup scripts for schema/table creation
- Synthetic data seeding
- Benchmark runner with scenario-level metrics
- CSV and chart reporting
- FastAPI dashboard for interactive viewing

## Table of Contents

- [What Is Benchmarked](#what-is-benchmarked)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Full Run Flow](#full-run-flow)
- [Setup Files and What They Do](#setup-files-and-what-they-do)
- [Benchmark Scenarios](#benchmark-scenarios)
- [Generated Outputs](#generated-outputs)
- [Dashboard and API](#dashboard-and-api)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Reset and Re-Run](#reset-and-re-run)

## What Is Benchmarked

The benchmark runs five scenarios:

1. Point lookup
2. Complex join / lookup workflow
3. Bulk write
4. Range query
5. Aggregation

Coverage by database:

| Scenario | PostgreSQL | DynamoDB Local | MongoDB |
|---|---|---|---|
| Point lookup | Yes | Yes | Yes |
| Complex join | Yes | No | Yes |
| Bulk write | Yes | Yes | Yes |
| Range query | Yes | No | Yes |
| Aggregation | Yes | No | Yes |

## Tech Stack

- Python scripts for setup, seed, benchmark, and analysis
- PostgreSQL 15 (Docker)
- MongoDB 7 (Docker)
- DynamoDB Local (Docker)
- FastAPI + Uvicorn for dashboard backend
- Chart.js frontend dashboard
- Pandas + Matplotlib for offline reporting

## Prerequisites

- Docker Desktop (running)
- Python 3.11+
- Terminal access in project root

## Quick Start

Run this sequence for a complete first-time setup:

```bash
python -m venv .venv
# macOS/Linux: source .venv/bin/activate
# Windows PowerShell: .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
docker-compose up -d
python setup/postgres_setup.py
python setup/dynamo_setup.py
python seed.py
python run.py
python analyze.py
uvicorn dashboard.main:app --reload
```

Open: http://localhost:8000

## Full Run Flow

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Start databases

```bash
docker-compose up -d
```

Services and default ports:

- PostgreSQL: localhost:5432
- MongoDB: localhost:27017
- DynamoDB Local: localhost:8000

### 3) Create schema and table objects

```bash
python setup/postgres_setup.py
python setup/dynamo_setup.py
```

### 4) Seed synthetic data (one-time or when refreshing)

```bash
python seed.py
```

Default seeded volume:

- 50,000 users
- 5,000 products
- 100,000 orders

### 5) Run benchmark scenarios

```bash
python run.py
```

Outputs latency metrics into [results/results.csv](results/results.csv).

### 6) Generate static chart report

```bash
python analyze.py
```

Creates [results/results.png](results/results.png) (after a successful benchmark run).

### 7) Launch web dashboard

```bash
uvicorn dashboard.main:app --reload
```

Open http://localhost:8000.

## Setup Files and What They Do

| File | Purpose |
|---|---|
| [docker-compose.yml](docker-compose.yml) | Starts PostgreSQL, MongoDB, and DynamoDB Local containers with mapped ports and health checks. |
| [requirements.txt](requirements.txt) | Python dependencies for setup, seeding, benchmark, analysis, and dashboard. |
| [setup/postgres_setup.py](setup/postgres_setup.py) | Creates PostgreSQL tables and indexes for users, products, orders, and order_items. |
| [setup/dynamo_setup.py](setup/dynamo_setup.py) | Creates DynamoDB Local table EcommerceDB using PK/SK single-table design. |
| [seed.py](seed.py) | Generates synthetic data once and seeds PostgreSQL, MongoDB, and DynamoDB Local. |

Related orchestration files:

| File | Purpose |
|---|---|
| [run.py](run.py) | Executes benchmark scenarios and writes [results/results.csv](results/results.csv). |
| [benchmark.py](benchmark.py) | Scenario implementations and latency measurement helper. |
| [analyze.py](analyze.py) | Reads CSV results, prints summary, and saves chart image. |
| [dashboard/main.py](dashboard/main.py) | FastAPI server for dashboard and JSON endpoints. |
| [dashboard/index.html](dashboard/index.html) | Single-page dashboard UI and charts. |
| [RUN_PROJECT.md](RUN_PROJECT.md) | Step-by-step run instructions focused on local execution. |

## Benchmark Scenarios

The scenario logic is implemented in [benchmark.py](benchmark.py):

- Scenario 1: point lookup by user ID
- Scenario 2: complex join/lookup for recent user orders
- Scenario 3: bulk write batch inserts
- Scenario 4: date-range query filtered by order status
- Scenario 5: category-level revenue aggregation

`run.py` collects metrics (`avg`, `p50`, `p95`, `p99`, `min`, `max`) and saves them in CSV.

## Generated Outputs

After running benchmark and analysis:

- [results/results.csv](results/results.csv): raw metrics per scenario and database
- [results/results.png](results/results.png): grouped chart image for quick comparison

## Dashboard and API

Run:

```bash
uvicorn dashboard.main:app --reload
```

Available endpoints:

- `GET /` serves the HTML dashboard
- `GET /api/results` returns all rows from CSV
- `GET /api/summary` returns per-db best metrics and per-scenario winner

## Project Structure

```text
PitLane/
|- analyze.py
|- benchmark.py
|- docker-compose.yml
|- README.md
|- requirements.txt
|- run.py
|- RUN_PROJECT.md
|- seed.py
|- dashboard/
|  |- index.html
|  \- main.py
|- results/
|  \- results.csv
|- scratch/
|  |- check_faker.py
|  \- test_mongo_aggr.py
\- setup/
   |- dynamo_setup.py
   \- postgres_setup.py
```

Notes:

- [results/results.csv](results/results.csv) is generated by [run.py](run.py).
- [results/results.png](results/results.png) is generated by [analyze.py](analyze.py).
- [scratch](scratch) contains local experiments and validation helpers.

## Troubleshooting

### DynamoDB table not found

Error example: `ResourceNotFoundException` on DynamoDB `GetItem`.

Fix:

```bash
python setup/dynamo_setup.py
python seed.py
```

### PostgreSQL connection failed

- Verify container is running: `docker ps`
- Ensure port `5432` is available
- Re-run: `docker-compose up -d`

### MongoDB connection failed

- Verify container is running: `docker ps`
- Ensure port `27017` is available

### Dashboard says results.csv not found

Run:

```bash
python run.py
```

## Reset and Re-Run

If you want a clean local reset:

```bash
docker-compose down -v
docker-compose up -d
python setup/postgres_setup.py
python setup/dynamo_setup.py
python seed.py
python run.py
python analyze.py
```

For fast reruns (without reseeding), use:

```bash
python run.py
python analyze.py
```



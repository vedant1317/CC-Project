# PitLane: Automated DB Selection & Regression Testing Tool

PitLane is an automated database evaluation suite for architecture decisions.

Instead of arguing SQL vs NoSQL in abstract, teams define expected read/write patterns in a JSON workload file and PitLane empirically measures:

- Latency under concurrent load
- Throughput and error rate
- Docker CPU and memory usage
- Cost-aware and latency-aware winners per scenario

PitLane then emits recommendation-friendly output that can also be used as a regression gate in CI/CD.

## Why Teams Use It

- Pick the right database engine for a new feature using real workload evidence.
- Prevent invisible performance regressions by comparing against a baseline run.
- Evaluate tradeoffs between speed and operational footprint, not just raw latency.

## Key Capabilities

- Workload-driven benchmarking from custom JSON files
- Built-in and custom mock query execution
- Concurrent execution to simulate real user load
- Docker stats telemetry capture (CPU/memory) during each benchmark
- Decision scoring for fastest, cheapest, and balanced recommendation
- Optional regression checks against a previous CSV baseline

## Supported Databases

- PostgreSQL (Docker)
- MongoDB (Docker)
- DynamoDB Local (Docker)

## Quick Start

```bash
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
docker-compose up -d
python setup/postgres_setup.py
python setup/dynamo_setup.py
python seed.py

# Run default workload
python run.py --workload workloads/default_workload.json

# Fast smoke validation
python run.py --workload workloads/default_workload.json --smoke

# Generate analysis image
python analyze.py

# Optional dashboard
uvicorn dashboard.main:app --reload
```

Open dashboard at http://localhost:8000.

## Workload JSON (Custom Use Cases)

Use [workloads/custom_template.json](workloads/custom_template.json) as a starting point.

Each workload contains:

- `name`, `version`, `description`
- `schema` metadata (your entities/index assumptions)
- `defaults` for runs/concurrency/warmup
- `scenarios` with either:
   - `type: "builtin"` for PitLane built-in scenarios
   - `type: "custom"` with `mock_queries` per database

### Built-in workload run

```bash
python run.py --workload workloads/default_workload.json
```

### Custom workload run

```bash
python run.py --workload workloads/custom_template.json
```

## Smoke Validation

Use smoke mode to run a lightweight pass through the full benchmark pipeline:

```bash
python run.py --workload workloads/default_workload.json --smoke
```

You can tune smoke limits when needed:

```bash
python run.py \
   --workload workloads/default_workload.json \
   --smoke \
   --smoke-max-runs 12 \
   --smoke-max-concurrency 6 \
   --smoke-max-warmup-runs 1
```

## Regression Testing Mode

Use the previous run output as baseline:

```bash
python run.py \
   --workload workloads/default_workload.json \
   --baseline results/baseline.csv \
   --regression-p95-threshold 15 \
   --regression-cost-threshold 20 \
   --fail-on-regression
```

If thresholds are exceeded, PitLane writes `results/regressions.csv` and exits non-zero when `--fail-on-regression` is enabled.

## Metrics Produced

Per scenario and database:

- Latency: `avg`, `p50`, `p95`, `p99`, `min`, `max`
- Load profile: `runs`, `concurrency`, `throughput_ops_s`, `error_rate_pct`
- Resource profile: `cpu_avg_pct`, `cpu_p95_pct`, `cpu_max_pct`, `mem_avg_mb`, `mem_p95_mb`, `mem_max_mb`
- Decision fields: `cost_index`, `decision_score`, `latency_winner`, `cost_winner`, `recommended`

## Output Files

- [results/results.csv](results/results.csv): full benchmark and decision data
- [results/results.png](results/results.png): chart summary from [analyze.py](analyze.py)
- `results/regressions.csv` (optional): threshold violations vs baseline

## API Endpoints

Run dashboard API:

```bash
uvicorn dashboard.main:app --reload
```

Endpoints:

- `GET /` serves [dashboard/index.html](dashboard/index.html)
- `GET /api/results` returns all benchmark rows
- `GET /api/summary` returns per-db best metrics and per-scenario winners

## Core Files

- [run.py](run.py): concurrent benchmark runner and regression checks
- [benchmark.py](benchmark.py): DB operations, thread-safe execution, custom query handlers
- [workload.py](workload.py): workload validation and token resolution
- [telemetry.py](telemetry.py): Docker CPU/memory sampling
- [workloads/default_workload.json](workloads/default_workload.json): built-in default workload
- [workloads/custom_template.json](workloads/custom_template.json): custom schema/query template

## Troubleshooting

### No user IDs loaded

Run:

```bash
python seed.py
```

### Docker stats are missing in results

- Ensure Docker is running.
- Ensure containers from [docker-compose.yml](docker-compose.yml) are up.
- Confirm container names match: `benchmark_postgres`, `benchmark_mongo`, `benchmark_dynamo`.

### Dashboard shows no results

Run:

```bash
python run.py --workload workloads/default_workload.json
```

## Reset and Re-Run

```bash
docker-compose down -v
docker-compose up -d
python setup/postgres_setup.py
python setup/dynamo_setup.py
python seed.py
python run.py --workload workloads/default_workload.json
python analyze.py
```



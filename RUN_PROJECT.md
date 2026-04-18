# Run Project Guide

This guide runs the full benchmark stack: PostgreSQL, MongoDB, DynamoDB Local, benchmark execution, and dashboard.

## Prerequisites

- Docker Desktop is installed and running
- Python 3.11+ is installed
- You are in the project root directory

## 1) Create and activate virtual environment (recommended)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If you already have `.venv`, only run the activate command.

## 2) Install dependencies

```powershell
pip install -r requirements.txt
```

## 3) Start databases with Docker

```powershell
docker-compose up -d
```

If your Docker instance is already started, you can keep this as-is or verify containers with:

```powershell
docker ps
```

## 4) Create database schemas/tables

```powershell
python setup/postgres_setup.py
python setup/dynamo_setup.py
```

## 5) Seed data (run once)

```powershell
python seed.py
```

Notes:
- This can take 5 to 15 minutes.
- It seeds around 50,000 users, 5,000 products, and 100,000 orders.

## 6) Run benchmark scenarios

```powershell
python run.py
```

This writes benchmark output to:
- `results/results.csv`

## 7) Generate chart image

```powershell
python analyze.py
```

Expected output:
- `results/results.png`

## 8) Launch dashboard

```powershell
uvicorn dashboard.main:app --reload
```

Open:
- http://localhost:8000

## Quick rerun path (after initial setup)

If you already seeded once and Docker containers are running:

```powershell
python run.py
python analyze.py
uvicorn dashboard.main:app --reload
```

## Troubleshooting

- Error: `ResourceNotFoundException` for DynamoDB table
  - Run `python setup/dynamo_setup.py` again.
- Empty/partial benchmark data
  - Re-run `python seed.py`, then `python run.py`.
- Container connection issues
  - Ensure `docker-compose up -d` is running and ports are free.

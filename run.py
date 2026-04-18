"""
run.py
Orchestrates all benchmark scenarios and saves results to results/results.csv.

Run: python run.py
Expected time: 3–5 minutes.
"""

import csv
import os
import random
import time
from datetime import datetime

from benchmark import (
    pg, measure,
    # Scenario 1
    pg_point_lookup, dynamo_point_lookup, mongo_point_lookup,
    # Scenario 2
    pg_complex_join, mongo_complex_join,
    # Scenario 3
    pg_bulk_write, dynamo_bulk_write, mongo_bulk_write,
    # Scenario 4
    pg_range_query, mongo_range_query,
    # Scenario 5
    pg_aggregation, mongo_aggregation,
)

random.seed(99)
os.makedirs("results", exist_ok=True)

# ── Load sample user IDs from PostgreSQL ─────────────────────────────

print("Loading sample user IDs from PostgreSQL...")
with pg.cursor() as cur:
    cur.execute("SELECT id FROM users ORDER BY random() LIMIT 500")
    user_ids = [str(r[0]) for r in cur.fetchall()]
print(f"  Loaded {len(user_ids)} user IDs\n")

# ── Benchmark inputs ──────────────────────────────────────────────────

uid = random.choice(user_ids)

BATCH_SIZE    = 100
sample_orders = [
    {
        "user_id": random.choice(user_ids),
        "total":   round(random.uniform(10, 500), 2),
        "status":  "pending",
    }
    for _ in range(BATCH_SIZE)
]

start_date = datetime(2025, 1, 1)
end_date   = datetime(2025, 6, 1)

# ── Result collector ──────────────────────────────────────────────────

results = []

def record(scenario, db, metrics):
    row = {"scenario": scenario, "db": db, **metrics}
    results.append(row)
    print(
        f"  {db:<12} avg={metrics['avg']:>8.3f}ms  "
        f"p50={metrics['p50']:>8.3f}ms  "
        f"p95={metrics['p95']:>8.3f}ms  "
        f"p99={metrics['p99']:>8.3f}ms"
    )

# ── Run scenarios ─────────────────────────────────────────────────────

total_start = time.time()

print("=" * 60)
print("Scenario 1 — Point Lookup  (200 runs each)")
print("=" * 60)
record("point_lookup", "postgres", measure(pg_point_lookup,    uid))
record("point_lookup", "dynamo",   measure(dynamo_point_lookup, uid))
record("point_lookup", "mongodb",  measure(mongo_point_lookup,  uid))

print()
print("=" * 60)
print("Scenario 2 — Complex JOIN  (200 runs each)")
print("  Note: DynamoDB excluded — not designed for joins")
print("=" * 60)
record("complex_join", "postgres", measure(pg_complex_join,    uid))
record("complex_join", "mongodb",  measure(mongo_complex_join, uid))

print()
print("=" * 60)
print(f"Scenario 3 — Bulk Write ({BATCH_SIZE} records/batch, 20 runs each)")
print("=" * 60)
record("bulk_write", "postgres", measure(pg_bulk_write,    sample_orders, runs=20))
record("bulk_write", "dynamo",   measure(dynamo_bulk_write, sample_orders, runs=20))
record("bulk_write", "mongodb",  measure(mongo_bulk_write,  sample_orders, runs=20))

print()
print("=" * 60)
print("Scenario 4 — Range Query  (200 runs each)")
print("  Note: DynamoDB excluded — requires GSI or full scan")
print("=" * 60)
record("range_query", "postgres", measure(pg_range_query,    start_date, end_date))
record("range_query", "mongodb",  measure(mongo_range_query, start_date, end_date))

print()
print("=" * 60)
print("Scenario 5 — Aggregation  (200 runs each)")
print("  Note: DynamoDB excluded — no native server-side aggregation")
print("=" * 60)
record("aggregation", "postgres", measure(pg_aggregation, runs=5))
record("aggregation", "mongodb",  measure(mongo_aggregation, runs=5))

# ── Save CSV ──────────────────────────────────────────────────────────

csv_path = os.path.join("results", "results.csv")
fieldnames = ["scenario", "db", "avg", "p50", "p95", "p99", "min", "max"]

with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print()
print("=" * 60)
print(f"[OK] Results saved to {csv_path}")
print(f"[OK] Total benchmark time: {time.time()-total_start:.1f}s")
print("  Next: python analyze.py  OR  uvicorn dashboard.main:app --reload")
print("=" * 60)

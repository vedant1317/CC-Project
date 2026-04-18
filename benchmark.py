"""
benchmark.py
Defines all benchmark scenarios as timed functions.
Each function returns elapsed time in SECONDS for one operation.
Import this module from run.py — do not run directly.

Scenarios:
  1. Point Lookup       — PG, DynamoDB, MongoDB
  2. Complex JOIN       — PG, MongoDB (DynamoDB N/A)
  3. Bulk Write         — PG, DynamoDB, MongoDB
  4. Range Query        — PG, MongoDB (DynamoDB N/A)
  5. Aggregation        — PG, MongoDB (DynamoDB N/A)
"""

import time
import uuid
import statistics
import psycopg2
import boto3
import pymongo

# ── Connections ──────────────────────────────────────────────────────

pg = psycopg2.connect(
    dbname="ecommerce", user="user",
    password="password", host="localhost", port=5432
)

dynamo = boto3.resource(
    "dynamodb", region_name="us-east-1",
    endpoint_url="http://localhost:8000",
    aws_access_key_id="fake", aws_secret_access_key="fake"
)
table = dynamo.Table("EcommerceDB")

mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
mongo = mongo_client["ecommerce"]


# ── Measurement helper ───────────────────────────────────────────────

def measure(fn, *args, runs=15):
    """
    Runs fn(*args) `runs` times and returns latency statistics in milliseconds.
    """
    times = []
    for _ in range(runs):
        elapsed = fn(*args)
        times.append(elapsed * 1000)  # convert to ms

    sorted_times = sorted(times)
    n = len(sorted_times)

    return {
        "avg": round(statistics.mean(sorted_times), 3),
        "p50": round(sorted_times[int(n * 0.50)], 3),
        "p95": round(sorted_times[int(n * 0.95)], 3),
        "p99": round(sorted_times[int(n * 0.99)], 3),
        "min": round(sorted_times[0], 3),
        "max": round(sorted_times[-1], 3),
    }


# ── Scenario 1: Point Lookup ─────────────────────────────────────────
# Fetch a single user record by primary key.

def pg_point_lookup(user_id):
    t = time.perf_counter()
    with pg.cursor() as cur:
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        cur.fetchone()
    return time.perf_counter() - t


def dynamo_point_lookup(user_id):
    t = time.perf_counter()
    table.get_item(Key={"PK": f"USER#{user_id}", "SK": "PROFILE"})
    return time.perf_counter() - t


def mongo_point_lookup(user_id):
    t = time.perf_counter()
    mongo.users.find_one({"id": user_id})
    return time.perf_counter() - t


# ── Scenario 2: Complex JOIN ──────────────────────────────────────────
# Fetch a user's last 10 orders with product details — a 4-table join in SQL.
# MongoDB uses an aggregation pipeline with $lookup (server-side join equivalent).
# DynamoDB cannot do this efficiently — excluded from this scenario.

def pg_complex_join(user_id):
    t = time.perf_counter()
    with pg.cursor() as cur:
        cur.execute("""
            SELECT u.name, o.id, o.total, o.status,
                   p.name AS product_name, oi.quantity
            FROM users u
            JOIN orders o       ON u.id = o.user_id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p     ON oi.product_id = p.id
            WHERE u.id = %s
            ORDER BY o.created_at DESC
            LIMIT 10
        """, (user_id,))
        cur.fetchall()
    return time.perf_counter() - t


def mongo_complex_join(user_id):
    t = time.perf_counter()
    list(mongo.orders.aggregate([
        {"$match": {"user_id": user_id}},
        {"$sort":  {"created_at": -1}},
        {"$limit": 10},
        {"$lookup": {
            "from": "users", "localField": "user_id",
            "foreignField": "id", "as": "user"
        }},
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "products", "localField": "items.product_id",
            "foreignField": "id", "as": "items.product"
        }},
    ]))
    return time.perf_counter() - t


# ── Scenario 3: Bulk Write (batch of 100 records) ────────────────────
# Inserts 100 order records in a single batch operation.
# Each DB uses its native batch mechanism.

def pg_bulk_write(batch):
    t = time.perf_counter()
    with pg.cursor() as cur:
        cur.executemany(
            "INSERT INTO orders(id, user_id, total, status) VALUES(%s,%s,%s,%s)",
            [(str(uuid.uuid4()), o["user_id"], o["total"], o["status"]) for o in batch]
        )
    pg.commit()
    return time.perf_counter() - t


def dynamo_bulk_write(batch):
    t = time.perf_counter()
    with table.batch_writer() as bw:
        for o in batch:
            bw.put_item(Item={
                "PK":     f"USER#{o['user_id']}",
                "SK":     f"ORDER#{uuid.uuid4()}",
                "total":  str(o["total"]),
                "status": o["status"],
            })
    return time.perf_counter() - t


def mongo_bulk_write(batch):
    t = time.perf_counter()
    docs = [{"user_id": o["user_id"], "total": o["total"],
              "status": o["status"]} for o in batch]
    mongo.orders.insert_many(docs, ordered=False)
    return time.perf_counter() - t


# ── Scenario 4: Range Query ───────────────────────────────────────────
# Fetch all 'shipped' orders within a date range, sorted by date.
# DynamoDB excluded (requires full table scan or GSI not set up here).

def pg_range_query(start, end):
    t = time.perf_counter()
    with pg.cursor() as cur:
        cur.execute("""
            SELECT * FROM orders
            WHERE created_at BETWEEN %s AND %s AND status = 'shipped'
            ORDER BY created_at DESC
        """, (start, end))
        cur.fetchall()
    return time.perf_counter() - t


def mongo_range_query(start, end):
    t = time.perf_counter()
    list(mongo.orders.find(
        {"created_at": {"$gte": start, "$lte": end}, "status": "shipped"}
    ).sort("created_at", -1))
    return time.perf_counter() - t


# ── Scenario 5: Aggregation ───────────────────────────────────────────
# Compute revenue and order count grouped by product category.
# DynamoDB excluded (no native server-side aggregation).

def pg_aggregation():
    t = time.perf_counter()
    with pg.cursor() as cur:
        cur.execute("""
            SELECT p.category,
                   COUNT(oi.id)                           AS order_count,
                   SUM(oi.quantity * oi.unit_price)       AS revenue
            FROM order_items oi
            JOIN products p ON oi.product_id = p.id
            GROUP BY p.category
            ORDER BY revenue DESC
        """)
        cur.fetchall()
    return time.perf_counter() - t


def mongo_aggregation():
    t = time.perf_counter()
    list(mongo.orders.aggregate([
        {"$unwind": "$items"},
        {"$lookup": {
            "from": "products", "localField": "items.product_id",
            "foreignField": "id", "as": "product"
        }},
        {"$unwind": "$product"},
        {"$group": {
            "_id":     "$product.category",
            "revenue": {"$sum": {"$multiply": ["$items.quantity", "$items.unit_price"]}},
            "count":   {"$sum": 1}
        }},
        {"$sort": {"revenue": -1}}
    ]))
    return time.perf_counter() - t

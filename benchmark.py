"""
benchmark.py
Thread-safe benchmark operations used by run.py.
"""

from __future__ import annotations

import os
import statistics
import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, Sequence

import boto3
import pymongo
from boto3.dynamodb.conditions import Key
from psycopg2.pool import ThreadedConnectionPool

DEFAULT_STATUSES = ("pending", "processing", "shipped", "delivered")
DB_CONTAINER_MAP = {
    "postgres": "benchmark_postgres",
    "mongodb": "benchmark_mongo",
    "dynamo": "benchmark_dynamo",
}

SCENARIO_COMPATIBILITY = {
    "point_lookup": {"postgres", "dynamo", "mongodb"},
    "complex_join": {"postgres", "mongodb"},
    "bulk_write": {"postgres", "dynamo", "mongodb"},
    "range_query": {"postgres", "mongodb"},
    "aggregation": {"postgres", "mongodb"},
}

MONGO_AGG_MAX_MS = int(os.getenv("PITLANE_MONGO_AGG_MAX_MS", "20000"))


PG_POOL = ThreadedConnectionPool(
    minconn=1,
    maxconn=int(os.getenv("PITLANE_PG_POOL_MAX", "80")),
    dbname="ecommerce",
    user="user",
    password="password",
    host="localhost",
    port=5432,
)

dynamo = boto3.resource(
    "dynamodb",
    region_name="us-east-1",
    endpoint_url="http://localhost:8000",
    aws_access_key_id="fake",
    aws_secret_access_key="fake",
)
table = dynamo.Table("EcommerceDB")

mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
mongo = mongo_client["ecommerce"]


def summarize_latencies_ms(latencies_ms: Sequence[float]) -> Dict[str, float]:
    """Return common percentile metrics from a latency sample in milliseconds."""
    if not latencies_ms:
        return {
            "avg": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "min": 0.0,
            "max": 0.0,
        }

    ordered = sorted(latencies_ms)
    return {
        "avg": round(statistics.mean(ordered), 3),
        "p50": round(_percentile(ordered, 0.50), 3),
        "p95": round(_percentile(ordered, 0.95), 3),
        "p99": round(_percentile(ordered, 0.99), 3),
        "min": round(ordered[0], 3),
        "max": round(ordered[-1], 3),
    }


def get_sample_user_ids(limit: int = 500) -> list[str]:
    """Get representative user IDs from PostgreSQL for dynamic benchmark inputs."""
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id::text FROM users ORDER BY random() LIMIT %s", (limit,))
            rows = cur.fetchall()
        conn.rollback()
        return [row[0] for row in rows]
    finally:
        PG_POOL.putconn(conn)


def pg_point_lookup(user_id: str) -> float:
    t0 = time.perf_counter()
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            cur.fetchone()
        conn.rollback()
        return time.perf_counter() - t0
    finally:
        PG_POOL.putconn(conn)


def dynamo_point_lookup(user_id: str) -> float:
    t0 = time.perf_counter()
    table.get_item(Key={"PK": f"USER#{user_id}", "SK": "PROFILE"})
    return time.perf_counter() - t0


def mongo_point_lookup(user_id: str) -> float:
    t0 = time.perf_counter()
    mongo.users.find_one({"id": user_id})
    return time.perf_counter() - t0


def pg_complex_join(user_id: str) -> float:
    t0 = time.perf_counter()
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.name, o.id, o.total, o.status,
                       p.name AS product_name, oi.quantity
                FROM users u
                JOIN orders o       ON u.id = o.user_id
                JOIN order_items oi ON o.id = oi.order_id
                JOIN products p     ON oi.product_id = p.id
                WHERE u.id = %s
                ORDER BY o.created_at DESC
                LIMIT 10
                """,
                (user_id,),
            )
            cur.fetchall()
        conn.rollback()
        return time.perf_counter() - t0
    finally:
        PG_POOL.putconn(conn)


def mongo_complex_join(user_id: str) -> float:
    t0 = time.perf_counter()
    list(
        mongo.orders.aggregate(
            [
                {"$match": {"user_id": user_id}},
                {"$sort": {"created_at": -1}},
                {"$limit": 10},
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "user_id",
                        "foreignField": "id",
                        "as": "user",
                    }
                },
                {"$unwind": "$items"},
                {
                    "$lookup": {
                        "from": "products",
                        "localField": "items.product_id",
                        "foreignField": "id",
                        "as": "items.product",
                    }
                },
            ]
        )
    )
    return time.perf_counter() - t0


def pg_bulk_write(batch: Iterable[Dict[str, Any]]) -> float:
    t0 = time.perf_counter()
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO orders(id, user_id, total, status) VALUES(%s,%s,%s,%s)",
                [
                    (str(uuid.uuid4()), item["user_id"], item["total"], item["status"])
                    for item in batch
                ],
            )
        conn.commit()
        return time.perf_counter() - t0
    finally:
        PG_POOL.putconn(conn)


def dynamo_bulk_write(batch: Iterable[Dict[str, Any]]) -> float:
    t0 = time.perf_counter()
    with table.batch_writer() as writer:
        for item in batch:
            writer.put_item(
                Item={
                    "PK": f"USER#{item['user_id']}",
                    "SK": f"ORDER#{uuid.uuid4()}",
                    "total": str(item["total"]),
                    "status": item["status"],
                }
            )
    return time.perf_counter() - t0


def mongo_bulk_write(batch: Iterable[Dict[str, Any]]) -> float:
    t0 = time.perf_counter()
    docs = [
        {
            "user_id": item["user_id"],
            "total": item["total"],
            "status": item["status"],
        }
        for item in batch
    ]
    mongo.orders.insert_many(docs, ordered=False)
    return time.perf_counter() - t0


def pg_range_query(start: datetime, end: datetime) -> float:
    t0 = time.perf_counter()
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM orders
                WHERE created_at BETWEEN %s AND %s AND status = 'shipped'
                ORDER BY created_at DESC
                """,
                (start, end),
            )
            cur.fetchall()
        conn.rollback()
        return time.perf_counter() - t0
    finally:
        PG_POOL.putconn(conn)


def mongo_range_query(start: datetime, end: datetime) -> float:
    t0 = time.perf_counter()
    list(
        mongo.orders.find(
            {"created_at": {"$gte": start, "$lte": end}, "status": "shipped"}
        ).sort("created_at", -1)
    )
    return time.perf_counter() - t0


def pg_aggregation() -> float:
    t0 = time.perf_counter()
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.category,
                       COUNT(oi.id)                     AS order_count,
                       SUM(oi.quantity * oi.unit_price) AS revenue
                FROM order_items oi
                JOIN products p ON oi.product_id = p.id
                GROUP BY p.category
                ORDER BY revenue DESC
                """
            )
            cur.fetchall()
        conn.rollback()
        return time.perf_counter() - t0
    finally:
        PG_POOL.putconn(conn)


def mongo_aggregation() -> float:
    t0 = time.perf_counter()
    list(
        mongo.orders.aggregate(
            [
                {"$unwind": "$items"},
                {
                    "$lookup": {
                        "from": "products",
                        "localField": "items.product_id",
                        "foreignField": "id",
                        "as": "product",
                    }
                },
                {"$unwind": "$product"},
                {
                    "$group": {
                        "_id": "$product.category",
                        "revenue": {
                            "$sum": {
                                "$multiply": ["$items.quantity", "$items.unit_price"]
                            }
                        },
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"revenue": -1}},
            ],
            allowDiskUse=True,
            maxTimeMS=MONGO_AGG_MAX_MS,
        )
    )
    return time.perf_counter() - t0


def execute_custom_query(db: str, query_spec: Dict[str, Any]) -> float:
    """Execute one custom query payload for the selected database."""
    normalized_db = db.strip().lower()

    if normalized_db == "postgres":
        return _execute_postgres_custom(query_spec)

    if normalized_db == "mongodb":
        return _execute_mongo_custom(query_spec)

    if normalized_db == "dynamo":
        return _execute_dynamo_custom(query_spec)

    raise ValueError(f"Unsupported database '{db}' for custom query execution.")


def _execute_postgres_custom(query_spec: Dict[str, Any]) -> float:
    mode = str(query_spec.get("mode", "fetchall")).lower()
    query = query_spec.get("query")
    if not query:
        raise ValueError("PostgreSQL custom query missing required field 'query'.")

    params = query_spec.get("params")
    param_list = query_spec.get("param_list")
    commit = bool(query_spec.get("commit", mode in {"execute", "executemany"}))

    t0 = time.perf_counter()
    conn = PG_POOL.getconn()
    try:
        with conn.cursor() as cur:
            if mode == "executemany":
                if not isinstance(param_list, list):
                    raise ValueError("PostgreSQL mode 'executemany' requires 'param_list' as a list.")
                cur.executemany(query, param_list)
            else:
                if params is None:
                    cur.execute(query)
                else:
                    cur.execute(query, params)

                if mode == "fetchone":
                    cur.fetchone()
                elif mode == "fetchall":
                    cur.fetchall()
                elif mode != "execute":
                    raise ValueError(f"Unsupported PostgreSQL custom mode '{mode}'.")

        if commit:
            conn.commit()
        else:
            conn.rollback()
        return time.perf_counter() - t0
    finally:
        PG_POOL.putconn(conn)


def _execute_mongo_custom(query_spec: Dict[str, Any]) -> float:
    collection_name = query_spec.get("collection")
    operation = str(query_spec.get("operation", "find_one")).lower()
    if not collection_name:
        raise ValueError("MongoDB custom query missing required field 'collection'.")

    collection = mongo[collection_name]
    t0 = time.perf_counter()

    if operation == "find_one":
        collection.find_one(query_spec.get("filter", {}), query_spec.get("projection"))
    elif operation == "find":
        cursor = collection.find(query_spec.get("filter", {}), query_spec.get("projection"))
        sort_spec = query_spec.get("sort")
        if isinstance(sort_spec, list):
            cursor = cursor.sort([(item[0], int(item[1])) for item in sort_spec])
        if "limit" in query_spec:
            cursor = cursor.limit(int(query_spec["limit"]))
        list(cursor)
    elif operation == "aggregate":
        list(collection.aggregate(query_spec.get("pipeline", [])))
    elif operation == "insert_one":
        collection.insert_one(query_spec.get("document", {}))
    elif operation == "insert_many":
        documents = query_spec.get("documents", [])
        if not isinstance(documents, list):
            raise ValueError("MongoDB operation 'insert_many' requires 'documents' as a list.")
        collection.insert_many(documents, ordered=bool(query_spec.get("ordered", False)))
    else:
        raise ValueError(f"Unsupported MongoDB custom operation '{operation}'.")

    return time.perf_counter() - t0


def _execute_dynamo_custom(query_spec: Dict[str, Any]) -> float:
    operation = str(query_spec.get("operation", "get_item")).lower()
    t0 = time.perf_counter()

    if operation == "get_item":
        key = _to_dynamo_values(query_spec.get("key", {}))
        table.get_item(Key=key)
    elif operation == "put_item":
        item = _to_dynamo_values(query_spec.get("item", {}))
        table.put_item(Item=item)
    elif operation == "query":
        condition = query_spec.get("key_condition", {})
        if not isinstance(condition, dict):
            raise ValueError("DynamoDB operation 'query' requires 'key_condition' object.")

        pk_name = str(condition.get("pk_name", "PK"))
        pk_value = _to_dynamo_values(condition.get("pk_value"))
        key_expression = Key(pk_name).eq(pk_value)

        sk_name = str(condition.get("sk_name", "SK"))
        if "sk_equals" in condition:
            key_expression = key_expression & Key(sk_name).eq(_to_dynamo_values(condition["sk_equals"]))
        elif "sk_begins_with" in condition:
            key_expression = key_expression & Key(sk_name).begins_with(
                _to_dynamo_values(condition["sk_begins_with"])
            )

        kwargs: Dict[str, Any] = {"KeyConditionExpression": key_expression}
        if "limit" in query_spec:
            kwargs["Limit"] = int(query_spec["limit"])
        table.query(**kwargs)
    else:
        raise ValueError(f"Unsupported DynamoDB custom operation '{operation}'.")

    return time.perf_counter() - t0


def _to_dynamo_values(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))

    if isinstance(value, dict):
        return {key: _to_dynamo_values(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_to_dynamo_values(item) for item in value]

    return value


def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
    if not sorted_values:
        return 0.0
    index = int((len(sorted_values) - 1) * percentile)
    return sorted_values[index]

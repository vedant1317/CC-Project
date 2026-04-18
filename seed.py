"""
seed.py
Generates synthetic e-commerce data and seeds all three databases.
Data is generated once in-memory and reused across PostgreSQL, MongoDB, and DynamoDB
to ensure a fair, identical comparison baseline.

Run: python seed.py
Expected time: 5–15 minutes depending on hardware.
"""

import uuid
import random
import sys
import time
import psycopg2
import boto3
import pymongo
from faker import Faker
from datetime import datetime
from decimal import Decimal

fake = Faker()
Faker.seed(42)
random.seed(42)

NUM_USERS    = 50_000
NUM_PRODUCTS = 5_000
NUM_ORDERS   = 100_000
CATEGORIES   = ["electronics", "clothing", "food", "books", "sports"]
STATUSES     = ["pending", "processing", "shipped", "delivered"]

# ── 1. Generate data ────────────────────────────────────────────────

def generate_data():
    print("Generating data...")
    t0 = time.time()

    users = []
    for _ in range(NUM_USERS):
        users.append({
            "id": str(uuid.uuid4()),
            "name": fake.name(),
            "email": fake.unique.email(),
            "created_at": fake.date_time_this_year(),
        })

    products = []
    for _ in range(NUM_PRODUCTS):
        products.append({
            "id": str(uuid.uuid4()),
            "name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(5, 2000), 2),
            "stock": random.randint(0, 500),
        })

    product_ids = [p["id"] for p in products]
    user_ids    = [u["id"] for u in users]

    orders = []
    for _ in range(NUM_ORDERS):
        n_items = random.randint(1, 6)
        items = []
        for _ in range(n_items):
            items.append({
                "product_id": random.choice(product_ids),
                "quantity":   random.randint(1, 5),
                "unit_price": round(random.uniform(5, 500), 2),
            })
        total = round(sum(i["quantity"] * i["unit_price"] for i in items), 2)
        orders.append({
            "id":         str(uuid.uuid4()),
            "user_id":    random.choice(user_ids),
            "items":      items,
            "total":      total,
            "status":     random.choice(STATUSES),
            "created_at": fake.date_time_this_year(),
        })

    elapsed = time.time() - t0
    print(f"[OK] Data generated in {elapsed:.1f}s")
    print(f"  {NUM_USERS:,} users | {NUM_PRODUCTS:,} products | {NUM_ORDERS:,} orders")
    return users, products, orders


# ── 2. Seed PostgreSQL ───────────────────────────────────────────────

def seed_postgres(users, products, orders):
    print("\nSeeding PostgreSQL...")
    t0 = time.time()

    try:
        conn = psycopg2.connect(
            dbname="ecommerce", user="user",
            password="password", host="localhost", port=5432
        )
    except Exception as e:
        print(f"[ERR] PostgreSQL connection failed: {e}")
        sys.exit(1)

    BATCH = 1000
    with conn.cursor() as cur:
        # Clear any existing data just in case
        print("  Truncating tables...")
        cur.execute("TRUNCATE TABLE order_items, orders, products, users CASCADE;")
        conn.commit()

        # Users
        print(f"  Inserting {NUM_USERS:,} users...")
        for i in range(0, len(users), BATCH):
            batch = users[i:i+BATCH]
            cur.executemany(
                "INSERT INTO users(id, name, email, created_at) VALUES(%s,%s,%s,%s)",
                [(u["id"], u["name"], u["email"], u["created_at"]) for u in batch]
            )
            conn.commit()
            if (i // BATCH) % 10 == 0:
                print(f"    ...inserted {i + len(batch):,} users")

        # Products
        print(f"  Inserting {NUM_PRODUCTS:,} products...")
        for i in range(0, len(products), BATCH):
            batch = products[i:i+BATCH]
            cur.executemany(
                "INSERT INTO products(id, name, category, price, stock) VALUES(%s,%s,%s,%s,%s)",
                [(p["id"], p["name"], p["category"], p["price"], p["stock"]) for p in batch]
            )
            conn.commit()

        # Orders + order_items
        print(f"  Inserting {NUM_ORDERS:,} orders + line items...")
        order_rows = []
        item_rows  = []
        for o in orders:
            order_rows.append((o["id"], o["user_id"], o["total"], o["status"], o["created_at"]))
            for item in o["items"]:
                item_rows.append((
                    str(uuid.uuid4()), o["id"],
                    item["product_id"], item["quantity"], item["unit_price"]
                ))

        for i in range(0, len(order_rows), BATCH):
            cur.executemany(
                "INSERT INTO orders(id, user_id, total, status, created_at) VALUES(%s,%s,%s,%s,%s)",
                order_rows[i:i+BATCH]
            )
            conn.commit()

        for i in range(0, len(item_rows), BATCH):
            cur.executemany(
                "INSERT INTO order_items(id, order_id, product_id, quantity, unit_price) VALUES(%s,%s,%s,%s,%s)",
                item_rows[i:i+BATCH]
            )
            conn.commit()

    conn.close()
    print(f"[OK] PostgreSQL seeded in {time.time()-t0:.1f}s")


# ── 3. Seed MongoDB ──────────────────────────────────────────────────

def seed_mongo(users, products, orders):
    print("\nSeeding MongoDB...")
    t0 = time.time()

    try:
        client = pymongo.MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client["ecommerce"]
    except Exception as e:
        print(f"[ERR] MongoDB connection failed: {e}")
        sys.exit(1)

    # Drop existing collections for clean re-seed
    db.users.drop()
    db.products.drop()
    db.orders.drop()

    BATCH = 2000
    print(f"  Inserting {NUM_USERS:,} users...")
    for i in range(0, len(users), BATCH):
        db.users.insert_many(users[i:i+BATCH])

    print(f"  Inserting {NUM_PRODUCTS:,} products...")
    for i in range(0, len(products), BATCH):
        db.products.insert_many(products[i:i+BATCH])

    print(f"  Inserting {NUM_ORDERS:,} orders...")
    mongo_orders = []
    for o in orders:
        doc = dict(o)
        doc["_id"] = doc.pop("id")
        mongo_orders.append(doc)

    for i in range(0, len(mongo_orders), BATCH):
        db.orders.insert_many(mongo_orders[i:i+BATCH])

    # Indexes
    db.users.create_index("id", unique=True)
    db.orders.create_index("user_id")
    db.orders.create_index("created_at")
    db.orders.create_index("status")
    db.products.create_index("id", unique=True)
    db.products.create_index("category")

    print(f"[OK] MongoDB seeded in {time.time()-t0:.1f}s")


# ── 4. Seed DynamoDB Local ───────────────────────────────────────────

def seed_dynamo(users, orders):
    print("\nSeeding DynamoDB Local...")
    t0 = time.time()

    try:
        dynamo = boto3.resource(
            "dynamodb", region_name="us-east-1",
            endpoint_url="http://localhost:8000",
            aws_access_key_id="fake", aws_secret_access_key="fake"
        )
        table = dynamo.Table("EcommerceDB")
        table.load()
    except Exception as e:
        print(f"[ERR] DynamoDB connection failed: {e}")
        sys.exit(1)

    print(f"  Inserting {NUM_USERS:,} user profiles...")
    with table.batch_writer() as bw:
        for u in users:
            bw.put_item(Item={
                "PK":         f"USER#{u['id']}",
                "SK":         "PROFILE",
                "id":         u["id"],
                "name":       u["name"],
                "email":      u["email"],
                "created_at": u["created_at"].isoformat(),
            })

    print(f"  Inserting {NUM_ORDERS:,} orders...")
    with table.batch_writer() as bw:
        for o in orders:
            bw.put_item(Item={
                "PK":         f"USER#{o['user_id']}",
                "SK":         f"ORDER#{o['id']}",
                "order_id":   o["id"],
                "user_id":    o["user_id"],
                "total":      str(o["total"]),   # Decimal-safe
                "status":     o["status"],
                "created_at": o["created_at"].isoformat(),
                "item_count": len(o["items"]),
            })

    print(f"[OK] DynamoDB seeded in {time.time()-t0:.1f}s")


# ── Main ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total_start = time.time()
    users, products, orders = generate_data()

    seed_postgres(users, products, orders)
    seed_mongo(users, products, orders)
    seed_dynamo(users, orders)

    print(f"\n{'='*50}")
    print(f"[OK] All databases seeded in {time.time()-total_start:.1f}s total")
    print("  Ready to run: python run.py")

"""
postgres_setup.py
Creates all tables and indexes in PostgreSQL for the e-commerce benchmark.
Run once before seeding: python setup/postgres_setup.py
"""

import psycopg2
import sys

def setup():
    try:
        conn = psycopg2.connect(
            dbname="ecommerce",
            user="user",
            password="password",
            host="localhost",
            port=5432
        )
        print("[OK] Connected to PostgreSQL")
    except Exception as e:
        print(f"[ERR] Could not connect to PostgreSQL: {e}")
        sys.exit(1)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE EXTENSION IF NOT EXISTS "pgcrypto";

            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS products CASCADE;
            DROP TABLE IF EXISTS users CASCADE;

            CREATE TABLE users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(100),
                email VARCHAR(150) UNIQUE,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS products (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(200),
                category VARCHAR(100),
                price DECIMAL(10,2),
                stock INT
            );

            CREATE TABLE IF NOT EXISTS orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID REFERENCES users(id),
                total DECIMAL(10,2),
                status VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS order_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                order_id UUID REFERENCES orders(id),
                product_id UUID REFERENCES products(id),
                quantity INT,
                unit_price DECIMAL(10,2)
            );

            CREATE INDEX IF NOT EXISTS idx_orders_user    ON orders(user_id);
            CREATE INDEX IF NOT EXISTS idx_orders_date    ON orders(created_at);
            CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_items_order    ON order_items(order_id);
            CREATE INDEX IF NOT EXISTS idx_items_product  ON order_items(product_id);
            CREATE INDEX IF NOT EXISTS idx_products_cat   ON products(category);
        """)

    conn.commit()
    conn.close()
    print("[OK] PostgreSQL schema created successfully")
    print("  Tables: users, products, orders, order_items")
    print("  Indexes: 6 indexes created")

if __name__ == "__main__":
    setup()

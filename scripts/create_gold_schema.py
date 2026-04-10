import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=5433,
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
cur = conn.cursor()

ddl_statements = [
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        customer_zip_code VARCHAR(10),
        customer_city VARCHAR(100),
        customer_state VARCHAR(5),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id VARCHAR(50) PRIMARY KEY,
        product_category_name VARCHAR(100),
        product_category_name_english VARCHAR(100),
        product_weight_g FLOAT,
        product_length_cm FLOAT,
        product_height_cm FLOAT,
        product_width_cm FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_seller (
        seller_id VARCHAR(50) PRIMARY KEY,
        seller_zip_code VARCHAR(10),
        seller_city VARCHAR(100),
        seller_state VARCHAR(5),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id DATE PRIMARY KEY,
        year INT,
        month INT,
        day INT,
        quarter INT,
        day_of_week INT,
        is_weekend BOOLEAN
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) REFERENCES dim_customer(customer_id),
        order_status VARCHAR(20),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        total_items INT,
        total_amount FLOAT,
        total_freight FLOAT,
        ingestion_timestamp TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_order_items (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(50) REFERENCES fact_orders(order_id),
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        item_sequence INT,
        price FLOAT,
        freight_value FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
]

for stmt in ddl_statements:
    table_name = [line for line in stmt.strip().split('\n') if 'CREATE TABLE' in line][0]
    print(f"Oluşturuluyor: {table_name.strip()}")
    cur.execute(stmt)

conn.commit()
print("\nTüm tablolar oluşturuldu.")

cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")
tables = cur.fetchall()
print("\nMevcut tablolar:")
for t in tables:
    print(f"  - {t[0]}")

cur.close()
conn.close()

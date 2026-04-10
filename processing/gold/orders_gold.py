import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, '/home/mustafa/spark-4.1.1-bin-hadoop3/python')
sys.path.insert(0, '/home/mustafa/spark-4.1.1-bin-hadoop3/python/lib/py4j-0.10.9.9-src.zip')

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, quarter,
    dayofweek, when, lit, count, sum as spark_sum
)

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION     = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET      = os.getenv('S3_BUCKET_NAME')

SILVER_PATH   = f's3a://{S3_BUCKET}/silver/orders'
CUSTOMERS_PATH = f's3a://{S3_BUCKET}/bronze/olist/raw/olist_customers_dataset.csv'
ITEMS_PATH     = f's3a://{S3_BUCKET}/bronze/olist/raw/olist_order_items_dataset.csv'
PRODUCTS_PATH  = f's3a://{S3_BUCKET}/bronze/olist/raw/olist_products_dataset.csv'
SELLERS_PATH   = f's3a://{S3_BUCKET}/bronze/olist/raw/olist_sellers_dataset.csv'
CATEGORY_PATH  = f's3a://{S3_BUCKET}/bronze/olist/raw/product_category_name_translation.csv'

PG_HOST = os.getenv('POSTGRES_HOST')
PG_PORT = 5433
PG_DB   = os.getenv('POSTGRES_DB')
PG_USER = os.getenv('POSTGRES_USER')
PG_PASS = os.getenv('POSTGRES_PASSWORD')
PG_URL  = f'jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}'
PG_PROPS = {'user': PG_USER, 'password': PG_PASS, 'driver': 'org.postgresql.Driver'}

def create_spark_session():
    return SparkSession.builder \
        .appName('StreamCart-Gold-Orders') \
        .master('local[2]') \
        .config('spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:3.4.1,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.583,'
                'org.postgresql:postgresql:42.6.0') \
        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY) \
        .config('spark.hadoop.fs.s3a.endpoint', f's3.{AWS_REGION}.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.connection.timeout', '300000') \
        .config('spark.hadoop.fs.s3a.socket.timeout', '300000') \
        .getOrCreate()

def write_to_postgres(df, table, mode='append'):
    logger.info(f"PostgreSQL'e yazılıyor: {table} ({df.count()} kayıt)")
    df.write.jdbc(url=PG_URL, table=table, mode=mode, properties=PG_PROPS)
    logger.info(f"{table} yazıldı.")

if __name__ == '__main__':
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    # Silver orders oku
    logger.info("Silver orders okunuyor...")
    orders = spark.read.parquet(SILVER_PATH)

    # CSV'leri oku
    logger.info("Dimension CSV'leri okunuyor...")
    customers = spark.read.option('header', True).csv(CUSTOMERS_PATH)
    items     = spark.read.option('header', True).csv(ITEMS_PATH)
    products  = spark.read.option('header', True).csv(PRODUCTS_PATH)
    sellers   = spark.read.option('header', True).csv(SELLERS_PATH)
    category  = spark.read.option('header', True).csv(CATEGORY_PATH)

    # dim_customer
    logger.info("dim_customer yükleniyor...")
    dim_customer = customers.select(
        col('customer_id'),
        col('customer_unique_id'),
        col('customer_zip_code_prefix').alias('customer_zip_code'),
        col('customer_city'),
        col('customer_state')
    ).dropDuplicates(['customer_id'])
    write_to_postgres(dim_customer, 'dim_customer', mode='append')

    # dim_product
    logger.info("dim_product yükleniyor...")
    dim_product = products.join(category, 'product_category_name', 'left').select(
        col('product_id'),
        col('product_category_name'),
        col('product_category_name_english'),
        col('product_weight_g').cast('float'),
        col('product_length_cm').cast('float'),
        col('product_height_cm').cast('float'),
        col('product_width_cm').cast('float')
    ).dropDuplicates(['product_id'])
    write_to_postgres(dim_product, 'dim_product', mode='append')

    # dim_seller
    logger.info("dim_seller yükleniyor...")
    dim_seller = sellers.select(
        col('seller_id'),
        col('seller_zip_code_prefix').alias('seller_zip_code'),
        col('seller_city'),
        col('seller_state')
    ).dropDuplicates(['seller_id'])
    write_to_postgres(dim_seller, 'dim_seller', mode='append')

    # dim_date
    logger.info("dim_date yükleniyor...")
    dim_date = orders.filter(col('order_purchase_timestamp').isNotNull()) \
        .select(col('order_purchase_timestamp').cast('date').alias('date_id')) \
        .dropDuplicates(['date_id']) \
        .withColumn('year', year('date_id')) \
        .withColumn('month', month('date_id')) \
        .withColumn('day', dayofmonth('date_id')) \
        .withColumn('quarter', quarter('date_id')) \
        .withColumn('day_of_week', dayofweek('date_id')) \
        .withColumn('is_weekend', when(dayofweek('date_id').isin(1, 7), True).otherwise(False))
    write_to_postgres(dim_date, 'dim_date', mode='append')

    # fact_orders
    logger.info("fact_orders yükleniyor...")
    items_agg = items.groupBy('order_id').agg(
        count('*').alias('total_items'),
        spark_sum(col('price').cast('float')).alias('total_amount'),
        spark_sum(col('freight_value').cast('float')).alias('total_freight')
    )
    fact_orders = orders.join(items_agg, 'order_id', 'left').select(
        col('order_id'),
        col('customer_id'),
        col('order_status'),
        col('order_purchase_timestamp'),
        col('order_approved_at'),
        col('order_delivered_carrier_date'),
        col('order_delivered_customer_date'),
        col('order_estimated_delivery_date'),
        col('total_items'),
        col('total_amount'),
        col('total_freight'),
        col('ingestion_timestamp')
    )
    write_to_postgres(fact_orders, 'fact_orders', mode='append')

    # fact_order_items
    logger.info("fact_order_items yükleniyor...")
    fact_items = items.select(
        col('order_id'),
        col('product_id'),
        col('seller_id'),
        col('order_item_id').cast('int').alias('item_sequence'),
        col('price').cast('float'),
        col('freight_value').cast('float')
    )
    write_to_postgres(fact_items, 'fact_order_items', mode='append')

    spark.stop()
    logger.info("Gold job tamamlandı.")

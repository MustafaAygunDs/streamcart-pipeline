import os
import sys
import logging
from dotenv import load_dotenv

sys.path.insert(0, '/home/mustafa/spark-4.1.1-bin-hadoop3/python')
sys.path.insert(0, '/home/mustafa/spark-4.1.1-bin-hadoop3/python/lib/py4j-0.10.9.9-src.zip')

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    try_to_timestamp,
    col, from_json, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION     = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET      = os.getenv('S3_BUCKET_NAME')

BRONZE_PATH = f's3a://{S3_BUCKET}/bronze/streaming/orders'
SILVER_PATH = f's3a://{S3_BUCKET}/silver/orders'

ORDER_SCHEMA = StructType([
    StructField('order_id', StringType()),
    StructField('customer_id', StringType()),
    StructField('order_status', StringType()),
    StructField('order_purchase_timestamp', StringType()),
    StructField('order_approved_at', StringType()),
    StructField('order_delivered_carrier_date', StringType()),
    StructField('order_delivered_customer_date', StringType()),
    StructField('order_estimated_delivery_date', StringType()),
    StructField('source', StringType()),
    StructField('ingestion_timestamp', StringType()),
])

def create_spark_session():
    return SparkSession.builder \
        .appName('StreamCart-Silver-Orders') \
        .master('local[2]') \
        .config('spark.jars.packages',
                'org.apache.hadoop:hadoop-aws:3.4.1,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.583') \
        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY) \
        .config('spark.hadoop.fs.s3a.endpoint', f's3.{AWS_REGION}.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config('spark.hadoop.fs.s3a.connection.timeout', '300000') \
        .config('spark.hadoop.fs.s3a.socket.timeout', '300000') \
        .getOrCreate()

def read_bronze(spark):
    logger.info(f"Bronze okunuyor: {BRONZE_PATH}")
    return spark.read.parquet(BRONZE_PATH)

def transform_to_silver(df):
    logger.info("Silver dönüşümü başlıyor...")

    # JSON payload'ı parse et
    parsed = df.withColumn('data', from_json(col('raw_payload'), ORDER_SCHEMA))

    # Kolonları düzleştir
    silver = parsed.select(
        col('data.order_id').alias('order_id'),
        col('data.customer_id').alias('customer_id'),
        col('data.order_status').alias('order_status'),
        try_to_timestamp(col('data.order_purchase_timestamp')).alias('order_purchase_timestamp'),
        try_to_timestamp(col('data.order_approved_at')).alias('order_approved_at'),
        try_to_timestamp(col('data.order_delivered_carrier_date')).alias('order_delivered_carrier_date'),
        try_to_timestamp(col('data.order_delivered_customer_date')).alias('order_delivered_customer_date'),
        try_to_timestamp(col('data.order_estimated_delivery_date')).alias('order_estimated_delivery_date'),
        col('kafka_timestamp'),
        col('ingestion_timestamp'),
        lit('silver').alias('layer')
    )

    # Deduplikasyon
    before = silver.count()
    silver = silver.dropDuplicates(['order_id'])
    after = silver.count()
    logger.info(f"Deduplikasyon: {before} → {after} kayıt ({before - after} duplicate silindi)")

    # Null filtrele
    silver = silver.filter(col('order_id').isNotNull())
    silver = silver.filter(col('customer_id').isNotNull())

    logger.info(f"Silver kayıt sayısı: {silver.count()}")
    return silver

def write_silver(df):
    logger.info(f"Silver S3'e yazılıyor: {SILVER_PATH}")
    df.write \
        .mode('overwrite') \
        .partitionBy('order_status') \
        .parquet(SILVER_PATH)
    logger.info("Silver katman yazıldı.")

if __name__ == '__main__':
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    bronze_df = read_bronze(spark)
    silver_df = transform_to_silver(bronze_df)
    write_silver(silver_df)

    spark.stop()
    logger.info("Silver job tamamlandı.")
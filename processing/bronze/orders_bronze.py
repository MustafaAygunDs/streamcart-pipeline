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
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StringType

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION     = os.getenv('AWS_DEFAULT_REGION')
S3_BUCKET      = os.getenv('S3_BUCKET_NAME')

KAFKA_BOOTSTRAP = 'localhost:9093'
ORDERS_TOPIC    = 'olist.orders'
BRONZE_PATH     = f's3a://{S3_BUCKET}/bronze/streaming/orders'
CHECKPOINT_PATH = f's3a://{S3_BUCKET}/bronze/checkpoints/orders'

def create_spark_session():
    return SparkSession.builder \
        .appName('StreamCart-Bronze-Orders') \
        .master('local[2]') \
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,'
                'org.apache.hadoop:hadoop-aws:3.3.4,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.262') \
        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY) \
        .config('spark.hadoop.fs.s3a.endpoint', f's3.{AWS_REGION}.amazonaws.com') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

def read_kafka_stream(spark):
    return spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', ORDERS_TOPIC) \
        .option('startingOffsets', 'earliest') \
        .load()

def process_bronze(df):
    return df.select(
        col('key').cast(StringType()).alias('order_id'),
        col('value').cast(StringType()).alias('raw_payload'),
        col('topic'),
        col('partition'),
        col('offset'),
        col('timestamp').alias('kafka_timestamp'),
        current_timestamp().alias('ingestion_timestamp'),
        lit('olist_replay').alias('source_system')
    )

def write_to_s3(df):
    return df.writeStream \
        .format('parquet') \
        .outputMode('append') \
        .option('path', BRONZE_PATH) \
        .option('checkpointLocation', CHECKPOINT_PATH) \
        .trigger(processingTime='30 seconds') \
        .start()

if __name__ == '__main__':
    logger.info("Bronze job başlatılıyor...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    raw_df = read_kafka_stream(spark)
    bronze_df = process_bronze(raw_df)
    query = write_to_s3(bronze_df)

    logger.info(f"Stream S3'e yazılıyor: {BRONZE_PATH}")
    query.awaitTermination()

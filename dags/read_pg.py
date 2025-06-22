import os
import logging
import boto3
from pyspark.sql import SparkSession
from datetime import datetime
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables
SPARK_HOME = os.getenv("SPARK_HOME", "/opt/spark")
SPARK_JARS = "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4"
PG_DB_URL = os.getenv("PG_DB_URL", "jdbc:postgresql://postgres:5432/bootcamp_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_secure_password")
PG_DB_TABLE = "actor_films"
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

logging.info(f"Spark Jars Packages: {SPARK_JARS}")
logging.info(f"SPARK_HOME set to: {SPARK_HOME}")
logging.info(f"Table name: {PG_DB_TABLE}")

# Create S3 bucket if it doesn't exist
logging.info(f"Ensuring S3 bucket {S3_BUCKET} exists")
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    s3_client.head_bucket(Bucket=S3_BUCKET)
    logging.info(f"Bucket {S3_BUCKET} already exists")
except s3_client.exceptions.ClientError as e:
    if e.response['Error']['Code'] == '404':
        logging.info(f"Creating bucket {S3_BUCKET}")
        s3_client.create_bucket(Bucket=S3_BUCKET)
    else:
        logging.error(f"Error checking bucket {S3_BUCKET}: {e}")
        raise

# Verify SPARK_HOME
if not os.path.exists(f"{SPARK_HOME}/bin/spark-submit"):
    logging.error(f"SPARK_HOME directory {SPARK_HOME}/bin/spark-submit does not exist.")
    raise FileNotFoundError(f"SPARK_HOME directory {SPARK_HOME}/bin/spark-submit does not exist.")

# Initialize SparkSession
logging.info("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("ReadPostgreSQL") \
    .config("spark.jars.packages", SPARK_JARS) \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read from PostgreSQL
logging.info("Connecting to PostgreSQL...")
try:
    df = spark.read \
        .format("jdbc") \
        .option("url", PG_DB_URL) \
        .option("dbtable", PG_DB_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    logging.info("Connected to DB")
    df.show(5)
except Exception as e:
    logging.error(f"Error reading from PostgreSQL: {e}")
    spark.stop()
    raise

# Write to S3 with table name and current date (CEST timezone)
current_date = datetime.now(pytz.timezone('Europe/Paris')).strftime("%Y%m%d")
output_path = f"s3a://{S3_BUCKET}/parquet/{PG_DB_TABLE}_{current_date}.parquet"
logging.info(f"Writing to Parquet: {output_path}")
try:
    df.coalesce(1).write.mode("overwrite").parquet(output_path)
except Exception as e:
    logging.error(f"An error occurred: {e}")
    spark.stop()
    raise

# Stop Spark session
logging.info("Stopping Spark Session...")
spark.stop()
logging.info("Spark Session stopped.")
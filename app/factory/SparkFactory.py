import os
from pyspark.sql import SparkSession

class SparkFactory():
    def build_postgres_connection_properties(self):
        jdbc_url = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
        connection_properties = {
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver"
        }

        return jdbc_url, connection_properties

    def build_spark(self):
        access_key = os.environ['AWS_ACCESS_KEY_ID']
        secret_key = os.environ['AWS_SECRET_ACCESS_KEY']

        return SparkSession.builder \
            .appName("tickers_postgres_to_lake") \
            .master("spark://spark-master:7077") \
            .config("spark.jars", "/shared-jars/postgresql-42.6.0.jar") \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
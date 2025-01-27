import os
from pyspark.sql import SparkSession
from app.constant.lakehouse import BRONZE

class SparkFactory():
    def __init__(self, iceberg=False, layer = BRONZE):
        self.layer = layer
        self.iceberg = iceberg

        self.access_key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.s3_bucket = os.environ["AWS_S3_BUCKET"]

    def build_postgres_connection_properties(self):
        jdbc_url = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB']}"
        connection_properties = {
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver"
        }

        return jdbc_url, connection_properties

    def build_spark(self):
        if self.iceberg:
            return SparkSession.builder \
                .appName("Iceberg with AWS Glue and S3") \
                .config("spark.jars", "/shared-jars/iceberg-spark-runtime.jar,/shared-jars/iceberg-aws-bundle.jar") \
                .config("spark.sql.defaultCatalog", "glue_catalog") \
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{self.s3_bucket}/{self.layer}") \
                .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
                .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                .getOrCreate()
        
        else:
            return SparkSession.builder \
                .appName("tickers_postgres_to_lake") \
                .master("spark://spark-master:7077") \
                .config("spark.jars", "/shared-jars/postgresql-42.6.0.jar") \
                .config("spark.hadoop.fs.s3a.access.key", self.access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", self.secret_key) \
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
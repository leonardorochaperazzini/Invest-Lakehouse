import os
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from app.constant.lakehouse import S3_BRONZE_PATH
from app.constant.source import POSTGRES_INVEST_SCHEMA
from app.factory.SparkFactory import SparkFactory

class IngestionBronze():
    def __init__(self, logger = os.environ.get("PRINT_LOG", True)):
        spark_factory = SparkFactory()

        self.logger = logger
        self.spark_session = spark_factory.build_spark()
        self.jdbc_url, self.connection_properties = spark_factory.build_postgres_connection_properties()

    def ingest(self, table, s3_bronze_path = S3_BRONZE_PATH, postgres_invest_schema = POSTGRES_INVEST_SCHEMA):
        df = self.spark_session.read.jdbc(url=self.jdbc_url, table=f"{postgres_invest_schema}.{table}", properties=self.connection_properties)
        
        df = df.withColumn("inserted_timestamp", to_utc_timestamp(current_timestamp(), "UTC"))

        df.write.mode("overwrite").parquet(f"{s3_bronze_path}/{table}")

        if self.logger:
            df.show()
            print(f"Data written to S3: {postgres_invest_schema}.{table}")

        self.spark_session.stop()
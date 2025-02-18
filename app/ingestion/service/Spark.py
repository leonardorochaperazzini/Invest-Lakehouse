import os
import pytz
from datetime import datetime
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from app.constant.lakehouse import S3_BRONZE_PATH
from app.constant.source import POSTGRES_INVEST_SCHEMA
from app.factory.SparkSessionFactory import SparkSessionFactory

class Spark():
    def __init__(self, logger = os.environ.get("PRINT_LOG")):
        spark_session_factory = SparkSessionFactory()

        self.logger = logger
        self.spark_session = spark_session_factory.build_spark()
        self.jdbc_url, self.connection_properties = spark_session_factory.build_postgres_connection_properties()

    def ingest(self, table, s3_bronze_path = S3_BRONZE_PATH, postgres_invest_schema = POSTGRES_INVEST_SCHEMA):
        df = self.spark_session.read.jdbc(url=self.jdbc_url, table=f"{postgres_invest_schema}.{table}", properties=self.connection_properties)
        
        df = df.withColumn("inserted_timestamp", to_utc_timestamp(current_timestamp(), "UTC"))

        utc_now = datetime.now(pytz.utc)
        current_date = utc_now.strftime("%Y-%m-%d")

        path = f"{s3_bronze_path}/{table}/inserted_date={current_date}"

        df.write.mode("overwrite").parquet(path)

        if self.logger:
            df.show()
            print(f"Data written to S3: {postgres_invest_schema}.{table}")

        self.spark_session.stop()
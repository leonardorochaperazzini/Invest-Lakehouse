from app.constants import LAKEHOUSE_TICKERS_BRONZE_PATH, POSTGRES_TABLE_TICKERS
from app.factory.SparkFactory import SparkFactory

def main():
    spark_factory = SparkFactory()

    jdbc_url, connection_properties = spark_factory.build_postgres_connection_properties()
    spark_session = spark_factory.build_spark()

    df = spark_session.read.jdbc(url=jdbc_url, table=POSTGRES_TABLE_TICKERS, properties=connection_properties)

    print(f"Data read from Postgres: {POSTGRES_TABLE_TICKERS}")
    
    df.write.mode("overwrite").parquet(LAKEHOUSE_TICKERS_BRONZE_PATH)

    df.show()

    print(f"Data written to S3: {LAKEHOUSE_TICKERS_BRONZE_PATH}")

    spark_session.stop()

if __name__ == "__main__":
    main()
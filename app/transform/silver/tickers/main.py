from app.constants import LAKEHOUSE_TICKERS_BRONZE_PATH
from app.factory.SparkFactory import SparkFactory

def main():
    spark_factory = SparkFactory()

    spark_session = spark_factory.build_spark()

    df_s3 = spark_session.read.parquet(LAKEHOUSE_TICKERS_BRONZE_PATH)

    print(f"Data read from S3: {LAKEHOUSE_TICKERS_BRONZE_PATH}")
    
    df_s3.show()

    spark_session.stop()

if __name__ == "__main__":
    main()
from app.constant.lakehouse import S3_BRONZE_PATH, TICKERS_TYPES
from app.transform.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.bronze_transform(table = TICKERS_TYPES)

if __name__ == "__main__":
    main()
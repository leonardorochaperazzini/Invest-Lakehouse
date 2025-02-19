from app.constant.lakehouse import TICKERS
from app.transform.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.bronze_transform(table = TICKERS)

if __name__ == "__main__":
    main()
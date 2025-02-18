from app.constant.lakehouse import SCRAPER_RUNS_TICKERS
from app.transform.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.bronze_transform(table = SCRAPER_RUNS_TICKERS)

if __name__ == "__main__":
    main()
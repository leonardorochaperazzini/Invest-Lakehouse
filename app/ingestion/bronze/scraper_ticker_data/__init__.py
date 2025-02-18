from app.constant.source import SCRAPER_TICKERS_DATA
from app.ingestion.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.ingest(SCRAPER_TICKERS_DATA)

if __name__ == "__main__":
    main()
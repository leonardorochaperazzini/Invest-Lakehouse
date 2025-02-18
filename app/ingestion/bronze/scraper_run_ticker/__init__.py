from app.constant.source import SCRAPER_RUNS_TICKERS
from app.ingestion.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.ingest(SCRAPER_RUNS_TICKERS)

if __name__ == "__main__":
    main()
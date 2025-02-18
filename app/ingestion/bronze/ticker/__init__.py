from app.constant.source import TICKERS
from app.ingestion.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.ingest(TICKERS)

if __name__ == "__main__":
    main()
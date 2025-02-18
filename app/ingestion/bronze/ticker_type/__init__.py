from app.constant.source import TICKERS_TYPES
from app.ingestion.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.ingest(TICKERS_TYPES)

if __name__ == "__main__":
    main()
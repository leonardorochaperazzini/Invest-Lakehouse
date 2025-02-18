from app.constant.source import SCRAPER_RUNS
from app.ingestion.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.ingest(SCRAPER_RUNS)

if __name__ == "__main__":
    main()
from app.constant.source import USERS
from app.ingestion.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.ingest(USERS)

if __name__ == "__main__":
    main()
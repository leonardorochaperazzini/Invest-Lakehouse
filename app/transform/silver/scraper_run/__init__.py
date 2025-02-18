from app.constant.lakehouse import SCRAPER_RUNS
from app.transform.service.Spark import Spark as SparkService

def main():
    spark_service = SparkService()

    spark_service.bronze_transform(table = SCRAPER_RUNS)

if __name__ == "__main__":
    main()
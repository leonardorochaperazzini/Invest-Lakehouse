from app.constant.source import SCRAPER_RUNS
from app.ingestion.service.IngestionBronze import IngestionBronze

def main():
    ingestion_bronze = IngestionBronze()

    ingestion_bronze.ingest(SCRAPER_RUNS)

if __name__ == "__main__":
    main()
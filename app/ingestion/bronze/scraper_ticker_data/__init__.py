from app.constant.source import SCRAPER_TICKERS_DATA
from app.ingestion.service.IngestionBronze import IngestionBronze

def main():
    ingestion_bronze = IngestionBronze()

    ingestion_bronze.ingest(SCRAPER_TICKERS_DATA)

if __name__ == "__main__":
    main()
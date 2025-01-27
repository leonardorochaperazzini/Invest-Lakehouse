from app.constant.source import SCRAPER_RUNS_TICKERS
from app.ingestion.service.IngestionBronze import IngestionBronze

def main():
    ingestion_bronze = IngestionBronze()

    ingestion_bronze.ingest(SCRAPER_RUNS_TICKERS)

if __name__ == "__main__":
    main()
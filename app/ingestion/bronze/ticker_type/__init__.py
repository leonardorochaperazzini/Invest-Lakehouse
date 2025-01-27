from app.constant.source import TICKERS_TYPES
from app.ingestion.service.IngestionBronze import IngestionBronze

def main():
    ingestion_bronze = IngestionBronze()

    ingestion_bronze.ingest(TICKERS_TYPES)

if __name__ == "__main__":
    main()
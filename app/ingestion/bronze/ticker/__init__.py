from app.constant.source import TICKERS
from app.ingestion.service.IngestionBronze import IngestionBronze

def main():
    ingestion_bronze = IngestionBronze()

    ingestion_bronze.ingest(TICKERS)

if __name__ == "__main__":
    main()
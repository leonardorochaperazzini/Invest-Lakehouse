from app.constant.source import USERS
from app.ingestion.service.IngestionBronze import IngestionBronze

def main():
    ingestion_bronze = IngestionBronze()

    ingestion_bronze.ingest(USERS)

if __name__ == "__main__":
    main()
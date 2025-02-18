import os

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

S3_BUCKET = os.environ["AWS_S3_BUCKET"]
S3_BRONZE_PATH = f"s3a://{S3_BUCKET}/{BRONZE}"
S3_SILVER_PATH = f"s3a://{S3_BUCKET}/{SILVER}"
S3_GOLD_PATH = f"s3a://{S3_BUCKET}/{GOLD}"

DATABASE_LAKEHOUSE_SILVER = "lakehouse_tutorial_silver"
DATABASE_LAKEHOUSE_GOLD = "lakehouse_tutorial_gold"

SCRAPER_RUNS = "scraper_runs"
SCRAPER_RUNS_TICKERS = "scraper_runs_tickers"
SCRAPER_TICKERS_DATA = "scraper_tickers_data"
TICKERS = "tickers"
TICKERS_TYPES = "tickers_types"
USERS = "users"

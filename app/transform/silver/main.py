from app.transform.silver.user import main as user_main
from app.transform.silver.ticker import main as ticker_main
from app.transform.silver.ticker_type import main as ticker_type_main 
from app.transform.silver.scraper_run import main as scraper_run_main 
from app.transform.silver.scraper_run_ticker import main as scraper_run_ticker_main 
from app.transform.silver.scraper_ticker_data import main as scraper_ticker_data_main 

def main():
    user_main()
    ticker_main()
    ticker_type_main()
    scraper_run_main()
    scraper_run_ticker_main()
    scraper_ticker_data_main()

if __name__ == "__main__":
    main()
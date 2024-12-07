# main.py
import logging
from fetch_data import fetch_all_currencies, fetch_exchange_rates
from load_data import initialize_staging_tables, load_staging_currencies, load_staging_exchange_rates

logging.basicConfig(level=logging.INFO)

def main():
    # Initialize staging tables in Snowflake
    initialize_staging_tables()

    # Fetch currencies data and load into staging
    currencies_data = fetch_all_currencies()
    if currencies_data:
        load_staging_currencies(currencies_data)
    else:
        logging.warning("No currencies data returned from source.")

    # Determine which currencies to get exchange rates for
    # Extract currency codes from currencies_data
    if currencies_data:
        target_currencies = [c["short_code"] for c in currencies_data if "short_code" in c]
        base_currency = "USD"
        exchange_rates_data = fetch_exchange_rates(base_currency, target_currencies)
        if exchange_rates_data:
            load_staging_exchange_rates(exchange_rates_data)
        else:
            logging.warning("No exchange rates data returned from source.")
    else:
        logging.warning("Skipping exchange rates since no currencies data available.")

    logging.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()

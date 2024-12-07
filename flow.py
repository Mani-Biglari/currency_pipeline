from prefect import flow, task
from fetch_data import fetch_all_currencies, fetch_exchange_rates
from load_data import initialize_staging_tables, load_staging_currencies, load_staging_exchange_rates

@task
def init_staging():
    # Initialize Snowflake staging tables
    initialize_staging_tables()

@task
def get_currencies():
    # Fetch currency data from the API
    return fetch_all_currencies()

@task
def load_currencies_to_snowflake(currencies):
    # Load fetched currencies into Snowflake staging
    if currencies and len(currencies) > 0:
        load_staging_currencies(currencies)
    else:
        print("No currencies data returned from API to load.")

@task
def get_exchange_rates(base_currency, target_currencies):
    # Fetch exchange rates for the given base and target currencies
    return fetch_exchange_rates(base_currency, target_currencies)

@task
def load_exchange_rates_to_snowflake(rates):
    # Load fetched exchange rates into Snowflake staging
    if rates and 'rates' in rates and rates['rates']:
        load_staging_exchange_rates(rates)
    else:
        print("No exchange rates data returned from API to load.")

@flow
def currency_pipeline_flow():
    # Run each step as a Prefect task within a flow
    init_staging()

    # 1. Fetch and load currencies
    currencies = get_currencies()
    load_currencies_to_snowflake(currencies)

    # 2. If we have currencies, fetch and load exchange rates
    if currencies and len(currencies) > 0:
        # Extract target currency codes from returned currencies data
        target_currencies = [c["short_code"] for c in currencies if "short_code" in c]
        base_currency = "USD"
        rates = get_exchange_rates(base_currency, target_currencies)
        load_exchange_rates_to_snowflake(rates)
    else:
        print("Skipping exchange rates since no currencies data is available.")

    print("Prefect Flow completed successfully.")

if __name__ == "__main__":
    # Running this file directly executes the flow
    currency_pipeline_flow()

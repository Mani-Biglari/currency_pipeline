from prefect import flow, task
import requests
from fetch_data import fetch_all_currencies, fetch_exchange_rates
from load_data import initialize_staging_tables, load_staging_currencies, load_staging_exchange_rates

# Task to initialize staging tables
@task
def init_staging():
    initialize_staging_tables()

# Task to fetch currency data
@task
def get_currencies():
    return fetch_all_currencies()

# Task to load currencies into Snowflake
@task
def load_currencies_to_snowflake(currencies):
    if currencies and len(currencies) > 0:
        load_staging_currencies(currencies)
    else:
        print("No currencies data returned from API to load.")

# Task to fetch exchange rates
@task
def get_exchange_rates(base_currency, target_currencies):
    return fetch_exchange_rates(base_currency, target_currencies)

# Task to load exchange rates into Snowflake
@task
def load_exchange_rates_to_snowflake(rates):
    if rates and 'rates' in rates and rates['rates']:
        load_staging_exchange_rates(rates)
    else:
        print("No exchange rates data returned from API to load.")

# Task to trigger dbt Cloud job
@task
def trigger_dbt_cloud_job():
    dbt_cloud_api_url = "https://iw734.us1.dbt.com/api/v2/accounts/70471823404751/jobs/70471823404154/run/"
    dbt_cloud_api_key = "dbtu_rPo86M46J5KOpaBcD33ibO9QsIaaI69WICs43yyHBv2xkDIHnc"

    headers = {
        "Authorization": f"Token {dbt_cloud_api_key}"
    }

    body = {
        "cause": "Triggered via API",
    }

    response = requests.post(dbt_cloud_api_url, headers=headers, json=body)

    if response.status_code == 200:
        print("dbt job triggered successfully!")
    else:
        print(f"Failed to trigger dbt job: {response.status_code} - {response.text}")

# Prefect flow combining all tasks
@flow
def currency_pipeline_flow():
    # Initialize Snowflake staging tables
    init_staging()

    # Step 1: Fetch and load currencies
    currencies = get_currencies()
    load_currencies_to_snowflake(currencies)

    # Step 2: If currencies exist, fetch and load exchange rates
    if currencies and len(currencies) > 0:
        target_currencies = [c["short_code"] for c in currencies if "short_code" in c]
        base_currency = "USD"
        rates = get_exchange_rates(base_currency, target_currencies)
        load_exchange_rates_to_snowflake(rates)
    else:
        print("Skipping exchange rates since no currencies data is available.")

    # Step 3: Trigger dbt Cloud job to update DWH
    trigger_dbt_cloud_job()

    print("Prefect Flow completed successfully.")

if __name__ == "__main__":
    # Running this file directly executes the flow
    currency_pipeline_flow()

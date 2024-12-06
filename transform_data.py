import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def transform_currencies_data(currencies_data):
    logging.info("Transforming currencies data.")
    df = pd.DataFrame(currencies_data)
    df = df.rename(columns={
        "short_code": "currency_code",
        "name": "currency_name",
        "code": "numeric_code",
        "id": "source_ident_nbr"
    })
    return df

def transform_exchange_rates_data(exchange_rates_data, base_currency):
    logging.info("Transforming exchange rates data.")
    rates = exchange_rates_data.get("rates", {})
    timestamp = pd.to_datetime(exchange_rates_data.get("timestamp", pd.Timestamp.utcnow()), unit='s', errors='coerce')
    if pd.isna(timestamp):
        timestamp = pd.Timestamp.utcnow()

    data = [
        {
            "base_currency": base_currency,
            "target_currency": target,
            "rate": rate,
            "timestamp": timestamp
        }
        for target, rate in rates.items() if rate is not None
    ]
    return pd.DataFrame(data)

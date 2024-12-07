# load_data.py
import logging
import pandas as pd
import snowflake.connector
from datetime import datetime
import os
import tempfile

from config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_WAREHOUSE
)

logging.basicConfig(level=logging.INFO)

def connect_to_snowflake():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )
    return conn

def initialize_staging_tables():
    conn = connect_to_snowflake()
    try:
        cursor = conn.cursor()
        # Ensure schema exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA};")
        
        # Create staging tables if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.stg_currencies (
                id INTEGER,
                short_code TEXT,
                name TEXT,
                code TEXT,
                precision INTEGER,
                subunit INTEGER,
                symbol TEXT,
                symbol_first BOOLEAN,
                decimal_mark TEXT,
                thousands_separator TEXT,
                load_timestamp TEXT
            )
        """)
        
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.stg_exchange_rates (
                load_timestamp TEXT,
                base TEXT,
                currency TEXT,
                rate FLOAT
            )
        """)

        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.stg_currencies;")
        cursor.execute(f"TRUNCATE TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.stg_exchange_rates;")
        
        conn.commit()
        logging.info("Staging tables ensured and truncated in Snowflake.")
    except Exception as e:
        logging.error(f"Error initializing staging tables: {e}")
    finally:
        conn.close()


def load_dataframe_to_snowflake(df, table_name):
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        df.to_csv(tmp.name, index=False, header=True)
        tmp_filename = tmp.name

    # Create a temporary stage
    stage_name = f"{table_name}_temp_stage_{int(datetime.utcnow().timestamp())}"
    cursor.execute(f"CREATE TEMPORARY STAGE {stage_name};")

    # PUT file to stage
    cursor.execute(f"PUT file://{tmp_filename} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

    # COPY INTO table
    copy_query = f"""
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
    FROM @{stage_name}
    FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)
    ON_ERROR='ABORT_STATEMENT';
    """
    cursor.execute(copy_query)

    # Commit and cleanup
    conn.commit()
    cursor.close()
    conn.close()

    # Remove temporary file
    os.remove(tmp_filename)
    logging.info(f"Data loaded into {table_name}.")

def load_staging_currencies(currencies_data):
    df = pd.DataFrame(currencies_data)
    df["load_timestamp"] = datetime.utcnow().isoformat()
    load_dataframe_to_snowflake(df, "stg_currencies")

def load_staging_exchange_rates(exchange_rates_data):
    timestamp = datetime.utcnow().isoformat()
    base = exchange_rates_data.get("base")
    rates = exchange_rates_data.get("rates", {})

    data = []
    for currency, rate in rates.items():
        data.append({
            "load_timestamp": timestamp,
            "base": base,
            "currency": currency,
            "rate": rate
        })
    df = pd.DataFrame(data)
    load_dataframe_to_snowflake(df, "stg_exchange_rates")

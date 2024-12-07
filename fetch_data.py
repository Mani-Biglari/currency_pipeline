import requests
import logging
from config import API_KEY, BASE_URL

logging.basicConfig(level=logging.INFO)

def fetch_all_currencies():
    url = f"{BASE_URL}/currencies"
    params = {
        "api_key": API_KEY,
        "type": "fiat"
    }
    try:
        logging.info(f"Fetching currencies from {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("response"):
            return data["response"]
        else:
            logging.error("Error in API response structure for currencies.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching currencies: {e}")

def fetch_exchange_rates(base_currency, target_currencies):
    url = f"{BASE_URL}/latest"
    params = {
        "api_key": API_KEY,
        "base": base_currency,
        "symbols": ",".join(target_currencies)
    }
    try:
        logging.info(f"Fetching exchange rates from {url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("response"):
            return data["response"]
        else:
            logging.error("Error in API response structure for exchange rates.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching exchange rates: {e}")

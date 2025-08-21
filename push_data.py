import requests
from datetime import datetime
import time
import yfinance as yf
import pandas as pd
import os

# Power BI Streaming Dataset URL
POWER_BI_URL = "URL"

# CSV file to save data
CSV_FILE = "finance_data.csv"

def fetch_data():
    # Get crypto prices
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "bitcoin,ethereum", "vs_currencies": "usd"}
    crypto = requests.get(url, params=params).json()

    # Get stock prices via yfinance
    def get_price(symbol):
        ticker = yf.Ticker(symbol)
        return ticker.info.get('currentPrice', ticker.info.get('regularMarketPrice'))

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "BTC": crypto["bitcoin"]["usd"],
        "ETH": crypto["ethereum"]["usd"],
        "AAPL": get_price("AAPL"),
        "TSLA": get_price("TSLA")
    }

# Continuously send data every 10 seconds
while True:
    data = fetch_data()

    # 1) Push to Power BI
    try:
        response = requests.post(POWER_BI_URL, json=[data])
        print("Sent to Power BI:", data, "Status:", response.status_code)
    except Exception as e:
        print("Error sending to Power BI:", e)

    # 3) Push to n8n webhook
    try:
        n8n_url = "URL"  
        response_n8n = requests.post(n8n_url, json=data)
        print("Sent to n8n:", response_n8n.status_code)
    except Exception as e:
        print("Error sending to n8n:", e)


    # 2) Save to CSV
    try:
        df = pd.DataFrame([data])
        if not os.path.isfile(CSV_FILE):
            df.to_csv(CSV_FILE, index=False)
        else:
            df.to_csv(CSV_FILE, mode="a", header=False, index=False)
        print("Saved to CSV:", CSV_FILE)
    except Exception as e:
        print("Error saving to CSV:", e)

    # Wait 10 seconds before next update
    time.sleep(10)

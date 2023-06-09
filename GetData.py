import requests
import json
import time
import pandas

def get_data():

    MAX_RETRIES_PER_DAY = 10
    SECONDS_PER_WEEK = 30#7 * 24 * 60 * 60
    retries = 0

    # Continuously retrieve data every week
    while True:
        try:
            # Try to get data
            make_request()
            retries = 0
            time.sleep(SECONDS_PER_WEEK)
        except Exception as e:

            # If failed, keep retrying up to 10 times
            print(f"API request failed: {e}")
            retries += 1

            # If retries exceed 10, skip the week, try next week
            if retries >= MAX_RETRIES_PER_DAY:
                print("Max retries reached for today. Skipping this week.")
                retries = 0
                time.sleep(SECONDS_PER_WEEK)

def make_request():
    SYMBOL = 'VOO'
    KEY = 'api_key'

    # Retrieve weekly adjusted StockData
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&' \
          f'symbol={SYMBOL}&apikey={KEY}&datatype=csv'

    data = requests.get(url)

    if data.status_code != 200:
        raise Exception("Status code not 200")


def read_models():
    pass

if __name__ == '__main__':
    get_data()
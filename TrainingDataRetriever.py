import pandas as pd
import requests
from io import StringIO
from time import sleep


def retrieve_and_store(technical_indicators):

    # Parameters
    KEY = 'insert_api_key'
    SYMBOL = 'VOO'
    TIME_PERIOD = '10'

    # Retrieve weekly adjusted StockData
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&' \
          f'symbol={SYMBOL}&apikey={KEY}&datatype=csv'

    data = requests.get(url)

    if data.status_code != 200:
        print('Failed to retrieve symbol StockData')
        raise Exception('Status code not 200')
        return

    df = pd.read_csv(StringIO(data.text))
    df.to_csv(f'StockData/stock.csv', index=False)


    for ti in technical_indicators:
        ti_url = f'https://www.alphavantage.co/query?function={ti}&symbol={SYMBOL}&interval=weekly&' \
                f'time_period={TIME_PERIOD}&series_type=open&apikey={KEY}&datatype=csv'

        if ti == 'VWAP':
            ti_url = f'https://www.alphavantage.co/query?function=VWAP&symbol={SYMBOL}&interval=15min&' \
                           f'apikey={KEY}&datatype=csv'

        data = requests.get(ti_url)

        if data.status_code != 200:
            print(f'Failed to get {ti} StockData...')
            print("Using last week's data.")
            continue

        ti_series = pd.read_csv(StringIO(data.text))
        ti_series.to_csv(f'StockData/{ti}.csv', index=False)
        sleep(15)


def format_stored_data(technical_indicators):

    df = pd.read_csv('StockData/stock.csv')



    for ti in technical_indicators:
        ti_df = pd.read_csv(f'StockData/{ti}.csv')
        df = pd.concat([df, ti_df.iloc[:, 1:]], axis=1)

    # Move adjusted close to the end
    col = df.pop('adjusted close')
    df['Adjusted Close'] = col

    df.to_csv('FormattedData.csv', index=False)

# TODO: Implement trending indicators only
def format_trending_data():
    trending_ti = ['SMA', 'EMA', 'VWAP', 'MACD', 'AROON', 'BBANDS', 'AD', 'OBV']

if __name__ == '__main__':
    technical_indicators = ['SMA', 'EMA', 'WMA', 'DEMA', 'TEMA', 'TRIMA', 'KAMA', 'MAMA', 'VWAP', 'T3', 'MACD',
                            'WILLR', 'ADXR', 'APO', 'PPO', 'MOM', 'BOP', 'CMO', 'ROC', 'ROCR',
                            'AROON', 'AROONOSC', 'MFI', 'TRIX', 'ULTOSC', 'DX', 'MINUS_DI', 'PLUS_DI', 'MINUS_DM',
                            'PLUS_DM', 'BBANDS', 'MIDPOINT', 'MIDPRICE', 'SAR', 'TRANGE', 'ATR', 'NATR', 'AD', 'ADOSC',
                            'OBV', 'HT_TRENDLINE', 'HT_SINE', 'HT_TRENDMODE', 'HT_DCPERIOD', 'HT_DCPHASE', 'HT_PHASOR']
    # Gets StockData and stores it in StockData folder
    #retrieve_and_store(technical_indicators)

    # Formats all the StockData into a single CSV called 'FormattedData' using Pandas (if not using postgreSQL and Spark)
    format_stored_data(technical_indicators)


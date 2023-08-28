import json
import requests
import yfinance as yf
import datetime
import missingno as msno
from pandas import DataFrame

END_DATE = datetime.datetime.now()
START_DATE = END_DATE - datetime.timedelta(days=5 * 365)
ESG_QUERY = 'https://query2.finance.yahoo.com/v1/finance/esgChart?symbol='


def main():
    symbols = ['AMZN', 'AAPL', 'XOM', 'NKE', 'PFE', 'TSLA']
    data = {}
    for symbol in symbols:
        data[symbol] = get_data(symbol)


if __name__ == '__main__':
    main()


def get_data(symbol: str):
    price_data: DataFrame = yf.download(symbol, start=START_DATE, end=END_DATE)
    print(price_data.index)
    esg_data = json.loads(open(f'esgChart{symbol}.json', 'r').read())['esgChart']['result'][0]

    symbol_series = esg_data['symbolSeries']
    matrix = []
    indices = []

    for i in range(len(symbol_series['timestamp'])):
        timestamp = datetime.datetime.fromtimestamp(symbol_series['timestamp'][i]).date()
        esgScore = symbol_series['esgScore'][i]
        governanceScore = symbol_series['governanceScore'][i]
        environmentScore = symbol_series['environmentScore'][i]
        socialScore = symbol_series['socialScore'][i]
        try:
            next_month = timestamp.replace(month=timestamp.month + 1)
        except ValueError:
            next_month = timestamp.replace(month=1)
        last_day_of_month = next_month - datetime.timedelta(days=1)

        while timestamp <= last_day_of_month:
            indices.append(timestamp)
            matrix.append((esgScore, governanceScore, environmentScore, socialScore))
            timestamp += datetime.timedelta(days=1)

    esg_df = DataFrame(matrix, columns=['ESG', 'Governance', 'Environment', 'Social'], index=indices)

    # Forward Fill Null Values
    esg_df.fillna(method='ffill', inplace=True)

    combined_df = esg_df.join(price_data, how='inner')
    return combined_df

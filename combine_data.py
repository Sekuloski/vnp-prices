import csv
import datetime
import json
import os
from collections import defaultdict

import yfinance as yf
from pandas import DataFrame

SYMBOLS = {
    'AMZN': 0,
    'AAPL': 1,
    'XOM': 2,
    'NKE': 3,
    'PFE': 4,
    'TSLA': 5
    }

date_string = "01/08/2022-00:00:00"
date_format = "%d/%m/%Y-%H:%M:%S"

END_DATE = datetime.datetime.strptime(date_string, date_format)
START_DATE = END_DATE - datetime.timedelta(days=5 * 365)


def main():
    os.chdir('original_data')
    with open('../esg.csv', 'w', newline='') as result:
        temp = defaultdict(dict[datetime.date, list])
        csv_writer = csv.writer(result)
        csv_writer.writerow(['Symbol', 'Time', 'ESG', 'Governance', 'Environment', 'Social', 'Close'])
        for file in os.listdir():
            symbol = file.split('esgChart')[1].split('.')[0]
            with open(file, 'r') as esg:
                esg_data = json.loads(esg.read())['esgChart']['result'][0]['symbolSeries']
                for i in range(len(esg_data['timestamp'])):
                    date = datetime.datetime.fromtimestamp(esg_data['timestamp'][i])
                    if date < START_DATE:
                        continue

                    esgScore = esg_data['esgScore'][i]
                    governanceScore = esg_data['governanceScore'][i]
                    environmentScore = esg_data['environmentScore'][i]
                    socialScore = esg_data['socialScore'][i]
                    temp[symbol][date.date()] = [esgScore, governanceScore, environmentScore, socialScore]

        for symbol in SYMBOLS:
            price_data: DataFrame = yf.download(symbol, start=START_DATE, end=END_DATE)
            for i in price_data.index:
                timestamp: datetime = i.date()

                if timestamp in temp[symbol].keys():
                    temp[symbol][timestamp].append(price_data['Close'][i])

        for symbol, timestamps in temp.items():
            for date, values in timestamps.items():
                timestamp = int(datetime.datetime.timestamp(datetime.datetime.combine(date, datetime.datetime.min.time())))
                values.insert(0, timestamp)
                values.insert(0, symbol)
                csv_writer.writerow(values)


if __name__ == '__main__':
    main()

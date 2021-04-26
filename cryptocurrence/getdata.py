import requests
from datetime import datetime
import pandas as pd

# HOW TO USE

# gd = GetData('LTC', 'BRL', 'hour')
# data = gd.download_data()
# df = gd.convert_to_dataframe(data)
# df = gd.filter_empty_datapoints(df)

# current_datetime = datetime.now().date().isoformat()
# filename = gd.get_filename(current_datetime)
# print('Saving data to %s' % filename)
# df.to_csv(filename, index=False)


class GetData:

    def __init__(self, from_symbol, to_symbol, datetime_interval):
        self.from_symbol = from_symbol
        self.to_symbol = to_symbol
        self.datetime_interval = datetime_interval
        self. url = "%s%s" % ('https://min-api.cryptocompare.com/data/histo', datetime_interval)

    def get_filename(self, download_date):
        return '%s_%s_%s_%s.csv' % (self.from_symbol, self.to_symbol, self.datetime_interval, download_date)

    def download_data(self):
        supported_intervals = {'minute', 'hour', 'day'}

        assert self.datetime_interval in supported_intervals
        
        print(f'Downloading {self.datetime_interval} trading data for {self.from_symbol} {self.to_symbol} from exchange')
        
        params = {'fsym': self.from_symbol, 'tsym': self.to_symbol,
                'limit': 200, 'aggregate': 1}
        request = requests.get(self.url, params=params)
        data = request.json()
        return data

    def convert_to_dataframe(self, data):
        df = pd.json_normalize(data, ['Data'])
        df['datetime'] = pd.to_datetime(df.time, unit='s')
        df = df[['datetime', 'low', 'high', 'open',
                'close', 'volumefrom', 'volumeto']]
        return df

    def filter_empty_datapoints(self, df):
        indices = df[df.sum(axis=1) == 0].index
        print('Filtering %d empty datapoints' % indices.shape[0])
        df = df.drop(indices)
        return df


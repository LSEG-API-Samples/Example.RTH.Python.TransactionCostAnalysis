import sys
sys.path.append("root/")
import pandas as pd
import datetime
import refinitiv.data as rd
from dateutil import parser
from refinitiv.data.errors import RDError
from helper.utilities import VenueTime
rd.open_session('desktop.workspace')

class DataEngineering:

    def __init__(self, ric,  start, end, venue) -> None:
        self.ric = ric
        self.start = parser.parse(start).replace(tzinfo=None)
        self.end = parser.parse(end).replace(tzinfo=None)
        self.venue = VenueTime.from_string(venue).value

    def get_pricing_data(self):
        minute_data_raw = self.get_minute_data()
        minute_data_raw.sort_index(inplace=True)
        minute_data_raw = self.add_exchange_time(minute_data_raw)
        minute_data_period = self.select_trades_within_period(
            minute_data_raw, '10:00', '14:00')

        return minute_data_period

    def get_minute_data(self):
        minute_df = pd.DataFrame()
        end_init = " "
        while self.end >= self.start and self.end != end_init: 
            try:
                end_init = self.end
                response = rd.get_history(universe=self.ric, end=self.end, count=10000, 
                                          fields=['TRDPRC_1'], interval='minute')
                minute_df = pd.concat([minute_df, response])
                self.end = response.index[0]
            except RDError as err:
                print(err)
                continue
        return minute_df

    def add_exchange_time(self, minute_df):
        minute_df.index = pd.to_datetime(minute_df.index)
        print(self.venue)
        minute_df['exchange_time'] = minute_df.index.tz_localize( 
            'GMT').tz_convert(self.venue).tz_localize(None)
        return minute_df


    def select_trades_within_period(self, prices, start, end):
        prices = prices.loc[prices['exchange_time']> self.start]
        prices['Time'] = prices['exchange_time'].dt.time
        prices = prices[(prices['Time'] >
                         datetime.datetime.strptime(start, '%H:%M').time()) & (prices['Time'] <
                        datetime.datetime.strptime(end, '%H:%M').time())]
        return prices.drop(columns='Time')

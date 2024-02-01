import pandas as pd
import numpy as np

class MASignalGenerator:
    def __init__(self, prices,  short_ma, long_ma):
        self.prices = prices
        self.short_ma = short_ma
        self.long_ma = long_ma

    def get_trading_data(self):
        prices_short_ma = self.calculate_ma(self.short_ma, 'short_ma')
        prices_long_ma = self.calculate_ma(self.long_ma, 'long_ma')
        trading_data = self.get_trading_signals(
            prices_short_ma, prices_long_ma)
        return trading_data

    def calculate_ma(self, ma, ma_type):
        prices_ma = pd.DataFrame(self.prices['exchange_time'])
        prices_ma[f'{ma_type}'] = self.prices['TRDPRC_1'].rolling(ma).mean()
        prices_ma.dropna(inplace=True)
        return pd.DataFrame(prices_ma)

    def get_trading_signals(self, prices_short_ma, prices_long_ma):
        trading_data = self.prices.merge(
            (prices_short_ma.merge(prices_long_ma, on='exchange_time', how='inner')), on='exchange_time', how='inner')
        trading_data.insert(loc=len(trading_data.columns), column='crossover',
                            value=np.where(trading_data['short_ma'] > trading_data['long_ma'], 1, 0))
        trading_data['signal'] = trading_data['crossover'].diff()
        trading_data['exchange_time'] = pd.to_datetime(trading_data['exchange_time'])
        trading_data.set_index('exchange_time', inplace= True)
        return trading_data
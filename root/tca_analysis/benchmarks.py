
from tca_analysis.MetricBenchmarkBase import MetricBenchmarkBase
import numpy as np
 
class ArrivalPriceBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'arrival_price'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Price'].iloc[-1]
        except IndexError:
            return np.nan
 
class HighBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'market_high'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['High'].iloc[-1]
        except IndexError:
            return np.nan
        
class SpreadBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'market_spread'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Ask Price'].iloc[-1] - trades_before_arrival['Bid Price'].iloc[-1]
        except IndexError:
            return np.nan

class MidBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'market_mid'
 
    def calculate(self, trades_before_arrival):
        try:
            return (trades_before_arrival['Ask Price'].iloc[-1] + trades_before_arrival['Bid Price'].iloc[-1])/2
        except IndexError:
            return np.nan
            
class LowBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'market_low'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Low'].iloc[-1]
        except IndexError:
            return np.nan 

class MarketVWAPBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'market_vwap'
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Market VWAP'].iloc[-1]
        except IndexError:
            return np.nan
        

class BestAskBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'best_ask'
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Ask Price'].iloc[-1]
        except IndexError:
            return np.nan
        
class BestBidBenchmark(MetricBenchmarkBase):

    @property
    def name(self):
        return 'best_bid'
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Bid Price'].iloc[-1]
        except IndexError:
            return np.nan

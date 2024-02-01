
from abc import ABC, abstractmethod
import numpy as np

class Benchmark(ABC):

    @property
    @abstractmethod
    def name(self):
        pass
 
    @abstractmethod
    def calculate(self, trades_before_arrival):
        pass
 
class ArrivalPriceBenchmark(Benchmark):

    @property
    def name(self):
        return 'arrival_price'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Price'].iloc[-1]
        except IndexError:
            return np.nan
 
class HighBenchmark(Benchmark):

    @property
    def name(self):
        return 'market_high'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['High'].iloc[-1]
        except IndexError:
            return np.nan
        
class SpreadBenchmark(Benchmark):

    @property
    def name(self):
        return 'market_spread'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Ask Price'].iloc[-1] - trades_before_arrival['Bid Price'].iloc[-1]
        except IndexError:
            return np.nan

class MidBenchmark(Benchmark):

    @property
    def name(self):
        return 'market_mid'
 
    def calculate(self, trades_before_arrival):
        try:
            return (trades_before_arrival['Ask Price'].iloc[-1] + trades_before_arrival['Bid Price'].iloc[-1])/2
        except IndexError:
            return np.nan
            
class LowBenchmark(Benchmark):

    @property
    def name(self):
        return 'market_low'
 
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Low'].iloc[-1]
        except IndexError:
            return np.nan 

class MarketVWAPBenchmark(Benchmark):

    @property
    def name(self):
        return 'market_vwap'
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Market VWAP'].iloc[-1]
        except IndexError:
            return np.nan
        

class BestAskBenchmark(Benchmark):

    @property
    def name(self):
        return 'best_ask'
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Ask Price'].iloc[-1]
        except IndexError:
            return np.nan
        
class BestBidBenchmark(Benchmark):

    @property
    def name(self):
        return 'best_bid'
    def calculate(self, trades_before_arrival):
        try:
            return trades_before_arrival['Bid Price'].iloc[-1]
        except IndexError:
            return np.nan

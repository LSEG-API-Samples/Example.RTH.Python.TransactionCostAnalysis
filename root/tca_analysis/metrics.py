     
from abc import ABC, abstractmethod
import numpy as np

class Metric(ABC):

    @property
    @abstractmethod
    def name(self):
        pass
 
    @abstractmethod
    def calculate(self, trades_ric, trades_before_arrival, vwap, twap):
        pass
 
 
class VWAPMetric(Metric):

    @property
    def name(self):
        return 'trades_vwap'
 
    def calculate(self, trades_ric):
        return (trades_ric.trade_volume*trades_ric.execution_price).cumsum() / trades_ric.trade_volume.cumsum()
 
class TWAPMetric(Metric):

    @property
    def name(self):
        return 'trades_twap'
    def calculate(self, trades_ric):
        return trades_ric.execution_price.cumsum()/trades_ric.order_id.notnull().cumsum()
 
 
class SlippageMetric(Metric):

    @property
    def name(self):
        return 'slippage'
 
    def calculate(self, trades_ric):
        return ((trades_ric['trades_vwap'] - trades_ric['signal_price'])*trades_ric['position'])/trades_ric['trades_vwap']*10000

class MarketImpactMetric(Metric):

    @property
    def name(self):
        return 'market_impact'
 
    def calculate(self, trades_ric):
        return ((trades_ric['trades_vwap'] - trades_ric['market_mid'])*trades_ric['position'])/trades_ric['trades_vwap']*10000

class BestPrice(Metric):

    @property
    def name(self):
        return 'best_price'
 
    def calculate(self, trades_ric):
        if trades_ric['position'].eq(1).any():
            best_price = trades_ric.execution_price.cummin()

        elif trades_ric['position'].eq(-1).any():
            best_price = trades_ric.execution_price.cummax()
        else:
            best_price = np.nan

        return best_price

class WorstPrice(Metric):

    @property
    def name(self):
        return 'worst_price'
 
    def calculate(self, trades_ric):
        if trades_ric['position'].eq(1).any():
            worst_price = trades_ric.execution_price.cummax()

        elif trades_ric['position'].eq(-1).any():
            worst_price = trades_ric.execution_price.cummin()
        else:
            worst_price = np.nan

        return worst_price
    
class SlippageMarketVWAP(Metric):

    @property
    def name(self):
        return 'sippage_to_market_vwap'
 
    def calculate(self, trades_ric):
        return ((trades_ric['trades_vwap'] - trades_ric['market_vwap'])*trades_ric['position'])/trades_ric['trades_vwap']*10000
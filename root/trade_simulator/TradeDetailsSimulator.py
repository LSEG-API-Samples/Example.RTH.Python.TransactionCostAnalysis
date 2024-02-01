import pandas as pd
import numpy as np
import random


class TredeDetailSimulator:
    def __init__(self, trade_dfs) -> None:
        self.trade_dfs = trade_dfs

    def add_trade_details(self):
        aggregated_trades = self.aggregate_trades(self.trade_dfs)
        random_brokers, random_traders = self.get_randomized_trade_details(
            aggregated_trades.shape[0])
        aggregated_trades['executed_amount'] = aggregated_trades['trade_volume'] * \
            aggregated_trades['execution_price']
        aggregated_trades['broker_id'] = random_brokers
        aggregated_trades['trader_id'] = random_traders
        aggregated_trades['position'] = np.where(
            aggregated_trades['position'] == 'buy', 1, -1)
        return aggregated_trades

    def aggregate_trades(self, trade_dfs):
        aggregated_trades = pd.DataFrame()
        for key, value in trade_dfs.items():
            asset_trade_df = trade_dfs[key]['trades'].reset_index()
            asset_trade_df = asset_trade_df[['trade_time','signal_time', 'volume_buy','volume_sell','signal_price', 'execution_price','trade_volume','order_id','position','stock_balance']][(
                asset_trade_df['position'] == 'buy') | (asset_trade_df['position'] == 'sell')]
            asset_trade_df['RIC'] = key
            asset_trade_df['currency'] = trade_dfs[key]['currency']
            asset_trade_df['venue'] = trade_dfs[key]['venue']
            aggregated_trades = pd.concat([aggregated_trades, asset_trade_df])
        return aggregated_trades

    def get_randomized_trade_details(self, n):
        random_brokers = []
        random_traders = []
        for _ in range(n):
            random_broker = random.randint(0, 5)
            random_trader = random.randint(0, 5)
            random_brokers.append(f'Broker {random_broker}')
            random_traders.append(f'Trader {random_trader}')
        return random_brokers, random_traders

import sys
sys.path.append("root/")
from helper.utilities import tranform_to_dict
import pandas as pd
import numpy as np
import uuid

class MATradesSimulator:

    def __init__(self, trading_data, market_depth_data,order_type, amount_buy):
        self.trading_data = trading_data
        self.market_depth_data = tranform_to_dict(market_depth_data)
        self.order_type = order_type
        self.amount_buy = amount_buy
        self.trading_activity = {'signal_time':[], 'trade_time': [], 'position': [],'order_id': [],'volume_buy':[], 'volume_sell':[], 'signal_price':[], 'execution_price': [], 'trade_volume': [],  
                                  'stock_balance': [], 'cash_balance': [], 'stock_value': [], 'portfolio_valuation': [], 'p_and_l': []}

    def run(self):
        activate = False
        self.trans_data = {'last_trans_date': '', 'last_trans_type': ''}
        for row in self.trading_data.itertuples():
            if row.signal != 1 and not activate:
                self.remain_idle(row)
            else:
                activate = True
            if activate:
                if self.is_buy_signal(row) and self.trans_data['last_trans_type'] != 'buy':
                    volume_to_buy = int(self.amount_buy/row.TRDPRC_1)
                    signal_price = row.TRDPRC_1
                    self.execute_order('buy', row.Index, volume_to_buy, signal_price)
                    self.update_last_trans_data(row.Index.date(), 'buy')

                elif self.is_sell_signal(row) and self.trans_data['last_trans_type'] == 'buy' and self.trans_data['last_trans_date'] != row.Index.date():
                    volume_to_sell = self.trading_activity['stock_balance'][-1]
                    signal_price = row.TRDPRC_1
                    self.execute_order('sell', row.Index, volume_to_sell, signal_price)
                    self.update_last_trans_data(row.Index.date(), 'sell')
                    activate = False
                else:
                    self.hold(row)
        return pd.DataFrame(self.trading_activity).set_index('trade_time')

    def is_sell_signal(self, row):
        sell_signal = row.signal == -1
        return sell_signal

    def is_buy_signal(self, row):
        buy_signal = row.signal == 1
        return buy_signal

    def update_last_trans_data(self, date, type):
        self.trans_data['last_trans_date'] = date
        self.trans_data['last_trans_type'] = type

    def remain_idle(self, row):
        self.add_hold_idle_info(row, 'idle')
        self.add_performance_info()

    def hold(self, row):
        self.add_hold_idle_info(row, 'hold')
        self.add_performance_info()

    def add_hold_idle_info(self, row, action_type):
        self.trading_activity['position'].append(action_type)
        self.trading_activity['signal_time'].append(np.nan)
        self.trading_activity['trade_time'].append(row.Index)
        self.trading_activity['execution_price'].append(row.TRDPRC_1)
        self.trading_activity['signal_price'].append(np.nan)
        self.trading_activity['trade_volume'].append(0)
        self.trading_activity['order_id'].append(np.nan)
        self.trading_activity['volume_buy'].append(0)
        self.trading_activity['volume_sell'].append(0)

    def add_performance_info(self):
        if len(self.trading_activity['stock_balance']) == 0:
            self.trading_activity['stock_balance'].append(0)
            self.trading_activity['stock_value'].append(0)
            self.trading_activity['cash_balance'].append(0)
            self.trading_activity['portfolio_valuation'].append(0)
            self.trading_activity['p_and_l'].append(0)
        else:
            self.trading_activity['stock_balance'].append(
                self.trading_activity['stock_balance'][-1])
            self.trading_activity['cash_balance'].append(
                self.trading_activity['cash_balance'][-1])
            self.trading_activity['stock_value'].append(
                self.trading_activity['stock_balance'][-1] * self.trading_activity['execution_price'][-1])
            self.trading_activity['portfolio_valuation'].append(
                self.trading_activity['stock_value'][-1]+self.trading_activity['cash_balance'][-1])
            self.trading_activity['p_and_l'].append(self.trading_activity['p_and_l'][-1])

    def execute_order(self, order, signal_time, volume, signal_price):
        order_id = uuid.uuid4()
        trading_data = pd.DataFrame(self.market_depth_data[signal_time])
        n_items_to_iterate = len(trading_data['timestamp'])
        self.execute_trades(trading_data, volume, signal_price,n_items_to_iterate, order_id, signal_time, order)

    def execute_trades(self, trading_data, volume, signal_price,n_items_to_iterate, order_id, signal_time, trade_type):
        for row in trading_data.itertuples():
            trade_details = {
                'buy': {'comparison_conditions':lambda row_price, signal_price: row_price <= signal_price, 'orderbook_price':row.ask_price, 'oderbook_volume': row.ask_size , 'max_volume': trading_data['ask_size'][:10].sum()},
                'sell': {'comparison_conditions':lambda row_price, signal_price: row_price >= signal_price, 'orderbook_price':row.bid_price, 'oderbook_volume': row.bid_size, 'max_volume': trading_data['bid_size'][:10].sum()}
            }
            orderbook_price = trade_details[trade_type]['orderbook_price']   
            orderbook_volume = trade_details[trade_type]['oderbook_volume']
            volume = min(trade_details[trade_type]['max_volume'], volume)
            if self.order_type == 'limit':
                    comparison_condition = trade_details[trade_type]['comparison_conditions']
                    if comparison_condition(orderbook_price, signal_price):
                        self.add_trade_info(row.timestamp, orderbook_price, max(volume, 0), order_id, signal_time, signal_price, trade_type)
                        volume, n_items_to_iterate = self.process_trade(row.timestamp, orderbook_volume, trading_data, trade_type, volume)
            elif self.order_type == 'market':
                self.add_trade_info(row.timestamp, orderbook_price, max(volume, 0), order_id, signal_time, signal_price, trade_type)
                volume, n_items_to_iterate = self.process_trade(row.timestamp, orderbook_volume, trading_data, trade_type, volume)
            if volume <= 0 or n_items_to_iterate == 0:
                break

    def add_trade_info(self, index, price, volume, order_id, signal_time, signal_price, trade_type):
        self.trading_activity['trade_time'].append(index)
        self.trading_activity['execution_price'].append(price)
        self.trading_activity['order_id'].append(order_id)
        self.trading_activity['signal_time'].append(signal_time)
        self.trading_activity['signal_price'].append(signal_price)
        if trade_type =='buy':
            self.trading_activity['volume_buy'].append(volume)
            self.trading_activity['volume_sell'].append(self.trading_activity['volume_sell'][-1])
        elif trade_type =='sell':
            self.trading_activity['volume_buy'].append(self.trading_activity['volume_buy'][-1])
            self.trading_activity['volume_sell'].append(volume)   

    def process_trade(self, timestamp, orderbook_volume, trading_data, trade_type, volume):
        trade_func = {'buy': self.buy, 'sell': self.sell}
        trade_func[trade_type](min(orderbook_volume, volume))
        volume -= orderbook_volume
        n_items_to_iterate = len(trading_data.loc[trading_data['timestamp'] > timestamp])
        return volume, n_items_to_iterate

    def buy(self, n_shares):
        self.trading_activity['position'].append('buy')
        self.trading_activity['trade_volume'].append(n_shares)
        self.trading_activity['stock_balance'].append(
            self.trading_activity['stock_balance'][-1] + n_shares)
        self.trading_activity['cash_balance'].append(
            self.trading_activity['cash_balance'][-1]-(self.trading_activity['execution_price'][-1]*n_shares))
        self.trading_activity['stock_value'].append(
            self.trading_activity['stock_balance'][-1] * self.trading_activity['execution_price'][-1])
        self.trading_activity['portfolio_valuation'].append(
            self.trading_activity['stock_value'][-1]+self.trading_activity['cash_balance'][-1])
        self.trading_activity['p_and_l'].append(self.trading_activity['p_and_l'][-1])

    def sell(self, n_shares):
        self.trading_activity['position'].append('sell')
        self.trading_activity['trade_volume'].append(n_shares)
        self.trading_activity['stock_balance'].append(max(
            self.trading_activity['stock_balance'][-1] - n_shares, 0))
        self.trading_activity['cash_balance'].append(self.trading_activity['cash_balance'][-1] + (
            self.trading_activity['execution_price'][-1] * n_shares))
        self.trading_activity['stock_value'].append(
            self.trading_activity['stock_balance'][-1] * self.trading_activity['execution_price'][-1])
        self.trading_activity['portfolio_valuation'].append(
            self.trading_activity['stock_value'][-1]+self.trading_activity['cash_balance'][-1])
        self.trading_activity['p_and_l'].append(
            self.trading_activity['portfolio_valuation'][-1])
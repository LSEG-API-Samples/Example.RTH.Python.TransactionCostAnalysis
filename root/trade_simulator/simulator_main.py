import sys
sys.path.append("root/")
from helper.utilities import get_signal_time_with_request_times, read_all_files_to_df
from MASignalGenerator import MASignalGenerator
from MATradesSimulator import MATradesSimulator
from DataEngineering import DataEngineering
from TradeDetailsSimulator import TredeDetailSimulator
from rth_extraction.rth_main import extract

assets = {
    'VOD.L': {'currency': 'GBp', 'venue': 'L'}, 'ORAN.PA': {'currency': 'EUR', 'venue': 'PA'},
    'BARC.L': {'currency': 'GBp', 'venue': 'L'}, 'BNPP.PA': {'currency': 'EUR', 'venue': 'PA'},
    'SHEL.L': {'currency': 'GBp', 'venue': 'L'}, 'SHEL.AS': {'currency': 'EUR', 'venue': 'AS'},
}

start = '2023-04-25T00:00:00'
end = '2023-07-27T23:59:00'
short_ma = 50
long_ma = 200
order_type = 'market' # market or limit
amount_buy = 10000000
file_path_depth = "data/market_depth/with_minute_signal"

simulated_trades = {}
for ric, values in assets.items():
    print(f'Simulating trades for {ric}')
    ric_dict = {}
    prices = DataEngineering(
        ric,start, end, assets[ric]['venue']).get_pricing_data()
    ma_signals = MASignalGenerator(prices, short_ma, long_ma).get_trading_data()
    signals_df = get_signal_time_with_request_times(ma_signals, ric, 100, 20000)
    # extract(signals_df, 'market_depth', f'{file_path_depth}/{ric}/')
    depth_data = read_all_files_to_df(f'{file_path_depth}/{ric}/')
    strategy_outcome = MATradesSimulator(ma_signals, depth_data, order_type, amount_buy).run()
    strategy_outcome.to_csv(f'data/strategy_outcome/str_out_{ric}{order_type}_order_depth_minute_10m_AS.csv')
    ric_dict['currency'] = assets[ric]['currency']
    ric_dict['venue'] = assets[ric]['venue']
    ric_dict['trades'] = strategy_outcome
    simulated_trades[ric] = ric_dict

simulated_trades_with_details = TredeDetailSimulator(simulated_trades).add_trade_details()
simulated_trades_with_details.to_csv(
    f'data/strategy_outcome/simulated_trades_{order_type}_order_depth_volume_min_minute.csv')
from datetime import datetime, timedelta
import pandas as pd
import os
from enum import Enum

class VenueTime(Enum):
    L = 'Europe/London'
    PA = 'Europe/Paris'
    AS = 'Europe/Amsterdam'

    @classmethod
    def from_string(cls, venue_str):
        if venue_str in cls.__members__:
            return cls[venue_str]
        raise ValueError(f"Unknown venue: {venue_str}")

def convert_datetime_string(datetime_str):

    datetime_obj = datetime.strptime(datetime_str.strftime(
        '%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f')
    return datetime_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')


def convert_datetime_object(datetime_str):

    datetime_obj = datetime.strptime(datetime_str.strftime(
        '%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f')
    return datetime_obj

def read_all_files_to_df(folder_path):
    
    all_files = os.listdir(folder_path)
    csv_files = [file for file in all_files if file.endswith('.csv.gz')]
    file_cols = pd.read_csv(os.path.join(folder_path, csv_files[0]), compression='gzip', index_col=[0]).columns
    is_depth_data = True if 'L1-AskSize' in file_cols else False
    df = pd.DataFrame()
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        series_df = pd.read_csv(file_path, compression='gzip', index_col=[0]).reset_index()
        if is_depth_data:
            series_df.dropna(inplace=True)
            series_df['arrival_time'] = pd.to_datetime(series_df['signal_time'])+timedelta(milliseconds=200)
        else:
            series_df.dropna(subset=['Market VWAP'], inplace=True)
        series_df['trade_time'] = pd.to_datetime(series_df['Date-Time']).dt.strftime('%Y-%m-%d')  + 'T' + series_df['Exch Time']
        series_df['trade_time'] = pd.to_datetime(series_df['trade_time'])
        series_df.set_index('trade_time', inplace=True)
        exch_code= series_df['#RIC'].iloc[0].split('.')[1]
        venue_time = VenueTime.from_string(exch_code).value
        series_df.index = series_df.index.tz_localize(  # type: ignore
            'GMT').tz_convert(venue_time).tz_localize(None)
        if is_depth_data:
            series_df= series_df.loc[series_df.index > series_df['arrival_time'].iloc[0]]
        df = pd.concat([df, series_df])
    if not is_depth_data:
        df = df[['#RIC', 'Date-Time', 'signal_time', 'Type', 'Price', 'Bid Price', 'Bid Size',
             'Ask Price', 'Ask Size', 'High','Low','Market VWAP']]
    df.rename(columns={'#RIC': 'RIC'}, inplace=True)
    df.reset_index(inplace=True)
    df['signal_time'] = pd.to_datetime(df['signal_time'])
    return df.sort_values(by=['trade_time'])

def tranform_to_dict(df):
    df.rename(columns={'#RIC': 'ric', 'Exch Time': 'exchange_time', 'L1-BidPrice':'l1_bid_price', 'L1-BidSize':'l1_bid_size', 'L1-AskPrice':'l1_ask_price', 'L1-AskSize': 'l1_ask_size',
                                                                'L2-BidPrice':'l2_bid_price', 'L2-BidSize':'l2_bid_size', 'L2-AskPrice':'l2_ask_price','L2-AskSize': 'l2_ask_size',
                                                                'L3-BidPrice':'l3_bid_price', 'L3-BidSize':'l3_bid_size', 'L3-AskPrice':'l3_ask_price', 'L3-AskSize': 'l3_ask_size',
                                                                'L4-BidPrice':'l4_bid_price', 'L4-BidSize':'l4_bid_size', 'L4-AskPrice':'l4_ask_price', 'L4-AskSize': 'l4_ask_size',
                                                                'L5-BidPrice':'l5_bid_price', 'L5-BidSize':'l5_bid_size', 'L5-AskPrice':'l5_ask_price', 'L5-AskSize': 'l5_ask_size',
                                                                'L6-BidPrice':'l6_bid_price', 'L6-BidSize':'l6_bid_size', 'L6-AskPrice':'l6_ask_price', 'L6-AskSize': 'l6_ask_size',
                                                                'L7-BidPrice':'l7_bid_price', 'L7-BidSize':'l7_bid_size', 'L7-AskPrice':'l7_ask_price', 'L7-AskSize': 'l7_ask_size',
                                                                'L8-BidPrice':'l8_bid_price', 'L8-BidSize':'l8_bid_size', 'L8-AskPrice':'l8_ask_price', 'L8-AskSize': 'l8_ask_size',
                                                                'L9-BidPrice':'l9_bid_price', 'L9-BidSize':'l9_bid_size', 'L9-AskPrice':'l9_ask_price', 'L9-AskSize': 'l9_ask_size',
                                                                'L10-BidPrice':'l10_bid_price', 'L10-BidSize':'l10_bid_size', 'L10-AskPrice':'l10_ask_price', 'L10-AskSize': 'l10_ask_size'}, inplace=True)
    result_dict = {}
    df['signal_time'] = pd.to_datetime(df['signal_time'])
    for row in df.itertuples():
        exch_time = row.signal_time
        if exch_time not in result_dict:
            result_dict[exch_time] = {
                'timestamp': [],
                'bid_price': [],
                'bid_size': [],
                'ask_price': [],
                'ask_size': []
            }
        for level in range(1, 11):
            result_dict[exch_time]['bid_price'].append(getattr(row, f'l{level}_bid_price'))
            result_dict[exch_time]['bid_size'].append(getattr(row, f'l{level}_bid_size'))
            result_dict[exch_time]['ask_price'].append(getattr(row, f'l{level}_ask_price'))
            result_dict[exch_time]['ask_size'].append(getattr(row, f'l{level}_ask_size'))
            result_dict[exch_time]['timestamp'].append(getattr(row, 'trade_time'))

    return result_dict

def get_signal_time_with_request_times(signals_df, ric, req_start_time, req_end_time):
    signals_df = signals_df.reset_index()
    signals_df['RIC'] = ric
    signals_df = signals_df[['exchange_time', 'RIC']].loc[signals_df['signal'] != 0].dropna()
    signals_df.rename(columns={'exchange_time':'signal_time'}, inplace=True)
    signals_df['request_start_time'] = pd.to_datetime(signals_df['signal_time']) + timedelta(milliseconds=req_start_time)
    signals_df['request_end_time'] = pd.to_datetime(signals_df['request_start_time']) + timedelta(milliseconds=req_end_time)
    signals_df['signal_time'] = pd.to_datetime(signals_df['signal_time']).apply(
                convert_datetime_string)
    signals_df['request_start_time'] = pd.to_datetime(signals_df['request_start_time']).apply(
                convert_datetime_string)
    signals_df['request_end_time'] = pd.to_datetime(signals_df['request_end_time']).apply(
                convert_datetime_string)
    return signals_df
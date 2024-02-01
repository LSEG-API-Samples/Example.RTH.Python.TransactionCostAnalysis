import sys
sys.path.append("root/")
from datetime import timedelta
import asyncio
from helper.utilities import convert_datetime_string
import pandas as pd

async def extract_tick_data_async(de, trade_df):
    async def extract_data(trade):
        ric, start, end, signal_time = get_extraction_data(trade)
        await de.extract_tick_data(ric, start, end, signal_time)

    tasks = []
    for trade in trade_df.itertuples():
        task = asyncio.create_task(extract_data(trade))
        tasks.append(task)
    await asyncio.gather(*tasks)

def get_extraction_data(trade):
    ric = trade.RIC
    start = trade.request_start_time
    end = trade.request_end_time
    signal_time = trade.signal_time
    return ric, start, end, signal_time

def add_extraction_timeline(trade_df, before, after):
    trade_df['trade_time'] = pd.to_datetime(trade_df['trade_time'])
    trade_df['signal_time'] = pd.to_datetime(trade_df['signal_time'])
    trade_df = trade_df.drop_duplicates(subset='order_id', keep='last')
    trade_df['request_start_time'] = trade_df['signal_time'] - \
        timedelta(milliseconds=before)
    trade_df['request_end_time'] = trade_df['trade_time'] + \
        timedelta(milliseconds=after)
    trade_df['request_start_time'] = trade_df['request_start_time'].apply(
        convert_datetime_string)
    trade_df['request_end_time'] = trade_df['request_end_time'].apply(
        convert_datetime_string)
    trade_df['signal_time'] = trade_df['signal_time'].apply(
        convert_datetime_string)
    return trade_df
import pandas as pd
from datetime import timedelta
from helper.utilities import read_all_files_to_df
pd.options.mode.chained_assignment = None
 
class TCAAnalysis:

    def __init__(self,market_data_path):
        self.df_analytics = pd.DataFrame()
        self.market_data_path = market_data_path
 
    def run(self, trades_df, arrival_latency, benchmark_classes, metric_classes):
        enriched_data = self.enrich_data_with_market_trades(trades_df)
        enriched_data['arrival_time'] = pd.to_datetime(enriched_data['signal_time']) + timedelta(milliseconds=arrival_latency)
        rics = enriched_data['RIC'].unique()
        for ric in rics:
            signals = enriched_data.loc[(enriched_data['RIC'] == ric)]['signal_time'].unique()
            for signal_time in signals:
                trades_ric = enriched_data.loc[((enriched_data['signal_time'] == signal_time) & (enriched_data['RIC'] == ric))]
                trades_before_arrival = trades_ric.loc[trades_ric['arrival_time'] > trades_ric['trade_time']]
                trades_ric = self.calculate_becnmarks(trades_ric, trades_before_arrival,benchmark_classes)
                trades_ric = self.calculate_metrics(trades_ric, metric_classes)
                self.df_analytics = pd.concat([self.df_analytics, trades_ric])
        return self.df_analytics
 
    def enrich_data_with_market_trades(self,trades_df):
        trades_df['trade_time'] = pd.to_datetime(
            trades_df['trade_time'])
        trades_df['signal_time'] = pd.to_datetime(
            trades_df['signal_time'])
        order_df = read_all_files_to_df(self.market_data_path)
        enriched_data = pd.concat([trades_df, order_df])
        enriched_data.sort_values(by=[ 'RIC', 'trade_time'], inplace=True)
        return enriched_data 

    def calculate_metrics(self, trades_ric, metric_classes):
        for metric_class in metric_classes:
            metric = metric_class()
            metric_values = metric.calculate(trades_ric)
            trades_ric = self.add_metrics_to_df(trades_ric, metric.name, metric_values)
        return trades_ric

    def calculate_becnmarks(self, trades_ric, trades_before_arrival, benchmark_classes):
        benchmarks = {}
        for benchmark_class in benchmark_classes:
            benchmark = benchmark_class()
            benchmarks[benchmark.name] = benchmark.calculate(trades_before_arrival)
        trades_ric = self.add_benchmarks_to_df(trades_ric, benchmarks)
        return trades_ric

    def add_benchmarks_to_df(self, trades_ric, benchmarks):
        for benchmark, value in benchmarks.items():
            trades_ric.loc[trades_ric['order_id'].notnull(), benchmark] = value
        return trades_ric

    def add_metrics_to_df(self, trades_ric, metric, metric_values):
        if hasattr(metric_values, '__len__'):
            trades_ric[metric] = metric_values
        else:
            trades_ric.loc[trades_ric['order_id'].notnull(), metric] = metric_values
        return trades_ric

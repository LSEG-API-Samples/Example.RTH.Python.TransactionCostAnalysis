import streamlit as st
import pandas as pd
from streamlit import cache_data
from helper.utilities import read_all_files_to_df
from tca_analysis.benchmarks import *
from tca_analysis.metrics import *
from tca_analysis.analysis import TCAAnalysis
from helper.plots import *

class StreamLitVisualisation:
    def __init__(self, market_data_path) -> None:
        st.set_page_config(page_title="TCA", layout="wide")
        st.sidebar.title('Transaction cost analysis')
        self.market_data_path = market_data_path
        self.BENCHMARKS = [ArrivalPriceBenchmark, HighBenchmark, LowBenchmark, MarketVWAPBenchmark, MidBenchmark, SpreadBenchmark, BestAskBenchmark, BestBidBenchmark]
        self.METRICS = [VWAPMetric, TWAPMetric, SlippageMetric, BestPrice, WorstPrice, SlippageMarketVWAP, MarketImpactMetric]
        self.TCA = TCAAnalysis()
        self.uploaded_file = st.sidebar.file_uploader("Upload a csv file to start the analysis", type=['csv'])

    def perform_analysis(self):
        if self.uploaded_file:
            trades_df = pd.read_csv(self.uploaded_file)
            enriched_df = self.enrich_data_with_market_trades(trades_df)
            tca_result_df = self.tca_analysis(enriched_df, 100)
            tca_result_df = self.add_datetime_components(tca_result_df)
            tca_result_df = self.add_latency_duration_components(tca_result_df)
            self.visualise_analysis(tca_result_df)

    @cache_data
    def enrich_data_with_market_trades(_self,trades_df):
        trades_df['trade_time'] = pd.to_datetime(
            trades_df['trade_time'])
        trades_df['signal_time'] = pd.to_datetime(
            trades_df['signal_time'])
        order_df = read_all_files_to_df(_self.market_data_path)
        enriched_data = pd.concat([trades_df, order_df])
        enriched_data.sort_values(by=[ 'RIC', 'trade_time'], inplace=True)
        return enriched_data  
    
    @cache_data
    def tca_analysis(_self, enriched_data, arrival_latency):
        result = _self.TCA.run(enriched_data, arrival_latency, _self.BENCHMARKS, _self.METRICS)
        result.to_csv('../data/tca_results.csv')
        return result

    def add_datetime_components(self, data):
        data['date'] = pd.to_datetime(data['trade_time']).dt.date
        data['hour'] = pd.to_datetime(data['trade_time']).dt.hour
        data['weekday'] = pd.to_datetime(data['trade_time']).dt.weekday
        data['month'] = pd.to_datetime(data['trade_time']).dt.month
        return data

    def add_latency_duration_components(self, data):
        data['arrival_latency'] = (pd.to_datetime(data['arrival_time']) - pd.to_datetime(data['signal_time']))
        data['execution_latency'] = (pd.to_datetime(data['trade_time']) - pd.to_datetime(data['signal_time']))
        data['arrival_latency_duration'] = data['arrival_latency'].dt.microseconds / 1000 + data['arrival_latency'].dt.seconds * 1000
        data['execution_latency_duration'] = data['execution_latency'].dt.microseconds / 1000 + data['execution_latency'].dt.seconds * 1000
        return data
    
    def visualise_analysis(self, data):

        data_trades = data.dropna(subset=['order_id'])
        start_date, end_date = st.sidebar.slider('Select a range of values', data_trades['date'].min(), data_trades['date'].max(),
                                                    (data_trades['date'].min(), data_trades['date'].max()))
        select_component = st.sidebar.radio(
            "Select the analysis level",
            ("Aggregated", "Company Level")
        )
        try:
            if select_component == "Company Level":
                selected_company = st.sidebar.selectbox(
                    "Select a Company",
                    data['RIC'].unique(),
                )
                filtered_data_company = data[(data['RIC'] == selected_company) &
                                        (data['date'] >= start_date) &
                                        (data['date'] <= end_date)]                
                self.render_company_level_analysis(filtered_data_company)
            
            elif select_component == "Aggregated":
                filtered_data_agg = data[(data['date'] >= start_date) &
                                        (data['date'] <= end_date)]
                self.render_aggregated_analyis(filtered_data_agg)

        except Exception as e:
            st.error(f"An error occurred: {e}")
            
    @cache_data
    def render_aggregated_analyis(_self, filtered_data):
        st.title('Summary Analytics of the trades')
        with st.expander("Descriptive Analytics"):
            col1, col2 = st.columns(2)
            with col1:
                display_desc_stats(filtered_data)
            with col2:
                plot_arrival_latemcy(filtered_data)

        with st.expander("Slippage accorss venue, currency and stock "):
            col1, col2 = st.columns(2)
            with col1:
                plot_slippage_by(filtered_data, 'venue',)
                plot_slippage_by(filtered_data, 'currency')
                plot_slippage_by(filtered_data, 'RIC')

            with col2:
                plot_slippage_dist_by(filtered_data, 'venue')
                plot_slippage_dist_by(filtered_data, 'currency') 
                plot_slippage_dist_by(filtered_data, 'RIC')

        with st.expander("Slippage accorss different timespan"):
            col1, col2 = st.columns(2)
            with col1:
                plot_slippage_by(filtered_data, 'hour')
                plot_slippage_by(filtered_data, 'weekday')
            with col2:
                plot_slippage_by(filtered_data, 'month')

        with st.expander("Slippage accorss brokers and traders"):
            col1, col2 = st.columns(2)
            with col1:
                plot_slippage_by(filtered_data, 'broker_id')

            with col2:
                plot_slippage_by(filtered_data, 'trader_id')

    def render_company_level_analysis(self,filtered_data):
        st.title('Company level trades analytics')
        buy, sell, all = st.tabs(["Buy", "Sell", "All Trades"])

        with buy:
            filtered_data_buy = filtered_data.loc[filtered_data['position']==1]
            with st.expander("Slippage Analytics"):
                col1, col2 = st.columns(2)
                with col1:
                    plot_slippage_through_time(filtered_data_buy)
                    plot_against_trade_volume(filtered_data_buy, 'slippage')
                with col2:
                    plot_slippage_distribution(filtered_data_buy)

            with st.expander("Trade Analytics"):
                col1, col2 = st.columns(2)
                with col1:
                    order_id = st.selectbox('order_id', filtered_data_buy.order_id.unique())
                    plot_trade_recap(filtered_data, order_id)
                    
                with col2:
                    plot_prices(filtered_data_buy, 'buy')                   

        with sell:
            col1, col2 = st.columns(2)
            filtered_data_sell = filtered_data.loc[filtered_data['position']==-1]

            with st.expander("Slippage Analytics"):
                col1, col2 = st.columns(2)
                with col1:
                    plot_slippage_through_time(filtered_data_sell)
                    plot_against_trade_volume(filtered_data_sell, 'slippage')
                with col2:
                    plot_slippage_distribution(filtered_data_sell)

            with st.expander("Trade Analytics"):
                col1, col2 = st.columns(2)
                with col1:
                    order_id = st.selectbox('order_id', filtered_data_sell.order_id.unique())
                    plot_trade_recap(filtered_data, order_id)
                    
                with col2:
                    plot_prices(filtered_data_sell, 'sell')                   

        with all:
            col1, col2 = st.columns(2)
            with col1:
                plot_slippage_distribution(filtered_data)
                order_id = st.selectbox('order_id', filtered_data.dropna(subset = 'order_id').order_id.unique())
                plot_trade_recap(filtered_data, order_id)
            with col2:
                plot_against_trade_volume(filtered_data, 'market_impact')
                plot_arrival_latemcy(filtered_data)

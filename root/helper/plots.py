import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import numpy as np

def plot_slippage_through_time(df):
    df = df.drop_duplicates(subset = 'order_id', keep = 'last')
    st.write('Slippage througout the time')
    fig_slippage_time = px.line(df, x = 'trade_time', y= 'slippage')
    st.plotly_chart(fig_slippage_time)    

def plot_against_trade_volume(df, metric):
    st.write(f'{metric} versus executed_amount')
    df['position_name'] = np.where(df['position']==1, 'buy', 'sell')
    df  = df.groupby('order_id').agg({'executed_amount': 'sum', 'position_name': 'last', metric: 'last'})
    fig_slippage_trade_vol =  px.scatter(df, x='executed_amount', y=metric, title=f'{metric} vs. executed_amount' , color='position_name')
    st.plotly_chart(fig_slippage_trade_vol)

def plot_slippage_distribution(df):
    st.write('Slippage distribution')
    hists = [df['slippage'].dropna()]
    groups = ['slippage']
    fig_sippage_dist = ff.create_distplot(hists, groups, show_hist=False)
    st.plotly_chart(fig_sippage_dist)

def plot_trade_and_market_averages(df):
    st.write('Trades and market averages')
    fig_vwaps = make_subplots()
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df['trades_vwap'],
                        name='trades vwap'))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df['market_vwap'], 
                        name='market vwap'))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df['trades_twap'], 
                        name='trades twap'))
    st.plotly_chart(fig_vwaps)


def build_prices_plot(df, trade_type, my_price):
    best = 'market_low' if trade_type == 'buy'else 'market_high'
    worst  = 'market_high' if trade_type == 'buy'else 'market_low'
    fig_vwaps = make_subplots()
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df[my_price],
                        name=my_price))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df['arrival_price'], 
                        name='arrival price'))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df['market_mid'], 
                        name='market mid'))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df[best], 
                        name='market best'))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df[worst], 
                        name='market worst'))
    fig_vwaps.add_trace(go.Line(x=df['trade_time'], y=df['market_vwap'], 
                        name='market vwap'))
    st.plotly_chart(fig_vwaps)

def plot_prices(df, trade_type):
    df = df.drop_duplicates(subset = 'order_id', keep = 'last')
    st.write('Trades and market benchmarks')
    tab1, tab2, tab3, tab4 = st.tabs(["vwap", "twap", "best", 'worst'])
    with tab1:
        build_prices_plot(df, trade_type,'trades_vwap')
    with tab2:
        build_prices_plot(df, trade_type,'trades_twap')
    with tab3:
        build_prices_plot(df, trade_type,'best_price')
    with tab4:
        build_prices_plot(df, trade_type,'worst_price')

def plot_slippage_by(df, metric):

    st.write(f'Average slippage by {metric}')
    df = df.dropna(subset = 'order_id')
    # df = df.drop_duplicates(subset = 'order_id', keep = 'last')
    df.sort_values(by = metric, inplace = True)
    grouped_df = df[[metric, 'market_spread', 'slippage']].groupby(by = metric).mean().reset_index()
    fig_slippage_venue = make_subplots(specs=[[{"secondary_y": True}]])
    fig_slippage_venue.add_trace(go.Bar(x=grouped_df[metric], y=grouped_df['slippage'], name = 'slippage') )
    fig_slippage_venue.add_trace(go.Line(x = grouped_df[metric], y = grouped_df['market_spread'], name = 'market_spread'), secondary_y = True)

    if metric == 'weekday':
        fig_slippage_venue.update_layout(
        xaxis = dict(
            tickmode = 'array',
            tickvals = [0, 1, 2, 3, 4, 5, 6],
            ticktext = ['Monday', 'Tuesday','Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        )
    )
    elif metric == 'month':
        fig_slippage_venue.update_layout(
        xaxis = dict(
            tickmode = 'array',
            tickvals = [1, 2, 3, 4, 5, 6, 7, 8, 9 ,10, 11, 12],
            ticktext = ['January', 'February', 'March', 'April','May','June','July','August','September','October','November','December']
        )
    )
    st.plotly_chart(fig_slippage_venue)

def plot_slippage_dist_by(df, metric):
    df = df.dropna(subset = 'order_id')
    st.write(f'Slippage distribution by {metric}')
    hists = [df.loc[df[metric] == value]['slippage'] for value in df[metric].unique()]
    groups = [value for value in df[metric].unique()]
    fig_sippage_dist = ff.create_distplot(hists, groups, show_hist=False)
    st.plotly_chart(fig_sippage_dist)

def plot_trade_recap(df, order_id):
    if order_id:
        signal_time = df.loc[df.order_id == order_id].signal_time.iloc[0]
        trades = df.loc[df['signal_time'] == signal_time]
        fig1 = px.line(trades.dropna(subset='Price'), x = 'trade_time', y='Price')
        fig1.update_traces(line=dict(color = 'red'), name="Market Price", showlegend=True)
        unique_trade_times = trades.dropna(subset = 'order_id').reset_index().index
        color_scale = px.colors.qualitative.Set1[:len(unique_trade_times)]
        color_mapping = dict(zip(unique_trade_times, color_scale))
        fig2 = px.scatter(trades.dropna(subset=['trade_time', 'trade_volume']), 
                        x='trade_time', 
                        y='execution_price', 
                        size='trade_volume', 
                        color=unique_trade_times, 
                        color_discrete_map=color_mapping)
        fig2.update_traces(showlegend=True, name='Execution Price with Volume')
        fig3 = px.scatter( trades[(~trades['order_id'].duplicated(keep='last')) | trades['order_id'].isna()], x = 'trade_time', y='trades_vwap')
        fig3.update_traces(marker=dict(color = 'green', symbol='triangle-up'), name='Trades VWAP', showlegend=True )
        fig4 = px.scatter( trades[(~trades['order_id'].duplicated(keep='last')) | trades['order_id'].isna()], x = 'trade_time', y='market_vwap')
        fig4.update_traces(marker=dict(color = 'blue', symbol='triangle-up'), name='Market VWAP', showlegend=True )
        fig5 = go.Figure(data=fig1.data + fig2.data+ fig3.data + fig4.data) # type: ignore
        fig5.update_layout(legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ))
        st.plotly_chart(fig5)

def plot_arrival_latemcy(df):
    st.write('Average arrival latency during the day')
    tca_trades  = df.dropna(subset='order_id')
    tca_trades_g = tca_trades[['hour', 'execution_latency_duration', 'slippage']].groupby(by='hour').mean().reset_index()
    fig_arrival_latency = px.bar(tca_trades_g, y = 'execution_latency_duration', x = 'hour')
    fig_arrival_latency.update_traces(name="Arival Latency", showlegend=True)
    st.plotly_chart(fig_arrival_latency)

def display_desc_stats(df):
    st.write('Descriptive statistics of the trades')
    tca_trades  = df.dropna(subset='order_id')
    df_for_stats = tca_trades[['trade_volume', 'slippage', 'market_spread', 'arrival_latency_duration', 'execution_latency_duration']]
    st.write(df_for_stats.mask(df_for_stats == 0).describe())
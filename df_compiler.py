# -*- coding: utf-8 -*-
"""
Created on Thu Dec 10 19:06:18 2020

@author: Mason
"""

# THIS SCRIPT IS USED TO MERGE TOGETHER SCRAPED CSVS #
# ONCE ticker_scrape.py FUNCTIONS AS INTENDED, THIS IS DEFUNCT #

import pandas

# IMPORTS TICKER CSV DATA INTO SEPERATE DATAFRAMES #
aapl_df = pandas.read_csv('./ticker_data/AAPL.csv')
amzn_df = pandas.read_csv('./ticker_data/AMZN.csv')
crm_df = pandas.read_csv('./ticker_data/CRM.csv')
jpm_df = pandas.read_csv('./ticker_data/JPM.csv')
msft_df = pandas.read_csv('./ticker_data/MSFT.csv')
tsla_df = pandas.read_csv('./ticker_data/TSLA.csv')
zm_df = pandas.read_csv('./ticker_data/ZM.csv')
pton_df = pandas.read_csv('./ticker_data/PTON.csv')

# ADDS DATAFRAMES INTO A LIST #
ticker_dfs = [aapl_df, amzn_df, crm_df, jpm_df, msft_df, tsla_df, zm_df, pton_df]

# CREATES EMPTY DATAFRAME TO BE APPENDED TO # 
concat_df = pandas.DataFrame(columns = ['search', 'date', 'id'])

# APPENDS TICKER DATAFRAMES TO SINGLE DATAFRAME #
for ticker in ticker_dfs:
    ticker = ticker.drop(['Unnamed: 0'], axis=1)
    concat_df = concat_df.append(ticker, sort = False)

# RENAMES COLUMN NAMES OF CONCATENATED DATAFRAME #    
concat_df.columns = ['Ticker', 'Date', 'TweetCount']

# GROUPS CONCATENATED DATAFRAME BY TICKER AND DATE #
agg_df = concat_df.groupby(['Date', 'Ticker']).count()

# WRITES AGGREGATED DATAFRANE TO A CSV #
agg_df.to_csv("./ticker_data/agg_data.csv")